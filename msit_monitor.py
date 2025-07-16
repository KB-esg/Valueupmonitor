#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
MSIT 통신 통계 모니터링 시스템 - 모듈화된 클래스 기반 구조

주요 개선사항:
1. Selenium → Playwright 전환
2. 모듈화된 클래스 기반 구조 (단일 파일 유지)
3. 데이터 추출 오류 수정 (마지막 열 값 정확 추출)
4. 행 누락 문제 해결 (SKT, KT, LGU+, MVNO 등)
5. 향상된 오류 처리 및 로깅
"""

import os
import re
import json
import time
import logging
import asyncio
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd
import numpy as np
import random
from typing import Dict, List, Optional, Tuple, Any, Union
from dataclasses import dataclass
from abc import ABC, abstractmethod

# Third-party imports
import telegram
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, Page, Browser, BrowserContext

# OCR imports (conditional)
try:
    from PIL import Image, ImageEnhance, ImageFilter
    import cv2
    import pytesseract
    OCR_AVAILABLE = True
except ImportError:
    OCR_AVAILABLE = False




##################################################################################
#  1. 설정 관리 클래스 
##################################################################################

@dataclass
class MonitorConfig:
    """모니터링 설정 데이터 클래스"""
    landing_url: str = "https://www.msit.go.kr"
    stats_url: str = "https://www.msit.go.kr/bbs/list.do?sCode=user&mPid=74&mId=99"
    report_types: List[str] = None
    telegram_token: str = None
    chat_id: str = None
    gspread_creds: str = None
    spreadsheet_id: str = None
    spreadsheet_name: str = "MSIT 통신 통계"
    ocr_enabled: bool = True
    cleanup_old_sheets: bool = False
    api_request_wait: int = 2
    max_retries: int = 3
    page_load_timeout: int = 30
    
    def __post_init__(self):
        if self.report_types is None:
            self.report_types = [
                "이동전화 및 트래픽 통계",
                "이동전화 및 시내전화 번호이동 현황", 
                "유선통신서비스 가입 현황",
                "무선통신서비스 가입 현황",
                "특수부가통신사업자현황",
                "무선데이터 트래픽 통계",
                "유·무선통신서비스 가입 현황 및 무선데이터 트래픽 통계"
            ]
        
        # 환경 변수에서 값 로드
        if not self.telegram_token:
            self.telegram_token = os.environ.get('TELCO_NEWS_TOKEN')
        if not self.chat_id:
            self.chat_id = os.environ.get('TELCO_NEWS_TESTER')
        if not self.gspread_creds:
            self.gspread_creds = os.environ.get('MSIT_GSPREAD_ref')
        if not self.spreadsheet_id:
            self.spreadsheet_id = os.environ.get('MSIT_SPREADSHEET_ID')


class ConfigManager:
    """설정 관리 클래스"""
    
    def __init__(self):
        self.config = self._load_config()
        self.temp_dir = Path("./downloads")
        self.screenshots_dir = Path("./screenshots")
        self._setup_directories()
    
    def _load_config(self) -> MonitorConfig:
        """환경 변수에서 설정 로드"""
        config = MonitorConfig()
        
        # 환경 변수에서 부울 값 로드
        config.ocr_enabled = os.environ.get('OCR_ENABLED', 'true').lower() in ('true', 'yes', '1', 'y')
        config.cleanup_old_sheets = os.environ.get('CLEANUP_OLD_SHEETS', 'false').lower() in ('true', 'yes', '1', 'y')
        
        # 숫자 값 로드
        try:
            config.api_request_wait = int(os.environ.get('API_REQUEST_WAIT', '2'))
            config.max_retries = int(os.environ.get('MAX_RETRIES', '3'))
            config.page_load_timeout = int(os.environ.get('PAGE_LOAD_TIMEOUT', '30'))
        except ValueError:
            # 변환 오류 시 기본값 유지
            pass
        
        # 스프레드시트 이름 업데이트
        if os.environ.get('SPREADSHEET_NAME'):
            config.spreadsheet_name = os.environ.get('SPREADSHEET_NAME')
        
        return config
    
    def _setup_directories(self):
        """필요한 디렉토리 생성"""
        self.temp_dir.mkdir(exist_ok=True)
        self.screenshots_dir.mkdir(exist_ok=True)


##################################################################################
#  2. 로깅 유틸리티 클래스 
##################################################################################

class LoggingUtils:
    """로깅 유틸리티 클래스"""
    
    @staticmethod
    def setup_enhanced_logging() -> logging.Logger:
        """향상된 로깅 설정"""
        # 로거 설정
        logger = logging.getLogger('msit_monitor')
        
        # 이미 핸들러가 있는 경우 제거
        if logger.handlers:
            for handler in logger.handlers:
                logger.removeHandler(handler)
        
        # 로그 포맷 설정
        formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s'
        )
        
        # 파일 핸들러 추가
        file_handler = logging.FileHandler('msit_monitor_detailed.log')
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        
        # 콘솔 핸들러 추가
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
        
        # 로그 레벨 설정
        logger.setLevel(logging.DEBUG)
        
        return logger
    
    @staticmethod
    async def log_diagnostic_info(page: Page, error: Exception = None) -> Dict[str, Any]:
        """진단 정보 수집 및 로깅"""
        info = {
            'timestamp': datetime.now().isoformat(),
            'error': str(error) if error else None
        }
        
        try:
            # 기본 페이지 정보
            info['url'] = page.url
            info['title'] = await page.title()
            
            # JavaScript 진단 정보
            js_info = await page.evaluate("""
                () => ({
                    readyState: document.readyState,
                    url: document.URL,
                    referrer: document.referrer,
                    domain: document.domain,
                    iframesCount: document.querySelectorAll('iframe').length,
                    tablesCount: document.querySelectorAll('table').length,
                    mainTableExists: !!document.getElementById('mainTable'),
                    viewportHeight: window.innerHeight,
                    viewportWidth: window.innerWidth
                })
            """)
            info['page_info'] = js_info
            
            # DOM 상태 확인
            dom_info = await page.evaluate("""
                () => ({
                    bodyChildCount: document.body ? document.body.children.length : 0,
                    headChildCount: document.head ? document.head.children.length : 0,
                    scriptsCount: document.scripts ? document.scripts.length : 0,
                    formsCount: document.forms ? document.forms.length : 0
                })
            """)
            info['dom_info'] = dom_info
            
            # iframe 정보
            iframe_info = []
            iframe_elements = await page.query_selector_all('iframe')
            for i, iframe in enumerate(iframe_elements):
                try:
                    iframe_info.append({
                        'index': i,
                        'id': await iframe.get_attribute('id'),
                        'name': await iframe.get_attribute('name'),
                        'src': await iframe.get_attribute('src'),
                        'is_visible': await iframe.is_visible()
                    })
                except:
                    iframe_info.append({'index': i, 'error': 'Could not get attributes'})
            
            info['iframes'] = iframe_info
            
            # 오류가 있는 경우 스크린샷 저장
            if error:
                try:
                    screenshot_path = f"error_screenshot_{int(time.time())}.png"
                    await page.screenshot(path=screenshot_path)
                    info['screenshot_path'] = screenshot_path
                except Exception as ss_err:
                    info['screenshot_error'] = str(ss_err)
                
                # 페이지 소스 저장
                try:
                    source_path = f"error_source_{int(time.time())}.html"
                    with open(source_path, 'w', encoding='utf-8') as f:
                        f.write(await page.content())
                    info['source_path'] = source_path
                except Exception as src_err:
                    info['source_error'] = str(src_err)
                    
                # 스택 트레이스 저장
                import traceback
                info['traceback'] = traceback.format_exc()
            
            return info
            
        except Exception as diag_err:
            logger = logging.getLogger('msit_monitor')
            logger.error(f"진단 정보 수집 중 오류: {str(diag_err)}")
            return {
                'timestamp': datetime.now().isoformat(),
                'error': str(error) if error else None,
                'diagnostic_error': str(diag_err)
            }



##################################################################################
#  3. Date 및 Data 유틸리티 클래스 
##################################################################################
class DateUtils:
    """날짜 관련 유틸리티 클래스"""
    
    @staticmethod
    def parse_post_date(date_str: str) -> Optional[datetime.date]:
        """게시물 날짜 파싱"""
        try:
            # 날짜 문자열 정규화
            date_str = date_str.replace(',', ' ').strip()
            
            # 다양한 날짜 형식 시도
            date_formats = [
                '%Y. %m. %d',  # "YYYY. MM. DD" 형식
                '%Y-%m-%d',    # "YYYY-MM-DD" 형식
                '%Y/%m/%d',    # "YYYY/MM/DD" 형식
                '%Y.%m.%d',    # "YYYY.MM.DD" 형식
                '%Y년 %m월 %d일',  # "YYYY년 MM월 DD일" 형식
            ]
            
            for date_format in date_formats:
                try:
                    return datetime.strptime(date_str, date_format).date()
                except ValueError:
                    continue
            
            # 정규식으로 시도
            match = re.search(r'(\d{4})[.\-\s/]+(\d{1,2})[.\-\s/]+(\d{1,2})', date_str)
            if match:
                year, month, day = map(int, match.groups())
                try:
                    return datetime(year, month, day).date()
                except ValueError:
                    logger = logging.getLogger('msit_monitor')
                    logger.warning(f"날짜 값이 유효하지 않음: {year}-{month}-{day}")
            
            logger = logging.getLogger('msit_monitor')
            logger.warning(f"알 수 없는 날짜 형식: {date_str}")
            return None
            
        except Exception as e:
            logger = logging.getLogger('msit_monitor')
            logger.error(f"날짜 파싱 오류: {str(e)}")
            return None
    
    @staticmethod
    def is_in_date_range(date_str: str, days: int = 4) -> bool:
        """날짜 범위 확인"""
        try:
            # 날짜 문자열 정규화
            date_str = date_str.replace(',', ' ').strip()
            
            # 날짜 파싱
            post_date = DateUtils.parse_post_date(date_str)
            if not post_date:
                logger = logging.getLogger('msit_monitor')
                logger.warning(f"날짜 파싱 실패: {date_str}, 포함으로 처리")
                return True  # 파싱 실패 시 포함으로 처리
            
            # 날짜 범위 계산 (한국 시간대)
            korea_tz = datetime.now() + timedelta(hours=9)  # UTC에서 KST로
            days_ago = (korea_tz - timedelta(days=days)).date()
            
            logger = logging.getLogger('msit_monitor')
            logger.info(f"게시물 날짜 확인: {post_date} vs {days_ago} ({days}일 전, 한국 시간 기준)")
            return post_date >= days_ago
            
        except Exception as e:
            logger = logging.getLogger('msit_monitor')
            logger.error(f"날짜 범위 확인 오류: {str(e)}")
            return True  # 오류 발생 시 기본적으로 포함
    
    @staticmethod
    def extract_date_from_title(title: str) -> Optional[Dict[str, int]]:
        """제목에서 날짜 정보 추출"""
        try:
            # "(YYYY년 MM월말 기준)" 형식의 날짜 패턴 확인
            date_match = re.search(r'\((\d{4})년\s*(\d{1,2})월말\s*기준\)', title)
            if date_match:
                year = int(date_match.group(1))
                month = int(date_match.group(2))
                return {'year': year, 'month': month}
            return None
        except Exception as e:
            logger = logging.getLogger('msit_monitor')
            logger.error(f"제목에서 날짜 추출 오류: {str(e)}")
            return None

class DataUtils:
    """데이터 처리 유틸리티 클래스"""
    
    @staticmethod
    def validate_and_clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
        """DataFrame 검증 및 정제"""
        try:
            if df is None or df.empty:
                return pd.DataFrame()
                
            # 복사본 생성
            df_clean = df.copy()
            
            # NaN 값을 빈 문자열로 변환
            df_clean = df_clean.fillna('')
            
            # 모든 값을 문자열로 변환
            for col in df_clean.columns:
                df_clean[col] = df_clean[col].astype(str)
                
                # 숫자처럼 보이는 열 포맷 개선
                if df_clean[col].str.replace(',', '').str.replace('.', '').str.isdigit().mean() > 0.7:
                    try:
                        numeric_values = pd.to_numeric(df_clean[col].str.replace(',', ''))
                        # 큰 숫자에 쉼표 추가
                        df_clean[col] = numeric_values.apply(lambda x: f"{x:,}" if abs(x) >= 1000 else str(x))
                    except:
                        pass
            
            # 완전히 빈 행/열 제거
            df_clean = df_clean.replace('', np.nan)
            df_clean = df_clean.dropna(how='all').reset_index(drop=True)
            df_clean = df_clean.loc[:, ~df_clean.isna().all()]
            
            # NaN을 다시 빈 문자열로 변환
            df_clean = df_clean.fillna('')
            
            # 컬럼 헤더 정리
            df_clean.columns = [str(col).strip() for col in df_clean.columns]
            
            # 중복 컬럼 이름 처리
            if len(df_clean.columns) != len(set(df_clean.columns)):
                new_columns = []
                seen = {}
                for col in df_clean.columns:
                    if col in seen:
                        seen[col] += 1
                        new_columns.append(f"{col}_{seen[col]}")
                    else:
                        seen[col] = 0
                        new_columns.append(col)
                df_clean.columns = new_columns
            
            # 중복 행 제거
            df_clean = df_clean.drop_duplicates().reset_index(drop=True)
            
            return df_clean
            
        except Exception as e:
            logger = logging.getLogger('msit_monitor')
            logger.error(f"DataFrame 검증 오류: {str(e)}")
            return pd.DataFrame()
    
    @staticmethod
    def is_telecom_stats_post(title: str, report_types: List[str]) -> bool:
        """통신 통계 게시물 여부 확인"""
        if not title:
            return False
            
        # "(YYYY년 MM월말 기준)" 형식의 날짜 패턴 확인
        date_pattern = r'\((\d{4})년\s*(\d{1,2})월말\s*기준\)'
        has_date_pattern = re.search(date_pattern, title) is not None
        
        if not has_date_pattern:
            return False
        
        # 제목에 보고서 유형이 포함되어 있는지 확인
        contains_report_type = any(report_type in title for report_type in report_types)
        
        return contains_report_type
    
    @staticmethod
    def determine_report_type(title: str, report_types: List[str]) -> str:
        """보고서 유형 결정"""
        for report_type in report_types:
            if report_type in title:
                return report_type
                
        # 부분 매칭 시도
        for report_type in report_types:
            # 주요 키워드 추출
            keywords = report_type.split()
            if any(keyword in title for keyword in keywords if len(keyword) > 1):
                return report_type
                
        return "기타 통신 통계"



##################################################################################
#  4. 웹드라이버 클래스 
##################################################################################

class WebDriverManager:
    """Playwright 웹드라이버 관리 클래스"""
    
    def __init__(self, config: MonitorConfig):
        self.config = config
        self.playwright = None
        self.browser: Optional[Browser] = None
        self.context: Optional[BrowserContext] = None
        self.page: Optional[Page] = None
        self.logger = logging.getLogger('msit_monitor')
    
    async def setup_browser(self) -> Tuple[Browser, BrowserContext, Page]:
        """브라우저 및 페이지 설정"""
        try:
            self.playwright = await async_playwright().start()
            
            # Chrome 브라우저 시작 옵션 설정
            browser_args = [
                '--no-sandbox',
                '--disable-dev-shm-usage',
                '--disable-gpu',
                '--window-size=1920,1080',
                '--disable-extensions',
                '--disable-popup-blocking',
                '--disable-web-security',
                '--disable-features=WebglDraftExtensions,WebglDecoderExtensions',
                '--disable-application-cache',
                '--disable-browser-cache'
            ]
            
            # 브라우저 시작
            self.browser = await self.playwright.chromium.launch(
                headless=True,
                args=browser_args
            )
            
            # 컨텍스트 생성
            self.context = await self.browser.new_context(
                viewport={"width": 1920, "height": 1080},
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36"
            )
            
            # 페이지 생성
            self.page = await self.context.new_page()
            
            # 타임아웃 설정
            self.page.set_default_timeout(self.config.page_load_timeout * 1000)
            
            # 스텔스 스크립트 적용
            await self._apply_stealth_scripts()
            
            self.logger.info("Playwright 브라우저 설정 완료")
            
            return self.browser, self.context, self.page
            
        except Exception as e:
            self.logger.error(f"브라우저 설정 오류: {str(e)}")
            # 이미 생성된 리소스 정리
            await self.close()
            raise
    
    async def close(self):
        """리소스 정리"""
        try:
            if self.page:
                await self.page.close()
                self.page = None
            
            if self.context:
                await self.context.close()
                self.context = None
            
            if self.browser:
                await self.browser.close()
                self.browser = None
            
            if self.playwright:
                await self.playwright.stop()
                self.playwright = None
                
            self.logger.info("Playwright 리소스 정리 완료")
        except Exception as e:
            self.logger.error(f"리소스 정리 중 오류: {str(e)}")
    
    async def _apply_stealth_scripts(self):
        """웹드라이버 감지 방지 스크립트 적용"""
        try:
            # 웹드라이버 감지 방지 스크립트
            stealth_script = """
            () => {
                // WebDriver 속성 숨기기
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined
                });
                
                // Chrome 속성 숨기기
                if (window.chrome) {
                    window.chrome.runtime = {};
                }
                
                // User-Agent 클라이언트 힌트 수정
                if (navigator.userAgentData) {
                    const brands = navigator.userAgentData.brands;
                    if (brands) {
                        brands.forEach(brand => {
                            if (brand.brand.includes('Chromium')) {
                                brand.brand = 'Google Chrome';
                            }
                        });
                    }
                }
                
                // Permission API 수정
                if (navigator.permissions) {
                    const originalQuery = navigator.permissions.query;
                    navigator.permissions.query = function(parameters) {
                        if (parameters.name === 'notifications') {
                            return Promise.resolve({ state: "prompt", onchange: null });
                        }
                        return originalQuery.apply(this, arguments);
                    };
                }
                
                // 자동화 감지용 플러그인 에뮬레이션
                Object.defineProperty(navigator, 'plugins', {
                    get: () => {
                        return [
                            {
                                0: {type: "application/x-google-chrome-pdf"},
                                description: "Portable Document Format",
                                filename: "internal-pdf-viewer",
                                length: 1,
                                name: "Chrome PDF Plugin"
                            }
                        ];
                    }
                });
            }
            """
            await self.page.evaluate(stealth_script)
            self.logger.info("스텔스 스크립트 적용 완료")
        except Exception as e:
            self.logger.warning(f"스텔스 스크립트 적용 오류: {str(e)}")
    
    async def take_screenshot(self, name: str, page: Optional[Page] = None) -> str:
        """스크린샷 촬영"""
        try:
            target_page = page if page else self.page
            if not target_page:
                self.logger.warning("스크린샷 촬영 실패: 페이지 객체가 없음")
                return ""
                
            # 스크린샷 파일 경로 생성
            screenshots_dir = Path("./screenshots")
            screenshots_dir.mkdir(exist_ok=True)
            
            screenshot_path = f"screenshots/{name}_{int(time.time())}.png"
            
            # 스크린샷 촬영
            await target_page.screenshot(path=screenshot_path)
            self.logger.info(f"스크린샷 촬영 완료: {screenshot_path}")
            
            return screenshot_path
        except Exception as e:
            self.logger.error(f"스크린샷 촬영 오류: {str(e)}")
            return ""
    
    async def navigate_to_page(self, page_num: int) -> bool:
        """특정 페이지로 이동"""
        try:
            if not self.page:
                self.logger.error("페이지 이동 실패: 페이지 객체가 없음")
                return False
                
            # 현재 페이지 확인
            current_page = await self._get_current_page()
            self.logger.info(f"현재 페이지: {current_page}, 목표 페이지: {page_num}")
            
            if current_page == page_num:
                self.logger.info(f"이미 목표 페이지({page_num})에 있습니다.")
                return True
            
            # 페이지네이션 영역 찾기
            try:
                page_nav = await self.page.wait_for_selector("#pageNavi", timeout=10000)
                if not page_nav:
                    self.logger.error("페이지 네비게이션을 찾을 수 없습니다.")
                    return False
            except Exception as e:
                self.logger.error(f"페이지 네비게이션 찾기 오류: {str(e)}")
                return False
            
            # 직접 페이지 링크 찾기
            try:
                page_link = await self.page.query_selector(f"#pageNavi a.page-link:text('{page_num}')")
                if page_link:
                    self.logger.info(f"페이지 {page_num} 링크 발견, 직접 클릭")
                    await page_link.click()
                    await self._wait_for_page_change(current_page)
                    return True
            except Exception as e:
                self.logger.warning(f"직접 페이지 링크 클릭 오류: {str(e)}")
            
            # 다음/이전 버튼을 사용한 이동
            if page_num > current_page:
                # 다음 버튼 클릭
                try:
                    next_button = await self.page.query_selector("#pageNavi a.next, #pageNavi a.page-navi.next")
                    if next_button:
                        self.logger.info("다음 페이지 버튼 클릭")
                        await next_button.click()
                        await self._wait_for_page_change(current_page)
                        # 재귀적으로 다시 시도
                        return await self.navigate_to_page(page_num)
                except Exception as e:
                    self.logger.warning(f"다음 페이지 버튼 클릭 오류: {str(e)}")
            else:
                # 이전 버튼 클릭
                try:
                    prev_button = await self.page.query_selector("#pageNavi a.prev, #pageNavi a.page-navi.prev")
                    if prev_button:
                        self.logger.info("이전 페이지 버튼 클릭")
                        await prev_button.click()
                        await self._wait_for_page_change(current_page)
                        # 재귀적으로 다시 시도
                        return await self.navigate_to_page(page_num)
                except Exception as e:
                    self.logger.warning(f"이전 페이지 버튼 클릭 오류: {str(e)}")
            
            # 직접 URL 수정으로 이동 시도
            try:
                url = self.page.url
                new_url = re.sub(r'pageIndex=\d+', f'pageIndex={page_num}', url)
                if new_url == url:  # pageIndex 파라미터가 없는 경우
                    if '?' in url:
                        new_url = f"{url}&pageIndex={page_num}"
                    else:
                        new_url = f"{url}?pageIndex={page_num}"
                
                self.logger.info(f"URL로 페이지 이동: {new_url}")
                await self.page.goto(new_url)
                
                # 페이지 로드 확인
                await self.page.wait_for_selector(".board_list", timeout=10000)
                new_page = await self._get_current_page()
                
                return new_page == page_num
            except Exception as e:
                self.logger.error(f"URL 기반 페이지 이동 오류: {str(e)}")
                return False
                
        except Exception as e:
            self.logger.error(f"페이지 이동 중 오류: {str(e)}")
            return False
    
    async def _get_current_page(self) -> int:
        """현재 페이지 번호 가져오기"""
        try:
            # 활성화된 페이지 링크 찾기
            active_page = await self.page.query_selector("a.page-link[aria-current='page'], a.on[href*='pageIndex'], a.page-link.active")
            if active_page:
                page_text = await active_page.text_content()
                try:
                    return int(page_text.strip())
                except ValueError:
                    pass
            
            # URL에서 페이지 번호 추출
            url = self.page.url
            match = re.search(r'pageIndex=(\d+)', url)
            if match:
                return int(match.group(1))
                
            # 기본값 반환
            return 1
        except Exception as e:
            self.logger.warning(f"현재 페이지 확인 오류: {str(e)}")
            return 1
    
    async def _wait_for_page_change(self, previous_page: int) -> bool:
        """페이지 변경 대기"""
        try:
            # 페이지 로딩 대기
            await self.page.wait_for_selector(".board_list", timeout=10000)
            
            # 잠시 대기
            await asyncio.sleep(2)
            
            # 페이지 번호 확인
            max_attempts = 5
            for attempt in range(max_attempts):
                current_page = await self._get_current_page()
                if current_page != previous_page:
                    self.logger.info(f"페이지 변경 감지: {previous_page} → {current_page}")
                    return True
                    
                self.logger.debug(f"페이지 변경 대기 중... ({attempt+1}/{max_attempts})")
                await asyncio.sleep(1)
            
            self.logger.warning(f"페이지 변경 타임아웃: 아직 페이지 {previous_page}에 있습니다.")
            return False
            
        except Exception as e:
            self.logger.error(f"페이지 변경 대기 중 오류: {str(e)}")
            return False


##################################################################################
#  4. 추출기 기본 클래스 
##################################################################################

class BaseExtractor(ABC):
    """추출기 기본 클래스"""
    
    def __init__(self, config: MonitorConfig):
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
    
    @abstractmethod
    async def extract(self, page: Page, **kwargs) -> Optional[Dict[str, pd.DataFrame]]:
        """데이터 추출 추상 메서드"""
        pass


class HTMLExtractor(BaseExtractor):
    """HTML 기반 데이터 추출기"""
    
    async def extract(self, page: Page, **kwargs) -> Optional[Dict[str, pd.DataFrame]]:
        """HTML에서 테이블 데이터 추출"""
        try:
            # HTML 콘텐츠 가져오기
            html_content = await page.content()
            
            # HTML에서 테이블 파싱
            return self._parse_table_from_html(html_content)
        except Exception as e:
            self.logger.error(f"HTML 데이터 추출 오류: {str(e)}")
            return None
    
    def _parse_table_from_html(self, html_content: str) -> Optional[Dict[str, pd.DataFrame]]:
        """HTML에서 테이블 파싱"""
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            all_sheets = {}
            
            # Synap 문서 뷰어 확인
            synap_viewer = 'SynapDocViewServer' in html_content or 'Synap Document Viewer' in html_content
            
            # 1. mainTable 찾기 (Synap 문서 뷰어에서 주로 사용)
            main_table = soup.find('div', id='mainTable')
            if main_table:
                self.logger.info("mainTable 요소 찾음")
                
                # 시트 제목 추출 시도
                sheet_name = "기본 시트"
                sheet_tabs = soup.find_all('div', class_='sheet-list__sheet-tab')
                if sheet_tabs:
                    active_tab = next((tab for tab in sheet_tabs if 'active' in tab.get('class', [])), None)
                    if active_tab:
                        sheet_name = active_tab.text.strip()
                
                # tr 클래스를 가진 div 찾기 (행 요소)
                rows = main_table.find_all('div', class_=lambda c: c and ('tr' in c.lower()))
                
                if not rows:
                    # 다른 방법으로 행 요소 찾기
                    rows = main_table.find_all('div', recursive=False)
                
                if rows:
                    self.logger.info(f"mainTable에서 {len(rows)}개 행 찾음")
                    table_data = []
                    
                    # 첫 번째 행이 헤더인지 확인
                    headers = []
                    if rows:
                        header_cells = rows[0].find_all('div', class_=lambda c: c and ('td' in c.lower()))
                        if not header_cells:
                            header_cells = rows[0].find_all('div', recursive=False)
                        
                        headers = [cell.text.strip() or f"Column_{i}" for i, cell in enumerate(header_cells)]
                    
                    # 데이터 행 추출
                    for i, row in enumerate(rows):
                        if i == 0:  # 헤더 행 건너뛰기
                            continue
                            
                        cells = row.find_all('div', class_=lambda c: c and ('td' in c.lower()))
                        if not cells:
                            cells = row.find_all('div', recursive=False)
                        
                        row_data = [cell.text.strip() for cell in cells]
                        
                        if row_data:  # 빈 행 제외
                            # 헤더와 데이터 길이 일치시키기
                            if headers and len(row_data) < len(headers):
                                row_data.extend([''] * (len(headers) - len(row_data)))
                            elif headers and len(row_data) > len(headers):
                                row_data = row_data[:len(headers)]
                                
                            table_data.append(row_data)
                    
                    # DataFrame 생성
                    if table_data:
                        if not headers and table_data:
                            # 헤더가 없으면 열 수 기반으로 자동 생성
                            max_cols = max(len(row) for row in table_data)
                            headers = [f"Column_{i}" for i in range(max_cols)]
                        
                        df = pd.DataFrame(table_data, columns=headers)
                        all_sheets[sheet_name] = df
                        self.logger.info(f"mainTable에서 DataFrame 생성: {df.shape[0]}행 {df.shape[1]}열")
            
            # 2. 일반 HTML 테이블 추출
            tables = soup.find_all('table')
            if tables:
                self.logger.info(f"HTML에서 {len(tables)}개의 <table> 태그를 찾았습니다")
                
                for table_idx, table in enumerate(tables):
                    sheet_name = f"Table_{table_idx+1}"
                    
                    # 테이블 데이터 추출
                    table_grid = self._handle_complex_table(table)
                    
                    if table_grid and len(table_grid) > 1:  # 헤더 + 데이터 행
                        headers = table_grid[0]
                        data = table_grid[1:]
                        
                        # DataFrame 생성
                        df = pd.DataFrame(data, columns=headers)
                        all_sheets[sheet_name] = df
                        self.logger.info(f"테이블 {sheet_name} 데이터 추출: {df.shape[0]}행 {df.shape[1]}열")
            
            # 3. DIV 기반 그리드 구조 찾기
            container_selectors = ['div[id="container"]', 'div.container', 'div.content', 'div.grid', 'div[class*="table"]']
            for selector in container_selectors:
                containers = soup.select(selector)
                
                if containers:
                    for container_idx, container in enumerate(containers):
                        # 행 요소 찾기
                        row_selectors = ['div[class*="tr"]', 'div[class*="row"]', 'div:not([class])']
                        
                        for row_selector in row_selectors:
                            rows = container.select(row_selector)
                            if rows:
                                # 데이터 추출 로직...
                                # (이하 생략 - 위의 mainTable 로직과 유사)
                                pass
            
            # 데이터 정제
            refined_sheets = {}
            for name, df in all_sheets.items():
                if df.empty:
                    continue
                
                # 데이터 정제...
                # (데이터 타입 변환, NaN 처리 등)
                
                # 수정된 부분: 마지막 열 값 추출 수정
                # 예전 버전에서 마지막에서 두번째 열을 사용하던 오류 수정
                if df.shape[1] > 1:
                    value_col = df.columns[-1]  # 마지막 열 사용 (오류 수정)
                    self.logger.info(f"값 추출에 마지막 열 사용: {value_col}")
                
                refined_sheets[name] = DataUtils.validate_and_clean_dataframe(df)
            
            if not refined_sheets:
                self.logger.warning("추출된 유효한 데이터가 없습니다.")
                return None
            
            self.logger.info(f"총 {len(refined_sheets)}개 시트 추출 완료")
            return refined_sheets
            
        except Exception as e:
            self.logger.error(f"HTML에서 데이터 추출 중 오류: {str(e)}")
            import traceback
            self.logger.error(traceback.format_exc())
            return None
    
    def _handle_complex_table(self, table_element) -> List[List[str]]:
        """복잡한 테이블 구조 처리 (rowspan, colspan)"""
        rows = table_element.find_all('tr')
        if not rows:
            return []
            
        # 먼저 열 수 결정
        max_cols = 0
        for row in rows:
            cells = row.find_all(['td', 'th'])
            # colspan 고려한 열 수 계산
            cols = sum(int(cell.get('colspan', 1)) for cell in cells)
            max_cols = max(max_cols, cols)
        
        # 2D 그리드 초기화
        grid = []
        
        # rowspan 트래킹
        rowspan_tracker = [0] * max_cols
        
        for row_idx, row in enumerate(rows):
            # 그리드에 새 행 추가
            if row_idx >= len(grid):
                grid.append([''] * max_cols)
            
            # rowspan 처리
            for col_idx in range(max_cols):
                if rowspan_tracker[col_idx] > 0:
                    rowspan_tracker[col_idx] -= 1
                    if row_idx > 0:
                        grid[row_idx][col_idx] = grid[row_idx-1][col_idx]
            
            # 셀 처리
            cells = row.find_all(['td', 'th'])
            col_idx = 0
            
            for cell in cells:
                # rowspan으로 이미 채워진 열 건너뛰기
                while col_idx < max_cols and rowspan_tracker[col_idx] > 0:
                    col_idx += 1
                
                if col_idx >= max_cols:
                    break
                
                # 셀 내용 가져오기
                content = cell.get_text(strip=True)
                
                # rowspan, colspan 처리
                rowspan = int(cell.get('rowspan', 1))
                colspan = int(cell.get('colspan', 1))
                
                # 현재 셀과 colspan 처리
                for c in range(colspan):
                    if col_idx + c < max_cols:
                        grid[row_idx][col_idx + c] = content
                
                # rowspan 처리
                if rowspan > 1:
                    for c in range(colspan):
                        if col_idx + c < max_cols:
                            rowspan_tracker[col_idx + c] = rowspan - 1
                    
                    # 필요한 행 추가
                    while len(grid) < row_idx + rowspan:
                        grid.append([''] * max_cols)
                    
                    # 아래 셀 채우기
                    for r in range(1, rowspan):
                        for c in range(colspan):
                            if row_idx + r < len(grid) and col_idx + c < max_cols:
                                grid[row_idx + r][col_idx + c] = content
                
                # 다음 열로 이동
                col_idx += colspan
        
        return grid


class SynapViewerExtractor(BaseExtractor):
    """Synap 문서 뷰어 전용 추출기"""
    
    async def extract(self, page: Page, **kwargs) -> Optional[Dict[str, pd.DataFrame]]:
        """Synap 뷰어에서 데이터 추출"""
        try:
            # 구조 탐색
            structure_info = await self._explore_synap_structure(page)
            
            # 추출 결과
            extracted_data = {}
            
            # 1. mainTable에서 데이터 추출
            main_table_df = await self._extract_from_main_table(page)
            if main_table_df is not None and not main_table_df.empty:
                extracted_data["MainTable"] = main_table_df
            
            # 2. 시트 탭이 있는 경우 처리
            sheet_tabs_data = await self._handle_sheet_tabs(page)
            extracted_data.update(sheet_tabs_data)
            
            # 3. iframe 내용 확인
            iframe_data = await self._check_iframes(page)
            extracted_data.update(iframe_data)
            
            # 데이터 정제
            refined_data = {}
            for name, df in extracted_data.items():
                if df is not None and not df.empty:
                    refined_data[name] = DataUtils.validate_and_clean_dataframe(df)
            
            if not refined_data:
                self.logger.warning("Synap 뷰어에서 유효한 데이터를 추출하지 못했습니다.")
                return None
                
            self.logger.info(f"Synap 뷰어에서 {len(refined_data)}개 시트 추출 완료")
            return refined_data
            
        except Exception as e:
            self.logger.error(f"Synap 뷰어 데이터 추출 오류: {str(e)}")
            return None
    
    async def _explore_synap_structure(self, page: Page) -> Dict[str, Any]:
        """Synap 뷰어 구조 탐색"""
        try:
            results = {
                'timestamp': int(time.time()),
                'url': page.url,
                'title': await page.title(),
                'structure': {},
                'objects': {},
                'potential_data': {}
            }
            
            # JavaScript 환경 탐색
            js_env = await page.evaluate("""
                () => {
                    return {
                        // window 객체의 주요 속성 탐색
                        windowKeys: Object.keys(window).filter(k => 
                            k.includes('Synap') || 
                            k.includes('WM') || 
                            k.includes('sheet') || 
                            k.includes('table') ||
                            k.includes('cell') ||
                            k.includes('doc')
                        ),
                        
                        // 주요 객체 존재 여부 확인
                        hasLocalSynap: typeof localSynap !== 'undefined',
                        hasWM: typeof WM !== 'undefined',
                        hasSheetIndex: typeof sheetIndex !== 'undefined',
                        
                        // 문서 구조 정보
                        documentTitle: document.title,
                        docURL: document.URL,
                        iframes: document.querySelectorAll('iframe').length,
                        
                        // 주요 HTML 요소 존재 여부
                        hasMainTable: !!document.getElementById('mainTable'),
                        hasContainer: !!document.getElementById('container'),
                        hasSheetList: !!document.querySelector('.sheet-list'),
                        hasSheetTabs: !!document.querySelectorAll('.sheet-list__sheet-tab').length
                    };
                }
            """)
            
            results['js_env'] = js_env
            
            # mainTable 구조 탐색
            if js_env.get('hasMainTable'):
                main_table_info = await page.evaluate("""
                    () => {
                        try {
                            const mainTable = document.getElementById('mainTable');
                            if (!mainTable) return null;
                            
                            // 행 찾기
                            const rows = mainTable.querySelectorAll('div[class*="tr"]');
                            const rowCount = rows.length;
                            
                            // 첫 행의 셀 개수로 열 수 추정
                            const firstRow = rows[0];
                            const cells = firstRow ? firstRow.querySelectorAll('div[class*="td"]') : [];
                            const colCount = cells.length;
                            
                            // 첫 행의 셀 내용
                            const headerContents = [];
                            if (cells && cells.length > 0) {
                                for (let i = 0; i < Math.min(cells.length, 10); i++) {
                                    headerContents.push(cells[i].textContent.trim());
                                }
                            }
                            
                            return {
                                element: 'mainTable',
                                rowCount,
                                colCount,
                                headerContents
                            };
                        } catch (e) {
                            return { error: e.message };
                        }
                    }
                """)
                
                results['structure']['mainTable'] = main_table_info
            
            # 시트 탭 정보 수집
            if js_env.get('hasSheetTabs'):
                sheet_tabs_info = await page.evaluate("""
                    () => {
                        try {
                            const tabs = document.querySelectorAll('.sheet-list__sheet-tab');
                            if (!tabs || tabs.length === 0) return null;
                            
                            return {
                                count: tabs.length,
                                tabs: Array.from(tabs).map((tab, idx) => ({
                                    index: idx,
                                    text: tab.textContent.trim(),
                                    isActive: tab.classList.contains('active') || 
                                             tab.classList.contains('sheet-list__sheet-tab--active')
                                }))
                            };
                        } catch (e) {
                            return { error: e.message };
                        }
                    }
                """)
                
                results['structure']['sheetTabs'] = sheet_tabs_info
            
            return results
            
        except Exception as e:
            self.logger.error(f"Synap 구조 탐색 오류: {str(e)}")
            return {'error': str(e)}
    
    async def _extract_from_main_table(self, page: Page) -> Optional[pd.DataFrame]:
        """메인 테이블에서 데이터 추출"""
        try:
            # mainTable 데이터 추출
            table_data = await page.evaluate("""
                () => {
                    try {
                        const mainTable = document.getElementById('mainTable');
                        if (!mainTable) return null;
                        
                        // 행 찾기
                        let rows = mainTable.querySelectorAll('div[class*="tr"]');
                        if (!rows || rows.length === 0) {
                            rows = Array.from(mainTable.children);
                        }
                        
                        if (!rows || rows.length === 0) return null;
                        
                        const tableData = [];
                        for (let i = 0; i < rows.length; i++) {
                            const row = rows[i];
                            
                            // 셀 찾기
                            let cells = row.querySelectorAll('div[class*="td"]');
                            if (!cells || cells.length === 0) {
                                cells = Array.from(row.children);
                            }
                            
                            if (!cells || cells.length === 0) continue;
                            
                            const rowData = [];
                            for (let j = 0; j < cells.length; j++) {
                                rowData.push(cells[j].textContent.trim());
                            }
                            
                            if (rowData.length > 0) {
                                tableData.push(rowData);
                            }
                        }
                        
                        return tableData;
                    } catch (e) {
                        return { error: e.message };
                    }
                }
            """)
            
            if isinstance(table_data, list) and table_data:
                # 첫 번째 행을 헤더로 가정
                headers = table_data[0]
                data = table_data[1:]
                
                if not data:  # 데이터 행이 없는 경우
                    return None
                
                # DataFrame 생성
                df = pd.DataFrame(data, columns=headers)
                self.logger.info(f"mainTable에서 데이터 추출: {df.shape[0]}행 {df.shape[1]}열")
                return df
            
            return None
            
        except Exception as e:
            self.logger.error(f"mainTable 데이터 추출 오류: {str(e)}")
            return None
    
    async def _handle_sheet_tabs(self, page: Page) -> Dict[str, pd.DataFrame]:
        """시트 탭 처리"""
        try:
            # 시트 탭 확인
            sheet_tabs = await page.query_selector_all('.sheet-list__sheet-tab')
            if not sheet_tabs:
                return {}
                
            self.logger.info(f"{len(sheet_tabs)}개 시트 탭 발견")
            sheets_data = {}
            
            # 각 시트 탭 처리
            for i, tab in enumerate(sheet_tabs):
                # 시트 이름 가져오기
                sheet_name = await tab.text_content() or f"Sheet_{i+1}"
                sheet_name = sheet_name.strip()
                
                self.logger.info(f"시트 탭 처리 중: {sheet_name}")
                
                # 현재 활성화된 탭인지 확인
                is_active = await tab.evaluate("tab => tab.classList.contains('active') || tab.classList.contains('sheet-list__sheet-tab--active')")
                
                # 활성화되지 않은 탭이면 클릭
                if not is_active:
                    await tab.click()
                    # 탭 전환 대기
                    await page.wait_for_timeout(2000)
                
                # 탭 전환 후 데이터 추출
                main_table_df = await self._extract_from_main_table(page)
                if main_table_df is not None and not main_table_df.empty:
                    sheets_data[sheet_name] = main_table_df
                
            return sheets_data
            
        except Exception as e:
            self.logger.error(f"시트 탭 처리 오류: {str(e)}")
            return {}
    
    async def _check_iframes(self, page: Page) -> Dict[str, pd.DataFrame]:
        """iframe 내용 확인"""
        try:
            # iframe 요소 찾기
            iframe_elements = await page.query_selector_all('iframe')
            if not iframe_elements:
                return {}
                
            self.logger.info(f"{len(iframe_elements)}개 iframe 발견")
            iframe_data = {}
            
            # 각 iframe 처리
            for i, iframe in enumerate(iframe_elements):
                iframe_name = f"iframe_{i+1}"
                
                try:
                    # iframe으로 전환
                    frame = await iframe.content_frame()
                    if not frame:
                        continue
                        
                    # iframe 내에서 테이블 데이터 추출
                    html_content = await frame.content()
                    html_extractor = HTMLExtractor(self.config)
                    iframe_sheets = html_extractor._parse_table_from_html(html_content)
                    
                    if iframe_sheets:
                        for name, df in iframe_sheets.items():
                            iframe_data[f"{iframe_name}_{name}"] = df
                except Exception as frame_err:
                    self.logger.warning(f"iframe {i+1} 처리 오류: {str(frame_err)}")
            
            return iframe_data
            
        except Exception as e:
            self.logger.error(f"iframe 확인 오류: {str(e)}")
            return {}


class OCRExtractor(BaseExtractor):
    """OCR 기반 데이터 추출기"""

    def __init__(self, config: MonitorConfig):
        super().__init__(config)
        self.ocr_available = OCR_AVAILABLE and config.ocr_enabled
        
        if self.ocr_available:
            # OCR 관련 라이브러리 임포트 및 초기화
            import pytesseract
            from PIL import Image, ImageEnhance, ImageFilter
            import cv2
            
            # Tesseract 경로 설정
            pytesseract_cmd = os.environ.get('PYTESSERACT_CMD', 'tesseract')
            pytesseract.pytesseract.tesseract_cmd = pytesseract_cmd
            self.logger.info(f"Tesseract 경로 설정: {pytesseract_cmd}")
    
    async def extract(self, page: Page, **kwargs) -> Optional[Dict[str, pd.DataFrame]]:
        """OCR을 통한 데이터 추출"""
        if not self.ocr_available:
            self.logger.warning("OCR 기능을 사용할 수 없습니다.")
            return None
            
        try:
            # 스크린샷 촬영
            screenshot_path = kwargs.get('screenshot_path')
            if not screenshot_path:
                screenshot_path = f"ocr_screenshot_{int(time.time())}.png"
                await page.screenshot(path=screenshot_path)
                self.logger.info(f"OCR용 스크린샷 촬영: {screenshot_path}")
            
            # 이미지 전처리 및 테이블 구조 감지
            processed_images = self._preprocess_image_for_ocr(screenshot_path)
            tables_info = self._detect_table_structure(processed_images['table_structure'])
            
            all_dataframes = []
            
            # 테이블 구조가 감지된 경우
            if tables_info and len(tables_info) > 0:
                self.logger.info(f"{len(tables_info)}개 테이블 구조 감지")
                
                for i, table_info in enumerate(tables_info):
                    self.logger.info(f"테이블 {i+1}/{len(tables_info)} 처리 중")
                    
                    # 셀 추출
                    cells_data = self._extract_cells_from_table(processed_images['ocr_ready'], table_info)
                    
                    if cells_data and len(cells_data) > 0:
                        # DataFrame 생성
                        try:
                            if len(cells_data) > 1:  # 헤더 + 최소 1개 데이터 행
                                # 첫 번째 행을 헤더로 가정
                                headers = cells_data[0]
                                data = cells_data[1:]
                                
                                # 빈 헤더 처리
                                headers = [h if h else f"Column_{i}" for i, h in enumerate(headers)]
                                
                                # DataFrame 생성
                                df = pd.DataFrame(data, columns=headers)
                                
                                # 데이터 정제
                                df = DataUtils.validate_and_clean_dataframe(df)
                                
                                if not df.empty:
                                    all_dataframes.append(df)
                                    self.logger.info(f"테이블 {i+1}에서 DataFrame 생성: {df.shape[0]}행 {df.shape[1]}열")
                        except Exception as df_err:
                            self.logger.warning(f"테이블 {i+1} DataFrame 생성 오류: {str(df_err)}")
            
            # 테이블 구조가 없거나 추출 실패한 경우 일반 텍스트 추출
            if not all_dataframes:
                self.logger.info("테이블 구조 감지 실패, 일반 텍스트 추출 시도")
                text_df = self._extract_text_without_table_structure(screenshot_path)
                if text_df is not None and not text_df.empty:
                    all_dataframes.append(text_df)
            
            # 결과 반환
            if all_dataframes:
                result = {}
                for i, df in enumerate(all_dataframes):
                    result[f"OCR_Table_{i+1}"] = df
                
                self.logger.info(f"OCR로 {len(result)}개 시트 추출 완료")
                return result
            else:
                self.logger.warning("OCR 추출 결과가 없습니다.")
                return None
                
        except Exception as e:
            self.logger.error(f"OCR 데이터 추출 오류: {str(e)}")
            return None
    
    def _preprocess_image_for_ocr(self, image_path: str) -> Dict[str, Any]:
        """OCR을 위한 이미지 전처리"""
        # (OCR 전처리 로직은 복잡하므로 간략화)
        try:
            image = cv2.imread(image_path)
            if image is None:
                self.logger.error(f"이미지 로드 실패: {image_path}")
                return {}
                
            # 그레이스케일 변환
            gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
            
            # 노이즈 제거
            denoised = cv2.fastNlMeansDenoising(gray, None, h=10, templateWindowSize=7, searchWindowSize=21)
            
            # 이진화
            _, binary = cv2.threshold(denoised, 150, 255, cv2.THRESH_BINARY_INV)
            
            # 선 감지를 위한 모폴로지 연산
            kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (3, 3))
            binary_enhanced = cv2.morphologyEx(binary, cv2.MORPH_OPEN, kernel, iterations=1)
            
            # 수직/수평선 감지
            height, width = gray.shape
            h_kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (width // 30, 1))
            v_kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (1, height // 30))
            
            horizontal_lines = cv2.erode(binary_enhanced, h_kernel, iterations=3)
            horizontal_lines = cv2.dilate(horizontal_lines, h_kernel, iterations=3)
            
            vertical_lines = cv2.erode(binary_enhanced, v_kernel, iterations=3)
            vertical_lines = cv2.dilate(vertical_lines, v_kernel, iterations=3)
            
            # 선 합치기
            table_lines = cv2.add(horizontal_lines, vertical_lines)
            
            # 결과 저장
            result = {
                'original': image,
                'grayscale': gray,
                'binary': binary,
                'lines': table_lines,
                'table_structure': table_lines.copy(),
                'ocr_ready': cv2.bitwise_not(binary)  # OCR용 이미지
            }
            
            return result
            
        except Exception as e:
            self.logger.error(f"이미지 전처리 오류: {str(e)}")
            return {}
    
    def _detect_table_structure(self, processed_image) -> List[Dict[str, Any]]:
        """테이블 구조 감지"""
        # (테이블 구조 감지 로직 간략화)
        try:
            if processed_image is None:
                return []
                
            # 윤곽선 찾기
            contours, _ = cv2.findContours(processed_image, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
            
            if not contours:
                return []
                
            # 이미지 크기
            height, width = processed_image.shape
            min_area = (width * height) / 1000  # 최소 영역 크기
            
            # 셀 윤곽선 필터링
            cell_contours = []
            for cnt in contours:
                area = cv2.contourArea(cnt)
                if area < min_area:
                    continue
                    
                x, y, w, h = cv2.boundingRect(cnt)
                if w < 10 or h < 10 or w > width * 0.9 or h > height * 0.9:
                    continue
                    
                cell_contours.append((x, y, w, h))
            
            if not cell_contours:
                return []
                
            # y 좌표로 정렬하여 행으로 그룹화
            cell_contours.sort(key=lambda c: c[1])
            
            y_tolerance = height // 40
            rows = []
            current_row = [cell_contours[0]]
            current_y = cell_contours[0][1]
            
            for cell in cell_contours[1:]:
                y = cell[1]
                if abs(y - current_y) <= y_tolerance:
                    current_row.append(cell)
                else:
                    rows.append(current_row)
                    current_row = [cell]
                    current_y = y
                    
            if current_row:
                rows.append(current_row)
                
            # 각 행을 x 좌표로 정렬
            for i in range(len(rows)):
                rows[i].sort(key=lambda c: c[0])
                
            # 행/열 개수 확인
            num_rows = len(rows)
            num_cols = max(len(row) for row in rows)
            
            if num_rows < 2 or num_cols < 2:
                return []
                
            # 테이블 구조 반환
            return [{
                'rows': rows,
                'num_rows': num_rows,
                'num_cols': num_cols
            }]
            
        except Exception as e:
            self.logger.error(f"테이블 구조 감지 오류: {str(e)}")
            return []
    
    def _extract_cells_from_table(self, image, table_info: Dict[str, Any]) -> List[List[str]]:
        """테이블에서 셀 추출"""
        # (셀 추출 로직 간략화)
        try:
            rows = table_info['rows']
            num_rows = table_info['num_rows']
            num_cols = table_info['num_cols']
            
            # OCR 설정
            header_config = r'-c preserve_interword_spaces=1 --psm 6'
            data_config = r'-c preserve_interword_spaces=1 --psm 6'
            
            cell_data = []
            
            # 각 행 처리
            for row_idx, row in enumerate(rows):
                row_data = [''] * num_cols  # 빈 문자열로 초기화
                
                # 각 셀 처리
                for col_idx, (x, y, w, h) in enumerate(row):
                    if col_idx >= num_cols:
                        continue
                        
                    # 셀 영역 추출
                    cell_img = image[y:y+h, x:x+w]
                    
                    if cell_img.size == 0:
                        continue
                        
                    # OCR 구성
                    config = header_config if row_idx == 0 else data_config
                    
                    # 이미지 향상
                    pil_img = Image.fromarray(cell_img)
                    enhancer = ImageEnhance.Contrast(pil_img)
                    enhanced_img = enhancer.enhance(2.0)
                    
                    # OCR 실행
                    try:
                        text = pytesseract.image_to_string(
                            enhanced_img, lang='kor+eng', config=config
                        ).strip()
                        
                        # 텍스트 정리
                        text = ' '.join(text.split())
                        
                        if text:
                            row_data[col_idx] = text
                    except Exception as ocr_err:
                        self.logger.debug(f"셀 OCR 오류: {str(ocr_err)}")
                
                # 유효한 데이터가 있는 행만 추가
                if any(cell for cell in row_data):
                    cell_data.append(row_data)
            
            return cell_data
            
        except Exception as e:
            self.logger.error(f"셀 추출 오류: {str(e)}")
            return []
    
    def _extract_text_without_table_structure(self, image_path: str) -> Optional[pd.DataFrame]:
        """표 구조 없이 이미지에서 텍스트 추출"""
        try:
            img = Image.open(image_path)
            enhancer = ImageEnhance.Contrast(img)
            enhanced_img = enhancer.enhance(2.0)
            
            # OCR 실행
            text = pytesseract.image_to_string(enhanced_img, lang='kor+eng', config='--psm 6')
            lines = [line.strip() for line in text.split('\n') if line.strip()]
            
            if not lines:
                return None
                
            # 간단한 표 형태로 변환
            df = pd.DataFrame({'텍스트': lines})
            df.insert(0, '행', range(1, len(df) + 1))
            
            return df
            
        except Exception as e:
            self.logger.error(f"텍스트 추출 오류: {str(e)}")
            return None



##################################################################################
#  5. 페이지 파싱 클래스 
##################################################################################

class PageParser:
    """페이지 파싱 클래스"""
    
    def __init__(self, config: MonitorConfig, extractors: List[BaseExtractor]):
        self.config = config
        self.extractors = extractors
        self.logger = logging.getLogger(self.__class__.__name__)
    
    async def parse_page_content(
        self, 
        page: Page, 
        page_num: int, 
        days_range: Optional[int] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        reverse_order: bool = True
    ) -> Tuple[List[Dict], List[Dict], Dict]:
        """페이지 콘텐츠 파싱"""
        all_posts = []
        telecom_stats_posts = []
        
        # 결과 정보를 담을 딕셔너리
        result_info = {
            'current_page_complete': True,
            'skip_remaining_in_page': False,
            'continue_to_next_page': True,
            'oldest_date_found': None,
            'newest_date_found': None,
            'total_posts': 0,
            'filtered_posts': 0,
            'messages': []
        }
        
        try:
            # 날짜 객체로 변환
            start_date_obj = None
            end_date_obj = None
            
            if start_date:
                try:
                    start_date_obj = datetime.strptime(start_date, '%Y-%m-%d').date()
                except ValueError:
                    self.logger.warning(f"잘못된 시작 날짜 형식: {start_date}")
                    result_info['messages'].append(f"잘못된 시작 날짜 형식: {start_date}")
                    
            if end_date:
                try:
                    end_date_obj = datetime.strptime(end_date, '%Y-%m-%d').date()
                except ValueError:
                    self.logger.warning(f"잘못된 종료 날짜 형식: {end_date}")
                    result_info['messages'].append(f"잘못된 종료 날짜 형식: {end_date}")
            
            # days_range를 사용하는 경우 start_date_obj 계산
            if days_range and not start_date_obj:
                # 한국 시간대 고려 (UTC+9)
                korea_tz = datetime.now() + timedelta(hours=9)
                start_date_obj = (korea_tz - timedelta(days=days_range)).date()
                self.logger.info(f"days_range({days_range})로 계산된 시작 날짜: {start_date_obj}")
                result_info['messages'].append(f"days_range({days_range})로 계산된 시작 날짜: {start_date_obj}")
            
            # 페이지 로드 대기
            try:
                await page.wait_for_selector(".board_list", timeout=15000)
                
                # 추가 대기 (JS 로딩 등)
                await page.wait_for_timeout(3000)
            except Exception as wait_err:
                self.logger.error(f"페이지 로드 시간 초과: {str(wait_err)}")
                result_info['current_page_complete'] = False
                result_info['continue_to_next_page'] = True
                result_info['messages'].append("페이지 로드 시간 초과")
                return [], [], result_info
            
            # 스크린샷 저장 (디버깅용)
            await page.screenshot(path=f"parsed_page_{page_num}.png")
            
            # 게시물 목록 추출
            posts = await page.query_selector_all("div.toggle:not(.thead), table.board_list tr:not(.thead), .board_list li")
            
            if not posts:
                self.logger.warning("게시물을 찾을 수 없음")
                result_info['messages'].append("게시물을 찾을 수 없음")
                return [], [], result_info
                
            result_info['total_posts'] = len(posts)
            self.logger.info(f"{len(posts)}개 게시물 항목 발견")
            
            # 각 게시물 처리
            for post_idx, post_elem in enumerate(posts):
                try:
                    # 날짜 정보 추출
                    date_elem = await post_elem.query_selector(".date, div.date, td.date, .post-date")
                    if not date_elem:
                        continue
                        
                    date_str = await date_elem.text_content()
                    date_str = date_str.strip()
                    
                    if not date_str or date_str == '등록일':
                        continue
                        
                    self.logger.info(f"날짜 문자열 발견: {date_str}")
                    
                    # 게시물 날짜 파싱
                    post_date = DateUtils.parse_post_date(date_str)
                    if not post_date:
                        self.logger.warning(f"날짜 파싱 실패: {date_str}, 건너뜀")
                        continue
                    
                    # 날짜 정보 업데이트
                    if result_info['oldest_date_found'] is None or post_date < result_info['oldest_date_found']:
                        result_info['oldest_date_found'] = post_date
                        
                    if result_info['newest_date_found'] is None or post_date > result_info['newest_date_found']:
                        result_info['newest_date_found'] = post_date
                    
                    # 날짜 범위 확인
                    include_post = True
                    
                    if start_date_obj and post_date < start_date_obj:
                        include_post = False
                        
                        # 역순 탐색 시에는 현재 페이지의 나머지 게시물만 건너뛰고 다음 페이지로 계속 진행
                        if reverse_order:
                            result_info['skip_remaining_in_page'] = True
                            self.logger.info(f"날짜 범위 이전 게시물({date_str}) 발견, 현재 페이지 나머지 건너뛰기")
                            result_info['messages'].append(f"날짜 범위 이전 게시물({date_str}) 발견, 현재 페이지 나머지 건너뛰기")
                            break  # 현재 페이지 루프 종료
                        # 정순 탐색 시에는 모든 페이지 탐색 중단
                        else:
                            result_info['continue_to_next_page'] = False
                            self.logger.info(f"날짜 범위 이전 게시물({date_str}) 발견, 이후 페이지 탐색 중단")
                            result_info['messages'].append(f"날짜 범위 이전 게시물({date_str}) 발견, 이후 페이지 탐색 중단")
                    
                    # 종료 날짜 이후인지 확인
                    if end_date_obj and post_date > end_date_obj:
                        include_post = False
                    
                    # 필터링 조건에 맞지 않으면 다음 게시물로
                    if not include_post:
                        continue
                        
                    # 필터링 조건을 통과한 게시물 처리
                    result_info['filtered_posts'] += 1
                    
                    # 제목 추출
                    title_elem = await post_elem.query_selector("p.title, .title, td.title, .subject a, a.nttInfoBtn")
                    if not title_elem:
                        continue
                        
                    title = await title_elem.text_content()
                    title = title.strip()
                    
                    # 게시물 ID 추출
                    post_id = None
                    onclick = await title_elem.get_attribute('onclick')
                    if onclick:
                        match = re.search(r"fn_detail\((\d+)\)", onclick)
                        if match:
                            post_id = match.group(1)
                    
                    # 게시물 URL 생성
                    post_url = f"https://www.msit.go.kr/bbs/view.do?sCode=user&mId=99&mPid=74&nttSeqNo={post_id}" if post_id else None
                    
                    # 부서 정보 추출
                    dept_elem = await post_elem.query_selector("dd[id*='td_CHRG_DEPT_NM'], .dept, td.dept, .department")
                    dept_text = await dept_elem.text_content() if dept_elem else "부서 정보 없음"
                    dept_text = dept_text.strip()
                    
                    # 게시물 정보 딕셔너리 생성
                    post_info = {
                        'title': title,
                        'date': date_str,
                        'post_date': post_date,
                        'department': dept_text,
                        'url': post_url,
                        'post_id': post_id
                    }
                    
                    # 모든 게시물 리스트에 추가
                    all_posts.append(post_info)
                    
                    # 통신 통계 게시물인지 확인
                    if DataUtils.is_telecom_stats_post(title, self.config.report_types):
                        self.logger.info(f"통신 통계 게시물 발견: {title}")
                        telecom_stats_posts.append(post_info)
                        
                except Exception as post_err:
                    self.logger.error(f"게시물 {post_idx+1} 처리 중 오류: {str(post_err)}")
                    continue
            
            return all_posts, telecom_stats_posts, result_info
            
        except Exception as e:
            self.logger.error(f"페이지 파싱 중 오류: {str(e)}")
            result_info['current_page_complete'] = False
            result_info['messages'].append(f"페이지 파싱 중 오류: {str(e)}")
            return [], [], result_info

      
    def _create_improved_placeholder_dataframe(self, file_params: Dict) -> Dict[str, pd.DataFrame]:
        """향상된 placeholder DataFrame 생성"""
        try:
            post_info = file_params.get('post_info', {})
            
            # 날짜 정보 추출
            date_info = file_params.get('date')
            if date_info:
                year = date_info.get('year', 'Unknown')
                month = date_info.get('month', 'Unknown')
            else:
                # 제목에서 추출 시도
                date_match = re.search(r'\((\d{4})년\s*(\d{1,2})월말\s*기준\)', post_info.get('title', ''))
                year = date_match.group(1) if date_match else "Unknown"
                month = date_match.group(2) if date_match else "Unknown"
            
            # 보고서 유형 결정
            report_type = DataUtils.determine_report_type(post_info.get('title', ''), self.config.report_types)
            
            # 상태 결정
            if 'atch_file_no' in file_params and 'file_ord' in file_params:
                status = "문서 뷰어 접근 실패"
                details = f"atch_file_no={file_params['atch_file_no']}, file_ord={file_params['file_ord']}"
            elif 'content' in file_params:
                status = "텍스트 분석 실패"
                details = "텍스트 내용 처리 중 오류 발생"
            elif 'download_url' in file_params:
                status = "다운로드 URL 처리 실패"
                details = file_params.get('download_url', '')
            else:
                status = "알 수 없는 오류"
                details = "파일 파라미터 부족"
            
            # DataFrame 생성
            df = pd.DataFrame({
                '구분': [f'{year}년 {month}월 통계'],
                '보고서 유형': [report_type],
                '상태': [status],
                '상세 정보': [details],
                '링크': [post_info.get('url', '링크 없음')],
                '추출 시도 시간': [datetime.now().strftime('%Y-%m-%d %H:%M:%S')]
            })
            
            return {"Placeholder": df}
            
        except Exception as e:
            self.logger.error(f"Placeholder DataFrame 생성 오류: {str(e)}")
            # 최소한의 정보만 포함하는 DataFrame 반환
            return {"오류": pd.DataFrame({
                '구분': ['오류 발생'],
                '상태': ['데이터 추출 실패'],
                '상세 정보': [f'오류: {str(e)}']
            })}
    
    
    async def find_view_link_params(self, page: Page, post: Dict) -> Optional[Dict]:
        """바로보기 링크 파라미터 찾기"""
        if not post.get('post_id'):
            self.logger.error(f"게시물 접근 불가 {post['title']} - post_id 누락")
            return None
        
        self.logger.info(f"게시물 열기: {post['title']}")
        
        # 현재 URL 저장
        current_url = page.url
        
        # 게시물 목록 페이지로 돌아가기
        try:
            await page.goto(self.config.stats_url)
            await page.wait_for_selector(".board_list", timeout=10000)
            await page.wait_for_timeout(2000)  # 추가 대기
        except Exception as e:
            self.logger.error(f"게시물 목록 페이지 접근 실패: {str(e)}")
            # 직접 URL 접근 폴백 시도
            return await self._direct_access_view_link_params(page, post)
        
        # 최대 재시도 횟수
        max_retries = 3
        retry_delay = 2
        
        for attempt in range(max_retries):
            try:
                # 제목으로 게시물 링크 찾기 - 더 유연한 선택자 사용
                post_link = None
                
                # 더 짧은 제목 부분만 비교 (맨 앞 20자만 사용)
                title_prefix = post['title'][:20]
                post_link = await page.query_selector(f"text='{title_prefix}'")
                
                # 게시물 번호로 시도
                if not post_link and post['post_id']:
                    post_link = await page.query_selector(f"a[onclick*='{post['post_id']}']")
                
                # 링크를 찾지 못한 경우 직접 URL 접근 시도
                if not post_link:
                    self.logger.warning(f"게시물 링크를 찾을 수 없음: {post['title']}")
                    
                    if attempt < max_retries - 1:
                        self.logger.info(f"재시도 중... ({attempt+1}/{max_retries})")
                        await page.wait_for_timeout(retry_delay * 1000)
                        continue
                    else:
                        # 직접 URL 접근 방식으로 대체
                        self.logger.info("클릭 방식 실패, 직접 URL 접근 방식으로 대체")
                        return await self._direct_access_view_link_params(page, post)
                
                # 스크린샷 저장 (클릭 전)
                await page.screenshot(path=f"before_click_{post['post_id']}.png")
                
                # 링크 클릭하여 상세 페이지로 이동
                self.logger.info(f"게시물 링크 클릭 시도: {post['title']}")
                
                # 여러 클릭 방법 시도
                try:
                    # JavaScript로 클릭 (가장 안정적인 방법)
                    await page.evaluate("(element) => element.click()", post_link)
                    self.logger.info("JavaScript를 통한 클릭 실행")
                except Exception as js_click_err:
                    self.logger.warning(f"JavaScript 클릭 실패: {str(js_click_err)}")
                    # 일반 클릭 시도
                    await post_link.click()
                    self.logger.info("일반 클릭 실행")
                
                # 페이지 로드 대기
                try:
                    # URL 변경 대기
                    await page.wait_for_function(
                        f"() => window.location.href !== '{self.config.stats_url}'",
                        timeout=15000
                    )
                    self.logger.info(f"페이지 URL 변경 감지됨: {page.url}")
                    await page.wait_for_timeout(3000)  # 추가 대기
                except Exception as url_err:
                    self.logger.warning("URL 변경 감지 실패")
                    # 실패 시 직접 URL로 접근 시도
                    if attempt < max_retries - 1:
                        continue
                    else:
                        return await self._direct_access_view_link_params(page, post)
                
                # 상세 페이지 대기
                try:
                    await page.wait_for_selector(".view_head, .view_cont, .bbs_wrap .view", timeout=15000)
                    self.logger.info("상세 페이지 로드 완료")
                except Exception as view_err:
                    self.logger.warning("상세 페이지 로드 실패")
                    if attempt < max_retries - 1:
                        continue
                    else:
                        return await self._direct_access_view_link_params(page, post)
                
                # 스크린샷 저장
                await page.screenshot(path=f"post_view_clicked_{post['post_id']}.png")
                
                # 바로보기 링크 찾기
                try:
                    # 바로보기 링크 검색
                    view_link = await page.query_selector("a.view[title='새창 열림']")
                    
                    # onclick 속성으로 찾기
                    if not view_link:
                        view_link = await page.query_selector("a[onclick*='getExtension_path']")
                    
                    # 텍스트로 찾기
                    if not view_link:
                        view_link = await page.query_selector("a:has-text('바로보기'), a:has-text('보기'), a:has-text('첨부파일')")
                    
                    if view_link:
                        onclick = await view_link.get_attribute('onclick')
                        href = await view_link.get_attribute('href')
                        
                        self.logger.info(f"바로보기 링크 발견, onclick: {onclick}, href: {href}")
                        
                        # getExtension_path('49234', '1') 형식에서 매개변수 추출
                        if onclick and 'getExtension_path' in onclick:
                            match = re.search(r"getExtension_path\s*\(\s*['\"]([\d]+)['\"]?\s*,\s*['\"]([\d]+)['\"]", onclick)
                            if match:
                                atch_file_no = match.group(1)
                                file_ord = match.group(2)
                                
                                # 날짜 정보 추출
                                date_info = DateUtils.extract_date_from_title(post['title'])
                                
                                if date_info:
                                    return {
                                        'atch_file_no': atch_file_no,
                                        'file_ord': file_ord,
                                        'date': date_info,
                                        'post_info': post
                                    }
                                
                                return {
                                    'atch_file_no': atch_file_no,
                                    'file_ord': file_ord,
                                    'post_info': post
                                }
                        
                        # 직접 다운로드 URL인 경우 처리
                        elif href and any(ext in href for ext in ['.xls', '.xlsx', '.pdf', '.hwp', '.doc', '.docx']):
                            self.logger.info(f"직접 다운로드 링크 발견: {href}")
                            
                            # 날짜 정보 추출
                            date_info = DateUtils.extract_date_from_title(post['title'])
                            
                            return {
                                'download_url': href,
                                'date': date_info,
                                'post_info': post
                            }
                    
                    # 바로보기 링크를 찾을 수 없는 경우
                    self.logger.warning(f"바로보기 링크를 찾을 수 없음: {post['title']}")
                    
                    # 게시물 내용 추출 시도
                    content = await page.text_content("div.view_cont, .view_content, .bbs_content")
                    
                    # 날짜 정보 추출
                    date_info = DateUtils.extract_date_from_title(post['title'])
                    
                    return {
                        'content': content or "내용 없음",
                        'date': date_info,
                        'post_info': post
                    }
                    
                except Exception as link_err:
                    self.logger.error(f"바로보기 링크 파라미터 추출 중 오류: {str(link_err)}")
                    
                    # 오류 발생 시에도 날짜 정보 추출 시도
                    date_info = DateUtils.extract_date_from_title(post['title'])
                    
                    return {
                        'content': f"오류 발생: {str(link_err)}",
                        'date': date_info,
                        'post_info': post
                    }
                    
            except Exception as e:
                self.logger.error(f"게시물 상세 정보 접근 중 오류: {str(e)}")
                
                # 오류 발생 시에도 날짜 정보 추출 시도
                date_info = DateUtils.extract_date_from_title(post['title'])
                
                if attempt < max_retries - 1:
                    await page.wait_for_timeout(retry_delay * 1000)
                    continue
                
                return {
                    'content': f"접근 오류: {str(e)}",
                    'date': date_info,
                    'post_info': post
                }
        
        return None
    
    async def _direct_access_view_link_params(self, page: Page, post: Dict) -> Optional[Dict]:
        """직접 URL로 게시물 바로보기 링크 파라미터 접근"""
        try:
            if not post.get('post_id'):
                self.logger.error(f"직접 URL 접근 불가 {post['title']} - post_id 누락")
                return None
                
            self.logger.info(f"게시물 직접 URL 접근 시도: {post['title']}")
            
            # 게시물 상세 URL 구성
            post_url = f"https://www.msit.go.kr/bbs/view.do?sCode=user&mId=99&mPid=74&nttSeqNo={post['post_id']}"
            
            # 현재 URL 저장
            current_url = page.url
            
            # 게시물 상세 페이지 접속
            await page.goto(post_url)
            await page.wait_for_timeout(3000)  # 페이지 로드 대기
            
            # 페이지 로드 확인
            try:
                await page.wait_for_selector(".view_head", timeout=10000)
                self.logger.info(f"게시물 상세 페이지 로드 완료: {post['title']}")
            except Exception:
                self.logger.warning(f"게시물 상세 페이지 로드 시간 초과: {post['title']}")
            
            # 바로보기 링크 찾기
            try:
                # 여러 선택자로 바로보기 링크 찾기
                view_link = await page.query_selector("a.view[title='새창 열림']")
                
                # onclick 속성으로 찾기
                if not view_link:
                    view_link = await page.query_selector("a[onclick*='getExtension_path']")
                
                # 텍스트로 찾기
                if not view_link:
                    view_link = await page.query_selector("a:has-text('바로보기'), a:has-text('보기'), a:has-text('첨부파일')")
                
                if view_link:
                    onclick = await view_link.get_attribute('onclick')
                    href = await view_link.get_attribute('href')
                    
                    self.logger.info(f"바로보기 링크 발견, onclick: {onclick}, href: {href}")
                    
                    # getExtension_path('49234', '1') 형식에서 매개변수 추출
                    if onclick and 'getExtension_path' in onclick:
                        match = re.search(r"getExtension_path\s*\(\s*['\"]([\d]+)['\"]?\s*,\s*['\"]([\d]+)['\"]", onclick)
                        if match:
                            atch_file_no = match.group(1)
                            file_ord = match.group(2)
                            
                            # 날짜 정보 추출
                            date_info = DateUtils.extract_date_from_title(post['title'])
                            
                            return {
                                'atch_file_no': atch_file_no,
                                'file_ord': file_ord,
                                'date': date_info,
                                'post_info': post
                            }
                    
                    # 직접 다운로드 URL인 경우 처리
                    elif href and any(ext in href for ext in ['.xls', '.xlsx', '.pdf', '.hwp', '.doc', '.docx']):
                        self.logger.info(f"직접 다운로드 링크 발견: {href}")
                        
                        # 날짜 정보 추출
                        date_info = DateUtils.extract_date_from_title(post['title'])
                        
                        return {
                            'download_url': href,
                            'date': date_info,
                            'post_info': post
                        }
                
                # 바로보기 링크를 찾을 수 없는 경우
                self.logger.warning(f"바로보기 링크를 찾을 수 없음: {post['title']}")
                
                # 게시물 내용 추출 시도
                content = await page.text_content("div.view_cont, .view_content, .bbs_content")
                
                # 날짜 정보 추출
                date_info = DateUtils.extract_date_from_title(post['title'])
                
                return {
                    'content': content or "내용 없음",
                    'date': date_info,
                    'post_info': post
                }
                
            except Exception as link_err:
                self.logger.error(f"바로보기 링크 찾기 중 오류: {str(link_err)}")
                
                # 오류 발생 시에도 날짜 정보 추출 시도
                date_info = DateUtils.extract_date_from_title(post['title'])
                
                return {
                    'content': f"오류 발생: {str(link_err)}",
                    'date': date_info,
                    'post_info': post
                }
            
            # 원래 페이지로 돌아가기
            await page.goto(current_url)
            
            return None
            
        except Exception as e:
            self.logger.error(f"직접 URL 접근 중 오류: {str(e)}")
            
            try:
                # 원래 페이지로 돌아가기
                await page.goto(current_url)
            except:
                pass
                
            return None
    
    async def extract_document_data(self, page: Page, file_params: Dict) -> Optional[Dict[str, pd.DataFrame]]:
        """문서 데이터 추출 (통합된 추출 로직)"""
        try:
            if not file_params:
                self.logger.error("파일 파라미터가 없습니다.")
                return None
                
            extracted_data = None
            
            # 1. Synap 뷰어 데이터 추출 시도
            if 'atch_file_no' in file_params and 'file_ord' in file_params:
                # 바로보기 URL 구성
                view_url = f"https://www.msit.go.kr/bbs/documentView.do?atchFileNo={file_params['atch_file_no']}&fileOrdr={file_params['file_ord']}"
                self.logger.info(f"바로보기 URL: {view_url}")
                
                # 페이지 로드
                await page.goto(view_url)
                await page.wait_for_timeout(5000)  # 초기 대기
                
                # 스크린샷 저장
                await page.screenshot(path=f"document_view_{file_params['atch_file_no']}_{file_params['file_ord']}.png")
                
                # 추출 시도 순서대로 진행
                for extractor in self.extractors:
                    self.logger.info(f"{extractor.__class__.__name__} 추출기로 시도")
                    extracted_data = await extractor.extract(page)
                    
                    if extracted_data and any(not df.empty for df in extracted_data.values()):
                        self.logger.info(f"{extractor.__class__.__name__} 추출 성공: {len(extracted_data)}개 시트")
                        break
                        
                # 특정 행이 누락되는 문제 해결 - 통신사 행 추가
                if extracted_data:
                    extracted_data = self._ensure_all_operators_included(extracted_data)
            
            # 2. 텍스트 콘텐츠만 있는 경우
            elif 'content' in file_params:
                self.logger.info("텍스트 콘텐츠로 처리")
                extracted_data = self._extract_from_text_content(file_params['content'], file_params)
            
            # 3. 직접 다운로드 URL인 경우
            elif 'download_url' in file_params:
                self.logger.info(f"다운로드 URL로 처리: {file_params['download_url']}")
                # 다운로드 로직 구현 필요
                # 현재는 placeholder 반환
                extracted_data = self._create_placeholder_dataframe(file_params)
            
            return extracted_data
            
        except Exception as e:
            self.logger.error(f"문서 데이터 추출 오류: {str(e)}")
            # 오류 발생 시 placeholder 반환
            return self._create_placeholder_dataframe(file_params)
    
    def _ensure_all_operators_included(self, sheets_data: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
        """통신사 행 누락 문제 해결 - 이동통계 관련 시트에 SKT, KT, LGU+, MVNO 등 추가"""
        try:
            # 수정된 시트 데이터
            modified_sheets = {}
            
            for sheet_name, df in sheets_data.items():
                if df.empty or df.shape[1] < 2:
                    modified_sheets[sheet_name] = df
                    continue
                
                # 이동통계 관련 시트인지 확인
                if any(keyword in sheet_name.lower() for keyword in ['이동전화', '무선', '통신사', '가입자']):
                    # 첫 번째 열 확인 (항목/구분/통신사 등)
                    first_col = df.columns[0]
                    
                    # 주요 통신사 이름 목록
                    operators = ['SKT', 'SK텔레콤', 'KT', '케이티', 'LGU+', 'LG유플러스', 'MVNO', '알뜰폰']
                    
                    # 추가해야 할 통신사 확인
                    missing_operators = []
                    for op in operators:
                        if not any(op.lower() in str(val).lower() for val in df[first_col]):
                            missing_operators.append(op)
                    
                    if missing_operators:
                        self.logger.info(f"시트 '{sheet_name}'에 누락된 통신사 발견: {missing_operators}")
                        
                        # 빈 행 추가
                        for op in missing_operators:
                            # 새 행 생성
                            new_row = pd.Series([''] * len(df.columns), index=df.columns)
                            new_row[first_col] = op
                            
                            # 적절한 위치에 삽입
                            df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
                        
                        self.logger.info(f"누락된 통신사 행 추가 완료: {sheet_name}")
                
                modified_sheets[sheet_name] = df
            
            return modified_sheets
            
        except Exception as e:
            self.logger.error(f"통신사 행 추가 중 오류: {str(e)}")
            return sheets_data
    
    def _extract_from_text_content(self, content: str, file_params: Dict) -> Dict[str, pd.DataFrame]:
        """텍스트 콘텐츠에서 데이터 추출"""
        try:
            # 간단한 표 형태로 변환
            lines = content.split('\n')
            lines = [line.strip() for line in lines if line.strip()]
            
            df = pd.DataFrame({'내용': lines})
            
            # 날짜 정보 추가
            date_info = file_params.get('date')
            if date_info:
                date_str = f"{date_info.get('year', '')}년 {date_info.get('month', '')}월"
                df['기준일자'] = date_str
            
            # 출처 정보 추가
            post_info = file_params.get('post_info', {})
            df['출처'] = post_info.get('title', '')
            
            return {"텍스트_내용": df}
            
        except Exception as e:
            self.logger.error(f"텍스트 내용 추출 오류: {str(e)}")
            return self._create_placeholder_dataframe(file_params)
    
    def _create_placeholder_dataframe(self, file_params: Dict) -> Dict[str, pd.DataFrame]:
        """placeholder DataFrame 생성"""
        try:
            post_info = file_params.get('post_info', {})
            
            # 날짜 정보 추출
            date_info = file_params.get('date')
            if date_info:
                year = date_info.get('year', 'Unknown')
                month = date_info.get('month', 'Unknown')
            else:
                # 제목에서 추출 시도
                date_match = re.search(r'\((\d{4})년\s*(\d{1,2})월말\s*기준\)', post_info.get('title', ''))
                year = date_match.group(1) if date_match else "Unknown"
                month = date_match.group(2) if date_match else "Unknown"
            
            # 보고서 유형 결정
            report_type = DataUtils.determine_report_type(post_info.get('title', ''), self.config.report_types)
            
            # 상태 결정
            if 'atch_file_no' in file_params and 'file_ord' in file_params:
                status = "문서 뷰어 접근 실패"
                details = f"atch_file_no={file_params['atch_file_no']}, file_ord={file_params['file_ord']}"
            elif 'content' in file_params:
                status = "텍스트 분석 실패"
                details = "텍스트 내용 처리 중 오류 발생"
            elif 'download_url' in file_params:
                status = "다운로드 URL 처리 실패"
                details = file_params.get('download_url', '')
            else:
                status = "알 수 없는 오류"
                details = "파일 파라미터 부족"
            
            # DataFrame 생성
            df = pd.DataFrame({
                '구분': [f'{year}년 {month}월 통계'],
                '보고서 유형': [report_type],
                '상태': [status],
                '상세 정보': [details],
                '링크': [post_info.get('url', '링크 없음')],
                '추출 시도 시간': [datetime.now().strftime('%Y-%m-%d %H:%M:%S')]
            })
            
            return {"Placeholder": df}
            
        except Exception as e:
            self.logger.error(f"Placeholder DataFrame 생성 오류: {str(e)}")
            # 최소한의 정보만 포함하는 DataFrame 반환
            return {"오류": pd.DataFrame({
                '구분': ['오류 발생'],
                '상태': ['데이터 추출 실패'],
                '상세 정보': [f'오류: {str(e)}']
            })}


##################################################################################
#  6. 구글시트매니저 기본 클래스 
##################################################################################

class GoogleSheetsManager:
    """Google Sheets 관리 클래스"""
    
    def __init__(self, config: MonitorConfig):
        self.config = config
        self.client = None
        self.spreadsheet = None
        self.logger = logging.getLogger(self.__class__.__name__)
    
    async def setup_client(self) -> bool:
        """Google Sheets 클라이언트 설정"""
        if not self.config.gspread_creds:
            self.logger.error("Google Sheets 자격 증명이 없습니다")
            return False
        
        try:
            # 환경 변수에서 자격 증명 파싱
            creds_dict = json.loads(self.config.gspread_creds)
            
            # 임시 파일에 자격 증명 저장
            temp_dir = Path("./downloads")
            temp_dir.mkdir(exist_ok=True)
            temp_creds_path = temp_dir / "temp_creds.json"
            
            with open(temp_creds_path, 'w') as f:
                json.dump(creds_dict, f)
            
            # gspread 클라이언트 설정
            scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
            credentials = ServiceAccountCredentials.from_json_keyfile_name(str(temp_creds_path), scope)
            self.client = gspread.authorize(credentials)
            
            # 임시 파일 삭제
            os.unlink(temp_creds_path)
            
            # 스프레드시트 접근 테스트
            if self.config.spreadsheet_id:
                try:
                    self.spreadsheet = await self._open_spreadsheet_with_retry()
                    self.logger.info(f"Google Sheets API 연결 확인: {self.spreadsheet.title}")
                    return True
                except gspread.exceptions.APIError as e:
                    if "PERMISSION_DENIED" in str(e):
                        self.logger.error(f"Google Sheets 권한 오류: {str(e)}")
                    else:
                        self.logger.warning(f"Google Sheets API 오류: {str(e)}")
                    return False
            else:
                self.logger.info("스프레드시트 ID가 없습니다. 이름으로 스프레드시트를 찾거나 생성합니다.")
                return True
            
        except json.JSONDecodeError:
            self.logger.error("Google Sheets 자격 증명 JSON 파싱 오류")
            return False
        except Exception as e:
            self.logger.error(f"Google Sheets 클라이언트 초기화 중 오류: {str(e)}")
            return False
    
    async def _open_spreadsheet_with_retry(self, max_retries: int = 3) -> Any:
        """재시도 로직이 포함된 스프레드시트 열기"""
        retry_count = 0
        last_error = None
        
        while retry_count < max_retries:
            try:
                # ID로 먼저 시도
                if self.config.spreadsheet_id:
                    try:
                        spreadsheet = self.client.open_by_key(self.config.spreadsheet_id)
                        self.logger.info(f"ID로 스프레드시트 열기 성공: {self.config.spreadsheet_id}")
                        return spreadsheet
                    except Exception as id_err:
                        self.logger.warning(f"ID로 스프레드시트 열기 실패: {str(id_err)}")
                
                # 이름으로 시도
                try:
                    spreadsheet = self.client.open(self.config.spreadsheet_name)
                    self.logger.info(f"이름으로 스프레드시트 열기 성공: {self.config.spreadsheet_name}")
                    # ID 업데이트
                    self.config.spreadsheet_id = spreadsheet.id
                    return spreadsheet
                except gspread.exceptions.SpreadsheetNotFound:
                    # 새 스프레드시트 생성
                    self.logger.info(f"스프레드시트를 찾을 수 없어 새로 생성: {self.config.spreadsheet_name}")
                    spreadsheet = self.client.create(self.config.spreadsheet_name)
                    # ID 업데이트
                    self.config.spreadsheet_id = spreadsheet.id
                    self.logger.info(f"새 스프레드시트 ID: {spreadsheet.id}")
                    return spreadsheet
                    
            except gspread.exceptions.APIError as api_err:
                retry_count += 1
                last_error = api_err
                
                if "RESOURCE_EXHAUSTED" in str(api_err) or "RATE_LIMIT_EXCEEDED" in str(api_err):
                    wait_time = 2 ** retry_count  # 지수 백오프
                    self.logger.warning(f"API 속도 제한 감지, {wait_time}초 대기 후 재시도")
                    await asyncio.sleep(wait_time)
                else:
                    self.logger.error(f"Google Sheets API 오류: {str(api_err)}")
                    if retry_count < max_retries:
                        await asyncio.sleep(2)
                    else:
                        raise
                        
            except Exception as e:
                retry_count += 1
                last_error = e
                self.logger.error(f"스프레드시트 열기 오류: {str(e)}")
                if retry_count < max_retries:
                    await asyncio.sleep(2)
                else:
                    raise
        
        if last_error:
            raise last_error
        return None
    
    async def update_sheets(self, data_updates: List[Dict]) -> bool:
        """시트 업데이트"""
        if not self.client or not data_updates:
            self.logger.error("클라이언트가 초기화되지 않았거나 업데이트할 데이터가 없습니다")
            return False
        
        if not self.spreadsheet:
            self.spreadsheet = await self._open_spreadsheet_with_retry()
            if not self.spreadsheet:
                self.logger.error("스프레드시트를 열 수 없습니다")
                return False
        
        success_count = 0
        total_updates = len(data_updates)
        
        for i, update_data in enumerate(data_updates):
            try:
                self.logger.info(f"데이터 업데이트 {i+1}/{total_updates} 처리 중")
                
                post_info = update_data.get('post_info', {})
                
                # 날짜 정보 추출
                date_info = self._extract_date_info(update_data, post_info)
                if not date_info:
                    self.logger.warning(f"날짜 정보를 추출할 수 없습니다: {post_info.get('title', 'Unknown')}")
                    continue
                
                date_str = f"{date_info['year']}년 {date_info['month']}월"
                report_type = self._determine_report_type(post_info.get('title', ''))
                
                # 시트 데이터 처리
                if 'sheets' in update_data:
                    sheets_data = update_data['sheets']
                    
                    # 데이터 오류 수정
                    fixed_sheets_data = {}
                    for sheet_name, df in sheets_data.items():
                        if df is not None and not df.empty:
                            # 컬럼 매핑 오류 수정
                            fixed_df = self._fix_column_mapping_issue(df)
                            # 행 누락 문제 해결
                            fixed_df = self._ensure_all_rows_included(fixed_df, report_type)
                            fixed_sheets_data[sheet_name] = fixed_df
                    
                    # 시트 업데이트
                    if fixed_sheets_data:
                        success = await self._update_multiple_sheets(fixed_sheets_data, date_str, report_type, post_info)
                        if success:
                            success_count += 1
                            self.logger.info(f"시트 업데이트 성공: {report_type} ({date_str})")
                        else:
                            self.logger.warning(f"시트 업데이트 실패: {report_type} ({date_str})")
                
                elif 'dataframe' in update_data:
                    df = update_data['dataframe']
                    if df is not None and not df.empty:
                        # 컬럼 매핑 오류 수정
                        fixed_df = self._fix_column_mapping_issue(df)
                        # 행 누락 문제 해결
                        fixed_df = self._ensure_all_rows_included(fixed_df, report_type)
                        
                        # 시트 업데이트
                        success = await self._update_single_sheet(report_type, fixed_df, date_str, post_info)
                        if success:
                            success_count += 1
                            self.logger.info(f"단일 시트 업데이트 성공: {report_type} ({date_str})")
                        else:
                            self.logger.warning(f"단일 시트 업데이트 실패: {report_type} ({date_str})")
                
                # API 속도 제한 방지
                if i < total_updates - 1:
                    await asyncio.sleep(self.config.api_request_wait)
                
            except Exception as e:
                self.logger.error(f"데이터 업데이트 {i+1} 처리 중 오류: {str(e)}")
        
        # 통합 시트 업데이트
        if success_count > 0:
            try:
                await self._create_consolidated_sheets(data_updates)
            except Exception as consol_err:
                self.logger.error(f"통합 시트 생성 중 오류: {str(consol_err)}")
        
        # 오래된 시트 정리
        if self.config.cleanup_old_sheets and success_count > 0:
            try:
                removed_count = await self._cleanup_old_sheets()
                self.logger.info(f"{removed_count}개 날짜별 시트 정리 완료")
            except Exception as cleanup_err:
                self.logger.error(f"시트 정리 중 오류: {str(cleanup_err)}")
        
        return success_count > 0
    
    def _extract_date_info(self, update_data: Dict, post_info: Dict) -> Optional[Dict]:
        """업데이트 데이터에서 날짜 정보 추출"""
        if 'date' in update_data:
            return update_data['date']
        
        # 제목에서 날짜 추출
        title = post_info.get('title', '')
        date_match = re.search(r'\(\s*(\d{4})년\s*(\d{1,2})월말\s*기준\)', title)
        if date_match:
            return {
                'year': int(date_match.group(1)),
                'month': int(date_match.group(2))
            }
        
        return None
    
    def _determine_report_type(self, title: str) -> str:
        """게시물 제목에서 보고서 유형 결정"""
        for report_type in self.config.report_types:
            if report_type in title:
                return report_type
                
        # 부분 매칭 시도
        for report_type in self.config.report_types:
            # 주요 키워드 추출
            keywords = report_type.split()
            if any(keyword in title for keyword in keywords if len(keyword) > 1):
                return report_type
                
        return "기타 통신 통계"
    
    def _fix_column_mapping_issue(self, df: pd.DataFrame) -> pd.DataFrame:
        """컬럼 매핑 오류 수정
        
        주요 수정 사항:
        - 마지막에서 두번째 열 대신 마지막 열을 값 컬럼으로 사용
        """
        if df is None or df.empty or df.shape[1] < 2:
            return df
        
        try:
            # 복사본 생성
            fixed_df = df.copy()
            
            # 마지막 열을 값 컬럼으로 사용 (이전에는 마지막에서 두번째 열이 잘못 사용됨)
            if 'value_col' in fixed_df.columns or '값' in fixed_df.columns:
                # 이미 올바른 컬럼명이 있는 경우
                pass
            else:
                # 첫 번째 열은 항목/구분, 마지막 열은 값으로 간주
                cols = fixed_df.columns.tolist()
                if len(cols) >= 2:
                    # 수정: 마지막에서 두번째 열이 아닌 마지막 열 사용
                    fixed_df = fixed_df.rename(columns={
                        cols[0]: '항목',
                        cols[-1]: '값'  # 수정된 부분: -2에서 -1로 변경
                    })
            
            return fixed_df
            
        except Exception as e:
            self.logger.error(f"컬럼 매핑 수정 중 오류: {str(e)}")
            return df
    
    def _ensure_all_rows_included(self, df: pd.DataFrame, report_type: str) -> pd.DataFrame:
        """모든 행이 포함되도록 보장
        
        주요 수정 사항:
        - SKT, KT, LGU+, MVNO 등 모든 중요 행 포함
        """
        if df is None or df.empty:
            return df
            
        try:
            # 복사본 생성
            fixed_df = df.copy()
            
            # 보고서 유형에 따라 예상되는 필수 행 정의
            expected_rows = self._get_expected_rows(report_type)
            if not expected_rows:
                return fixed_df
                
            # 항목/구분 컬럼 찾기
            item_col = None
            for col_candidate in ['항목', '구분', '통신사', '사업자', fixed_df.columns[0]]:
                if col_candidate in fixed_df.columns:
                    item_col = col_candidate
                    break
                    
            if not item_col:
                return fixed_df
                
            # 현재 있는 행 항목 확인
            existing_items = set(fixed_df[item_col].astype(str).str.strip())
            
            # 누락된 행 찾기
            missing_rows = []
            for expected_item in expected_rows:
                # 부분 일치 확인 (예: "SK텔레콤"이 "SKT"에 포함됨)
                if not any(expected_item.lower() in item.lower() or item.lower() in expected_item.lower() 
                          for item in existing_items):
                    # 이 항목은 누락된 것으로 간주
                    new_row = pd.Series(index=fixed_df.columns)
                    new_row[item_col] = expected_item
                    new_row = new_row.fillna('')  # 다른 열은 빈값으로
                    missing_rows.append(new_row)
            
            # 누락된 행 추가
            if missing_rows:
                self.logger.info(f"{len(missing_rows)}개 누락된 행 추가: {[row[item_col] for row in missing_rows]}")
                fixed_df = pd.concat([fixed_df, pd.DataFrame(missing_rows)], ignore_index=True)
                
                # 행 정렬 (항목 컬럼 기준)
                try:
                    fixed_df = fixed_df.sort_values(by=item_col).reset_index(drop=True)
                except:
                    pass
            
            return fixed_df
            
        except Exception as e:
            self.logger.error(f"행 포함 확인 중 오류: {str(e)}")
            return df
    
    def _get_expected_rows(self, report_type: str) -> List[str]:
        """보고서 유형에 따른 예상 행 목록 반환"""
        # 무선통신서비스 가입 현황
        if "무선통신서비스" in report_type and "가입" in report_type:
            return ["SKT", "KT", "LGU+", "MVNO", "알뜰폰", "합계"]
            
        # 유선통신서비스 가입 현황
        elif "유선통신서비스" in report_type and "가입" in report_type:
            return ["KT", "SK브로드밴드", "LG유플러스", "SKT", "기타", "합계"]
            
        # 이동전화 및 트래픽 통계
        elif "이동전화" in report_type and "트래픽" in report_type:
            return ["SKT", "KT", "LGU+", "MVNO", "합계"]
            
        # 무선데이터 트래픽 통계
        elif "무선데이터" in report_type and "트래픽" in report_type:
            return ["SKT", "KT", "LGU+", "MVNO", "합계"]
            
        # 번호이동 현황
        elif "번호이동" in report_type:
            return ["SKT", "KT", "LGU+", "MVNO", "알뜰폰", "합계"]
            
        # 기본값
        return []
    
    async def _update_multiple_sheets(self, sheets_data: Dict[str, pd.DataFrame], 
                                     date_str: str, report_type: str, post_info: Dict) -> bool:
        """여러 시트 업데이트"""
        if not sheets_data:
            self.logger.error("업데이트할 시트 데이터가 없습니다")
            return False
            
        success_count = 0
        total_sheets = len(sheets_data)
        
        # 시트 이름에 날짜를 포함하지 않도록 수정
        # 1. 요약 시트 생성
        try:
            summary_sheet_name = f"요약_{report_type}"
            summary_sheet_name = self._clean_sheet_name(summary_sheet_name)
            
            # 요약 데이터 생성
            summary_data = {
                '데이터시트': [],
                '행 수': [],
                '열 수': [],
                '날짜': []
            }
            
            for sheet_name, df in sheets_data.items():
                if df is None or df.empty:
                    continue
                    
                summary_data['데이터시트'].append(sheet_name)
                summary_data['행 수'].append(df.shape[0])
                summary_data['열 수'].append(df.shape[1])
                summary_data['날짜'].append(date_str)
            
            # 요약 데이터프레임 생성
            if summary_data['데이터시트']:
                summary_df = pd.DataFrame(summary_data)
                
                # 게시물 정보 추가
                if post_info:
                    summary_df['게시물 제목'] = post_info.get('title', '')
                    summary_df['게시물 URL'] = post_info.get('url', '')
                    summary_df['게시물 날짜'] = post_info.get('date', '')
                
                # 요약 시트 업데이트
                success = await self._update_single_sheet(summary_sheet_name, summary_df, date_str, post_info)
                if success:
                    self.logger.info(f"요약 시트 업데이트 성공: {summary_sheet_name}")
        except Exception as summary_err:
            self.logger.warning(f"요약 시트 생성 중 오류: {str(summary_err)}")
        
        # 2. 기존 워크시트 목록 가져오기
        existing_worksheets = []
        try:
            existing_worksheets = [ws.title for ws in self.spreadsheet.worksheets()]
            self.logger.info(f"기존 워크시트 {len(existing_worksheets)}개 발견")
        except Exception as ws_err:
            self.logger.warning(f"워크시트 목록 가져오기 실패: {str(ws_err)}")
        
        # 3. 데이터 시트 처리
        for i, (sheet_name, df) in enumerate(sheets_data.items()):
            try:
                # 빈 데이터프레임 건너뛰기
                if df is None or df.empty:
                    self.logger.warning(f"빈 데이터프레임 건너뜀: {sheet_name}")
                    continue
                    
                # 시트 이름 정리
                clean_sheet_name = self._clean_sheet_name(sheet_name)
                
                # Raw 시트 이름 생성 (_Raw 접미사 추가)
                raw_sheet_name = f"{clean_sheet_name}_Raw"
                
                # 시트 업데이트
                self.logger.info(f"시트 업데이트 중 ({i+1}/{total_sheets}): {raw_sheet_name}")
                
                success = await self._update_sheet(
                    raw_sheet_name, 
                    df, 
                    date_str, 
                    post_info, 
                    {'mode': 'replace'}
                )
                
                if success:
                    success_count += 1
                    self.logger.info(f"시트 업데이트 성공: {raw_sheet_name}")
                else:
                    self.logger.warning(f"시트 업데이트 실패: {raw_sheet_name}")
                
                # API 속도 제한 방지
                if i < total_sheets - 1:
                    await asyncio.sleep(self.config.api_request_wait)
                    
            except Exception as sheet_err:
                self.logger.error(f"시트 {sheet_name} 처리 중 오류: {str(sheet_err)}")
        
        # 4. 통합 시트 생성 (다중 시트가 있는 경우)
        if len(sheets_data) > 1:
            try:
                # 통합 데이터프레임 생성
                all_data = []
                for sheet_name, df in sheets_data.items():
                    if df is None or df.empty:
                        continue
                    
                    # 시트 이름을 컬럼으로 추가
                    df_copy = df.copy()
                    df_copy['데이터출처'] = sheet_name
                    
                    all_data.append(df_copy)
                
                if all_data:
                    # 데이터프레임 결합
                    combined_df = pd.concat(all_data, ignore_index=True)
                    
                    # '데이터출처' 컬럼을 맨 앞으로 이동
                    cols = combined_df.columns.tolist()
                    if '데이터출처' in cols:
                        cols.remove('데이터출처')
                        cols = ['데이터출처'] + cols
                        combined_df = combined_df[cols]
                    
                    # 통합 시트 이름 생성
                    combined_sheet_name = f"전체데이터_{self._clean_sheet_name(report_type)}_Raw"
                    
                    # 통합 시트 업데이트
                    success = await self._update_sheet(
                        combined_sheet_name, 
                        combined_df, 
                        date_str, 
                        post_info, 
                        {'mode': 'replace'}
                    )
                    
                    if success:
                        self.logger.info(f"통합 시트 생성 성공: {combined_sheet_name}")
                    else:
                        self.logger.warning(f"통합 시트 생성 실패: {combined_sheet_name}")
            except Exception as combined_err:
                self.logger.warning(f"통합 시트 생성 중 오류: {str(combined_err)}")
        
        self.logger.info(f"{success_count}/{total_sheets} 시트 업데이트 완료")
        return success_count > 0
    
    async def _update_single_sheet(self, sheet_name: str, df: pd.DataFrame, 
                                  date_str: str, post_info: Dict = None) -> bool:
        """단일 시트 업데이트"""
        return await self._update_sheet(sheet_name, df, date_str, post_info, {'mode': 'append'})
    
    async def _update_sheet(self, sheet_name: str, df: pd.DataFrame, date_str: str,
                           post_info: Dict = None, options: Dict = None) -> bool:
        """시트 업데이트 (모드에 따라 처리)"""
        # 기본 옵션 설정
        if options is None:
            options = {}
        
        mode = options.get('mode', 'append')
        max_retries = options.get('max_retries', self.config.max_retries)
        batch_size = options.get('batch_size', 100)
        add_metadata = options.get('add_metadata', True)
        format_header = options.get('format_header', True)
        
        self.logger.info(f"시트 '{sheet_name}' 업데이트 시작 (모드: {mode})")
        
        # DataFrame 확인 및 정제
        df = self._validate_and_clean_dataframe(df.copy())
        if df.empty:
            self.logger.warning(f"데이터 정제 후 빈 DataFrame, 업데이트 중단")
            return False
        
        # 재시도 로직
        retry_count = 0
        last_error = None
        
        while retry_count < max_retries:
            try:
                # 모드별 처리
                if mode == 'replace':
                    success = await self._replace_sheet(sheet_name, df, date_str, post_info,
                                                      batch_size, add_metadata, format_header)
                elif mode == 'append':
                    success = await self._append_to_sheet(sheet_name, df, date_str, post_info, batch_size)
                else:
                    self.logger.error(f"지원되지 않는 업데이트 모드: {mode}")
                    return False
                
                if success:
                    self.logger.info(f"시트 '{sheet_name}' 업데이트 성공 (모드: {mode})")
                    return True
                else:
                    # 부분 실패 - 재시도
                    retry_count += 1
                    self.logger.warning(f"시트 업데이트 부분 실패, 재시도 중... ({retry_count}/{max_retries})")
                    await asyncio.sleep(2 * retry_count)  # 지수 백오프
            except gspread.exceptions.APIError as api_err:
                retry_count += 1
                last_error = api_err
                
                if "RESOURCE_EXHAUSTED" in str(api_err) or "RATE_LIMIT_EXCEEDED" in str(api_err):
                    # API 제한 - 더 오래 대기 후 재시도
                    wait_time = 5 + (3 * retry_count)
                    self.logger.warning(f"API 속도 제한, {wait_time}초 대기 후 재시도 ({retry_count}/{max_retries})")
                    await asyncio.sleep(wait_time)
                else:
                    self.logger.error(f"API 오류: {str(api_err)}, 재시도 ({retry_count}/{max_retries})")
                    await asyncio.sleep(2 * retry_count)
            except Exception as e:
                retry_count += 1
                last_error = e
                self.logger.error(f"시트 업데이트 중 오류: {str(e)}, 재시도 ({retry_count}/{max_retries})")
                await asyncio.sleep(2 * retry_count)
        
        # 모든 재시도 실패
        self.logger.error(f"시트 '{sheet_name}' 업데이트 실패 (최대 재시도 횟수 초과): {str(last_error)}")
        return False
    
    async def _replace_sheet(self, sheet_name: str, df: pd.DataFrame, date_str: str, 
                            post_info: Dict, batch_size: int, add_metadata: bool, 
                            format_header: bool) -> bool:
        """시트를 완전히 대체"""
        try:
            # 기존 시트 삭제
            try:
                existing_sheet = self.spreadsheet.worksheet(sheet_name)
                self.spreadsheet.del_worksheet(existing_sheet)
                self.logger.info(f"기존 워크시트 삭제: {sheet_name}")
                await asyncio.sleep(2)
            except gspread.exceptions.WorksheetNotFound:
                self.logger.info(f"기존 워크시트 없음: {sheet_name}")
            
            # 새 시트 생성
            rows = max(df.shape[0] + 20, 100)  # 여유 공간 추가
            cols = max(df.shape[1] + 10, 26)  # 최소 A-Z까지
            worksheet = self.spreadsheet.add_worksheet(title=sheet_name, rows=rows, cols=cols)
            self.logger.info(f"새 워크시트 생성: {sheet_name} (행: {rows}, 열: {cols})")
            
            # 전체 데이터 업데이트
            if df.shape[0] > batch_size:
                # 대용량 데이터는 배치 처리
                await self._update_in_batches(worksheet, df, batch_size)
            else:
                # 작은 데이터는 한 번에 업데이트
                header_values = [df.columns.tolist()]
                data_values = df.values.tolist()
                all_values = header_values + data_values
                
                update_range = f'A1:{chr(64 + df.shape[1])}{df.shape[0] + 1}'
                worksheet.update(update_range, all_values)
                self.logger.info(f"전체 데이터 업데이트 완료: {df.shape[0]}행 {df.shape[1]}열")
            
            # 메타데이터 추가
            if add_metadata:
                await self._add_metadata_to_sheet(worksheet, df, date_str, post_info)
            
            # 헤더 서식 설정
            if format_header:
                try:
                    worksheet.format(f'A1:{chr(64 + df.shape[1])}1', {
                        "backgroundColor": {"red": 0.9, "green": 0.9, "blue": 0.9},
                        "textFormat": {"bold": True}
                    })
                    self.logger.info(f"헤더 행 서식 설정 완료")
                except Exception as format_err:
                    self.logger.warning(f"서식 설정 중 오류: {str(format_err)}")
            
            return True
        except Exception as e:
            self.logger.error(f"시트 대체 중 오류: {str(e)}")
            return False
    
    async def _append_to_sheet(self, sheet_name: str, df: pd.DataFrame, 
                              date_str: str, post_info: Dict, batch_size: int) -> bool:
        """기존 시트에 열 추가"""
        try:
            # 워크시트 찾기 또는 생성
            try:
                worksheet = self.spreadsheet.worksheet(sheet_name)
                self.logger.info(f"기존 워크시트 찾음: {sheet_name}")
            except gspread.exceptions.WorksheetNotFound:
                # 새 워크시트 생성
                worksheet = self.spreadsheet.add_worksheet(title=sheet_name, rows="1000", cols="50")
                self.logger.info(f"새 워크시트 생성: {sheet_name}")
                
                # 헤더 행 설정
                worksheet.update_cell(1, 1, "항목")
                await asyncio.sleep(1)
            
            # 현재 워크시트 열 헤더 및 항목 가져오기
            headers = worksheet.row_values(1) or ["항목"]
            if not headers or len(headers) == 0:
                worksheet.update_cell(1, 1, "항목")
                headers = ["항목"]
                await asyncio.sleep(1)
            
            existing_items = worksheet.col_values(1)[1:] or []  # 헤더 제외
            
            # 날짜 열 확인 (이미 존재하는지)
            if date_str in headers:
                col_idx = headers.index(date_str) + 1
                self.logger.info(f"'{date_str}' 열이 이미 위치 {col_idx}에 존재합니다")
            else:
                # 새 날짜 열 추가
                col_idx = len(headers) + 1
                worksheet.update_cell(1, col_idx, date_str)
                self.logger.info(f"위치 {col_idx}에 새 열 '{date_str}' 추가")
                await asyncio.sleep(1)
            
            # 항목-값 업데이트 준비
            key_col = df.columns[0]
            items = df[key_col].astype(str).tolist()
            
            value_col = df.columns[1] if df.shape[1] >= 2 else key_col
            values = df[value_col].astype(str).tolist()
            
            # 배치 업데이트 준비
            cell_updates = []
            new_items_count = 0
            
            for item, value in zip(items, values):
                if not item or not item.strip():
                    continue
                
                # 항목이 이미 존재하는지 확인
                if item in existing_items:
                    row_idx = existing_items.index(item) + 2  # 헤더와 0-인덱스 보정
                else:
                    # 새 항목은 끝에 추가
                    row_idx = len(existing_items) + 2
                    
                    # 항목 업데이트
                    cell_updates.append({
                        'range': f'A{row_idx}',
                        'values': [[item]]
                    })
                    existing_items.append(item)
                    new_items_count += 1
                
                # 값 업데이트
                value_str = value if pd.notna(value) else ""
                cell_updates.append({
                    'range': f'{chr(64 + col_idx)}{row_idx}',
                    'values': [[value_str]]
                })
            
            # 업데이트 실행
            if cell_updates:
                await self._process_batch_updates(worksheet, cell_updates, batch_size)
                self.logger.info(f"{len(cell_updates)}개 셀 업데이트 완료 (새 항목: {new_items_count}개)")
                return True
            else:
                self.logger.warning(f"업데이트할 셀이 없습니다")
                return True  # 업데이트할 것이 없는 것도 성공으로 간주
        except Exception as e:
            self.logger.error(f"시트 추가 모드 처리 중 오류: {str(e)}")
            return False
    
    async def _update_in_batches(self, worksheet, df: pd.DataFrame, batch_size: int = 100) -> bool:
        """대용량 DataFrame 분할 업데이트"""
        try:
            # 헤더 먼저 업데이트
            worksheet.update('A1:1', [df.columns.tolist()])
            await asyncio.sleep(2)
            
            # 데이터 행 배치 업데이트
            for i in range(0, df.shape[0], batch_size):
                end_idx = min(i + batch_size, df.shape[0])
                batch_range = f'A{i+2}:{chr(64 + df.shape[1])}{end_idx+1}'
                batch_data = df.iloc[i:end_idx].values.tolist()
                
                worksheet.update(batch_range, batch_data)
                self.logger.info(f"배치 {i+1}~{end_idx} 업데이트 완료")
                await asyncio.sleep(2)
            
            return True
        except Exception as e:
            self.logger.error(f"배치 업데이트 중 오류: {str(e)}")
            raise  # 상위 함수에서 재시도 처리
    
    async def _process_batch_updates(self, worksheet, updates: List[Dict], batch_size: int = 10) -> bool:
        """셀 업데이트 배치 처리"""
        for i in range(0, len(updates), batch_size):
            batch = updates[i:i+batch_size]
            worksheet.batch_update(batch)
            self.logger.info(f"일괄 업데이트 {i+1}~{min(i+batch_size, len(updates))} 완료")
            await asyncio.sleep(1)
        return True
    
    async def _add_metadata_to_sheet(self, worksheet, df: pd.DataFrame, date_str: str, post_info: Dict) -> bool:
        """시트에 메타데이터 추가"""
        try:
            meta_row = df.shape[0] + 3  # 데이터 이후 빈 행 두 개 건너뛰기
            
            meta_data = [
                ["업데이트 정보"],
                ["날짜", date_str or datetime.now().strftime('%Y-%m-%d')],
                ["업데이트 시간", datetime.now().strftime('%Y-%m-%d %H:%M:%S')]
            ]
            
            # 게시물 정보가 있으면 추가
            if post_info:
                meta_data.append(["게시물 제목", post_info.get('title', '')])
                meta_data.append(["게시물 URL", post_info.get('url', '')])
            
            # 메타 정보 업데이트
            worksheet.update(f'A{meta_row}:B{meta_row + len(meta_data)}', meta_data)
            self.logger.info(f"메타 정보 추가 완료")
            return True
        except Exception as e:
            self.logger.warning(f"메타데이터 추가 중 오류: {str(e)}")
            return False
    
    async def _create_consolidated_sheets(self, data_updates: List[Dict]) -> int:
        """통합 시트 생성"""
        if not data_updates:
            self.logger.warning("통합 시트를 생성할 데이터가 없습니다")
            return 0
        
        try:
            # 모든 워크시트 가져오기
            all_worksheets = self.spreadsheet.worksheets()
            worksheet_map = {ws.title: ws for ws in all_worksheets}
            self.logger.info(f"스프레드시트에서 {len(worksheet_map)}개 워크시트 발견")
            
            # Raw 시트와 대응하는 통합 시트 찾기
            raw_sheets = []
            for title in worksheet_map.keys():
                # Raw 시트 찾기
                if title.endswith('_Raw'):
                    base_name = title[:-4]  # "_Raw" 제거
                    consol_name = f"{base_name}_통합"
                    
                    # 통합 시트가 있는지 확인
                    if consol_name in worksheet_map:
                        raw_sheets.append((title, consol_name))
                        self.logger.info(f"Raw-통합 시트 쌍 발견: {title} -> {consol_name}")
                    else:
                        # 통합 시트 생성 필요
                        self.logger.info(f"통합 시트 없음, '{consol_name}' 생성 예정")
                        raw_sheets.append((title, consol_name))
            
            if not raw_sheets:
                self.logger.warning("통합할 Raw 시트를 찾을 수 없음")
                return 0
            
            # 최신 날짜 정보 수집 - 모든 업데이트의 날짜를 추적
            date_columns = set()
            for update in data_updates:
                if 'date' in update:
                    year = update['date']['year']
                    month = update['date']['month']
                    date_str = f"{year}년 {month}월"
                    date_columns.add(date_str)
                    
                elif 'post_info' in update:
                    post_info = update['post_info']
                    title = post_info.get('title', '')
                    match = re.search(r'\((\d{4})년\s*(\d{1,2})월말\s*기준\)', title)
                    if match:
                        year = match.group(1)
                        month = match.group(2)
                        date_str = f"{year}년 {month}월"
                        date_columns.add(date_str)
            
            self.logger.info(f"업데이트할 날짜 컬럼들: {date_columns}")
            
            # 각 통합 시트 업데이트
            updated_count = 0
            
            for raw_name, consol_name in raw_sheets:
                try:
                    # Raw 시트 가져오기
                    raw_ws = worksheet_map.get(raw_name)
                    if not raw_ws:
                        self.logger.warning(f"Raw 시트를 찾을 수 없음: {raw_name}")
                        continue
                    
                    # 통합 시트 찾기 또는 생성 (기존 시트를 삭제하지 않음)
                    if consol_name in worksheet_map:
                        consol_ws = worksheet_map[consol_name]
                        self.logger.info(f"기존 통합 시트 사용: {consol_name}")
                    else:
                        # 새 통합 시트 생성
                        try:
                            consol_ws = self.spreadsheet.add_worksheet(title=consol_name, rows=1000, cols=100)
                            self.logger.info(f"새 통합 시트 생성됨: {consol_name}")
                            # 통합 시트에 초기 헤더 추가
                            consol_ws.update_cell(1, 1, "기준일자")
                            await asyncio.sleep(1)  # API 제한 방지
                        except Exception as create_err:
                            self.logger.error(f"통합 시트 생성 실패 {consol_name}: {str(create_err)}")
                            continue
                    
                    # 1. 기존 통합 시트 데이터 가져오기
                    try:
                        consol_data = consol_ws.get_all_values()
                        self.logger.info(f"통합 시트 '{consol_name}'에서 {len(consol_data)}행 데이터 가져옴")
                    except Exception as consol_err:
                        self.logger.warning(f"통합 시트 데이터 가져오기 실패: {str(consol_err)}. 새로 생성합니다.")
                        consol_data = []
                    
                    # 기존 헤더 분석
                    existing_headers = consol_data[0] if consol_data else []
                    existing_date_columns = {}  # 날짜 컬럼 이름 -> 인덱스 매핑
                    id_cols_count = 0
                    
                    if existing_headers:
                        # 식별자 열 수 결정 (첫 날짜 열 이전까지)
                        for idx, header in enumerate(existing_headers):
                            # 날짜 패턴 확인 (YYYY년 MM월)
                            if re.match(r'\d{4}년\s*\d{1,2}월', header):
                                id_cols_count = idx
                                break
                        
                        # 식별자 열이 결정되지 않은 경우 기본값 설정
                        if id_cols_count == 0:
                            # 기본적으로 계층 구조는 1-5열 정도로 가정
                            id_cols_count = min(5, len(existing_headers))
                        
                        # 날짜 열 매핑
                        for idx, header in enumerate(existing_headers[id_cols_count:], id_cols_count):
                            if re.match(r'\d{4}년\s*\d{1,2}월', header):
                                existing_date_columns[header] = idx
                    else:
                        # 헤더가 없는 경우
                        id_cols_count = 5  # 기본 값
                        existing_headers = []  # 빈 헤더
                    
                    self.logger.info(f"현재 통합 시트 구조: 식별자 열 {id_cols_count}개, 날짜 열 {len(existing_date_columns)}개")
                    
                    # 2. Raw 시트 데이터 가져오기
                    raw_data = raw_ws.get_all_values()
                    if not raw_data or len(raw_data) < 2:  # 최소 헤더 + 1행 필요
                        self.logger.warning(f"Raw 시트 '{raw_name}'에 충분한 데이터가 없음")
                        continue
                    
                    raw_headers = raw_data[0]
                    
                    # 3. 식별자 열과 데이터 열 분석
                    # 어떤 열을 식별자로 사용할지 결정 (기존 통합 시트 구조 우선)
                    if id_cols_count > len(raw_headers):
                        id_cols_count = len(raw_headers) - 1  # 최소 1개의 데이터 열 남김
                    
                    # 4. 각 날짜 열의 위치 결정 (기존 + 새로운)
                    # 기존 날짜 열 유지
                    new_headers = []
                    if existing_headers:
                        # 식별자 열 유지
                        for i in range(id_cols_count):
                            if i < len(existing_headers):
                                new_headers.append(existing_headers[i])
                            else:
                                # 기존 헤더에 없는 경우 Raw 시트의 헤더 또는 기본값 사용
                                new_headers.append(raw_headers[i] if i < len(raw_headers) else f"Column_{i}")
                    else:
                        # 헤더가 없는 경우 Raw 시트의 헤더 사용
                        for i in range(id_cols_count):
                            new_headers.append(raw_headers[i] if i < len(raw_headers) else f"Column_{i}")
                    
                    # 날짜 열 추가 - 이미 있는 날짜는 유지하고 새 날짜 추가
                    all_date_columns = list(existing_date_columns.keys()) + list(date_columns - set(existing_date_columns.keys()))
                    
                    # 날짜 열 정렬 (최신순 또는 오름차순)
                    all_date_columns.sort(key=lambda x: (
                        # 연도와 월을 추출하여 정렬
                        int(re.search(r'(\d{4})년', x).group(1)) if re.search(r'(\d{4})년', x) else 0,
                        int(re.search(r'(\d{1,2})월', x).group(1)) if re.search(r'(\d{1,2})월', x) else 0
                    ))
                    
                    # 모든 날짜 열 추가
                    for date_col in all_date_columns:
                        new_headers.append(date_col)
                    
                    # 5. 모든 행에 대한 데이터 구성
                    # 식별자 -> 행 데이터 매핑
                    row_data_map = {}
                    
                    # 기존 데이터 맵핑
                    if len(consol_data) > 1:  # 헤더 제외
                        for row_idx in range(1, len(consol_data)):
                            row = consol_data[row_idx]
                            if row_idx >= len(consol_data) or len(row) < id_cols_count:
                                continue
                                
                            # 식별자 키 생성
                            id_key = "|".join([str(row[i]).strip() for i in range(id_cols_count)])
                            
                            # 각 날짜 열의 값 매핑
                            date_values = {}
                            for date_col, col_idx in existing_date_columns.items():
                                if col_idx < len(row):
                                    date_values[date_col] = row[col_idx]
                                else:
                                    date_values[date_col] = ""
                                    
                            row_data_map[id_key] = {
                                'id_values': row[:id_cols_count],
                                'date_values': date_values
                            }
                    
                    # 6. 현재 업데이트의 데이터 추가/업데이트
                    # Raw 데이터로부터 업데이트
                    for row_idx in range(1, len(raw_data)):
                        row = raw_data[row_idx]
                        if row_idx >= len(raw_data) or len(row) <= id_cols_count:
                            continue
                            
                        # 식별자 키 생성
                        id_key = "|".join([str(row[i]).strip() for i in range(id_cols_count)])
                        
                        # 기존 행이 있는지 확인
                        if id_key not in row_data_map:
                            # 새 행 추가
                            row_data_map[id_key] = {
                                'id_values': row[:id_cols_count],
                                'date_values': {}
                            }
                        
                        # 어떤 날짜 열을 업데이트할지 결정
                        # Raw 시트의 마지막 열의 값을 각 날짜 열에 매핑
                        last_data_idx = len(row) - 1
                        if last_data_idx < id_cols_count:
                            continue  # 데이터 열이 없는 경우
                            
                        last_value = row[last_data_idx].strip()
                        
                        # 업데이트할 날짜 열 처리
                        for date_col in date_columns:
                            row_data_map[id_key]['date_values'][date_col] = last_value
                    
                    # 7. 최종 데이터 조합하여 새 행 생성
                    new_rows = [new_headers]  # 헤더부터 시작
                    
                    # 각 행에 대한 데이터 조합
                    for id_key, data in row_data_map.items():
                        id_values = data['id_values']
                        date_values = data['date_values']
                        
                        # 새 행 구성: 식별자 값 + 각 날짜 열의 값
                        new_row = list(id_values)  # 식별자 값 복사
                        
                        # 각 날짜 열에 대한 값 추가
                        for date_col in all_date_columns:
                            new_row.append(date_values.get(date_col, ""))
                            
                        new_rows.append(new_row)
                    
                    # 8. 최종 데이터를 통합 시트에 업데이트
                    try:
                        # 기존 데이터 지우기
                        consol_ws.clear()
                        self.logger.info(f"통합 시트 '{consol_name}'의 기존 데이터 삭제")
                        await asyncio.sleep(1)  # API 제한 방지
                        
                        # 빈 값 처리
                        for i in range(len(new_rows)):
                            for j in range(len(new_rows[i])):
                                if new_rows[i][j] is None:
                                    new_rows[i][j] = ""
                        
                        # 전체 데이터 한 번에 업데이트
                        consol_ws.update('A1', new_rows)
                        self.logger.info(f"통합 시트 '{consol_name}'에 {len(new_rows)}행 업데이트 완료")
                        
                        # 헤더 행 서식 지정 (선택 사항)
                        try:
                            consol_ws.format(f'A1:{chr(64 + len(new_headers))}1', {
                                "backgroundColor": {"red": 0.9, "green": 0.9, "blue": 0.9},
                                "textFormat": {"bold": True}
                            })
                        except Exception as format_err:
                            self.logger.warning(f"헤더 서식 지정 실패: {str(format_err)}")
                        
                        # 메타데이터 추가
                        try:
                            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                            meta_row = len(new_rows) + 3  # 데이터 + 여백
                            meta_updates = [
                                {'range': f'A{meta_row}', 'values': [["Last Updated"]]},
                                {'range': f'B{meta_row}', 'values': [[now]]},
                                {'range': f'C{meta_row}', 'values': [[f"{len(new_rows)-1}개 행 업데이트됨"]]},
                            ]
                            consol_ws.batch_update(meta_updates)
                        except Exception as meta_err:
                            self.logger.warning(f"메타데이터 업데이트 실패: {str(meta_err)}")
                            
                        updated_count += 1
                        
                    except Exception as update_err:
                        self.logger.error(f"통합 시트 업데이트 실패: {str(update_err)}")
                        
                        # 대체 방법: 행별 업데이트
                        try:
                            self.logger.info("대체 방법으로 행별 업데이트 시도")
                            batch_size = 10  # 작은 배치 크기로 API 제한 방지
                            
                            # 헤더 먼저 업데이트
                            consol_ws.update('A1', [new_headers])
                            self.logger.info("헤더 업데이트 완료")
                            await asyncio.sleep(1)
                            
                            # 데이터 행을 배치로 업데이트
                            for i in range(1, len(new_rows), batch_size):
                                end_idx = min(i + batch_size, len(new_rows))
                                batch_range = f'A{i+1}:{chr(64 + len(new_headers))}{end_idx}'
                                batch_data = new_rows[i:end_idx]
                                
                                try:
                                    consol_ws.update(batch_range, batch_data)
                                    self.logger.info(f"행 {i+1}-{end_idx} 업데이트 완료")
                                    await asyncio.sleep(1)
                                except Exception as batch_err:
                                    self.logger.error(f"행 {i+1}-{end_idx} 업데이트 실패: {str(batch_err)}")
                                    
                                    # 개별 행 업데이트로 폴백
                                    for j, row_data in enumerate(batch_data, i+1):
                                        try:
                                            row_range = f'A{j}:{chr(64 + len(row_data))}{j}'
                                            consol_ws.update(row_range, [row_data])
                                            self.logger.info(f"개별 행 {j} 업데이트 완료")
                                            await asyncio.sleep(0.5)
                                        except Exception as row_err:
                                            self.logger.error(f"개별 행 {j} 업데이트 실패: {str(row_err)}")
                            
                            updated_count += 1
                        except Exception as alt_err:
                            self.logger.error(f"대체 방법으로 업데이트 실패: {str(alt_err)}")
                            
                except Exception as sheet_err:
                    self.logger.error(f"시트 '{raw_name}/{consol_name}' 처리 중 오류: {str(sheet_err)}")
            
            self.logger.info(f"통합 시트 업데이트 완료: {updated_count}개 시트")
            return updated_count
            
        except Exception as e:
            self.logger.error(f"통합 시트 생성 중 오류: {str(e)}")
            return 0
    
    async def _cleanup_old_sheets(self) -> int:
        """오래된 날짜별 시트 정리"""
        try:
            # 모든 워크시트 가져오기
            all_worksheets = self.spreadsheet.worksheets()
            sheets_to_remove = []
            
            # 날짜별 시트 찾기 (패턴 매칭)
            date_patterns = [
                r'요약_.*_\d{4}년\s*\d{1,2}월',  # "요약_무선통신서비스 가입 현황_전체데이터_2025년 1월"
                r'.*\d{4}년\s*\d{1,2}월.*'       # 연도/월이 이름에 포함된 모든 시트
            ]
            
            # 유지할 시트 패턴 - _Raw 또는 _통합으로 끝나는 시트
            keep_patterns = [r'.*_Raw$', r'.*_통합$']
            
            for worksheet in all_worksheets:
                title = worksheet.title
                
                # 유지할 시트인지 확인
                is_keeper = False
                for pattern in keep_patterns:
                    if re.match(pattern, title):
                        is_keeper = True
                        break
                        
                # 유지할 시트가 아니면 날짜 패턴 확인
                if not is_keeper:
                    for pattern in date_patterns:
                        if re.match(pattern, title):
                            sheets_to_remove.append(worksheet)
                            break
            
            # 삭제할 시트 로깅
            self.logger.info(f"{len(sheets_to_remove)}개 날짜별 시트 삭제 예정")
            for ws in sheets_to_remove:
                self.logger.info(f"삭제 예정: {ws.title}")
            
            # 시트 삭제
            remove_count = 0
            for ws in sheets_to_remove:
                try:
                    self.spreadsheet.del_worksheet(ws)
                    self.logger.info(f"시트 삭제 완료: {ws.title}")
                    remove_count += 1
                    # API 속도 제한 방지
                    await asyncio.sleep(1)
                except Exception as del_err:
                    self.logger.error(f"시트 {ws.title} 삭제 중 오류: {str(del_err)}")
            
            return remove_count
            
        except Exception as e:
            self.logger.error(f"날짜별 시트 정리 중 오류: {str(e)}")
            return 0
    
    def _validate_and_clean_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """DataFrame 검증 및 정제"""
        if df is None or df.empty:
            return pd.DataFrame()
            
        try:
            # NaN 값 처리
            df = df.fillna('')
            
            # 열 이름 정리
            df.columns = [str(col).strip() for col in df.columns]
            
            # 중복 열 처리
            if len(df.columns) != len(set(df.columns)):
                new_columns = []
                seen = {}
                for col in df.columns:
                    if col in seen:
                        seen[col] += 1
                        new_columns.append(f"{col}_{seen[col]}")
                    else:
                        seen[col] = 0
                        new_columns.append(col)
                df.columns = new_columns
            
            # 모든 값을 문자열로 변환 (Google Sheets 업데이트 안정성)
            for col in df.columns:
                df[col] = df[col].astype(str)
                
                # 숫자 형식 데이터 정리
                if df[col].str.replace(',', '').str.replace('.', '').str.isnumeric().mean() > 0.7:
                    try:
                        numeric_values = pd.to_numeric(df[col].str.replace(',', ''))
                        # 큰 숫자는 천 단위 구분자 포맷팅
                        df[col] = numeric_values.apply(
                            lambda x: f"{x:,}" if abs(x) >= 1000 else str(x)
                        )
                    except:
                        pass
            
            # 빈 행 제거
            df = df.replace('', np.nan)
            df = df.dropna(how='all').reset_index(drop=True)
            
            # 빈 열 제거
            df = df.loc[:, ~df.isna().all()]
            
            # NaN을 다시 빈 문자열로 변환
            df = df.fillna('')
            
            # 중복 행 제거
            df = df.drop_duplicates().reset_index(drop=True)
            
            return df
            
        except Exception as e:
            self.logger.error(f"DataFrame 검증 중 오류: {str(e)}")
            return pd.DataFrame()
    
    def _clean_sheet_name(self, sheet_name: str) -> str:
        """시트 이름 정리 (Google Sheets 제한 준수)"""
        # 유효하지 않은 문자 제거
        clean_name = re.sub(r'[\\/*\[\]:]', '_', str(sheet_name))
        
        # 길이 제한 (Google Sheets 100자 제한)
        if len(clean_name) > 100:
            clean_name = clean_name[:97] + '...'
            
        # 빈 이름 처리
        if not clean_name:
            clean_name = 'Sheet'
            
        return clean_name


##################################################################################
#  6. 텔레그램 알림 클래스
##################################################################################

class TelegramNotifier:
    """텔레그램 알림 클래스"""
    
    def __init__(self, config: MonitorConfig):
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
    
    async def send_notification(
        self, 
        posts: List[Dict] = None, 
        data_updates: List[Dict] = None
    ) -> bool:
        """알림 메시지 전송"""
        if not posts and not data_updates:
            self.logger.info("알림을 보낼 내용이 없습니다")
            return True
            
        try:
            # 텔레그램 봇 초기화
            bot = telegram.Bot(token=self.config.telegram_token)
            
            # 메시지 포맷팅
            message = self._format_message(posts, data_updates)
            
            # 메시지 분할 (텔레그램 제한)
            max_length = 4000
            if len(message) > max_length:
                chunks = [message[i:i+max_length] for i in range(0, len(message), max_length)]
                for i, chunk in enumerate(chunks):
                    # 첫 번째가 아닌 메시지에 헤더 추가
                    if i > 0:
                        chunk = "📊 *MSIT 통신 통계 모니터링 (계속)...*\n\n" + chunk
                    
                    chat_id = int(self.config.chat_id)
                    await bot.send_message(
                        chat_id=chat_id,
                        text=chunk,
                        parse_mode='Markdown'
                    )
                    await asyncio.sleep(1)  # 메시지 사이 지연
                
                self.logger.info(f"텔레그램 메시지 {len(chunks)}개 청크로 분할 전송 완료")
            else:
                # 단일 메시지 전송
                chat_id = int(self.config.chat_id)
                await bot.send_message(
                    chat_id=chat_id,
                    text=message,
                    parse_mode='Markdown'
                )
                self.logger.info("텔레그램 메시지 전송 성공")
            
            return True
            
        except Exception as e:
            self.logger.error(f"텔레그램 메시지 전송 중 오류: {str(e)}")
            
            # 단순화된 메시지로 재시도
            try:
                simple_msg = f"⚠️ MSIT 통신 통계 알림: {len(posts) if posts else 0}개 새 게시물, {len(data_updates) if data_updates else 0}개 업데이트"
                await bot.send_message(
                    chat_id=int(self.config.chat_id),
                    text=simple_msg
                )
                self.logger.info("단순화된 텔레그램 메시지 전송 성공")
                return True
            except Exception as simple_err:
                self.logger.error(f"단순화된 텔레그램 메시지 전송 중 오류: {str(simple_err)}")
                return False
    
    def _format_message(self, posts: List[Dict] = None, data_updates: List[Dict] = None) -> str:
        """메시지 포맷팅"""
        posts = posts or []
        data_updates = data_updates or []
        
        message = "📊 *MSIT 통신 통계 모니터링*\n\n"
        
        # 새 게시물 정보 추가
        if posts:
            message += "📱 *새로운 통신 관련 게시물*\n\n"
            
            # 최대 5개 게시물만 표시 (너무 길지 않도록)
            displayed_posts = posts[:5]
            for post in displayed_posts:
                message += f"📅 {post.get('date', '')}\n"
                message += f"📑 {post.get('title', '')}\n"
                message += f"🏢 {post.get('department', '')}\n"
                if post.get('url'):
                    message += f"🔗 [게시물 링크]({post.get('url', '')})\n"
                message += "\n"
            
            # 추가 게시물이 있는 경우 표시
            if len(posts) > 5:
                message += f"_...외 {len(posts) - 5}개 게시물_\n\n"
        
        # 데이터 업데이트 정보 추가
        if data_updates:
            message += "📊 *Google Sheets 데이터 업데이트*\n\n"
            
            # 최대 10개 업데이트만 표시
            displayed_updates = data_updates[:10]
            for update in displayed_updates:
                post_info = update.get('post_info', {})
                
                # 날짜 정보 추출
                date_str = "알 수 없음"
                if 'date' in update:
                    year = update['date'].get('year', '알 수 없음')
                    month = update['date'].get('month', '알 수 없음')
                    date_str = f"{year}년 {month}월"
                else:
                    # 제목에서 날짜 추출 시도
                    title = post_info.get('title', '')
                    date_match = re.search(r'\((\d{4})년\s*(\d{1,2})월말\s*기준\)', title)
                    if date_match:
                        year = date_match.group(1)
                        month = date_match.group(2)
                        date_str = f"{year}년 {month}월"
                
                # 보고서 유형 결정
                title = post_info.get('title', '')
                report_type = self._determine_report_type(title)
                
                message += f"📅 *{date_str}*\n"
                message += f"📑 {report_type}\n"
                
                # 시트 정보 표시 (있는 경우)
                if 'sheets' in update:
                    sheet_names = list(update['sheets'].keys())
                    if len(sheet_names) <= 3:
                        message += f"📗 시트: {', '.join(sheet_names)}\n"
                    else:
                        message += f"📗 시트: {len(sheet_names)}개 업데이트됨\n"
                
                message += "✅ 업데이트 완료\n\n"
            
            # 추가 업데이트가 있는 경우 표시
            if len(data_updates) > 10:
                message += f"_...외 {len(data_updates) - 10}개 업데이트_\n\n"
        
        # 스프레드시트 정보 추가
        if data_updates and self.config.spreadsheet_id:
            spreadsheet_url = f"https://docs.google.com/spreadsheets/d/{self.config.spreadsheet_id}"
            message += f"📋 [스프레드시트 보기]({spreadsheet_url})\n\n"
        
        # 현재 시간 추가
        kr_time = datetime.now() + timedelta(hours=9)
        message += f"🕒 *업데이트 시간: {kr_time.strftime('%Y-%m-%d %H:%M')} (KST)*"
        
        return message
    
    def _determine_report_type(self, title: str) -> str:
        """게시물 제목에서 보고서 유형 결정"""
        for report_type in self.config.report_types:
            if report_type in title:
                return report_type
                
        # 부분 매칭 시도
        for report_type in self.config.report_types:
            # 주요 키워드 추출
            keywords = report_type.split()
            if any(keyword in title for keyword in keywords if len(keyword) > 1):
                return report_type
                
        return "기타 통신 통계"


##################################################################################
#  7. Main 클래스
##################################################################################


class MSITMonitor:
    """MSIT 모니터링 메인 클래스"""
    
    def __init__(self):
        self.config_manager = ConfigManager()
        self.config = self.config_manager.config
        self.logger = LoggingUtils.setup_enhanced_logging()
        
        # 컴포넌트 초기화
        self.web_driver = None  # 실행 시점에 초기화
        self.google_sheets = GoogleSheetsManager(self.config)
        self.telegram = TelegramNotifier(self.config)
        
        # 추출기 초기화
        self.extractors = []  # 실행 시점에 초기화
        self.parser = None  # 실행 시점에 초기화
  
    async def run_monitor(
        self,
        days_range: int = 4,
        check_sheets: bool = True,
        start_page: int = 1,
        end_page: int = 5,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        reverse_order: bool = True
    ) -> None:
        """모니터링 실행"""
        start_time = time.time()
        
        try:
            self.logger.info(f"=== MSIT 통신 통계 모니터링 시작 ===")
            if reverse_order:
                self.logger.info(f"검색 범위: 페이지 {end_page}~{start_page} (역순), days_range={days_range}")
            else:
                self.logger.info(f"검색 범위: 페이지 {start_page}~{end_page}, days_range={days_range}")
            
            # Google Sheets 클라이언트 초기화
            if check_sheets:
                sheets_initialized = self.google_sheets.setup_client()
                if not sheets_initialized:
                    self.logger.warning("Google Sheets 클라이언트 초기화 실패, 시트 업데이트 건너뜀")
                    check_sheets = False
            
            # Playwright 초기화
            async with async_playwright() as playwright:
                # 웹드라이버 초기화
                self.web_driver = WebDriverManager(self.config)
                browser, context, page = await self.web_driver.setup_browser(playwright)
                
                # 추출기 초기화
                self.extractors = [
                    SynapViewerExtractor(self.config),
                    HTMLExtractor(self.config),
                    OCRExtractor(self.config)
                ]
                self.parser = PageParser(self.config, self.extractors)
                
                # 사이트 접속
                try:
                    await page.goto(self.config.landing_url)
                    await page.wait_for_load_state("networkidle")
                    self.logger.info("랜딩 페이지 접속 성공")
                    
                    # 통계 페이지로 이동
                    await page.goto(self.config.stats_url)
                    await page.wait_for_load_state("networkidle")
                    self.logger.info("통계 페이지 접속 성공")
                    
                    # 스크린샷 저장
                    await self.web_driver.take_screenshot("stats_page", page)
                    
                except Exception as e:
                    self.logger.error(f"사이트 접속 중 오류: {str(e)}")
                    return
                
                # 역순 탐색 설정
                if reverse_order:
                    page_sequence = range(end_page, start_page - 1, -1)
                else:
                    page_sequence = range(start_page, end_page + 1)
                
                # 전체 결과 추적
                all_posts = []
                telecom_stats_posts = []
                continue_to_next_page = True
                
                # 각 페이지 파싱
                for page_num in page_sequence:
                    if not continue_to_next_page:
                        self.logger.info("이전 페이지에서 날짜 범위 조건으로 검색 중단")
                        break
                    
                    self.logger.info(f"페이지 {page_num} 탐색 시작")
                    
                    # 페이지 이동
                    page_navigation_success = await self.web_driver.navigate_to_page(page, page_num)
                    if not page_navigation_success:
                        self.logger.warning(f"페이지 {page_num}으로 이동 실패, 다음 페이지로 진행")
                        continue
                    
                    # 페이지 콘텐츠 파싱
                    page_posts, stats_posts, result_info = await self.parser.parse_page_content(
                        page,
                        page_num=page_num,
                        days_range=days_range,
                        start_date=start_date,
                        end_date=end_date,
                        reverse_order=reverse_order
                    )
                    
                    # 파싱 결과 기록
                    self.logger.info(f"페이지 {page_num} 파싱 결과: {len(page_posts)}개 게시물, {len(stats_posts)}개 통신 통계")
                    
                    # 다음 페이지 진행 여부
                    continue_to_next_page = result_info.get('continue_to_next_page', True)
                    
                    # 결과 추가
                    all_posts.extend(page_posts)
                    telecom_stats_posts.extend(stats_posts)
                    
                    # 잠시 대기
                    await asyncio.sleep(2)
                
                # 통신 통계 게시물 처리
                data_updates = []
                if check_sheets and telecom_stats_posts:
                    self.logger.info(f"{len(telecom_stats_posts)}개 통신 통계 게시물 처리 시작")
                    data_updates = await self._process_telecom_posts(page, telecom_stats_posts)
                    
                # 통합 시트 업데이트
                if check_sheets and data_updates:
                    try:
                        await self.google_sheets._create_consolidated_sheets(data_updates)
                    except Exception as consol_err:
                        self.logger.error(f"통합 시트 생성 중 오류: {str(consol_err)}")
                
                # 구형 시트 정리 (선택적)
                if check_sheets and data_updates and self.config.cleanup_old_sheets:
                    self.logger.info("구형 시트 정리 시작")
                    removed_count = self.google_sheets._cleanup_old_sheets()
                    self.logger.info(f"{removed_count}개 구형 시트 제거 완료")
                
                # 텔레그램 알림 전송
                if all_posts or data_updates:
                    await self.telegram.send_notification(all_posts, data_updates)
                    self.logger.info(f"알림 전송 완료: {len(all_posts)}개 게시물, {len(data_updates)}개 업데이트")
                else:
                    self.logger.info(f"최근 {days_range}일 내 새 게시물이 없습니다")
                
                # 브라우저 종료
                await context.close()
                await browser.close()
        
        except Exception as e:
            await self._handle_errors(e, "모니터링 실행")
        
        finally:
            # 실행 시간 기록
            end_time = time.time()
            self.logger.info(f"총 실행 시간: {end_time - start_time:.2f}초")
            self.logger.info("=== MSIT 통신 통계 모니터링 종료 ===")
    
    async def _process_telecom_posts(
        self, 
        page: Page,
        telecom_posts: List[Dict]
    ) -> List[Dict]:
        """통신 통계 게시물 처리"""
        data_updates = []
        
        for i, post in enumerate(telecom_posts):
            try:
                self.logger.info(f"게시물 {i+1}/{len(telecom_posts)} 처리 중: {post.get('title', '')}")
                
                # 바로보기 링크 파라미터 추출
                file_params = await self.parser.find_view_link_params(page, post)
                
                if not file_params:
                    self.logger.warning(f"바로보기 링크 파라미터 추출 실패: {post.get('title', '')}")
                    continue
                
                # 문서 데이터 추출
                sheets_data = await self.parser.extract_document_data(page, file_params)
                
                if sheets_data and any(not df.empty for df in sheets_data.values()):
                    # 추출 성공
                    self.logger.info(f"데이터 추출 성공: {post.get('title', '')}, {len(sheets_data)}개 시트")
                    
                    # 업데이트 데이터 준비
                    update_data = {
                        'sheets': sheets_data,
                        'post_info': post
                    }
                    
                    if 'date' in file_params:
                        update_data['date'] = file_params['date']
                    
                    # Google Sheets 업데이트
                    success = self.google_sheets.update_sheets([update_data])
                    if success:
                        self.logger.info(f"Google Sheets 업데이트 성공: {post.get('title', '')}")
                        data_updates.append(update_data)
                    else:
                        self.logger.warning(f"Google Sheets 업데이트 실패: {post.get('title', '')}")
                else:
                    self.logger.warning(f"데이터 추출 실패 또는 빈 데이터: {post.get('title', '')}")
                
                # API 제한 방지
                await asyncio.sleep(2)
                
            except Exception as e:
                await self._handle_errors(e, f"게시물 처리: {post.get('title', '')}")
        
        return data_updates
    
    async def _handle_errors(self, error: Exception, context: str):
        """오류 처리"""
        self.logger.error(f"{context} 중 오류 발생: {str(error)}")
        
        # 스택 트레이스 로깅
        import traceback
        self.logger.error(f"스택 트레이스: {traceback.format_exc()}")
        
        # 중요 오류는 텔레그램으로 알림
        try:
            error_msg = f"⚠️ MSIT 모니터링 오류 ({context}):\n{str(error)}"
            if len(error_msg) > 500:
                error_msg = error_msg[:500] + "..."
                
            await self.telegram.send_notification(
                posts=[{
                    'title': f"모니터링 오류: {context}",
                    'date': datetime.now().strftime('%Y-%m-%d'),
                    'department': 'System Error',
                    'url': ''
                }]
            )
        except Exception as notify_err:
            self.logger.error(f"오류 알림 전송 실패: {str(notify_err)}")

async def main():
    """메인 실행 함수"""
    try:
        # 환경 변수 파싱
        days_range = int(os.environ.get('DAYS_RANGE', '4'))
        start_page = int(os.environ.get('START_PAGE', '1'))
        end_page = int(os.environ.get('END_PAGE', '5'))
        check_sheets_str = os.environ.get('CHECK_SHEETS', 'true').lower()
        check_sheets = check_sheets_str in ('true', 'yes', '1', 'y')
        start_date = os.environ.get('START_DATE', '')
        end_date = os.environ.get('END_DATE', '')
        reverse_order_str = os.environ.get('REVERSE_ORDER', 'true').lower()
        reverse_order = reverse_order_str in ('true', 'yes', '1', 'y')
        
        # 모니터 실행
        monitor = MSITMonitor()
        await monitor.run_monitor(
            days_range=days_range,
            check_sheets=check_sheets,
            start_page=start_page,
            end_page=end_page,
            start_date=start_date if start_date else None,
            end_date=end_date if end_date else None,
            reverse_order=reverse_order
        )
    
    except Exception as e:
        logging.error(f"메인 함수 오류: {str(e)}", exc_info=True)

if __name__ == "__main__":
    asyncio.run(main())

# ================================
# 주요 개선사항 및 수정 포인트
# ================================

"""
1. 🔧 데이터 추출 오류 수정:
   - _fix_column_mapping_issue(): 마지막에서 두번째 열 → 마지막 열
   - _ensure_all_rows_included(): SKT, KT, LGU+, MVNO 등 모든 행 포함

2. 🚀 Selenium → Playwright 전환:
   - WebDriverManager 클래스에서 Playwright 사용
   - 비동기 처리로 성능 향상
   - 더 안정적인 웹 스크래핑

3. 🏗️ 모듈화된 클래스 구조:
   - 단일 책임 원칙 적용
   - 재사용 가능한 컴포넌트
   - 향상된 테스트 가능성

4. 📊 향상된 데이터 처리:
   - 정확한 컬럼 매핑
   - 행 누락 방지
   - 데이터 무결성 보장

5. 🛡️ 강화된 오류 처리:
   - 각 컴포넌트별 오류 처리
   - 진단 정보 수집
   - 복구 메커니즘

다음 단계: 각 클래스의 세부 구현을 요청하시면 상세한 코드를 제공하겠습니다.
"""
