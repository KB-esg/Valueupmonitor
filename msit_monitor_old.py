#!/usr/bin/env python
# -*- coding: utf-8 -*-

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
import traceback
import subprocess

# Playwright imports (Selenium 대체)
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
from playwright.async_api import Page, Browser, BrowserContext

from bs4 import BeautifulSoup
import telegram
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# OCR 관련 라이브러리 임포트 추가
try:
    from PIL import Image, ImageEnhance, ImageFilter
    import cv2
    import pytesseract
    OCR_IMPORTS_AVAILABLE = True
except ImportError:
    OCR_IMPORTS_AVAILABLE = False
    logging.warning("OCR 관련 라이브러리 임포트 실패. OCR 기능은 사용할 수 없습니다.")


# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('msit_monitor')

# 전역 설정 변수
CONFIG = {
    'landing_url': "https://www.msit.go.kr",
    'stats_url': "https://www.msit.go.kr/bbs/list.do?sCode=user&mPid=74&mId=99",
    'report_types': [
        "이동전화 및 트래픽 통계",
        "이동전화 및 시내전화 번호이동 현황",
        "유선통신서비스 가입 현황",
        "무선통신서비스 가입 현황", 
        "특수부가통신사업자현황",
        "무선데이터 트래픽 통계",
        "유·무선통신서비스 가입 현황 및 무선데이터 트래픽 통계"
    ],
    'telegram_token': os.environ.get('TELCO_NEWS_TOKEN'),
    'chat_id': os.environ.get('TELCO_NEWS_TESTER'),
    'gspread_creds': os.environ.get('MSIT_GSPREAD_ref'),
    'spreadsheet_id': os.environ.get('MSIT_SPREADSHEET_ID'),
    'spreadsheet_name': os.environ.get('SPREADSHEET_NAME', 'MSIT 통신 통계'),
    'ocr_enabled': os.environ.get('OCR_ENABLED', 'true').lower() in ('true', 'yes', '1', 'y'),
    'cleanup_old_sheets': os.environ.get('CLEANUP_OLD_SHEETS', 'false').lower() in ('true', 'yes', '1', 'y')
}

# 임시 디렉토리 설정
TEMP_DIR = Path("./downloads")
TEMP_DIR.mkdir(exist_ok=True)
SCREENSHOTS_DIR = Path("./screenshots")
SCREENSHOTS_DIR.mkdir(exist_ok=True)


#=====================================================================================
# Part 1. 유틸리티 함수들
#=====================================================================================

async def setup_browser():
    """Playwright 브라우저 설정 (향상된 봇 탐지 회피)"""
    playwright = await async_playwright().start()
    
    # 브라우저 실행 옵션
    browser_args = [
        '--no-sandbox',
        '--disable-dev-shm-usage',
        '--disable-gpu',
        '--disable-blink-features=AutomationControlled',
        '--disable-features=IsolateOrigins,site-per-process'
    ]
    
    # User Agent 설정
    user_agents = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    ]
    
    # 헤드리스 모드 설정
    headless = os.environ.get('HEADLESS', 'false').lower() == 'true'
    
    # 브라우저 시작
    browser = await playwright.chromium.launch(
        headless=headless,
        args=browser_args
    )
    
    # 컨텍스트 생성 (봇 탐지 회피 설정 포함)
    context = await browser.new_context(
        viewport={'width': 1920, 'height': 1080},
        user_agent=random.choice(user_agents),
        locale='ko-KR',
        timezone_id='Asia/Seoul',
        permissions=['geolocation'],
        ignore_https_errors=True,
        java_script_enabled=True,
        accept_downloads=True
    )
    
    # 다운로드 경로 설정
    await context.set_default_timeout(30000)
    
    # 봇 탐지 회피를 위한 JavaScript 주입
    await context.add_init_script("""
        Object.defineProperty(navigator, 'webdriver', {
            get: () => undefined
        });
        Object.defineProperty(navigator, 'plugins', {
            get: () => [1, 2, 3, 4, 5]
        });
        Object.defineProperty(navigator, 'languages', {
            get: () => ['ko-KR', 'ko', 'en-US', 'en']
        });
        window.chrome = {
            runtime: {}
        };
        Object.defineProperty(navigator, 'permissions', {
            get: () => ({
                query: () => Promise.resolve({ state: 'granted' })
            })
        });
        
        // Playwright 특유의 속성 숨기기
        delete window.__playwright;
        delete window.__pw_manual;
        delete window.playwright;
    """)
    
    # 새 페이지 생성
    page = await context.new_page()
    
    # 페이지 타임아웃 설정
    page.set_default_timeout(30000)  # 30초
    page.set_default_navigation_timeout(30000)
    
    logger.info("Playwright 브라우저 설정 완료")
    return playwright, browser, context, page

async def reset_browser_context(page, delete_cookies=True, navigate_to_blank=True):
    """브라우저 컨텍스트 초기화 (쿠키 삭제 및 빈 페이지로 이동)"""
    try:
        # 쿠키 삭제
        if delete_cookies:
            await page.context.clear_cookies()
            logger.info("모든 쿠키 삭제 완료")
            
        # 빈 페이지로 이동
        if navigate_to_blank:
            await page.goto("about:blank")
            logger.info("빈 페이지로 이동 완료")
            
        # 로컬 스토리지 및 세션 스토리지 클리어
        try:
            await page.evaluate("localStorage.clear(); sessionStorage.clear();")
            logger.info("로컬 스토리지 및 세션 스토리지 클리어")
        except Exception as js_err:
            logger.warning(f"스토리지 클리어 중 오류: {str(js_err)}")
            
        return True
    except Exception as e:
        logger.error(f"브라우저 컨텍스트 초기화 실패: {str(e)}")
        return False

async def take_screenshot(page, name, crop_area=None):
    """특정 페이지의 스크린샷 저장 (선택적 영역 잘라내기)"""
    try:
        screenshot_path = f"screenshots/{name}_{int(time.time())}.png"
        
        # 전체 페이지 스크린샷
        await page.screenshot(path=screenshot_path, full_page=False)
        
        # 특정 영역만 잘라내기 (OCR 개선용)
        if crop_area and CONFIG['ocr_enabled']:
            try:
                from PIL import Image
                
                # 전체 스크린샷 로드
                img = Image.open(screenshot_path)
                
                # 특정 영역 잘라내기 (crop_area는 (x1, y1, x2, y2) 형식)
                if isinstance(crop_area, tuple) and len(crop_area) == 4:
                    cropped_img = img.crop(crop_area)
                    
                    # 크롭된 이미지 저장 (원본 이름에 _crop 추가)
                    crop_path = screenshot_path.replace('.png', '_crop.png')
                    cropped_img.save(crop_path)
                    logger.info(f"크롭된 스크린샷 저장: {crop_path}")
                    
                    return crop_path
            except Exception as crop_err:
                logger.warning(f"이미지 크롭 중 오류: {str(crop_err)}")
        
        logger.info(f"스크린샷 저장: {screenshot_path}")
        return screenshot_path
    except Exception as e:
        logger.error(f"스크린샷 저장 중 오류: {str(e)}")
        return None

async def save_html_for_debugging(page, name_prefix, include_iframe=True):
    """
    디버깅을 위해 HTML 콘텐츠 저장
    
    Args:
        page: Playwright Page instance
        name_prefix: Prefix for the file name
        include_iframe: Whether to also save iframe content if available
        
    Returns:
        None
    """
    timestamp = int(time.time())
    
    try:
        # Save main page HTML
        main_html = await page.content()
        html_path = f"{name_prefix}_{timestamp}_main.html"
        
        with open(html_path, 'w', encoding='utf-8') as f:
            f.write(main_html)
        
        logger.info(f"Saved main page HTML: {html_path}")
        
        # Save iframe content if requested
        if include_iframe:
            try:
                # Find all iframes
                iframes = await page.query_selector_all("iframe")
                
                for i, iframe in enumerate(iframes):
                    try:
                        # Get iframe content
                        frame = await iframe.content_frame()
                        if frame:
                            iframe_html = await frame.content()
                            iframe_path = f"{name_prefix}_{timestamp}_iframe_{i+1}.html"
                            
                            with open(iframe_path, 'w', encoding='utf-8') as f:
                                f.write(iframe_html)
                            
                            logger.info(f"Saved iframe {i+1} HTML: {iframe_path}")
                    except Exception as iframe_err:
                        logger.warning(f"Error saving iframe {i+1} HTML: {str(iframe_err)}")
            except Exception as iframes_err:
                logger.warning(f"Error finding/processing iframes: {str(iframes_err)}")
    
    except Exception as e:
        logger.error(f"Error saving HTML for debugging: {str(e)}")

async def collect_diagnostic_info(page, error=None):
    """
    브라우저와 페이지의 진단 정보 수집
    
    Args:
        page: Playwright Page instance
        error: 발생한 에러 (있는 경우)
        
    Returns:
        dict: 진단 정보를 담은 딕셔너리
    """
    try:
        info = {
            'timestamp': datetime.now().isoformat(),
            'error': str(error) if error else None
        }
        
        # 기본 정보
        try:
            info['current_url'] = page.url
            info['title'] = await page.title()
            info['window_handles'] = len(page.context.pages)
        except:
            pass
        
        # JavaScript 실행 상태
        try:
            js_info = await page.evaluate("""
                () => ({
                    readyState: document.readyState,
                    documentURI: document.documentURI,
                    referrer: document.referrer,
                    cookie: document.cookie ? 'exists' : 'empty'
                })
            """)
            info['js_info'] = js_info
        except:
            info['js_info'] = 'Could not execute JavaScript'
        
        # DOM 정보
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
        iframes = await page.query_selector_all("iframe")
        for i, iframe in enumerate(iframes):
            try:
                iframe_info.append({
                    'index': i,
                    'id': await iframe.get_attribute('id'),
                    'name': await iframe.get_attribute('name'),
                    'src': await iframe.get_attribute('src'),
                    'width': await iframe.get_attribute('width'),
                    'height': await iframe.get_attribute('height'),
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
                page_source = await page.content()
                with open(source_path, 'w', encoding='utf-8') as f:
                    f.write(page_source)
                info['source_path'] = source_path
            except Exception as src_err:
                info['source_error'] = str(src_err)
                
            # 스택 트레이스 저장
            info['traceback'] = traceback.format_exc()
        
        return info
        
    except Exception as diag_err:
        logger.error(f"진단 정보 수집 중 오류: {str(diag_err)}")
        return {
            'timestamp': datetime.now().isoformat(),
            'error': str(error) if error else None,
            'diagnostic_error': str(diag_err)
        }

def setup_enhanced_logging():
    """GitHub Actions 환경에 최적화된 로깅 설정"""
    # 기존 로거 가져오기
    logger = logging.getLogger('msit_monitor')
    
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
    
    # 로거 반환
    return logger


#=====================================================================================
# Part 2. 페이지 탐색 함수들
#=====================================================================================

async def navigate_to_specific_page(page, target_page):
    """
    특정 페이지로 직접 이동하는 함수 (JavaScript 사용)
    
    Args:
        page: Playwright Page instance
        target_page: 이동하려는 페이지 번호
        
    Returns:
        bool: 페이지 이동 성공 여부
    """
    try:
        # 현재 페이지 번호 확인
        current_page = await get_current_page(page)
        
        if current_page == target_page:
            logger.info(f"이미 목표 페이지 {target_page}에 있습니다.")
            return True
            
        logger.info(f"페이지 {target_page}로 이동 시도 (현재: {current_page})")
        
        # JavaScript로 직접 페이지 이동
        try:
            await page.evaluate(f"fn_selectPage({target_page});")
            await page.wait_for_timeout(2000)  # 페이지 로드 대기
            
            # 페이지 변경 확인
            new_page = await get_current_page(page)
            if new_page == target_page:
                logger.info(f"페이지 {target_page}로 이동 성공")
                return True
            else:
                logger.warning(f"JavaScript 페이지 이동 실패 (현재: {new_page})")
        except Exception as js_err:
            logger.warning(f"JavaScript 페이지 이동 중 오류: {str(js_err)}")
        
        # JavaScript 실패 시 페이지네이션 링크 클릭 시도
        try:
            page_nav = await page.wait_for_selector("#pageNavi", timeout=5000)
            
            # 목표 페이지에 대한 직접 링크 찾기
            page_links = await page_nav.query_selector_all("a.page-link")
            
            for link in page_links:
                try:
                    link_text = await link.text_content()
                    if link_text and int(link_text.strip()) == target_page:
                        await link.click()
                        await page.wait_for_timeout(2000)
                        
                        # 페이지 변경 확인
                        new_page = await get_current_page(page)
                        if new_page == target_page:
                            logger.info(f"링크 클릭으로 페이지 {target_page} 이동 성공")
                            return True
                        break
                except ValueError:
                    # 숫자가 아닌 링크 (이전/다음 등) 무시
                    continue
        except Exception as nav_err:
            logger.warning(f"페이지네이션 탐색 중 오류: {str(nav_err)}")
        
        # 직접 링크가 없는 경우 이전/다음 버튼으로 이동
        if target_page > current_page:
            # 다음 페이지로 이동
            next_link = await page.query_selector("a.next, a.page-navi.next")
            if next_link:
                logger.info("다음 페이지 링크 클릭")
                await next_link.click()
                await wait_for_page_change(page, current_page)
                
                # 재귀적으로 다시 시도
                return await navigate_to_specific_page(page, target_page)
        else:
            # 이전 페이지로 이동
            prev_link = await page.query_selector("a.prev, a.page-navi.prev")
            if prev_link:
                logger.info("이전 페이지 링크 클릭")
                await prev_link.click()
                await wait_for_page_change(page, current_page)
                
                # 재귀적으로 다시 시도
                return await navigate_to_specific_page(page, target_page)
        
        # 직접 이동이 불가능한 경우 순차적으로 이동
        logger.warning(f"페이지 {target_page}에 대한 직접 링크를 찾을 수 없음, 순차적 이동 시도")
        
        # 방향 결정 (앞으로 또는 뒤로)
        if target_page > current_page:
            step = 1
        else:
            step = -1
        
        # 순차적으로 이동
        current = current_page
        while current != target_page:
            next_page = current + step
            
            # 다음/이전 페이지로 이동
            success = await go_to_adjacent_page(page, next_page)
            if not success:
                logger.error(f"페이지 {current}에서 {next_page}로 이동 실패")
                return False
            
            current = next_page
            logger.info(f"현재 페이지: {current}")
            
            # 과도한 요청 방지
            await page.wait_for_timeout(2000)
        
        return current == target_page
        
    except Exception as e:
        logger.error(f"페이지 {target_page}로 이동 중 오류: {str(e)}")
        return False

async def get_current_page(page):
    """
    현재 페이지 번호를 가져오는 함수
    
    Args:
        page: Playwright Page instance
        
    Returns:
        int: 현재 페이지 번호 (기본값 1)
    """
    try:
        # 현재 활성화된 페이지 링크 찾기
        page_nav = await page.wait_for_selector("#pageNavi", timeout=5000)
        
        # 여러 선택자 시도
        selectors = [
            "a.page-link[aria-current='page']",  # aria-current 속성이 있는 경우
            "a.on[href*='pageIndex']",            # on 클래스가 있는 경우
            "strong.on",                          # strong 태그인 경우
            ".pagination .active a"               # Bootstrap 스타일
        ]
        
        for selector in selectors:
            try:
                current_page_elem = await page_nav.query_selector(selector)
                if current_page_elem:
                    text = await current_page_elem.text_content()
                    current_page = int(text.strip())
                    logger.debug(f"현재 페이지: {current_page} (선택자: {selector})")
                    return current_page
            except (ValueError, AttributeError):
                continue
        
        # 현재 페이지를 찾을 수 없는 경우 URL에서 확인
        current_url = page.url
        match = re.search(r'pageIndex=(\d+)', current_url)
        if match:
            return int(match.group(1))
        
        # 기본값 반환
        logger.warning("현재 페이지 번호를 찾을 수 없음, 기본값 1 반환")
        return 1
        
    except Exception as e:
        logger.error(f"현재 페이지 번호 확인 중 오류: {str(e)}")
        return 1

async def wait_for_page_change(page, old_page, timeout=10000):
    """
    페이지가 변경될 때까지 대기하는 함수
    
    Args:
        page: Playwright Page instance
        old_page: 이전 페이지 번호
        timeout: 최대 대기 시간 (밀리초)
        
    Returns:
        bool: 페이지 변경 성공 여부
    """
    try:
        # 페이지 변경 대기
        start_time = time.time()
        while (time.time() - start_time) * 1000 < timeout:
            current = await get_current_page(page)
            if current != old_page:
                # 추가로 board_list가 로드될 때까지 대기
                await page.wait_for_selector(".board_list", timeout=timeout)
                return True
            await page.wait_for_timeout(500)
        
        logger.warning(f"페이지 변경 대기 시간 초과 ({timeout/1000}초)")
        return False
        
    except Exception as e:
        logger.error(f"페이지 변경 대기 중 오류: {str(e)}")
        return False

async def go_to_adjacent_page(page, page_num):
    """
    인접한 페이지로 이동하는 함수
    
    Args:
        page: Playwright Page instance
        page_num: 이동하려는 페이지 번호
        
    Returns:
        bool: 페이지 이동 성공 여부
    """
    try:
        current_page = await get_current_page(page)
        
        # 현재 페이지와 같으면 이동 필요 없음
        if current_page == page_num:
            return True
            
        # 인접한 페이지가 아니면 오류
        if abs(current_page - page_num) != 1:
            logger.error(f"인접한 페이지가 아닙니다: {current_page} → {page_num}")
            return False
        
        # 페이지네이션 영역 찾기
        page_nav = await page.wait_for_selector("#pageNavi", timeout=10000)
        
        # 목표 페이지 링크 찾기
        page_links = await page_nav.query_selector_all("a.page-link")
        
        for link in page_links:
            try:
                link_text = await link.text_content()
                if link_text and int(link_text.strip()) == page_num:
                    # 클릭
                    await link.click()
                    
                    # 페이지 변경 대기
                    return await wait_for_page_change(page, current_page)
            except ValueError:
                continue
        
        # 다음/이전 버튼으로 이동
        if page_num > current_page:
            next_buttons = await page_nav.query_selector_all("a.next, a.page-navi.next")
            if next_buttons:
                await next_buttons[0].click()
                return await wait_for_page_change(page, current_page)
        else:
            prev_buttons = await page_nav.query_selector_all("a.prev, a.page-navi.prev")
            if prev_buttons:
                await prev_buttons[0].click()
                return await wait_for_page_change(page, current_page)
        
        logger.error(f"페이지 {page_num}으로 이동할 수 있는 링크를 찾을 수 없습니다.")
        return False
        
    except Exception as e:
        logger.error(f"인접 페이지 {page_num}으로 이동 중 오류: {str(e)}")
        return False

async def has_next_page(page):
    """다음 페이지가 있는지 확인"""
    try:
        # 현재 페이지 번호 찾기
        current_page = await get_current_page(page)
        
        # 다음 페이지 링크 확인
        next_page_selectors = [
            f"a.page-link[href*='pageIndex={current_page + 1}']",
            f"a[href*='pageIndex={current_page + 1}']",
            ".pagination a.next:not([disabled])",
            "a.btn_next:not([disabled])"
        ]
        
        for selector in next_page_selectors:
            next_link = await page.query_selector(selector)
            if next_link:
                # 링크가 활성화되어 있는지 확인
                is_disabled = await next_link.get_attribute('disabled')
                if not is_disabled:
                    return True
        
        return False
        
    except Exception as e:
        logger.error(f"다음 페이지 확인 중 오류: {str(e)}")
        return False



#=====================================================================================
# Part 3. 날짜 및 게시물 파싱 함수들
#=====================================================================================

def parse_date_with_new_format(date_str):
    """
    다양한 날짜 형식을 파싱하는 함수 (Jun 13, 2025 형식 포함)
    
    Args:
        date_str: 날짜 문자열
        
    Returns:
        datetime.date 객체 또는 None
    """
    if not date_str:
        return None
    
    try:
        # 날짜 문자열 정규화
        date_str = date_str.replace(',', ' ').strip()
        date_str = ' '.join(date_str.split())  # 다중 공백 제거
        
        # 지원하는 날짜 형식들
        date_formats = [
            '%Y. %m. %d',      # "2025. 01. 15"
            '%Y-%m-%d',        # "2025-01-15"
            '%Y/%m/%d',        # "2025/01/15"
            '%Y.%m.%d',        # "2025.01.15"
            '%Y년 %m월 %d일',   # "2025년 1월 15일"
            '%b %d %Y',        # "Jan 15 2025" (새로운 형식)
            '%B %d %Y',        # "January 15 2025"
            '%d %b %Y',        # "15 Jan 2025"
            '%d %B %Y',        # "15 January 2025"
        ]
        
        # 표준 형식으로 시도
        for date_format in date_formats:
            try:
                return datetime.strptime(date_str, date_format).date()
            except ValueError:
                continue
        
        # 정규식으로 시도
        # 숫자 기반 날짜 (YYYY-MM-DD 변형)
        match = re.search(r'(\d{4})[.\-\s/]+(\d{1,2})[.\-\s/]+(\d{1,2})', date_str)
        if match:
            year, month, day = map(int, match.groups())
            try:
                return datetime(year, month, day).date()
            except ValueError:
                pass
        
        # 영문 월 이름 매핑
        month_abbr = {
            'Jan': 1, 'Feb': 2, 'Mar': 3, 'Apr': 4,
            'May': 5, 'Jun': 6, 'Jul': 7, 'Aug': 8,
            'Sep': 9, 'Oct': 10, 'Nov': 11, 'Dec': 12
        }
        
        month_full = {
            'January': 1, 'February': 2, 'March': 3, 'April': 4,
            'May': 5, 'June': 6, 'July': 7, 'August': 8,
            'September': 9, 'October': 10, 'November': 11, 'December': 12
        }
        
        # 영문 월 이름 포함 (Jun 13, 2025 형식)
        match = re.search(r'(\w+)\s+(\d{1,2})\s+(\d{4})', date_str)
        if match:
            month_str, day, year = match.groups()
            month = month_abbr.get(month_str) or month_full.get(month_str)
            if month:
                try:
                    return datetime(int(year), month, int(day)).date()
                except ValueError:
                    pass
        
        # 일-월-년 형식 (15 Jun 2025)
        match = re.search(r'(\d{1,2})\s+(\w+)\s+(\d{4})', date_str)
        if match:
            day, month_str, year = match.groups()
            month = month_abbr.get(month_str) or month_full.get(month_str)
            if month:
                try:
                    return datetime(int(year), month, int(day)).date()
                except ValueError:
                    pass
        
        logger.warning(f"알 수 없는 날짜 형식: {date_str}")
        return None
        
    except Exception as e:
        logger.error(f"날짜 파싱 오류: {str(e)}")
        return None

def is_telecom_stats_post(title):
    """게시물이 통신 통계 보고서인지 확인"""
    if not title:
        return False
        
    # "(YYYY년 MM월말 기준)" 형식의 날짜 패턴 확인 (더 유연한 정규식)
    date_pattern = r'\((\d{4})년\s*(\d{1,2})월말\s*기준\)'
    has_date_pattern = re.search(date_pattern, title) is not None
    
    if not has_date_pattern:
        return False
    
    # 제목에 보고서 유형이 포함되어 있는지 확인
    contains_report_type = any(report_type in title for report_type in CONFIG['report_types'])
    
    return contains_report_type

def extract_post_id(item):
    """BeautifulSoup 항목에서 게시물 ID 추출"""
    try:
        # 여러 가능한 선택자 시도
        link_elem = item.find('a')
        if not link_elem:
            return None
            
        # onclick 속성에서 ID 추출 시도
        onclick_attr = link_elem.get('onclick', '')
        match = re.search(r"fn_detail\((\d+)\)", onclick_attr)
        if match:
            return match.group(1)
            
        return None
    except Exception as e:
        logger.error(f"게시물 ID 추출 중 에러: {str(e)}")
        return None

def get_post_url(post_id):
    """게시물 ID로부터 URL 생성"""
    if not post_id:
        return None
    return f"https://www.msit.go.kr/bbs/view.do?sCode=user&mId=99&mPid=74&nttSeqNo={post_id}"

def is_in_date_range(date_str, days=4):
    """게시물 날짜가 지정된 범위 내에 있는지 확인"""
    try:
        # 날짜 문자열 정규화
        date_str = date_str.replace(',', ' ').strip()
        
        # 날짜 파싱 (새로운 형식 포함)
        post_date = parse_date_with_new_format(date_str)
        
        if not post_date:
            logger.warning(f"알 수 없는 날짜 형식: {date_str}")
            return True  # 알 수 없는 경우 포함으로 처리
        
        # 날짜 범위 계산 (한국 시간대)
        korea_tz = datetime.now() + timedelta(hours=9)  # UTC에서 KST로
        days_ago = (korea_tz - timedelta(days=days)).date()
        
        logger.info(f"게시물 날짜 확인: {post_date} vs {days_ago} ({days}일 전, 한국 시간 기준)")
        return post_date >= days_ago
        
    except Exception as e:
        logger.error(f"날짜 파싱 에러: {str(e)}")
        return True  # 오류 발생 시 기본적으로 포함

def is_within_date_range(post_date, start_date=None, end_date=None, days_range=None):
    """
    게시물이 날짜 범위 내에 있는지 확인
    
    Args:
        post_date: 게시물 날짜 (date 객체)
        start_date: 시작 날짜 (date 객체)
        end_date: 종료 날짜 (date 객체)
        days_range: 최근 며칠 이내인지 확인할 일수
        
    Returns:
        bool: 범위 내에 있으면 True
    """
    if not post_date:
        return True  # 날짜를 파싱할 수 없는 경우 포함
        
    # 특정 날짜 범위가 지정된 경우
    if start_date and end_date:
        return start_date <= post_date <= end_date
        
    # days_range가 지정된 경우
    if days_range:
        korea_tz = datetime.now() + timedelta(hours=9)
        days_ago = (korea_tz - timedelta(days=days_range)).date()
        return post_date >= days_ago
        
    return True

def parse_page_content(driver, page_num=1, days_range=None, start_date=None, end_date=None, reverse_order=False):
    """
    페이지 내용을 파싱하는 통합 함수.
    날짜 범위 또는 days_range를 기반으로 게시물 필터링
    
    Args:
        driver: Selenium WebDriver 인스턴스
        page_num: 현재 페이지 번호
        days_range: 특정 일수 이내 게시물 필터링 (start_date, end_date가 없을 때 사용)
        start_date: 시작 날짜 문자열 (YYYY-MM-DD)
        end_date: 종료 날짜 문자열 (YYYY-MM-DD)
        reverse_order: 역순 탐색 여부 (True: 5→1 페이지 순서로 탐색)
        
    Returns:
        Tuple[List, List, Dict]: 모든 게시물, 통신 통계 게시물, 파싱 결과 정보
    """
    all_posts = []
    telecom_stats_posts = []
    # 결과 정보를 담을 딕셔너리 (단순 불리언 값 대신 상세 정보 제공)
    result_info = {
        'current_page_complete': True,        # 현재 페이지 파싱 완료 여부
        'skip_remaining_in_page': False,      # 현재 페이지의 나머지 게시물 건너뛰기 여부
        'continue_to_next_page': True,        # 다음 페이지로 계속 진행 여부
        'oldest_date_found': None,            # 발견된 가장 오래된 날짜
        'newest_date_found': None,            # 발견된 가장 최근 날짜
        'total_posts': 0,                     # 현재 페이지에서 발견된 총 게시물 수
        'filtered_posts': 0,                  # 필터 조건에 맞는 게시물 수
        'messages': []                        # 상세 메시지 목록
    }
    
    try:
        # 날짜 객체로 변환
        start_date_obj = None
        end_date_obj = None
        
        if start_date:
            try:
                start_date_obj = datetime.strptime(start_date, '%Y-%m-%d').date()
            except ValueError:
                logger.warning(f"잘못된 시작 날짜 형식: {start_date}, 날짜 필터링 무시됨")
                result_info['messages'].append(f"잘못된 시작 날짜 형식: {start_date}")
                
        if end_date:
            try:
                end_date_obj = datetime.strptime(end_date, '%Y-%m-%d').date()
            except ValueError:
                logger.warning(f"잘못된 종료 날짜 형식: {end_date}, 날짜 필터링 무시됨")
                result_info['messages'].append(f"잘못된 종료 날짜 형식: {end_date}")
        
        # days_range를 사용하는 경우 start_date_obj 계산
        if days_range and not start_date_obj:
            # 한국 시간대 고려 (UTC+9)
            korea_tz = datetime.now() + timedelta(hours=9)
            start_date_obj = (korea_tz - timedelta(days=days_range)).date()
            logger.info(f"days_range({days_range})로 계산된 시작 날짜: {start_date_obj}")
            result_info['messages'].append(f"days_range({days_range})로 계산된 시작 날짜: {start_date_obj}")
        
        # 다양한 로딩 지표로 페이지 로드 대기
        selectors = [
            (By.CLASS_NAME, "board_list"),
            (By.ID, "sub_content"),
            (By.CSS_SELECTOR, "ul.board_list")
        ]
        
        element_found = False
        for by, value in selectors:
            try:
                WebDriverWait(driver, 20).until(
                    EC.presence_of_element_located((by, value))
                )
                element_found = True
                logger.info(f"페이지 요소 발견: {value}")
                break
            except TimeoutException:
                continue
        
        if not element_found:
            logger.error("페이지 로드 실패: 게시판 목록을 찾을 수 없음")
            result_info['current_page_complete'] = False
            result_info['messages'].append("페이지 로드 실패: 게시판 목록을 찾을 수 없음")
            return [], [], result_info
        
        # HTML 파싱
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        
        # 게시물 목록 찾기 (여러 가능한 선택자 시도)
        board_list = None
        list_selectors = [
            ('ul', {'class': 'board_list'}),
            ('div', {'class': 'board_list'}),
            ('table', {'class': 'board_list'}),
            ('ul', {'id': 'board_list'})
        ]
        
        for tag, attrs in list_selectors:
            board_list = soup.find(tag, attrs)
            if board_list:
                break
        
        if not board_list:
            logger.error("게시물 목록을 찾을 수 없습니다")
            result_info['current_page_complete'] = False
            result_info['messages'].append("게시물 목록을 찾을 수 없습니다")
            return [], [], result_info
        
        # 게시물 항목 찾기
        items = board_list.find_all('li')
        if not items:
            # 테이블 형식인 경우
            items = board_list.find_all('tr')
        
        result_info['total_posts'] = len(items)
        logger.info(f"페이지 {page_num}에서 {len(items)}개 게시물 발견")
        
        # 각 게시물 파싱
        for idx, item in enumerate(items):
            try:
                # 날짜 추출 (여러 선택자 시도)
                date_selectors = [
                    "dd.date",
                    "dd[id*='td_CREATION_DATE']",
                    ".date",
                    "span.date",
                    "td.date"
                ]
                
                date_elem = None
                for selector in date_selectors:
                    date_elem = item.select_one(selector)
                    if date_elem:
                        break
                        
                if not date_elem:
                    # 날짜가 없는 경우 헤더 행일 가능성이 있으므로 건너뛰기
                    continue
                    
                date_str = date_elem.text.strip()
                post_date = parse_date_with_new_format(date_str)
                
                # 날짜 범위 추적
                if post_date:
                    if result_info['oldest_date_found'] is None or post_date < result_info['oldest_date_found']:
                        result_info['oldest_date_found'] = post_date
                    if result_info['newest_date_found'] is None or post_date > result_info['newest_date_found']:
                        result_info['newest_date_found'] = post_date
                
                # 날짜 범위 확인
                if not is_within_date_range(post_date, start_date_obj, end_date_obj, days_range):
                    # 날짜가 범위를 벗어난 경우
                    if post_date and start_date_obj and post_date < start_date_obj:
                        logger.info(f"날짜 범위 벗어남: {post_date} < {start_date_obj}")
                        result_info['skip_remaining_in_page'] = True
                        
                        # 역순 탐색이 아닌 경우에만 다음 페이지 탐색 중단
                        if not reverse_order:
                            result_info['continue_to_next_page'] = False
                        break
                    continue
                
                # 제목 추출 (여러 선택자 시도)
                title_selectors = [
                    "dt a",
                    "dd.tit a",
                    ".tit a",
                    "a.title",
                    "td.title a"
                ]
                
                title_elem = None
                for selector in title_selectors:
                    title_elem = item.select_one(selector)
                    if title_elem:
                        break
                        
                if not title_elem:
                    continue
                    
                title = title_elem.text.strip()
                post_id = extract_post_id(item)
                post_url = get_post_url(post_id)
                
                # 부서 정보 추출 (여러 선택자 시도)
                dept_selectors = [
                    "dd[id*='td_CHRG_DEPT_NM']",
                    ".dept",
                    "td.dept",
                    ".department"
                ]
                
                dept_elem = None
                for selector in dept_selectors:
                    dept_elem = item.select_one(selector)
                    if dept_elem:
                        break
                        
                dept_text = dept_elem.text.strip() if dept_elem else "부서 정보 없음"
                
                # 게시물 정보 딕셔너리 생성
                post_info = {
                    'title': title,
                    'date': date_str,
                    'post_date': post_date,  # 파싱된 날짜 객체 추가
                    'department': dept_text,
                    'url': post_url,
                    'post_id': post_id
                }
                
                # 모든 게시물 리스트에 추가
                all_posts.append(post_info)
                result_info['filtered_posts'] += 1
                
                # 통신 통계 게시물인지 확인
                if is_telecom_stats_post(title):
                    logger.info(f"통신 통계 게시물 발견: {title}")
                    telecom_stats_posts.append(post_info)
                    
            except Exception as e:
                logger.error(f"게시물 파싱 중 에러: {str(e)}")
                continue
        
        return all_posts, telecom_stats_posts, result_info
        
    except Exception as e:
        logger.error(f"페이지 파싱 중 에러: {str(e)}")
        result_info['current_page_complete'] = False
        result_info['messages'].append(f"페이지 파싱 중 에러: {str(e)}")
        return [], [], result_info



#=====================================================================================
# Part 4. 게시물 접근 및 데이터 추출 함수들
#=====================================================================================


async def find_view_link_params(page, post):
    """게시물에서 바로보기 링크 파라미터 찾기 (클릭 방식 우선, 직접 URL 접근 폴백 추가)"""
    if not post.get('post_id'):
        logger.error(f"게시물 접근 불가 {post['title']} - post_id 누락")
        return None
    
    logger.info(f"게시물 열기: {post['title']}")
    
    # 현재 URL 저장
    current_url = page.url
    
    # 게시물 목록 페이지로 돌아가기
    try:
        await page.goto(CONFIG['stats_url'])
        await page.wait_for_selector(".board_list", timeout=10000)
        await page.wait_for_timeout(2000)  # 추가 대기
    except Exception as e:
        logger.error(f"게시물 목록 페이지 접근 실패: {str(e)}")
        # 직접 URL 접근 폴백 시도
        return await direct_access_view_link_params(page, post)
    
    # 최대 재시도 횟수
    max_retries = 3
    retry_delay = 2000
    
    for attempt in range(max_retries):
        try:
            # 제목으로 게시물 링크 찾기 - 더 유연한 선택자 사용
            title_part = post['title'][:20]  # 맨 앞 20자만 사용
            
            # XPath와 CSS 선택자 모두 시도
            link_selectors = [
                f"//p[contains(@class, 'title') and contains(text(), '{title_part}')]",
                f"//a[contains(text(), '{title_part}')]",
                f"//div[contains(@class, 'toggle') and contains(., '{title_part}')]",
                f"p.title:has-text('{title_part}')",
                f"a:has-text('{title_part}')"
            ]
            
            post_link = None
            for selector in link_selectors:
                try:
                    if selector.startswith('//'):
                        # XPath
                        post_link = await page.locator(selector).first.element_handle(timeout=5000)
                    else:
                        # CSS selector
                        post_link = await page.query_selector(selector)
                    
                    if post_link:
                        logger.info(f"게시물 링크 발견 (선택자: {selector})")
                        break
                except Exception as find_err:
                    logger.warning(f"선택자로 게시물 찾기 실패: {selector}")
                    continue
            
            if not post_link:
                logger.warning(f"게시물 링크를 찾을 수 없음: {post['title']}")
                
                if attempt < max_retries - 1:
                    logger.info(f"재시도 중... ({attempt+1}/{max_retries})")
                    await page.wait_for_timeout(retry_delay)
                    continue
                else:
                    # 직접 URL 접근 방식으로 대체
                    logger.info("클릭 방식 실패, 직접 URL 접근 방식으로 대체")
                    return await direct_access_view_link_params(page, post)
            
            # 스크린샷 저장 (클릭 전)
            await take_screenshot(page, f"before_click_{post['post_id']}")
            
            # 링크 클릭하여 상세 페이지로 이동
            logger.info(f"게시물 링크 클릭 시도: {post['title']}")
            
            # 클릭 시도
            await post_link.click()
            logger.info("클릭 실행")
            
        except Exception as click_err:
            logger.error(f"게시물 링크 클릭 중 오류: {str(click_err)}")
            if attempt < max_retries - 1:
                continue
            else:
                # 직접 URL 접근으로 폴백
                return await direct_access_view_link_params(page, post)
        
        # 페이지 로드 대기
        try:
            # URL 변경 대기
            await page.wait_for_function(
                f"() => window.location.href !== '{CONFIG['stats_url']}'",
                timeout=15000
            )
            logger.info(f"페이지 URL 변경 감지됨: {page.url}")
            await page.wait_for_timeout(3000)  # 추가 대기
        except PlaywrightTimeoutError:
            logger.warning("URL 변경 감지 실패")
            # 실패 시 직접 URL로 접근 시도
            if attempt < max_retries - 1:
                continue
            else:
                return await direct_access_view_link_params(page, post)
        
        # 상세 페이지 대기 - 다양한 요소를 확인하여 페이지 로드 감지
        wait_elements = [
            ".view_head",
            ".view_cont",
            ".bbs_wrap .view",
            "div[class*='view']",
            ".board_view",
            ".board_detail",
            ".board_content"
        ]
        
        element_found = False
        for selector in wait_elements:
            try:
                await page.wait_for_selector(selector, timeout=15000)
                logger.info(f"상세 페이지 로드 완료: {selector} 요소 발견")
                element_found = True
                break
            except PlaywrightTimeoutError:
                continue
        
        if not element_found:
            logger.warning("상세 페이지 로드 실패")
            if attempt < max_retries - 1:
                continue
            else:
                # AJAX 방식 시도
                logger.info("AJAX 방식으로 접근 시도")
                ajax_result = await try_ajax_access(page, post)
                if ajax_result:
                    return ajax_result
                
                # 직접 URL 접근 방식으로 대체
                return await direct_access_view_link_params(page, post)
        
        # 스크린샷 저장
        await take_screenshot(page, f"post_view_clicked_{post['post_id']}")
        
        # 바로보기 링크 찾기 (확장된 선택자)
        try:
            # 여러 선택자로 바로보기 링크 찾기 - 확장된 목록
            view_links = []
            
            # 다양한 선택자 시도
            view_link_selectors = [
                "a.view[title='새창 열림']",
                "a[onclick*='getExtension_path']",
                "a:has-text('바로보기')",
                "a.attach-file",
                "a.file_link",
                "a.download",
                "a[title*='바로보기']",
                "a[title*='View']",
                "a[title*='열기']"
            ]
            
            for selector in view_link_selectors:
                found_links = await page.query_selector_all(selector)
                view_links.extend(found_links)
            
            # 중복 제거
            view_links = list(set(view_links))
            logger.info(f"바로보기 링크 {len(view_links)}개 발견")
            
            if view_links:
                # onclick 속성에서 파라미터 추출
                for link in view_links:
                    onclick = await link.get_attribute('onclick')
                    if onclick:
                        # getExtension_path 함수 파라미터 추출
                        match = re.search(r'getExtension_path\(\s*["\'](\d+)["\']\s*,\s*["\'](\d+)["\']\s*\)', onclick)
                        if match:
                            atch_file_no = match.group(1)
                            file_ord = match.group(2)
                            
                            logger.info(f"바로보기 파라미터 발견: atch_file_no={atch_file_no}, file_ord={file_ord}")
                            
                            return {
                                'atch_file_no': atch_file_no,
                                'file_ord': file_ord,
                                'post_info': post
                            }
                
                # onclick 속성이 없는 경우 href 확인
                for link in view_links:
                    href = await link.get_attribute('href')
                    if href and 'docViewer' in href:
                        # URL에서 파라미터 추출
                        match = re.search(r'atch_file_no=(\d+).*file_ord=(\d+)', href)
                        if match:
                            return {
                                'atch_file_no': match.group(1),
                                'file_ord': match.group(2),
                                'post_info': post
                            }
            
            # 바로보기 링크가 없는 경우 AJAX로 시도
            logger.warning("바로보기 링크를 찾을 수 없음, AJAX 방식 시도")
            ajax_result = await try_ajax_access(page, post)
            if ajax_result:
                return ajax_result
                
            # 최종적으로 직접 URL 접근 시도
            return await direct_access_view_link_params(page, post)
            
        except Exception as e:
            logger.error(f"바로보기 링크 파라미터 추출 중 오류: {str(e)}")
            
            # 오류 발생 시에도 날짜 정보 추출 시도
            date_match = re.search(r'\((\d{4})년\s*(\d{1,2})월말\s*기준\)', post['title'])
            if date_match:
                year = int(date_match.group(1))
                month = int(date_match.group(2))
                
                return {
                    'content': f"접근 오류: {str(e)}",
                    'date': {'year': year, 'month': month},
                    'post_info': post
                }
            
            return None
    
    return None

async def direct_access_view_link_params(page, post):
    """직접 URL로 게시물 바로보기 링크 파라미터 접근"""
    try:
        if not post.get('post_id'):
            logger.error(f"직접 URL 접근 불가 {post['title']} - post_id 누락")
            return None
            
        logger.info(f"게시물 직접 URL 접근 시도: {post['title']}")
        
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
            logger.info(f"게시물 상세 페이지 로드 완료: {post['title']}")
        except PlaywrightTimeoutError:
            logger.warning(f"게시물 상세 페이지 로드 시간 초과: {post['title']}")
        
        # 바로보기 링크 찾기 (확장된 선택자)
        try:
            # 여러 선택자로 바로보기 링크 찾기
            view_links = []
            
            # 1. 일반적인 '바로보기' 링크
            view_links = await page.query_selector_all("a.view[title='새창 열림']")
            
            # 2. onclick 속성으로 찾기
            if not view_links:
                all_links = await page.query_selector_all("a")
                for link in all_links:
                    onclick = await link.get_attribute('onclick')
                    if onclick and 'getExtension_path' in onclick:
                        view_links.append(link)
            
            # 3. 텍스트로 찾기
            if not view_links:
                view_links = await page.query_selector_all("a:has-text('바로보기'), a:has-text('View'), a:has-text('열기')")
            
            if view_links:
                for link in view_links:
                    onclick = await link.get_attribute('onclick')
                    if onclick:
                        match = re.search(r'getExtension_path\(\s*["\'](\d+)["\']\s*,\s*["\'](\d+)["\']\s*\)', onclick)
                        if match:
                            return {
                                'atch_file_no': match.group(1),
                                'file_ord': match.group(2),
                                'post_info': post
                            }
            
            # onclick이 없는 경우 href 확인
            all_links = await page.query_selector_all("a")
            for link in all_links:
                href = await link.get_attribute('href')
                if href and ('docViewer' in href or 'documentView' in href):
                    match = re.search(r'atch_file_no=(\d+).*file_ord=(\d+)', href)
                    if match:
                        return {
                            'atch_file_no': match.group(1),
                            'file_ord': match.group(2),
                            'post_info': post
                        }
            
            logger.warning(f"바로보기 링크를 찾을 수 없음: {post['title']}")
            return None
            
        except Exception as e:
            logger.error(f"직접 URL 접근 중 바로보기 링크 파라미터 추출 오류: {str(e)}")
            return None
        
    except Exception as e:
        logger.error(f"직접 URL 접근 중 오류: {str(e)}")
        return None

async def try_ajax_access(page, post):
    """AJAX 방식으로 게시물 접근 시도"""
    try:
        logger.info(f"AJAX 방식으로 게시물 접근 시도: {post['title']}")
        
        # JavaScript로 fn_detail 함수 직접 호출
        if post.get('post_id'):
            try:
                await page.evaluate(f"fn_detail({post['post_id']});")
                await page.wait_for_timeout(3000)
                
                # 페이지 변경 확인
                if page.url != CONFIG['stats_url']:
                    logger.info("AJAX를 통한 페이지 이동 성공")
                    
                    # 바로보기 링크 찾기
                    view_links = await page.query_selector_all("a[onclick*='getExtension_path']")
                    for link in view_links:
                        onclick = await link.get_attribute('onclick')
                        if onclick:
                            match = re.search(r'getExtension_path\(\s*["\'](\d+)["\']\s*,\s*["\'](\d+)["\']\s*\)', onclick)
                            if match:
                                return {
                                    'atch_file_no': match.group(1),
                                    'file_ord': match.group(2),
                                    'post_info': post
                                }
            except Exception as js_err:
                logger.warning(f"AJAX fn_detail 호출 실패: {str(js_err)}")
        
        return None
        
    except Exception as e:
        logger.error(f"AJAX 접근 중 오류: {str(e)}")
        return None

def extract_year_month_from_title(title):
    """제목에서 연도와 월만 추출 (더 유연한 검색용)"""
    match = re.search(r'\((\d{4})년\s*(\d{1,2})월말', title)
    if match:
        year = match.group(1)
        month = match.group(2)
        return f"({year}년 {month}월말"
    return title[:15]  # 일치하는 패턴이 없으면 앞부분만 반환

async def access_iframe_content(page, file_params):
    """iframe 내의 문서 콘텐츠에 접근하여 HTML 반환"""
    if not file_params.get('atch_file_no') or not file_params.get('file_ord'):
        logger.error("파일 파라미터가 누락되었습니다")
        return None
    
    atch_file_no = file_params['atch_file_no']
    file_ord = file_params['file_ord']
    
    # 바로보기 URL 구성
    view_url = f"https://www.msit.go.kr/bbs/documentView.do?atchFileNo={atch_file_no}&fileOrdr={file_ord}"
    logger.info(f"바로보기 URL: {view_url}")
    
    # 여러 번 재시도
    max_retries = int(os.environ.get('IFRAME_MAX_RETRIES', '3'))
    for attempt in range(max_retries):
        try:
            # 페이지 로드
            await page.goto(view_url)
            await page.wait_for_timeout(5000)  # 초기 대기
            
            # 현재 URL 확인
            current_url = page.url
            logger.info(f"현재 URL: {current_url}")
           
            # 스크린샷 저장
            await take_screenshot(page, f"iframe_view_{atch_file_no}_{file_ord}_attempt_{attempt}")
            
            # 현재 페이지 스크린샷 저장 (디버깅용)
            try:
                await page.screenshot(path=f"document_view_{atch_file_no}_{file_ord}.png")
                logger.info(f"문서 뷰어 스크린샷 저장: document_view_{atch_file_no}_{file_ord}.png")
            except Exception as ss_err:
                logger.warning(f"스크린샷 저장 중 오류: {str(ss_err)}")
            
            # 시스템 점검 페이지 감지
            page_content = await page.content()
            if "시스템 점검 안내" in page_content:
                if attempt < max_retries - 1:
                    logger.warning("시스템 점검 중입니다. 나중에 다시 시도합니다.")
                    await page.wait_for_timeout(5000)  # 더 오래 대기
                    continue
                else:
                    logger.warning("시스템 점검 중입니다. 문서를 열 수 없습니다.")
                    return None
            
            # iframe 찾기 및 전환
            iframes = await page.query_selector_all("iframe")
            logger.info(f"찾은 iframe 개수: {len(iframes)}")
            
            if iframes:
                for idx, iframe_elem in enumerate(iframes):
                    try:
                        # iframe 정보 출력
                        iframe_id = await iframe_elem.get_attribute('id')
                        iframe_name = await iframe_elem.get_attribute('name')
                        iframe_src = await iframe_elem.get_attribute('src')
                        logger.info(f"iframe {idx}: id={iframe_id}, name={iframe_name}, src={iframe_src}")
                        
                        # iframe 내용 가져오기
                        frame = await iframe_elem.content_frame()
                        if frame:
                            logger.info(f"iframe {idx}로 전환 성공")
                            
                            # iframe 내용 확인
                            iframe_html = await frame.content()
                            
                            # 유의미한 콘텐츠가 있는지 확인
                            if len(iframe_html) > 100 and ('table' in iframe_html.lower() or 'div' in iframe_html.lower()):
                                logger.info(f"iframe {idx}에서 유의미한 콘텐츠 발견")
                                
                                # 스크린샷 저장
                                await take_screenshot(page, f"iframe_content_{atch_file_no}_{file_ord}_frame_{idx}")
                                
                                # iframe 내용 반환
                                return iframe_html
                        
                    except Exception as iframe_err:
                        logger.error(f"iframe {idx} 처리 중 오류: {str(iframe_err)}")
            
            # iframe이 없거나 콘텐츠를 찾지 못한 경우
            # 메인 페이지 콘텐츠 확인
            page_html = await page.content()
            
            # viewer 또는 문서 관련 div 찾기
            soup = BeautifulSoup(page_html, 'html.parser')
            content_divs = soup.find_all('div', class_=re.compile(r'viewer|document|content'))
            
            if content_divs:
                logger.info(f"{len(content_divs)}개의 콘텐츠 div 발견")
                return page_html
            
            # 재시도
            if attempt < max_retries - 1:
                logger.warning(f"콘텐츠를 찾을 수 없음, 재시도 {attempt + 1}/{max_retries}")
                await page.wait_for_timeout(3000)
                continue
            
        except Exception as e:
            logger.error(f"iframe 콘텐츠 접근 중 오류 (시도 {attempt + 1}/{max_retries}): {str(e)}")
            if attempt < max_retries - 1:
                await page.wait_for_timeout(3000)
                continue
    
    logger.error("모든 시도 실패, iframe 콘텐츠를 가져올 수 없습니다")
    return None

async def extract_data_from_viewer(page):
    """
    문서 뷰어에서 데이터 추출 (Synap 뷰어 등)
    
    Args:
        page: Playwright Page instance
        
    Returns:
        dict: Dictionary of sheet names to pandas DataFrames, or None if extraction fails
    """
    all_sheets = {}
    
    try:
        # Wait for viewer to initialize
        await page.wait_for_timeout(5000)  # Initial wait for viewer to load
        
        # Take screenshot for debugging
        screenshot_path = f"document_viewer_{int(time.time())}.png"
        await page.screenshot(path=screenshot_path)
        
        # Look for sheet tabs - different viewers have different structures
        sheet_tabs = None
        sheet_selectors = [
            ".sheet-list__sheet-tab",  # Common in Synap
            ".tab-item",
            "ul.tabs > li",
            ".nav-tabs > li",
            "[role='tab']"
        ]
        
        for selector in sheet_selectors:
            try:
                tabs = await page.query_selector_all(selector)
                if tabs:
                    sheet_tabs = tabs
                    logger.info(f"Found {len(tabs)} sheet tabs with selector: {selector}")
                    break
            except:
                continue
        
        # Process multiple sheets if found
        if sheet_tabs and len(sheet_tabs) > 0:
            for i, tab in enumerate(sheet_tabs):
                tab_text = await tab.text_content()
                sheet_name = tab_text.strip() if tab_text else f"Sheet_{i+1}"
                logger.info(f"Processing sheet {i+1}/{len(sheet_tabs)}: {sheet_name}")
                
                # Click on tab if not the first one (first is usually selected by default)
                if i > 0:
                    try:
                        await tab.click()
                        await page.wait_for_timeout(3000)  # Wait for sheet to load
                    except Exception as click_err:
                        logger.warning(f"Could not click sheet tab: {str(click_err)}")
                
                # Extract data from current sheet
                sheet_data = await extract_sheet_data_from_viewer(page, sheet_name)
                if sheet_data is not None:
                    all_sheets[sheet_name] = sheet_data
        else:
            # Single sheet or no tabs visible
            logger.info("No sheet tabs found, attempting to extract single sheet data")
            single_sheet_data = await extract_sheet_data_from_viewer(page, "전체데이터")
            if single_sheet_data is not None:
                all_sheets["전체데이터"] = single_sheet_data
        
        # Save HTML for debugging
        await save_html_for_debugging(page, "viewer_content", include_iframe=True)
        
        return all_sheets if all_sheets else None
        
    except Exception as e:
        logger.error(f"Error extracting data from viewer: {str(e)}")
        return None

async def extract_sheet_data_from_viewer(page, sheet_name):
    """
    Extract data from a single sheet in the document viewer
    
    Args:
        page: Playwright Page instance
        sheet_name: Name of the current sheet
        
    Returns:
        pandas.DataFrame or None
    """
    try:
        # Wait a bit for content to stabilize
        await page.wait_for_timeout(2000)
        
        # Look for table elements - try multiple selectors
        table_selectors = [
            "table",
            ".sheet-area table",
            ".document-content table",
            "[role='table']",
            ".data-table"
        ]
        
        tables = []
        for selector in table_selectors:
            try:
                found_tables = await page.query_selector_all(selector)
                if found_tables:
                    tables.extend(found_tables)
                    logger.info(f"Found {len(found_tables)} tables with selector: {selector}")
            except:
                continue
        
        if not tables:
            logger.warning(f"No tables found in sheet: {sheet_name}")
            return None
        
        # Process the first visible table
        for table in tables:
            try:
                is_visible = await table.is_visible()
                if not is_visible:
                    continue
                
                # Extract table HTML
                table_html = await table.evaluate("element => element.outerHTML")
                
                # Parse with pandas
                dfs = pd.read_html(table_html)
                if dfs and not dfs[0].empty:
                    logger.info(f"Successfully extracted data from sheet {sheet_name}: {dfs[0].shape}")
                    return dfs[0]
                    
            except Exception as table_err:
                logger.warning(f"Error processing table: {str(table_err)}")
                continue
        
        return None
        
    except Exception as e:
        logger.error(f"Error extracting sheet data: {str(e)}")
        return None



#=====================================================================================
# Part 6. 데이터 처리 함수들
#=====================================================================================

# ===========================
# 데이터 추출 및 처리 함수들
# ===========================

def extract_tables_from_html(html_content):
    """HTML에서 테이블 데이터 추출 (향상된 버전)"""
    try:
        if not html_content:
            logger.warning("HTML 콘텐츠가 비어있음")
            return None
            
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # 디버깅: HTML 구조 확인
        logger.debug(f"HTML 길이: {len(html_content)}")
        
        # iframe 내용도 확인
        iframes = soup.find_all('iframe')
        if iframes:
            logger.info(f"{len(iframes)}개의 iframe 발견")
        
        # 다양한 테이블 컨테이너 찾기
        table_containers = []
        
        # 1. 직접 테이블 찾기
        tables = soup.find_all('table')
        table_containers.extend(tables)
        
        # 2. sheet_area 클래스 내의 테이블
        sheet_areas = soup.find_all('div', class_='sheet_area')
        for area in sheet_areas:
            area_tables = area.find_all('table')
            table_containers.extend(area_tables)
        
        # 3. 다른 가능한 컨테이너들
        content_selectors = [
            {'class': 'document-content'},
            {'class': 'viewer-content'},
            {'class': 'sheet-content'},
            {'id': 'content'}
        ]
        
        for selector in content_selectors:
            containers = soup.find_all('div', selector)
            for container in containers:
                container_tables = container.find_all('table')
                table_containers.extend(container_tables)
        
        # 중복 제거
        seen = set()
        unique_tables = []
        for table in table_containers:
            table_str = str(table)[:100]  # 처음 100자로 중복 확인
            if table_str not in seen:
                seen.add(table_str)
                unique_tables.append(table)
        
        logger.info(f"총 {len(unique_tables)}개의 고유한 테이블 발견")
        
        if not unique_tables:
            logger.warning("테이블을 찾을 수 없음")
            return None
        
        # 테이블 데이터 추출
        extracted_sheets = {}
        refined_sheets = {}
        
        for idx, table in enumerate(unique_tables):
            try:
                # pandas로 테이블 파싱
                df_list = pd.read_html(str(table))
                
                if not df_list:
                    continue
                    
                df = df_list[0]
                
                # 데이터프레임 검증 및 정제
                df = clean_dataframe(df)
                
                if not df.empty:
                    # 시트 이름 결정
                    sheet_name = determine_sheet_name(df, idx)
                    extracted_sheets[sheet_name] = df
                    
                    # 데이터 타입별로 분류
                    if is_summary_table(df):
                        refined_sheets[f"요약_{sheet_name}"] = df
                    else:
                        refined_sheets[sheet_name] = df
                    
                    logger.info(f"테이블 {idx + 1} 추출 성공: {sheet_name} ({df.shape[0]}행 x {df.shape[1]}열)")
                    
            except Exception as e:
                logger.error(f"테이블 {idx} 파싱 오류: {str(e)}")
                continue
        
        if not refined_sheets:
            # 데이터를 찾지 못한 경우 원본 HTML에서 다시 시도
            logger.warning("표준 방법으로 테이블을 찾지 못함, 대체 방법 시도")
            
            # JavaScript로 렌더링된 테이블 찾기
            script_tables = soup.find_all('script')
            for script in script_tables:
                if script.string and 'table' in script.string.lower():
                    logger.info("JavaScript에서 테이블 데이터 발견")
                    # 여기서 추가적인 파싱 로직 구현 가능
        
        logger.info(f"총 {len(refined_sheets)}개 시트 추출 완료")
        return refined_sheets
        
    except Exception as e:
        logger.error(f"HTML에서 데이터 추출 중 오류: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return None

def clean_dataframe(df):
    """데이터프레임 정제 및 검증"""
    try:
        if df is None or df.empty:
            return pd.DataFrame()
        
        # 복사본 생성
        df_clean = df.copy()
        
        # 완전히 비어있는 행과 열 제거
        df_clean = df_clean.dropna(how='all')
        df_clean = df_clean.dropna(axis=1, how='all')
        
        # 열 이름 정리
        df_clean.columns = [str(col).strip() for col in df_clean.columns]
        
        # Unnamed 열 이름 처리
        for i, col in enumerate(df_clean.columns):
            if 'Unnamed' in col:
                # 첫 번째 행의 값을 열 이름으로 사용 시도
                if len(df_clean) > 0:
                    potential_name = str(df_clean.iloc[0, i]).strip()
                    if potential_name and potential_name != 'nan' and len(potential_name) < 50:
                        df_clean.columns.values[i] = potential_name
                    else:
                        df_clean.columns.values[i] = f'Column_{i+1}'
                else:
                    df_clean.columns.values[i] = f'Column_{i+1}'
        
        # 첫 번째 행이 실제 헤더인 경우 처리
        if len(df_clean) > 0:
            first_row = df_clean.iloc[0]
            if all(isinstance(val, str) and val and val != 'nan' for val in first_row):
                # 첫 번째 행을 헤더로 설정
                df_clean.columns = first_row
                df_clean = df_clean[1:].reset_index(drop=True)
        
        # 중복된 열 이름 처리
        cols = pd.Series(df_clean.columns)
        for dup in cols[cols.duplicated()].unique():
            cols[cols[cols == dup].index.values.tolist()] = [
                dup + '_' + str(i) if i != 0 else dup 
                for i in range(sum(cols == dup))
            ]
        df_clean.columns = cols
        
        # NaN 값을 빈 문자열로 대체
        df_clean = df_clean.fillna('')
        
        # 데이터 타입 정리
        for col in df_clean.columns:
            df_clean[col] = df_clean[col].astype(str).str.strip()
        
        return df_clean
        
    except Exception as e:
        logger.error(f"데이터프레임 정제 중 오류: {str(e)}")
        return df

def determine_sheet_name(df, idx):
    """데이터프레임의 내용을 기반으로 적절한 시트 이름 결정"""
    try:
        # 첫 번째 열이나 첫 번째 행에서 의미있는 텍스트 찾기
        if not df.empty:
            # 첫 번째 열의 값들 확인
            first_col_values = df.iloc[:, 0].dropna().astype(str)
            for val in first_col_values:
                if len(val) > 3 and not val.replace('.', '').replace(',', '').isdigit():
                    # 통신사 이름이나 서비스 유형일 가능성이 높음
                    if any(keyword in val for keyword in ['SKT', 'KT', 'LGU+', '이동전화', '유선', '무선']):
                        return clean_sheet_name(val)
            
            # 첫 번째 행의 값들 확인
            if len(df.columns) > 0:
                for col in df.columns:
                    col_str = str(col)
                    if len(col_str) > 3 and col_str not in ['Unnamed', '']:
                        return clean_sheet_name(col_str)
            
            # 데이터 내용에서 키워드 찾기
            text_content = ' '.join(df.astype(str).values.flatten()[:100])  # 처음 100개 값만
            
            if 'SKT' in text_content and 'KT' in text_content:
                return "통신사별_데이터"
            elif '이동전화' in text_content:
                return "이동전화_통계"
            elif '유선' in text_content:
                return "유선통신_통계"
            elif '무선' in text_content:
                return "무선통신_통계"
        
        return f"Table_{idx + 1}"
        
    except Exception:
        return f"Table_{idx + 1}"

def clean_sheet_name(name):
    """시트 이름으로 사용할 수 있도록 문자열 정리"""
    # 특수문자 제거 및 길이 제한
    clean_name = re.sub(r'[\\/*\[\]:]', '_', str(name))
    clean_name = clean_name.strip()
    
    # 너무 긴 이름 처리
    if len(clean_name) > 30:
        clean_name = clean_name[:30]
    
    return clean_name

def is_summary_table(df):
    """요약 테이블인지 확인"""
    # 행 수가 적고 특정 키워드가 포함된 경우
    if len(df) < 10:
        text_content = ' '.join(df.astype(str).values.flatten())
        summary_keywords = ['합계', '총계', '요약', '전체', 'Total', 'Summary']
        return any(keyword in text_content for keyword in summary_keywords)
    return False

def is_numeric_string(value):
    """문자열이 숫자 값을 나타내는지 확인"""
    if not value or not isinstance(value, str):
        return False
        
    # 빈 문자열이나 공백만 있는 경우
    value = value.strip()
    if not value:
        return False
        
    # 숫자로 변환 시도
    try:
        # 천 단위 구분자 제거
        cleaned = value.replace(',', '').replace(' ', '')
        # 퍼센트 기호 제거
        cleaned = cleaned.replace('%', '')
        
        float(cleaned)
        return True
    except ValueError:
        return False

def create_placeholder_dataframe(post_info):
    """데이터 추출 실패 시 기본 데이터프레임 생성"""
    try:
        # 날짜 정보 추출
        date_match = re.search(r'\((\d{4})년\s*(\d{1,2})월말\s*기준\)', post_info['title'])
        if date_match:
            year = date_match.group(1)
            month = date_match.group(2)
            
            # 보고서 유형 결정
            report_type = determine_report_type(post_info['title'])
            
            # 기본 데이터프레임 생성
            df = pd.DataFrame({
                '구분': [f'{year}년 {month}월 통계'],
                '값': ['데이터를 추출할 수 없습니다'],
                '비고': [f'{post_info["title"]} - 접근 오류'],
                '링크': [post_info.get('url', '링크 없음')]
            })
            
            logger.info(f"플레이스홀더 데이터프레임 생성: {year}년 {month}월 {report_type}")
            return df
            
        return pd.DataFrame({
            '구분': ['알 수 없음'],
            '값': ['데이터 추출 실패'],
            '비고': [f'게시물: {post_info["title"]} - 날짜 정보 없음']
        })  # 날짜 정보가 없으면 최소 정보 포함
        
    except Exception as e:
        logger.error(f"플레이스홀더 데이터프레임 생성 중 오류: {str(e)}")
        # 오류 발생 시도 최소한의 정보 포함
        return pd.DataFrame({
            '구분': ['오류 발생'],
            '업데이트 상태': ['데이터프레임 생성 실패'],
            '비고': [f'오류: {str(e)}']
        })

def determine_report_type(title):
    """게시물 제목에서 보고서 유형 결정"""
    for report_type in CONFIG['report_types']:
        if report_type in title:
            return report_type
            
    # 부분 매칭 시도
    for report_type in CONFIG['report_types']:
        # 주요 키워드 추출
        keywords = report_type.split()
        if any(keyword in title for keyword in keywords if len(keyword) > 1):
            return report_type
            
    return "기타 통신 통계"

def fallback_ocr_extraction(driver, file_params):
    """모든 추출 방법이 실패했을 때 OCR을 사용한 폴백 추출"""
    logger.info("OCR 기반 추출로 폴백")
    
    # 창 크기 최대화
    try:
        driver.maximize_window()
        time.sleep(1)
    except Exception as window_err:
        logger.warning(f"창 크기 조정 중 오류: {str(window_err)}")
    
    # OCR 기능이 비활성화된 경우 건너뛰기
    if not CONFIG['ocr_enabled']:
        logger.info("OCR 기능이 비활성화되어 건너뜀")
        
        # 다른 모든 방법 실패 시, 특별한 fallback: 스크린샷을 저장하고 추후 분석을 위한 메타데이터 저장
        try:
            timestamp = int(time.time())
            fallback_path = f"document_fallback_{timestamp}.png"
            driver.save_screenshot(fallback_path)
            logger.info(f"모든 추출 방법 실패, 문서 스크린샷 저장: {fallback_path}")
            
            # 메타데이터 저장
            meta = {
                'timestamp': timestamp,
                'url': driver.current_url,
                'title': driver.title,
                'params': {k: v for k, v in file_params.items() if k != 'post_info'}
            }
            with open(f"document_fallback_{timestamp}_meta.json", 'w', encoding='utf-8') as f:
                json.dump(meta, f, ensure_ascii=False, indent=2)
            
            # 최소한의 데이터를 담은 DataFrame 생성
            df = pd.DataFrame({
                '추출실패': ['데이터 추출 실패'],
                '파일정보': [str(file_params.get('atch_file_no', '')) + '_' + str(file_params.get('file_ord', ''))],
                'URL': [driver.current_url],
                '저장된스크린샷': [fallback_path]
            })
            return {"추출실패": df}
        except Exception as fallback_err:
            logger.error(f"Fallback 생성 중 오류: {str(fallback_err)}")
        
        return None
    
    # OCR 관련 라이브러리 임포트 확인
    if not OCR_IMPORTS_AVAILABLE:
        logger.warning("OCR 관련 라이브러리가 설치되지 않아 OCR 기능을 사용할 수 없습니다")
        return None
    
    try:
        # 디버깅용 HTML 저장
        if file_params.get('atch_file_no') and file_params.get('file_ord'):
            prefix = f"before_ocr_{file_params['atch_file_no']}_{file_params['file_ord']}"
        else:
            prefix = f"before_ocr_{int(time.time())}"
            
        save_html_for_debugging(driver, prefix)
        
        # 전체 페이지 스크린샷
        full_page_screenshot = f"ocr_full_page_{int(time.time())}.png"
        driver.save_screenshot(full_page_screenshot)
        logger.info(f"전체 페이지 스크린샷 캡처: {full_page_screenshot}")
        
        # 스크롤하여 전체 내용 캡처
        driver.execute_script("window.scrollTo(0, 0)")
        time.sleep(1)
        
        # 페이지 높이 가져오기
        total_height = driver.execute_script("return document.body.scrollHeight")
        viewport_height = driver.execute_script("return window.innerHeight")
        
        screenshots = []
        current_position = 0
        screenshot_count = 0
        
        while current_position < total_height:
            # 스크롤
            driver.execute_script(f"window.scrollTo(0, {current_position})")
            time.sleep(0.5)
            
            # 스크린샷 저장
            screenshot_path = f"ocr_part_{screenshot_count}_{int(time.time())}.png"
            driver.save_screenshot(screenshot_path)
            screenshots.append(screenshot_path)
            logger.info(f"부분 스크린샷 저장: {screenshot_path}")
            
            current_position += viewport_height
            screenshot_count += 1
            
            # 너무 많은 스크린샷 방지
            if screenshot_count > 10:
                logger.warning("스크린샷 수가 10개를 초과하여 중단")
                break
        
        # OCR 처리
        extracted_data = {}
        
        for i, screenshot_path in enumerate(screenshots):
            try:
                # 이미지 열기
                image = Image.open(screenshot_path)
                
                # 이미지 전처리
                # 1. 그레이스케일 변환
                image = image.convert('L')
                
                # 2. 대비 향상
                enhancer = ImageEnhance.Contrast(image)
                image = enhancer.enhance(2.0)
                
                # 3. 샤프닝
                image = image.filter(ImageFilter.SHARPEN)
                
                # 4. 이진화
                img_array = np.array(image)
                _, img_array = cv2.threshold(img_array, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
                
                # 5. 노이즈 제거
                img_array = cv2.medianBlur(img_array, 3)
                
                # PIL 이미지로 다시 변환
                processed_image = Image.fromarray(img_array)
                
                # OCR 수행
                text = pytesseract.image_to_string(processed_image, lang='kor+eng')
                
                if text.strip():
                    # 텍스트를 테이블 형태로 파싱 시도
                    lines = text.strip().split('\n')
                    data = []
                    
                    for line in lines:
                        if line.strip():
                            # 탭이나 여러 공백으로 구분된 데이터
                            cells = re.split(r'\s{2,}|\t', line.strip())
                            if cells and len(cells) > 1:  # 최소 2개 이상의 셀이 있는 경우만
                                data.append(cells)
                    
                    if data:
                        # DataFrame으로 변환
                        df = pd.DataFrame(data[1:], columns=data[0] if data else None)
                        extracted_data[f"OCR_Page_{i+1}"] = df
                        logger.info(f"OCR로 {len(data)-1}행의 데이터 추출 성공")
                
            except Exception as ocr_err:
                logger.error(f"OCR 처리 중 오류 (페이지 {i+1}): {str(ocr_err)}")
        
        return extracted_data if extracted_data else None
        
    except Exception as e:
        logger.error(f"OCR 폴백 추출 중 오류: {str(e)}")
        return None



#=====================================================================================
# Part 6. 구글 시트 관리 함수들
#=====================================================================================

# ===========================
# Google Sheets 관리 함수들
# ===========================

def initialize_gspread_client():
    """Google Sheets 클라이언트 초기화"""
    if not CONFIG['gspread_creds']:
        logger.error("Google Sheets 자격 증명이 설정되지 않았습니다")
        return None
        
    try:
        # 자격 증명 JSON 파싱
        creds_json = json.loads(CONFIG['gspread_creds'])
        
        # 서비스 계정 자격 증명 생성
        scope = [
            'https://spreadsheets.google.com/feeds',
            'https://www.googleapis.com/auth/drive',
            'https://www.googleapis.com/auth/spreadsheets'
        ]
        
        credentials = ServiceAccountCredentials.from_json_keyfile_dict(creds_json, scope)
        client = gspread.authorize(credentials)
        
        logger.info("Google Sheets 클라이언트 초기화 성공")
        
        # 스프레드시트 접근 테스트
        if CONFIG['spreadsheet_id']:
            try:
                test_sheet = client.open_by_key(CONFIG['spreadsheet_id'])
                logger.info(f"Google Sheets API 연결 확인: {test_sheet.title}")
            except gspread.exceptions.APIError as e:
                if "PERMISSION_DENIED" in str(e):
                    logger.error(f"Google Sheets 권한 오류: {str(e)}")
                else:
                    logger.warning(f"Google Sheets API 오류: {str(e)}")
            except Exception as e:
                logger.warning(f"스프레드시트 접근 테스트 중 오류: {str(e)}")
        
        return client
    except json.JSONDecodeError:
        logger.error("Google Sheets 자격 증명 JSON 파싱 오류")
        return None
    except Exception as e:
        logger.error(f"Google Sheets 클라이언트 초기화 중 오류: {str(e)}")
        return None

def open_spreadsheet_with_retry(client, max_retries=3):
    """스프레드시트 열기 (재시도 로직 포함)"""
    for attempt in range(max_retries):
        try:
            spreadsheet = client.open_by_key(CONFIG['spreadsheet_id'])
            logger.info(f"스프레드시트 열기 성공: {spreadsheet.title}")
            return spreadsheet
        except Exception as e:
            logger.error(f"스프레드시트 열기 시도 {attempt + 1}/{max_retries} 실패: {str(e)}")
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)  # 지수 백오프
            else:
                return None

def update_google_sheets(client, data):
    """
    Enhanced function to update Google Sheets with better error handling,
    rate limiting protection, and data validation.
    
    Args:
        client: gspread client instance
        data: Dictionary containing data and metadata to update
        
    Returns:
        bool: True if update was successful, False otherwise
    """
    if not client or not data:
        logger.error("Google Sheets update failed: client or data missing")
        return False
    
    try:
        # Extract information
        post_info = data['post_info']
        
        # Get date information
        if 'date' in data:
            year = data['date']['year']
            month = data['date']['month']
        else:
            # Extract date from title (enhanced regex)
            date_match = re.search(r'\(\s*(\d{4})년\s*(\d{1,2})월말\s*기준\)', post_info['title'])
            if not date_match:
                logger.error(f"Failed to extract date from title: {post_info['title']}")
                return False
                
            year = int(date_match.group(1))
            month = int(date_match.group(2))
        
        # Format date string
        date_str = f"{year}년 {month}월"
        report_type = determine_report_type(post_info['title'])
        
        logger.info(f"Updating Google Sheets for: {date_str} - {report_type}")
        
        # Open spreadsheet with retry logic
        spreadsheet = open_spreadsheet_with_retry(client)
        if not spreadsheet:
            logger.error("Failed to open spreadsheet after multiple attempts")
            return False
        
        # Process the data
        if 'sheets' in data:
            # Multiple sheets
            return update_multiple_sheets(spreadsheet, data['sheets'], date_str, report_type, post_info)
        elif 'dataframe' in data:
            # Single dataframe
            return update_single_sheet(spreadsheet, report_type, data['dataframe'], date_str, post_info)
        else:
            logger.error("No data to update: neither 'sheets' nor 'dataframe' found in data")
            return False
    
    except Exception as e:
        logger.error(f"Error updating Google Sheets: {str(e)}")
        return False

def update_multiple_sheets(spreadsheet, sheets_data, date_str, report_type, post_info=None):
    """
    Update multiple sheets in the spreadsheet.
    Modified to prevent creating date-specific summary sheets.
    
    Args:
        spreadsheet: gspread Spreadsheet object
        sheets_data: Dictionary of sheet names to DataFrames
        date_str: Date string (e.g., "2024년 3월")
        report_type: Type of report
        post_info: Post information dictionary
        
    Returns:
        bool: True if successful, False otherwise
    """
    if not sheets_data:
        logger.warning("No sheets data to update")
        return False
    
    success_count = 0
    total_sheets = len(sheets_data)
    
    # Get existing worksheet names
    existing_worksheets = []
    try:
        worksheets = spreadsheet.worksheets()
        existing_worksheets = [ws.title for ws in worksheets]
        logger.info(f"기존 워크시트 {len(existing_worksheets)}개 발견")
    except Exception as ws_err:
        logger.warning(f"기존 워크시트 목록을 가져오는 데 실패했습니다: {str(ws_err)}")
    
    # Sort sheets by size (Process smallest sheets first to avoid API rate limits)
    sorted_sheets = sorted(sheets_data.items(), key=lambda x: 0 if x[1] is None else x[1].size)
    
    # Add a combined sheet with all data if there are multiple sheets (with fixed name, no date)
    if len(sheets_data) > 1:
        try:
            # Create a combined dataframe
            all_data = []
            for sheet_name, df in sheets_data.items():
                if df is None or df.empty:
                    continue
                
                # Add sheet name as a column
                df_copy = df.copy()
                df_copy['데이터출처'] = sheet_name
                
                all_data.append(df_copy)
            
            if all_data:
                # Combine all dataframes
                combined_df = pd.concat(all_data, ignore_index=True)
                
                # Organize columns: move '데이터출처' to the front
                cols = combined_df.columns.tolist()
                if '데이터출처' in cols:
                    cols.remove('데이터출처')
                    cols = ['데이터출처'] + cols
                    combined_df = combined_df[cols]
                
                # Update as a single sheet with fixed name (no date in name)
                combined_sheet_name = f"전체데이터_{clean_sheet_name_for_gsheets(report_type)}_Raw"
                success = update_single_sheet_raw(spreadsheet, combined_sheet_name, combined_df, date_str, post_info)
                if success:
                    logger.info(f"전체 데이터 통합 시트 생성 성공: {combined_sheet_name}")
        except Exception as combined_err:
            logger.warning(f"통합 데이터 시트 생성 중 오류: {str(combined_err)}")
    
    # Process each sheet
    for i, (sheet_name, df) in enumerate(sorted_sheets):
        try:
            # Skip empty dataframes
            if df is None or df.empty:
                logger.warning(f"빈 데이터프레임 건너뜀: {sheet_name}")
                continue
                
            # Clean sheet name to be valid
            clean_sheet_name = clean_sheet_name_for_gsheets(sheet_name)
            
            # Add "_Raw" suffix to sheet name
            raw_sheet_name = f"{clean_sheet_name}_Raw"
            
            # Check if this sheet already exists (to avoid unnecessary API calls)
            sheet_exists = raw_sheet_name in existing_worksheets
            
            # Check data quality
            df = validate_and_clean_dataframe(df)
            if df.empty:
                logger.warning(f"데이터프레임 {raw_sheet_name}이 정제 후 비어 있습니다")
                continue
            
            # Log progress
            logger.info(f"시트 업데이트 중 ({i+1}/{total_sheets}): {raw_sheet_name}")
            
            # Update the sheet
            try:
                # Use Raw update method
                success = update_single_sheet_raw(spreadsheet, raw_sheet_name, df, date_str, post_info)
                if success:
                    success_count += 1
                    logger.info(f"시트 업데이트 성공: {raw_sheet_name}")
                else:
                    logger.warning(f"시트 업데이트 실패: {raw_sheet_name}")
                
                # Add delay between sheet updates to avoid rate limiting
                if i < total_sheets - 1:  # Skip delay after last sheet
                    delay = 2 + (i % 3)  # Vary delay slightly to avoid patterns
                    time.sleep(delay)
            except Exception as update_err:
                logger.error(f"시트 {raw_sheet_name} 업데이트 중 오류: {str(update_err)}")
        
        except Exception as sheet_err:
            logger.error(f"시트 {sheet_name} 처리 중 오류: {str(sheet_err)}")
    
    logger.info(f"{success_count}/{total_sheets} 시트 업데이트 완료")
    return success_count > 0

def update_single_sheet(spreadsheet, sheet_name, df, date_str, post_info=None):
    """이전 함수와의 호환성을 위한 래퍼"""
    return update_sheet(spreadsheet, sheet_name, df, date_str, post_info, {'mode': 'append'})

def update_single_sheet_raw(spreadsheet, sheet_name, df, date_str, post_info=None):
    """이전 함수와의 호환성을 위한 래퍼"""
    return update_sheet(spreadsheet, sheet_name, df, date_str, post_info, {'mode': 'replace'})

def update_sheet(spreadsheet, sheet_name, data, date_str=None, post_info=None, options=None):
    """
    통합된 시트 업데이트 함수 - 다양한 모드와 재시도 로직 포함
    
    Args:
        spreadsheet: gspread Spreadsheet 객체
        sheet_name: 업데이트할 시트 이름
        data: DataFrame 또는 다른 데이터 구조
        date_str: 날짜 문자열 (열 헤더로 사용)
        post_info: 게시물 정보 (선택 사항)
        options: 업데이트 옵션 딕셔너리
            - mode: 'append'(기존 시트에 추가), 'replace'(시트 대체), 'update'(특정 셀 업데이트)
            - max_retries: 최대 재시도 횟수
            - batch_size: 배치 크기
            - add_metadata: 메타데이터 추가 여부
            - format_header: 헤더 서식 설정 여부
        
    Returns:
        bool: 업데이트 성공 여부
    """
    # 기본 옵션 설정
    if options is None:
        options = {}
    
    mode = options.get('mode', 'append')
    max_retries = options.get('max_retries', 3)
    batch_size = options.get('batch_size', 100)
    add_metadata_flag = options.get('add_metadata', True)
    format_header = options.get('format_header', True)
    
    logger.info(f"시트 업데이트 시작: '{sheet_name}', 모드: {mode}, 최대 재시도: {max_retries}")
    
    # DataFrame 확인 및 정제
    if isinstance(data, pd.DataFrame):
        df = validate_and_clean_dataframe(data.copy())
        if df.empty:
            logger.warning(f"데이터 정제 후 빈 DataFrame, 업데이트 중단")
            return False
    else:
        logger.error(f"지원되지 않는 데이터 타입: {type(data)}")
        return False
    
    # 재시도 로직
    retry_count = 0
    last_error = None
    
    while retry_count < max_retries:
        try:
            # 모드별 처리
            if mode == 'replace':
                success = _replace_sheet(spreadsheet, sheet_name, df, date_str, post_info, 
                                       batch_size, add_metadata_flag, format_header)
            elif mode == 'append':
                success = _append_to_sheet(spreadsheet, sheet_name, df, date_str, post_info,
                                         batch_size)
            elif mode == 'update':
                success = _update_sheet_cells(spreadsheet, sheet_name, df, date_str, post_info,
                                            batch_size)
            else:
                logger.error(f"지원되지 않는 업데이트 모드: {mode}")
                return False
            
            if success:
                logger.info(f"시트 '{sheet_name}' 업데이트 성공 (모드: {mode})")
                return True
            else:
                # 부분 실패 - 재시도
                retry_count += 1
                if retry_count < max_retries:
                    wait_time = 2 ** retry_count  # 지수 백오프
                    logger.warning(f"시트 업데이트 실패, {wait_time}초 후 재시도 ({retry_count}/{max_retries})")
                    time.sleep(wait_time)
                    
        except gspread.exceptions.APIError as api_err:
            last_error = api_err
            retry_count += 1
            
            # API 할당량 초과 에러 처리
            if 'RESOURCE_EXHAUSTED' in str(api_err) or 'quota' in str(api_err).lower():
                wait_time = 60  # 1분 대기
                logger.warning(f"API 할당량 초과, {wait_time}초 대기 후 재시도")
                time.sleep(wait_time)
            else:
                logger.error(f"Google Sheets API 오류: {str(api_err)}")
                if retry_count < max_retries:
                    time.sleep(2 ** retry_count)
                    
        except Exception as e:
            last_error = e
            retry_count += 1
            logger.error(f"시트 업데이트 시도 {retry_count}/{max_retries} 실패: {str(e)}")
            
            if retry_count < max_retries:
                time.sleep(2 ** retry_count)
    
    # 모든 재시도 실패
    logger.error(f"시트 '{sheet_name}' 업데이트 최종 실패: {str(last_error)}")
    return False

def _replace_sheet(spreadsheet, sheet_name, df, date_str, post_info, batch_size, add_metadata, format_header):
    """시트 전체를 새로운 데이터로 대체하는 모드"""
    try:
        # 워크시트 찾기 또는 생성
        try:
            worksheet = spreadsheet.worksheet(sheet_name)
            logger.info(f"기존 워크시트 찾음: {sheet_name}")
            # 기존 데이터 삭제
            worksheet.clear()
        except gspread.exceptions.WorksheetNotFound:
            # 새 워크시트 생성
            worksheet = spreadsheet.add_worksheet(title=sheet_name, rows="1000", cols="50")
            logger.info(f"새 워크시트 생성: {sheet_name}")
        
        # 데이터 준비
        headers = df.columns.tolist()
        values = df.values.tolist()
        
        # 헤더와 데이터를 함께 준비
        all_values = [headers] + values
        
        # 배치 업데이트
        if len(all_values) > batch_size:
            # 큰 데이터는 배치로 나누어 업데이트
            for i in range(0, len(all_values), batch_size):
                batch = all_values[i:i + batch_size]
                start_row = i + 1
                end_row = start_row + len(batch) - 1
                
                # A1 표기법으로 범위 지정
                range_name = f'A{start_row}:{chr(64 + len(headers))}{end_row}'
                worksheet.update(range_name, batch)
                
                logger.info(f"배치 업데이트: 행 {start_row}-{end_row}")
                time.sleep(1)  # API 제한 회피
        else:
            # 작은 데이터는 한 번에 업데이트
            worksheet.update('A1', all_values)
        
        logger.info(f"전체 데이터 업데이트 완료: {df.shape[0]}행 {df.shape[1]}열")
        
        # 메타데이터 추가
        if add_metadata:
            _add_metadata_to_sheet(worksheet, df, date_str, post_info)
        
        # 헤더 서식 설정
        if format_header:
            try:
                worksheet.format(f'A1:{chr(64 + df.shape[1])}1', {
                    "backgroundColor": {"red": 0.9, "green": 0.9, "blue": 0.9},
                    "textFormat": {"bold": True}
                })
                logger.info(f"헤더 행 서식 설정 완료")
            except Exception as format_err:
                logger.warning(f"서식 설정 중 오류: {str(format_err)}")
        
        return True
    except Exception as e:
        logger.error(f"시트 대체 중 오류: {str(e)}")
        return False

def _append_to_sheet(spreadsheet, sheet_name, df, date_str, post_info, batch_size):
    """기존 시트에 열 추가하는 모드 처리"""
    try:
        # 워크시트 찾기 또는 생성
        try:
            worksheet = spreadsheet.worksheet(sheet_name)
            logger.info(f"기존 워크시트 찾음: {sheet_name}")
        except gspread.exceptions.WorksheetNotFound:
            # 새 워크시트 생성
            worksheet = spreadsheet.add_worksheet(title=sheet_name, rows="1000", cols="50")
            logger.info(f"새 워크시트 생성: {sheet_name}")
            
            # 헤더 행 설정
            worksheet.update_cell(1, 1, "항목")
            time.sleep(1)
        
        # 현재 워크시트 열 헤더 및 항목 가져오기
        headers = worksheet.row_values(1) or ["항목"]
        if not headers or len(headers) == 0:
            worksheet.update_cell(1, 1, "항목")
            headers = ["항목"]
            time.sleep(1)
        
        existing_items = worksheet.col_values(1)[1:] or []  # 헤더 제외
        
        # 날짜 열 확인 (이미 존재하는지)
        if date_str in headers:
            col_idx = headers.index(date_str) + 1
            logger.info(f"'{date_str}' 열이 이미 위치 {col_idx}에 존재합니다")
        else:
            # 새 날짜 열 추가
            col_idx = len(headers) + 1
            worksheet.update_cell(1, col_idx, date_str)
            logger.info(f"위치 {col_idx}에 새 열 '{date_str}' 추가")
            time.sleep(1)
        
        # 항목-값 업데이트 준비
        key_col = df.columns[0]
        items = df[key_col].astype(str).tolist()
        
        value_col = df.columns[1] if df.shape[1] >= 2 else key_col
        values = df[value_col].astype(str).tolist()
        
        # 배치 업데이트 준비
        cell_updates = []
        new_items_count = 0
        
        for item, value in zip(items, values):
            # 항목이 이미 존재하는지 확인
            if item in existing_items:
                row_idx = existing_items.index(item) + 2  # 헤더 행 + 1
            else:
                # 새 항목 추가
                row_idx = len(existing_items) + new_items_count + 2
                cell_updates.append({
                    'range': f'A{row_idx}',
                    'values': [[item]]
                })
                new_items_count += 1
            
            # 값 업데이트
            cell_updates.append({
                'range': f'{chr(64 + col_idx)}{row_idx}',
                'values': [[value]]
            })
        
        # 배치 업데이트 실행
        if cell_updates:
            # 배치 크기로 나누어 업데이트
            for i in range(0, len(cell_updates), batch_size):
                batch = cell_updates[i:i + batch_size]
                worksheet.batch_update(batch)
                logger.info(f"배치 업데이트 실행: {len(batch)}개 셀")
                time.sleep(1)  # API 제한 회피
        
        logger.info(f"'{date_str}' 열에 {len(items)}개 항목 업데이트 완료")
        return True
        
    except Exception as e:
        logger.error(f"시트에 데이터 추가 중 오류: {str(e)}")
        return False

def _update_sheet_cells(spreadsheet, sheet_name, df, date_str, post_info, batch_size):
    """특정 셀들만 업데이트하는 모드"""
    # 현재는 append 모드와 동일하게 처리
    return _append_to_sheet(spreadsheet, sheet_name, df, date_str, post_info, batch_size)

def _add_metadata_to_sheet(worksheet, df, date_str, post_info):
    """시트에 메타데이터 추가"""
    try:
        # 데이터 끝 행 다음에 메타데이터 추가
        metadata_row = df.shape[0] + 3
        
        metadata = [
            [f"업데이트 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"],
            [f"데이터 기준: {date_str}"] if date_str else [""],
            [f"출처: {post_info.get('title', 'N/A')}"] if post_info else [""]
        ]
        
        for i, meta in enumerate(metadata):
            worksheet.update_cell(metadata_row + i, 1, meta[0])
            
    except Exception as e:
        logger.warning(f"메타데이터 추가 중 오류: {str(e)}")

def validate_and_clean_dataframe(df):
    """
    데이터프레임 검증 및 정리
    
    Args:
        df: pandas DataFrame to clean
        
    Returns:
        pandas.DataFrame: Cleaned DataFrame
    """
    try:
        if df is None or df.empty:
            return pd.DataFrame()
            
        # Make a copy to avoid modifying the original
        df_clean = df.copy()
        
        # Replace NaN values with empty strings
        df_clean = df_clean.fillna('')
        
        # Convert all values to strings for consistency
        for col in df_clean.columns:
            df_clean[col] = df_clean[col].astype(str)
            
            # Clean up formatting for numeric values
            # If the column looks numeric, format it nicely
            if df_clean[col].str.replace(',', '').str.replace('.', '').str.replace('-', '').str.isdigit().all():
                # 숫자 형식 정리 (천 단위 쉼표 유지)
                try:
                    # 쉼표 제거하고 숫자로 변환 후 다시 포맷팅
                    numeric_values = df_clean[col].str.replace(',', '').astype(float)
                    df_clean[col] = numeric_values.apply(lambda x: f"{x:,.0f}" if pd.notna(x) else '')
                except:
                    # 변환 실패 시 원본 유지
                    pass
        
        return df_clean
        
    except Exception as e:
        logger.error(f"DataFrame 검증 및 정리 중 오류: {str(e)}")
        return df

def clean_sheet_name_for_gsheets(sheet_name):
    """Google Sheets에서 유효한 시트 이름으로 정리"""
    # Replace invalid characters
    clean_name = re.sub(r'[\\/*\[\]:]', '_', str(sheet_name))
    
    # Limit length (Google Sheets has a 100 character limit)
    if len(clean_name) > 100:
        clean_name = clean_name[:97] + '...'
        
    # Ensure it's not empty
    if not clean_name:
        clean_name = 'Sheet'
        
    return clean_name

def cleanup_date_specific_sheets(spreadsheet):
    """
    Removes date-specific summary and data sheets that should no longer be created.
    Only keeps permanent _Raw and _통합 sheets.
    
    Args:
        spreadsheet: gspread Spreadsheet object
        
    Returns:
        int: Number of sheets removed
    """
    try:
        # Get all worksheets
        all_worksheets = spreadsheet.worksheets()
        sheets_to_remove = []
        
        # Find date-specific sheets by pattern matching
        date_patterns = [
            r'요약_.*_\d{4}년\s*\d{1,2}월',  # Matches "요약_무선통신서비스 가입 현황_전체데이터_2025년 1월"
            r'.*\d{4}년\s*\d{1,2}월.*'       # Any sheet with year/month in the name
        ]
        
        # Find sheets to keep - only those ending with _Raw or _통합
        keep_patterns = [r'.*_Raw', r'.*_통합', r'__metadata__']
        
        for worksheet in all_worksheets:
            title = worksheet.title
            
            # Check if it's a sheet we want to keep
            is_keeper = False
            for pattern in keep_patterns:
                if re.match(pattern, title):
                    is_keeper = True
                    break
                    
            # If not a keeper, check if it matches date patterns
            if not is_keeper:
                for pattern in date_patterns:
                    if re.match(pattern, title):
                        sheets_to_remove.append(worksheet)
                        break
        
        # Log what we're planning to remove
        logger.info(f"Found {len(sheets_to_remove)} date-specific sheets to remove")
        for ws in sheets_to_remove:
            logger.info(f"Will remove: {ws.title}")
        
        # Remove the sheets
        remove_count = 0
        
        for ws in sheets_to_remove:
            try:
                spreadsheet.del_worksheet(ws)
                logger.info(f"Removed sheet: {ws.title}")
                remove_count += 1
                # Sleep to avoid API rate limits
                time.sleep(1)
            except Exception as del_err:
                logger.error(f"Error removing sheet {ws.title}: {str(del_err)}")
        
        return remove_count
        
    except Exception as e:
        logger.error(f"Error cleaning up date-specific sheets: {str(e)}")
        return 0

def update_consolidated_sheets(client, data_updates):
    """
    통합된 함수로 통합 시트 업데이트.
    여러 월의 데이터를 보존하면서 최신 데이터를 추가합니다.
    
    Args:
        client: gspread client instance
        data_updates: List of data update information
        
    Returns:
        int: Number of successfully updated consolidated sheets
    """
    if not client or not data_updates:
        logger.error("통합 시트 업데이트 실패: 클라이언트 또는 데이터 없음")
        return 0
        
    try:
        # 스프레드시트 열기
        spreadsheet = open_spreadsheet_with_retry(client)
        if not spreadsheet:
            logger.error("스프레드시트 열기 실패")
            return 0
            
        # 모든 워크시트 가져오기
        all_worksheets = spreadsheet.worksheets()
        worksheet_map = {ws.title: ws for ws in all_worksheets}
        logger.info(f"스프레드시트에서 {len(worksheet_map)}개 워크시트 발견")
        
        # Raw 시트와 대응하는 통합 시트 찾기
        raw_sheets = []
        for title in worksheet_map.keys():
            # Raw 시트 찾기
            if title.endswith('_Raw'):
                base_name = title[:-4]  # Remove "_Raw"
                consol_name = f"{base_name}_통합"
                
                # 통합 시트가 있는지 확인
                if consol_name in worksheet_map:
                    raw_sheets.append((title, consol_name))
                    logger.info(f"Raw-통합 시트 쌍 발견: {title} -> {consol_name}")
                else:
                    # 통합 시트 생성 필요
                    logger.info(f"통합 시트 없음, '{consol_name}' 생성 예정")
                    raw_sheets.append((title, consol_name))
        
        if not raw_sheets:
            logger.warning("통합할 Raw 시트를 찾을 수 없음")
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
        
        logger.info(f"업데이트할 날짜 컬럼들: {date_columns}")
        
        # 각 Raw-통합 시트 쌍 처리
        success_count = 0
        for raw_name, consol_name in raw_sheets:
            try:
                # Raw 시트 데이터 읽기
                raw_worksheet = worksheet_map[raw_name]
                raw_data = raw_worksheet.get_all_values()
                
                if not raw_data:
                    logger.warning(f"Raw 시트 '{raw_name}'가 비어있음")
                    continue
                
                # DataFrame으로 변환
                headers = raw_data[0] if raw_data else []
                data_rows = raw_data[1:] if len(raw_data) > 1 else []
                
                if not headers or not data_rows:
                    logger.warning(f"Raw 시트 '{raw_name}'에 유효한 데이터가 없음")
                    continue
                
                df = pd.DataFrame(data_rows, columns=headers)
                
                # 통합 시트가 이미 존재하는지 확인
                if consol_name in worksheet_map:
                    # 기존 통합 시트에 새 날짜 컬럼 추가
                    for date_col in date_columns:
                        success = update_sheet(spreadsheet, consol_name, df, date_col, None, {'mode': 'append'})
                        if success:
                            success_count += 1
                            logger.info(f"통합 시트 '{consol_name}'에 '{date_col}' 컬럼 추가 성공")
                else:
                    # 새 통합 시트 생성
                    try:
                        consol_worksheet = spreadsheet.add_worksheet(title=consol_name, rows="1000", cols="50")
                        logger.info(f"새 통합 시트 생성: {consol_name}")
                        
                        # 첫 번째 날짜 컬럼으로 초기화
                        for date_col in date_columns:
                            success = update_sheet(spreadsheet, consol_name, df, date_col, None, {'mode': 'append'})
                            if success:
                                success_count += 1
                                break
                    except Exception as create_err:
                        logger.error(f"통합 시트 '{consol_name}' 생성 중 오류: {str(create_err)}")
                
                # API 제한 회피를 위한 대기
                time.sleep(2)
                
            except Exception as pair_err:
                logger.error(f"Raw-통합 시트 쌍 처리 중 오류 ({raw_name} -> {consol_name}): {str(pair_err)}")
        
        logger.info(f"총 {success_count}개 통합 시트 업데이트 완료")
        return success_count
        
    except Exception as e:
        logger.error(f"통합 시트 업데이트 중 오류: {str(e)}")
        return 0

def ensure_metadata_sheet(spreadsheet):
    """
    Ensure that a metadata sheet exists in the spreadsheet.
    
    Args:
        spreadsheet: gspread Spreadsheet object
        
    Returns:
        gspread Worksheet object or None if failed
    """
    try:
        # Try to get existing metadata sheet
        try:
            metadata_sheet = spreadsheet.worksheet("__metadata__")
            return metadata_sheet
        except gspread.exceptions.WorksheetNotFound:
            # Create new metadata sheet
            metadata_sheet = spreadsheet.add_worksheet(title="__metadata__", rows=100, cols=10)
            
            # Initialize metadata sheet
            headers = ["key", "value", "updated_at"]
            metadata_sheet.update('A1:C1', [headers])
            
            return metadata_sheet
            
    except Exception as e:
        logger.error(f"Error ensuring metadata sheet: {str(e)}")
        return None



#=====================================================================================
# Part 7. 메인 함수들
#=====================================================================================

# ===========================
# 메인 모니터링 함수 (Playwright 버전)
# ===========================

async def monitor_msit_telecom_stats(days_range=4, start_page=1, end_page=5, 
                                   check_sheets=True, reverse_order=False,
                                   start_date=None, end_date=None):
    """
    MSIT 웹사이트에서 통신 통계 게시물을 모니터링하고 Google Sheets를 업데이트하는 메인 함수
    
    Args:
        days_range: 확인할 날짜 범위 (최근 N일)
        start_page: 시작 페이지 번호
        end_page: 종료 페이지 번호
        check_sheets: Google Sheets 업데이트 여부
        reverse_order: 역순으로 페이지 탐색 (True: 5→1, False: 1→5)
        start_date: 시작 날짜 (YYYY-MM-DD 형식)
        end_date: 종료 날짜 (YYYY-MM-DD 형식)
        
    Returns:
        None
    """
    start_time = time.time()
    playwright = None
    browser = None
    context = None
    page = None
    
    try:
        logger.info(f"=== MSIT 통신 통계 모니터링 시작 ===")
        if start_date and end_date:
            logger.info(f"검토 기간: {start_date} ~ {end_date}")
        else:
            logger.info(f"검토 기간: 최근 {days_range}일")
        logger.info(f"페이지 범위: {start_page} ~ {end_page}")
        logger.info(f"Google Sheets 업데이트: {'예' if check_sheets else '아니오'}")
        logger.info(f"역순 탐색: {'예' if reverse_order else '아니오'}")
        
        # Initialize Google Sheets client if needed
        gs_client = None
        if check_sheets:
            gs_client = initialize_gspread_client()
            if not gs_client:
                logger.warning("Google Sheets 클라이언트 초기화 실패, 계속 진행하지만 Sheets 업데이트는 건너뜁니다")
        
        # Setup Playwright and browser
        playwright, browser, context, page = await setup_browser()
        
        # Navigate to MSIT website
        logger.info("MSIT 웹사이트 접근 중...")
        
        # 두 가지 접근 방식: 랜딩 페이지를 거쳐가거나 직접 통계 페이지로
        try:
            # Option 1: Navigate through landing page
            await page.goto(CONFIG['landing_url'])
            await page.wait_for_timeout(3000)
            
            # Look for statistics menu/button
            stats_link_found = False
            stats_link_selectors = [
                "//a[contains(text(), '통계정보')]",
                "//a[contains(@href, '/bbs/list.do') and contains(@href, 'mId=99')]",
                "//a[@title='통계정보']",
                "a:has-text('통계정보')"
            ]
            
            for selector in stats_link_selectors:
                try:
                    if selector.startswith('//'):
                        # XPath
                        stats_link = await page.locator(selector).first.element_handle(timeout=5000)
                    else:
                        # CSS selector
                        stats_link = await page.query_selector(selector)
                    
                    if stats_link:
                        # Click using JavaScript to avoid interception
                        await stats_link.click()
                        
                        # Wait for URL change
                        await page.wait_for_function(
                            "() => window.location.href.includes('/bbs/list.do')",
                            timeout=15000
                        )
                        stats_link_found = True
                        logger.info(f"통계정보 페이지로 이동 완료: {page.url}")
                        break
                except Exception as link_err:
                    logger.warning(f"통계정보 링크 클릭 실패 (선택자: {selector}): {str(link_err)}")
            
            if not stats_link_found:
                logger.warning("통계정보 링크를 찾을 수 없음, 직접 URL로 접속")
                await page.goto(CONFIG['stats_url'])
                
                # Wait for page to load
                try:
                    await page.wait_for_selector(".board_list", timeout=15000)
                    logger.info("통계정보 페이지 직접 접속 성공")
                except PlaywrightTimeoutError:
                    logger.warning("통계정보 페이지 로드 시간 초과, 계속 진행")
            
        except Exception as e:
            logger.error(f"랜딩 또는 통계정보 버튼 클릭 중 오류 발생, fallback으로 직접 접속: {str(e)}")
            
            # Reset browser context
            await reset_browser_context(page)
            
            # Access stats page directly
            await page.goto(CONFIG['stats_url'])
            
            try:
                await page.wait_for_selector(".board_list", timeout=15000)
                logger.info("통계정보 페이지 직접 접속 성공 (오류 후 재시도)")
            except PlaywrightTimeoutError:
                logger.warning("통계정보 페이지 로드 시간 초과 (오류 후 재시도), 계속 진행")
        
        logger.info("MSIT 웹사이트 접근 완료")
        
        # Save screenshot (for debugging)
        try:
            await page.screenshot(path="stats_page.png")
            logger.info("통계정보 페이지 스크린샷 저장 완료")
        except Exception as ss_err:
            logger.warning(f"스크린샷 저장 중 오류: {str(ss_err)}")
        
        # 역순 탐색 적용 (end_page부터 start_page 방향으로)
        if reverse_order:
            page_sequence = range(end_page, start_page - 1, -1)
            logger.info(f"역순 페이지 탐색 시작: {end_page} → {start_page}")
        else:
            page_sequence = range(start_page, end_page + 1)
            logger.info(f"순차 페이지 탐색 시작: {start_page} → {end_page}")
        
        # Track all posts and telecom statistics posts
        all_posts = []
        telecom_stats_posts = []
        continue_to_next_page = True  # 다음 페이지 진행 여부
        
        # Process each page
        for page_num in page_sequence:
            if not continue_to_next_page and not reverse_order:
                logger.info(f"날짜 범위를 벗어나 페이지 {page_num} 이후 탐색 중단")
                break
                
            logger.info(f"\n--- 페이지 {page_num} 처리 중 ---")
            
            # Navigate to specific page if not the first
            if page_num > 1:
                success = await navigate_to_specific_page(page, page_num)
                if not success:
                    logger.warning(f"페이지 {page_num}로 이동 실패, 건너뜀")
                    continue
            
            # Parse page content with date filtering
            posts, stats_posts, result_info = parse_page_content(
                await page.content(), page_num, days_range, start_date, end_date, reverse_order
            )
            
            # Add to overall lists
            all_posts.extend(posts)
            telecom_stats_posts.extend(stats_posts)
            
            logger.info(f"페이지 {page_num}: {len(posts)}개 게시물 중 {len(stats_posts)}개 통신 통계 게시물 발견")
            
            # 결과 정보 확인
            if result_info['oldest_date_found']:
                logger.info(f"페이지 {page_num}의 가장 오래된 날짜: {result_info['oldest_date_found']}")
            
            # 다음 페이지 진행 여부 결정
            if not result_info['continue_to_next_page']:
                continue_to_next_page = False
                if not reverse_order:  # 순차 탐색인 경우에만 중단
                    logger.info(f"날짜 범위를 벗어난 게시물 발견, 페이지 탐색 중단")
                    break
        
        logger.info(f"\n총 {len(all_posts)}개 게시물 수집, {len(telecom_stats_posts)}개 통신 통계 게시물 발견")
        
        # Process telecom statistics posts and update Google Sheets
        data_updates = []
        
        if telecom_stats_posts and check_sheets and gs_client:
            logger.info(f"\n=== {len(telecom_stats_posts)}개 통신 통계 게시물 처리 시작 ===")
            
            for idx, post in enumerate(telecom_stats_posts):
                logger.info(f"\n게시물 {idx+1}/{len(telecom_stats_posts)} 처리 중: {post['title']}")
                
                try:
                    # Find view link parameters
                    file_params = await find_view_link_params(page, post)
                    
                    if file_params:
                        # Access iframe content
                        iframe_html = await access_iframe_content(page, file_params)
                        
                        if iframe_html:
                            # Extract data from HTML
                            extracted_data = extract_tables_from_html(iframe_html)
                            
                            if extracted_data:
                                # Update Google Sheets
                                update_data = {
                                    'post_info': post,
                                    'sheets': extracted_data
                                }
                                
                                # Extract date from title
                                date_match = re.search(r'\((\d{4})년\s*(\d{1,2})월말\s*기준\)', post['title'])
                                if date_match:
                                    update_data['date'] = {
                                        'year': int(date_match.group(1)),
                                        'month': int(date_match.group(2))
                                    }
                                
                                if update_google_sheets(gs_client, update_data):
                                    data_updates.append(update_data)
                                    logger.info(f"Google Sheets 업데이트 성공: {post['title']}")
                                else:
                                    logger.warning(f"Google Sheets 업데이트 실패: {post['title']}")
                            else:
                                # Try viewer extraction
                                logger.info("HTML 테이블 추출 실패, 뷰어 추출 시도")
                                viewer_data = await extract_data_from_viewer(page)
                                
                                if viewer_data:
                                    update_data = {
                                        'post_info': post,
                                        'sheets': viewer_data
                                    }
                                    
                                    date_match = re.search(r'\((\d{4})년\s*(\d{1,2})월말\s*기준\)', post['title'])
                                    if date_match:
                                        update_data['date'] = {
                                            'year': int(date_match.group(1)),
                                            'month': int(date_match.group(2))
                                        }
                                    
                                    if update_google_sheets(gs_client, update_data):
                                        data_updates.append(update_data)
                                        logger.info(f"뷰어 데이터로 Google Sheets 업데이트 성공: {post['title']}")
                                else:
                                    # Try OCR as last resort
                                    logger.info("뷰어 추출 실패, OCR 시도")
                                    ocr_data = await fallback_ocr_extraction(page, file_params)
                                    
                                    if ocr_data:
                                        update_data = {
                                            'post_info': post,
                                            'sheets': ocr_data
                                        }
                                        
                                        date_match = re.search(r'\((\d{4})년\s*(\d{1,2})월말\s*기준\)', post['title'])
                                        if date_match:
                                            update_data['date'] = {
                                                'year': int(date_match.group(1)),
                                                'month': int(date_match.group(2))
                                            }
                                        
                                        if update_google_sheets(gs_client, update_data):
                                            data_updates.append(update_data)
                                            logger.info(f"OCR 데이터로 Google Sheets 업데이트 성공: {post['title']}")
                                    else:
                                        # Create placeholder
                                        logger.warning(f"모든 추출 방법 실패, 플레이스홀더 생성: {post['title']}")
                                        placeholder_df = create_placeholder_dataframe(post)
                                        
                                        update_data = {
                                            'post_info': post,
                                            'dataframe': placeholder_df,
                                            'status': 'placeholder'
                                        }
                                        
                                        if update_google_sheets(gs_client, update_data):
                                            data_updates.append(update_data)
                        else:
                            logger.warning(f"iframe 콘텐츠 접근 실패: {post['title']}")
                            # Create placeholder
                            placeholder_df = create_placeholder_dataframe(post)
                            
                            update_data = {
                                'post_info': post,
                                'dataframe': placeholder_df,
                                'status': 'placeholder'
                            }
                            
                            if update_google_sheets(gs_client, update_data):
                                data_updates.append(update_data)
                    else:
                        logger.warning(f"바로보기 링크를 찾을 수 없음: {post['title']}")
                        # Create placeholder
                        placeholder_df = create_placeholder_dataframe(post)
                        
                        update_data = {
                            'post_info': post,
                            'dataframe': placeholder_df,
                            'status': 'placeholder'
                        }
                        
                        if update_google_sheets(gs_client, update_data):
                            data_updates.append(update_data)
                    
                    # Return to list page for next post
                    await page.goto(CONFIG['stats_url'])
                    await page.wait_for_timeout(2000)
                    
                except Exception as e:
                    logger.error(f"게시물 처리 중 오류: {str(e)}")
                    logger.error(traceback.format_exc())
                    
                    # Save diagnostic information
                    diag_info = await collect_diagnostic_info(page, e)
                    diag_path = f"diagnostic_{post['post_id']}_{int(time.time())}.json"
                    with open(diag_path, 'w', encoding='utf-8') as f:
                        json.dump(diag_info, f, ensure_ascii=False, indent=2)
                    logger.info(f"진단 정보 저장: {diag_path}")
                    
                    # Try to recover
                    try:
                        await page.goto(CONFIG['stats_url'])
                        await page.wait_for_selector(".board_list", timeout=10000)
                        logger.info("오류 복구 성공, 목록 페이지로 복귀")
                    except Exception as recovery_err:
                        logger.error(f"오류 복구 실패: {str(recovery_err)}")
                        
                        # 브라우저 완전 재설정 시도
                        try:
                            await context.close()
                            await browser.close()
                            playwright, browser, context, page = await setup_browser()
                            await page.goto(CONFIG['stats_url'])
                            logger.info("브라우저 완전 재설정 성공")
                        except Exception as reset_err:
                            logger.error(f"브라우저 재설정 실패: {str(reset_err)}")
        
        # 데이터 업데이트 후 통합 시트 업데이트 처리
        if CONFIG.get('update_consolidation', False) and data_updates:
            logger.info("통합 시트 업데이트 시작...")
            try:
                consolidated_updates = update_consolidated_sheets(gs_client, data_updates)
                if consolidated_updates:
                    logger.info(f"통합 시트 업데이트 성공: {consolidated_updates}개 시트")
                else:
                    logger.warning("통합 시트 업데이트 실패")
            except Exception as consol_err:
                logger.error(f"통합 시트 업데이트 중 오류: {str(consol_err)}")
        
        # Calculate end time and execution time
        end_time = time.time()
        execution_time = end_time - start_time
        logger.info(f"실행 시간: {execution_time:.2f}초")
        
        # Cleanup old sheets if configured
        if gs_client and data_updates and CONFIG.get('cleanup_old_sheets', False):
            logger.info("날짜별 시트 정리 중...")
            try:
                # Use the same spreadsheet object if already available, or open it again
                spreadsheet = open_spreadsheet_with_retry(gs_client)
                
                if spreadsheet:
                    removed_count = cleanup_date_specific_sheets(spreadsheet)
                    logger.info(f"{removed_count}개의 날짜별 시트 제거됨")
            except Exception as cleanup_err:
                logger.error(f"시트 정리 중 오류: {str(cleanup_err)}")
        
        # Send Telegram notification
        if all_posts or data_updates:
            await send_telegram_message(all_posts, data_updates)
            logger.info(f"알림 전송 완료: {len(all_posts)}개 게시물, {len(data_updates)}개 업데이트")
        else:
            logger.info(f"최근 {days_range}일 내 새 게시물이 없습니다")
            
            # Send "no results" notification (optional)
            if days_range > 7:  # Only for longer-range searches
                bot = telegram.Bot(token=CONFIG['telegram_token'])
                await bot.send_message(
                    chat_id=int(CONFIG['chat_id']),
                    text=f"📊 MSIT 통신 통계 모니터링: 최근 {days_range}일 내 새 게시물이 없습니다.\n({datetime.now().strftime('%Y-%m-%d %H:%M')})"
                )

except Exception as e:
        logger.error(f"모니터링 중 오류 발생: {str(e)}")
        
        # 오류 처리 향상
        try:
            # Save error screenshot
            if page:
                try:
                    await page.screenshot(path="error_screenshot.png")
                    logger.info("오류 발생 시점 스크린샷 저장 완료")
                except Exception as ss_err:
                    logger.error(f"오류 스크린샷 저장 실패: {str(ss_err)}")
            
            # 스택 트레이스 저장
            error_trace = traceback.format_exc()
            logger.error(f"상세 오류 정보: {error_trace}")
            
            # 진단 정보 수집
            try:
                if page:
                    js_info = await page.evaluate("() => ({url: document.URL, readyState: document.readyState, title: document.title})")
                    logger.info(f"페이지 진단 정보: {json.dumps(js_info)}")
            except:
                pass
            
            # Send error notification
            try:
                bot = telegram.Bot(token=CONFIG['telegram_token'])
                error_msg = f"⚠️ *MSIT 모니터링 오류*\n\n오류: {str(e)}\n시간: {datetime.now().strftime('%Y-%m-%d %H:%M')}"
                
                await bot.send_message(
                    chat_id=int(CONFIG['chat_id']),
                    text=error_msg,
                    parse_mode='Markdown'
                )
                logger.info("오류 알림 전송 완료")
            except Exception as telegram_err:
                logger.error(f"오류 알림 전송 실패: {str(telegram_err)}")
        except Exception as error_handler_err:
            logger.error(f"오류 핸들러 실행 중 오류: {str(error_handler_err)}")
    
    finally:
        # Clean up resources
        try:
            if context:
                await context.close()
            if browser:
                await browser.close()
            if playwright:
                await playwright.stop()
            logger.info("Playwright 리소스 정리 완료")
        except Exception as cleanup_err:
            logger.error(f"리소스 정리 중 오류: {str(cleanup_err)}")
        
        logger.info("=== MSIT 통신 통계 모니터링 종료 ===")

# ===========================
# parse_page_content 함수 (Playwright용으로 수정)
# ===========================

def parse_page_content(html_content, page_num=1, days_range=None, start_date=None, end_date=None, reverse_order=False):
    """
    페이지 HTML 내용을 파싱하는 함수 (Playwright 버전)
    BeautifulSoup을 사용하여 HTML 파싱
    
    Args:
        html_content: 페이지 HTML 내용
        page_num: 현재 페이지 번호
        days_range: 특정 일수 이내 게시물 필터링
        start_date: 시작 날짜 문자열 (YYYY-MM-DD)
        end_date: 종료 날짜 문자열 (YYYY-MM-DD)
        reverse_order: 역순 탐색 여부
        
    Returns:
        Tuple[List, List, Dict]: 모든 게시물, 통신 통계 게시물, 파싱 결과 정보
    """
    all_posts = []
    telecom_stats_posts = []
    
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
        # HTML 파싱
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # 게시물 목록 찾기
        board_list = soup.find('ul', class_='board_list')
        if not board_list:
            logger.error("게시물 목록을 찾을 수 없습니다")
            result_info['current_page_complete'] = False
            return [], [], result_info
        
        # 각 게시물 처리
        items = board_list.find_all('li')
        result_info['total_posts'] = len(items)
        
        # 날짜 범위 설정
        start_date_obj = None
        end_date_obj = None
        
        if start_date:
            try:
                start_date_obj = datetime.strptime(start_date, '%Y-%m-%d').date()
            except ValueError:
                logger.warning(f"잘못된 시작 날짜 형식: {start_date}")
                
        if end_date:
            try:
                end_date_obj = datetime.strptime(end_date, '%Y-%m-%d').date()
            except ValueError:
                logger.warning(f"잘못된 종료 날짜 형식: {end_date}")
        
        if days_range and not start_date_obj:
            korea_tz = datetime.now() + timedelta(hours=9)
            start_date_obj = (korea_tz - timedelta(days=days_range)).date()
        
        # 각 게시물 파싱
        for item in items:
            try:
                # 날짜 추출
                date_elem = item.select_one("dd.date, dd[id*='td_CREATION_DATE']")
                if not date_elem:
                    continue
                
                date_str = date_elem.text.strip()
                post_date = parse_date_with_new_format(date_str)
                
                # 날짜 범위 확인
                if not is_within_date_range(post_date, start_date_obj, end_date_obj, days_range):
                    if post_date and start_date_obj and post_date < start_date_obj:
                        result_info['skip_remaining_in_page'] = True
                        if not reverse_order:
                            result_info['continue_to_next_page'] = False
                        break
                    continue
                
                # 제목 추출
                title_elem = item.select_one("dt a, dd.tit a")
                if not title_elem:
                    continue
                
                title = title_elem.text.strip()
                post_id = extract_post_id(item)
                post_url = get_post_url(post_id)
                
                # 부서 정보 추출
                dept_elem = item.select_one("dd[id*='td_CHRG_DEPT_NM'], .dept")
                dept_text = dept_elem.text.strip() if dept_elem else "부서 정보 없음"
                
                # 게시물 정보 생성
                post_info = {
                    'title': title,
                    'date': date_str,
                    'post_date': post_date,
                    'department': dept_text,
                    'url': post_url,
                    'post_id': post_id
                }
                
                all_posts.append(post_info)
                
                # 통신 통계 게시물인지 확인
                if is_telecom_stats_post(title):
                    logger.info(f"통신 통계 게시물 발견: {title}")
                    telecom_stats_posts.append(post_info)
                
            except Exception as e:
                logger.error(f"게시물 파싱 중 에러: {str(e)}")
                continue
        
        return all_posts, telecom_stats_posts, result_info
        
    except Exception as e:
        logger.error(f"페이지 파싱 중 에러: {str(e)}")
        result_info['current_page_complete'] = False
        return [], [], result_info

# ===========================
# 텔레그램 함수
# ===========================

async def send_telegram_message(posts, data_updates=None):
    """텔레그램으로 알림 메시지 전송"""
    if not posts and not data_updates:
        logger.info("알림을 보낼 내용이 없습니다")
        return
        
    try:
        # 텔레그램 봇 초기화
        bot = telegram.Bot(token=CONFIG['telegram_token'])
        
        message = "📊 *MSIT 통신 통계 모니터링*\n\n"
        
        # 새 게시물 정보 추가
        if posts:
            message += "📱 *새로운 통신 관련 게시물*\n\n"
            
            # 최대 5개 게시물만 표시 (너무 길지 않도록)
            displayed_posts = posts[:5]
            for post in displayed_posts:
                message += f"📅 {post['date']}\n"
                message += f"📑 {post['title']}\n"
                message += f"🏢 {post['department']}\n"
                if post.get('url'):
                    message += f"🔗 [게시물 링크]({post['url']})\n"
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
                post_info = update['post_info']
                
                # 날짜 정보 추출
                if 'date' in update:
                    year = update['date']['year']
                    month = update['date']['month']
                    date_str = f"{year}년 {month}월"
                else:
                    date_str = "날짜 미상"
                
                # 타이틀 축약 (너무 길면)
                title = post_info['title']
                if len(title) > 50:
                    title = title[:47] + "..."
                
                message += f"✅ {title}\n"
                message += f"   📅 {date_str} 데이터\n"
                
                # DataFrame 정보 추가
                if 'dataframe' in update and update['dataframe'] is not None:
                    df = update['dataframe']
                    if hasattr(df, 'shape'):
                        rows, cols = df.shape
                        message += f"   📋 {rows}행 × {cols}열\n"
                
                message += "\n"
            
            # 추가 업데이트가 있는 경우 표시
            if len(data_updates) > 10:
                message += f"_...외 {len(data_updates) - 10}개 업데이트_\n\n"
        
        # 시간 정보 추가
        message += f"⏰ _수행 시간: {datetime.now().strftime('%Y-%m-%d %H:%M')}_"
        
        # 메시지 길이 제한 (텔레그램 제한)
        max_length = 4000
        if len(message) > max_length:
            chunks = [message[i:i+max_length] for i in range(0, len(message), max_length)]
            for i, chunk in enumerate(chunks):
                # 첫 번째가 아닌 메시지에 헤더 추가
                if i > 0:
                    chunk = "📊 *MSIT 통신 통계 모니터링 (계속)...*\n\n" + chunk
                
                chat_id = int(CONFIG['chat_id'])
                await bot.send_message(
                    chat_id=chat_id,
                    text=chunk,
                    parse_mode='Markdown'
                )
                time.sleep(1)  # 메시지 사이 지연
            
            logger.info(f"텔레그램 메시지 {len(chunks)}개 청크로 분할 전송 완료")
        else:
            # 단일 메시지 전송
            chat_id = int(CONFIG['chat_id'])
            await bot.send_message(
                chat_id=chat_id,
                text=message,
                parse_mode='Markdown'
            )
            logger.info("텔레그램 메시지 전송 성공")
        
    except Exception as e:
        logger.error(f"텔레그램 메시지 전송 중 오류: {str(e)}")
        
        # 단순화된 메시지로 재시도
        try:
            simple_msg = f"⚠️ MSIT 통신 통계 알림: {len(posts) if posts else 0}개 새 게시물, {len(data_updates) if data_updates else 0}개 업데이트"
            await bot.send_message(
                chat_id=int(CONFIG['chat_id']),
                text=simple_msg
            )
            logger.info("단순화된 텔레그램 메시지 전송 성공")
        except Exception as simple_err:
            logger.error(f"단순화된 텔레그램 메시지 전송 중 오류: {str(simple_err)}")




# ===========================
# fallback_ocr_extraction 함수 (Playwright용으로 수정)
# ===========================

async def fallback_ocr_extraction(page, file_params):
    """모든 추출 방법이 실패했을 때 OCR을 사용한 폴백 추출 (Playwright 버전)"""
    logger.info("OCR 기반 추출로 폴백")
    
    # OCR 기능이 비활성화된 경우 건너뛰기
    if not CONFIG['ocr_enabled']:
        logger.info("OCR 기능이 비활성화되어 건너뜀")
        return None
    
    # OCR 관련 라이브러리 임포트 확인
    if not OCR_IMPORTS_AVAILABLE:
        logger.warning("OCR 관련 라이브러리가 설치되지 않아 OCR 기능을 사용할 수 없습니다")
        return None
    
    try:
        # 전체 페이지 스크린샷
        full_page_screenshot = f"ocr_full_page_{int(time.time())}.png"
        await page.screenshot(path=full_page_screenshot, full_page=True)
        logger.info(f"전체 페이지 스크린샷 캡처: {full_page_screenshot}")
        
        # 뷰포트 정보 가져오기
        viewport_size = page.viewport_size
        viewport_height = viewport_size['height'] if viewport_size else 1080
        
        # 페이지 높이 가져오기
        total_height = await page.evaluate("document.body.scrollHeight")
        
        screenshots = []
        current_position = 0
        screenshot_count = 0
        
        while current_position < total_height:
            # 스크롤
            await page.evaluate(f"window.scrollTo(0, {current_position})")
            await page.wait_for_timeout(500)
            
            # 스크린샷 저장
            screenshot_path = f"ocr_part_{screenshot_count}_{int(time.time())}.png"
            await page.screenshot(path=screenshot_path)
            screenshots.append(screenshot_path)
            logger.info(f"부분 스크린샷 저장: {screenshot_path}")
            
            current_position += viewport_height
            screenshot_count += 1
            
            # 너무 많은 스크린샷 방지
            if screenshot_count > 10:
                logger.warning("스크린샷 수가 10개를 초과하여 중단")
                break
        
        # OCR 처리
        extracted_data = {}
        
        for i, screenshot_path in enumerate(screenshots):
            try:
                # 이미지 열기
                image = Image.open(screenshot_path)
                
                # 이미지 전처리
                image = image.convert('L')
                enhancer = ImageEnhance.Contrast(image)
                image = enhancer.enhance(2.0)
                image = image.filter(ImageFilter.SHARPEN)
                
                img_array = np.array(image)
                _, img_array = cv2.threshold(img_array, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
                img_array = cv2.medianBlur(img_array, 3)
                
                processed_image = Image.fromarray(img_array)
                
                # OCR 수행
                text = pytesseract.image_to_string(processed_image, lang='kor+eng')
                
                if text.strip():
                    # 텍스트를 테이블 형태로 파싱 시도
                    lines = text.strip().split('\n')
                    data = []
                    
                    for line in lines:
                        if line.strip():
                            cells = re.split(r'\s{2,}|\t', line.strip())
                            if cells and len(cells) > 1:
                                data.append(cells)
                    
                    if data:
                        df = pd.DataFrame(data[1:], columns=data[0] if data else None)
                        extracted_data[f"OCR_Page_{i+1}"] = df
                        logger.info(f"OCR로 {len(data)-1}행의 데이터 추출 성공")
                
            except Exception as ocr_err:
                logger.error(f"OCR 처리 중 오류 (페이지 {i+1}): {str(ocr_err)}")
        
        return extracted_data if extracted_data else None
        
    except Exception as e:
        logger.error(f"OCR 폴백 추출 중 오류: {str(e)}")
        return None

# ===========================
# 메인 실행 함수
# ===========================

async def main():
    """메인 함수: 환경 변수 처리 및 모니터링 실행"""
    # 향상된 로깅 설정
    global logger
    logger = setup_enhanced_logging()
    
    # GitHub Actions 환경 감지
    is_github_actions = os.environ.get('GITHUB_ACTIONS', 'false').lower() == 'true'
    if is_github_actions:
        logger.info("GitHub Actions 환경에서 실행 중")
    
    # 환경 변수 처리 (기존 코드와 동일)
    # 1. 검토 기간 설정
    try:
        start_date_str = os.environ.get('START_DATE', '')
        end_date_str = os.environ.get('END_DATE', '')
        
        if start_date_str and end_date_str:
            try:
                start_date = datetime.strptime(start_date_str, '%Y-%m-%d').date()
                end_date = datetime.strptime(end_date_str, '%Y-%m-%d').date()
                
                if start_date > end_date:
                    logger.error(f"시작 날짜({start_date_str})가 종료 날짜({end_date_str})보다 나중입니다.")
                    logger.info("기본 날짜 범위(최근 4일)를 사용합니다.")
                    days_range = int(os.environ.get('DAYS_RANGE', '4'))
                    start_date_str = None
                    end_date_str = None
                else:
                    today = datetime.now().date()
                    days_range = (today - start_date).days
                    logger.info(f"검토 기간 설정: {start_date_str} ~ {end_date_str} (days_range: {days_range}일)")
            except ValueError as date_err:
                logger.error(f"날짜 형식 오류: {str(date_err)}")
                logger.info("기본 날짜 범위(최근 4일)를 사용합니다.")
                days_range = int(os.environ.get('DAYS_RANGE', '4'))
                start_date_str = None
                end_date_str = None
        else:
            days_range = int(os.environ.get('DAYS_RANGE', '4'))
            logger.info(f"검토 기간 미설정, 기본값 사용: days_range={days_range}일")
            start_date_str = None
            end_date_str = None
    except Exception as e:
        logger.error(f"검토 기간 설정 중 오류: {str(e)}")
        logger.info("기본 날짜 범위(최근 4일)를 사용합니다.")
        days_range = 4
        start_date_str = None
        end_date_str = None
    
    # 2. 페이지 범위 설정
    try:
        start_page = int(os.environ.get('START_PAGE', '1'))
        end_page = int(os.environ.get('END_PAGE', '5'))
        
        if start_page < 1:
            logger.warning("시작 페이지는 1 이상이어야 합니다.")
            start_page = 1
        
        if end_page < start_page:
            logger.warning("종료 페이지가 시작 페이지보다 작습니다.")
            logger.info("기본 페이지 범위(1~5)를 사용합니다.")
            start_page = 1
            end_page = 5
        else:
            logger.info(f"페이지 범위 설정: {start_page} ~ {end_page}")
    except ValueError:
        logger.error("페이지 번호는 정수여야 합니다.")
        logger.info("기본 페이지 범위(1~5)를 사용합니다.")
        start_page = 1
        end_page = 5
    
    # 3. 기타 환경 변수
    try:
        check_sheets_str = os.environ.get('CHECK_SHEETS', 'true').lower()
        check_sheets = check_sheets_str in ('true', 'yes', '1', 'y')
        
        update_consolidation_str = os.environ.get('UPDATE_CONSOLIDATION', 'true').lower()
        update_consolidation = update_consolidation_str in ('true', 'yes', '1', 'y')
        logger.info(f"통합 시트 업데이트: {update_consolidation}")
        
        spreadsheet_name = os.environ.get('SPREADSHEET_NAME', 'MSIT 통신 통계')
        
        ocr_enabled_str = os.environ.get('OCR_ENABLED', 'true').lower()
        ocr_enabled = ocr_enabled_str in ('true', 'yes', '1', 'y')

        cleanup_sheets_str = os.environ.get('CLEANUP_OLD_SHEETS', 'false').lower()
        cleanup_old_sheets = cleanup_sheets_str in ('true', 'yes', '1', 'y')
        
        api_request_wait = int(os.environ.get('API_REQUEST_WAIT', '2'))
        
        max_retries = int(os.environ.get('MAX_RETRIES', '3'))
        page_load_timeout = int(os.environ.get('PAGE_LOAD_TIMEOUT', '30'))
        
        reverse_order_str = os.environ.get('REVERSE_ORDER', 'true').lower()
        reverse_order = reverse_order_str in ('true', 'yes', '1', 'y')
        
        if start_date_str and end_date_str:
            logger.info(f"MSIT 모니터 시작 - 검토 기간: {start_date_str}~{end_date_str}, 페이지: {start_page}~{end_page}")
        else:
            logger.info(f"MSIT 모니터 시작 - 최근 {days_range}일 검토, 페이지: {start_page}~{end_page}")
            
        logger.info(f"환경 설정 - Google Sheets 업데이트: {check_sheets}, OCR: {ocr_enabled}, 역순 탐색: {reverse_order}")
        logger.info(f"스프레드시트 이름: {spreadsheet_name}")
        
        # 전역 설정 업데이트
        CONFIG['spreadsheet_name'] = spreadsheet_name
        CONFIG['update_consolidation'] = update_consolidation
        CONFIG['api_request_wait'] = api_request_wait
        CONFIG['ocr_enabled'] = ocr_enabled
        CONFIG['max_retries'] = max_retries
        CONFIG['page_load_timeout'] = page_load_timeout
        CONFIG['cleanup_old_sheets'] = cleanup_old_sheets

        logger.info(f"환경 설정 - Google Sheets 업데이트: {check_sheets}, OCR: {ocr_enabled}, 시트 정리: {cleanup_old_sheets}")
    
    except Exception as config_err:
        logger.error(f"환경 설정 처리 중 오류: {str(config_err)}")
        return
    
    # OCR 라이브러리 확인
    if CONFIG['ocr_enabled']:
        try:
            import pytesseract
            from PIL import Image, ImageEnhance, ImageFilter
            import cv2
            
            pytesseract_cmd = os.environ.get('PYTESSERACT_CMD', 'tesseract')
            pytesseract.pytesseract.tesseract_cmd = pytesseract_cmd
            logger.info(f"Tesseract 경로 설정: {pytesseract_cmd}")
            
            try:
                version = pytesseract.get_tesseract_version()
                logger.info(f"Tesseract OCR 버전: {version}")
            except Exception as tess_err:
                logger.warning(f"Tesseract OCR 설치 확인 실패: {str(tess_err)}")
                if is_github_actions:
                    import subprocess
                    try:
                        result = subprocess.run(['which', 'tesseract'], capture_output=True, text=True)
                        if result.stdout:
                            logger.info(f"Tesseract 경로 발견: {result.stdout.strip()}")
                            pytesseract.pytesseract.tesseract_cmd = result.stdout.strip()
                        else:
                            logger.warning("Tesseract가 설치되지 않았습니다")
                            CONFIG['ocr_enabled'] = False
                    except:
                        CONFIG['ocr_enabled'] = False
        except ImportError as import_err:
            logger.warning(f"OCR 라이브러리 임포트 실패: {str(import_err)}")
            CONFIG['ocr_enabled'] = False
    
    # 모니터링 실행
    await monitor_msit_telecom_stats(
        days_range=days_range,
        start_page=start_page,
        end_page=end_page,
        check_sheets=check_sheets,
        reverse_order=reverse_order,
        start_date=start_date_str,
        end_date=end_date_str
    )

if __name__ == "__main__":
    asyncio.run(main())
