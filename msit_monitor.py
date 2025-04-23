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

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, StaleElementReferenceException

from bs4 import BeautifulSoup
import telegram
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# OCR 관련 라이브러리 임포트 추가 (문제 해결을 위한 부분)
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
    
    # ADD THIS LINE - new configuration option for cleanup
    'cleanup_old_sheets': os.environ.get('CLEANUP_OLD_SHEETS', 'false').lower() in ('true', 'yes', '1', 'y')
}

# 임시 디렉토리 설정
TEMP_DIR = Path("./downloads")
TEMP_DIR.mkdir(exist_ok=True)
SCREENSHOTS_DIR = Path("./screenshots")
SCREENSHOTS_DIR.mkdir(exist_ok=True)

def setup_driver():
    """Selenium WebDriver 설정 (향상된 봇 탐지 회피)"""
    options = Options()
    # 비-headless 모드 실행 (GitHub Actions에서 Xvfb 사용 시)
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-gpu')
    options.add_argument('--window-size=1920,1080')
    
    # 추가 성능 및 안정성 옵션
    options.add_argument('--disable-extensions')
    options.add_argument('--disable-popup-blocking')
    options.add_argument('--disable-web-security')
    options.add_argument('--blink-settings=imagesEnabled=true')  # 이미지 로드 허용
    
    # WebGL 지문을 숨기기 위한 설정
    options.add_argument('--disable-features=WebglDraftExtensions,WebglDecoderExtensions,WebglExtensionForceEnable,WebglImageChromium,WebglOverlays,WebglProgramCacheControl')
    
    # 캐시 비활성화 (항상 새로운 세션처럼 보이도록)
    options.add_argument('--disable-application-cache')
    options.add_argument('--disable-browser-cache')
    
    # 무작위 사용자 데이터 디렉토리 생성 (추적 방지)
    temp_user_data_dir = f"/tmp/chrome-user-data-{int(time.time())}-{random.randint(1000, 9999)}"
    options.add_argument(f'--user-data-dir={temp_user_data_dir}')
    logger.info(f"임시 사용자 데이터 디렉토리 생성: {temp_user_data_dir}")
    
    # 랜덤 User-Agent 설정
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.71 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.1 Safari/605.1.15",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:96.0) Gecko/20100101 Firefox/96.0",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.45 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.55 Safari/537.36"
    ]
    selected_ua = random.choice(user_agents)
    options.add_argument(f"user-agent={selected_ua}")
    logger.info(f"선택된 User-Agent: {selected_ua}")
    
    # 자동화 감지 우회를 위한 옵션
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)
    options.add_argument("--disable-blink-features=AutomationControlled")
    
    # 불필요한 로그 비활성화
    options.add_experimental_option('excludeSwitches', ['enable-logging'])
    
    try:
        # webdriver-manager 사용
        from webdriver_manager.chrome import ChromeDriverManager
        service = Service(ChromeDriverManager().install())
        logger.info("ChromeDriverManager를 통한 드라이버 설치 완료")
    except Exception as e:
        logger.error(f"WebDriver 설정 중 오류: {str(e)}")
        # 기본 경로 사용
        if os.path.exists('/usr/bin/chromedriver'):
            service = Service('/usr/bin/chromedriver')
            logger.info("기본 경로 chromedriver 사용")
        else:
            raise Exception("ChromeDriver를 찾을 수 없습니다")
    
    driver = webdriver.Chrome(service=service, options=options)
    
    # 페이지 로드 타임아웃 증가
    driver.set_page_load_timeout(90)
    
    # Selenium Stealth 적용 (있는 경우)
    try:
        from selenium_stealth import stealth
        stealth(driver,
            languages=["ko-KR", "ko", "en-US", "en"],
            vendor="Google Inc.",
            platform="Win32",
            webgl_vendor="Intel Inc.",
            renderer="Intel Iris OpenGL Engine",
            fix_hairline=True)
        logger.info("Selenium Stealth 적용 완료")
    except ImportError:
        logger.warning("selenium-stealth 라이브러리를 찾을 수 없습니다. 기본 모드로 계속합니다.")
    
    # 추가 스텔스 설정
    try:
        # 웹드라이버 탐지 방지를 위한 JavaScript 실행
        driver.execute_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
        )
        logger.info("추가 스텔스 설정 적용 완료")
    except Exception as js_err:
        logger.warning(f"추가 스텔스 설정 적용 중 오류: {str(js_err)}")
    
    return driver

def setup_gspread_client():
    """Google Sheets 클라이언트 초기화"""
    if not CONFIG['gspread_creds']:
        return None
    
    try:
        # 환경 변수에서 자격 증명 파싱
        creds_dict = json.loads(CONFIG['gspread_creds'])
        
        # 임시 파일에 자격 증명 저장
        temp_creds_path = TEMP_DIR / "temp_creds.json"
        with open(temp_creds_path, 'w') as f:
            json.dump(creds_dict, f)
        
        # gspread 클라이언트 설정
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        credentials = ServiceAccountCredentials.from_json_keyfile_name(str(temp_creds_path), scope)
        client = gspread.authorize(credentials)
        
        # 임시 파일 삭제
        os.unlink(temp_creds_path)
        
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
        
        # 다양한 날짜 형식 시도
        post_date = None
        date_formats = [
            '%Y. %m. %d',  # "YYYY. MM. DD" 형식
            '%b %d %Y',    # "MMM DD YYYY" 형식
            '%Y-%m-%d',    # "YYYY-MM-DD" 형식
            '%Y/%m/%d',    # "YYYY/MM/DD" 형식
            '%d %b %Y',    # "DD MMM YYYY" 형식
        ]
        
        for date_format in date_formats:
            try:
                post_date = datetime.strptime(date_str, date_format).date()
                break
            except ValueError:
                continue
        
        # 정규식으로 시도
        if not post_date:
            match = re.search(r'(\d{4})[.\-\s/]+(\d{1,2})[.\-\s/]+(\d{1,2})', date_str)
            if match:
                year, month, day = map(int, match.groups())
                try:
                    post_date = datetime(year, month, day).date()
                except ValueError:
                    logger.warning(f"날짜 값이 유효하지 않음: {year}-{month}-{day}")
                    return True  # 오류 발생 시 포함으로 처리
        
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

def find_view_link_params(driver, post):
    """게시물에서 바로보기 링크 파라미터 찾기 (클릭 방식 우선)"""
    if not post.get('post_id'):
        logger.error(f"게시물 접근 불가 {post['title']} - post_id 누락")
        return None
    
    logger.info(f"게시물 열기: {post['title']}")
    
    # 현재 URL 저장
    current_url = driver.current_url
    
    # 게시물 목록 페이지로 돌아가기
    try:
        driver.get(CONFIG['stats_url'])
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CLASS_NAME, "board_list"))
        )
        time.sleep(2)  # 추가 대기
    except Exception as e:
        logger.error(f"게시물 목록 페이지 접근 실패: {str(e)}")
        return None
    
    # 최대 재시도 횟수
    max_retries = 3
    retry_delay = 2
    
    for attempt in range(max_retries):
        try:
            # 제목으로 게시물 링크 찾기
            xpath_selectors = [
                f"//p[contains(@class, 'title') and contains(text(), '{post['title'][:20]}')]",
                f"//a[contains(text(), '{post['title'][:20]}')]",
                f"//div[contains(@class, 'toggle') and contains(., '{post['title'][:20]}')]"
            ]
            
            post_link = None
            for selector in xpath_selectors:
                try:
                    elements = driver.find_elements(By.XPATH, selector)
                    if elements:
                        post_link = elements[0]
                        logger.info(f"게시물 링크 발견 (선택자: {selector})")
                        break
                except Exception as find_err:
                    logger.warning(f"선택자로 게시물 찾기 실패: {selector}")
                    continue
            
            if not post_link:
                logger.warning(f"게시물 링크를 찾을 수 없음: {post['title']}")
                
                if attempt < max_retries - 1:
                    logger.info(f"재시도 중... ({attempt+1}/{max_retries})")
                    time.sleep(retry_delay)
                    continue
                else:
                    # 모든 재시도 실패 시 처리
                    logger.warning("게시물 링크 찾기 최대 재시도 횟수 초과")
                    return None
            
            # 스크린샷 저장 (클릭 전)
            take_screenshot(driver, f"before_click_{post['post_id']}")
            
            # 링크 클릭하여 상세 페이지로 이동
            logger.info(f"게시물 링크 클릭 시도: {post['title']}")
            
            # JavaScript로 클릭 시도 (더 신뢰성 있는 방법)
            try:
                driver.execute_script("arguments[0].click();", post_link)
                logger.info("JavaScript를 통한 클릭 실행")
            except Exception as js_click_err:
                logger.warning(f"JavaScript 클릭 실패: {str(js_click_err)}")
                # 일반 클릭 시도
                post_link.click()
                logger.info("일반 클릭 실행")
            
            # 페이지 로드 대기
            try:
                WebDriverWait(driver, 15).until(
                    lambda d: d.current_url != CONFIG['stats_url']
                )
                logger.info(f"페이지 URL 변경 감지됨: {driver.current_url}")
                time.sleep(3)  # 추가 대기
            except TimeoutException:
                logger.warning("URL 변경 감지 실패")
            
            # 상세 페이지 대기
            wait_elements = [
                (By.CLASS_NAME, "view_head"),
                (By.CLASS_NAME, "view_cont"),
                (By.CSS_SELECTOR, ".bbs_wrap .view"),
                (By.XPATH, "//div[contains(@class, 'view')]")
            ]
            
            element_found = False
            for by_type, selector in wait_elements:
                try:
                    WebDriverWait(driver, 15).until(
                        EC.presence_of_element_located((by_type, selector))
                    )
                    logger.info(f"상세 페이지 로드 완료: {selector} 요소 발견")
                    element_found = True
                    break
                except TimeoutException:
                    continue
            
            if not element_found:
                logger.warning("상세 페이지 로드 실패")
                if attempt < max_retries - 1:
                    continue
                else:
                    # AJAX 방식 시도
                    logger.info("AJAX 방식으로 접근 시도")
                    ajax_result = try_ajax_access(driver, post)
                    if ajax_result:
                        return ajax_result
                    
                    # 모든 방법 실패
                    logger.error("모든 접근 방식 실패")
                    return None
            
            # 스크린샷 저장
            take_screenshot(driver, f"post_view_clicked_{post['post_id']}")
            
            # 바로보기 링크 찾기 (확장된 선택자)
            try:
                # 여러 선택자로 바로보기 링크 찾기
                view_links = []
                
                # 1. 일반적인 '바로보기' 링크
                view_links = driver.find_elements(By.CSS_SELECTOR, "a.view[title='새창 열림']")
                
                # 2. onclick 속성으로 찾기
                if not view_links:
                    all_links = driver.find_elements(By.TAG_NAME, "a")
                    view_links = [link for link in all_links if 'getExtension_path' in (link.get_attribute('onclick') or '')]
                
                # 3. 텍스트로 찾기
                if not view_links:
                    all_links = driver.find_elements(By.TAG_NAME, "a")
                    view_links = [link for link in all_links if '바로보기' in (link.text or '')]
                
                # 4. class 속성으로 찾기
                if not view_links:
                    view_links = driver.find_elements(By.CSS_SELECTOR, "a.attach-file, a.file_link, a.download")
                
                # 5. 제목에 포함된 키워드로 관련 링크 찾기
                if not view_links and '통계' in post['title']:
                    all_links = driver.find_elements(By.TAG_NAME, "a")
                    view_links = [link for link in all_links if 
                                any(ext in (link.get_attribute('href') or '')  
                                   for ext in ['.xls', '.xlsx', '.pdf', '.hwp'])]
                
                if view_links:
                    view_link = view_links[0]
                    onclick_attr = view_link.get_attribute('onclick')
                    href_attr = view_link.get_attribute('href')
                    
                    logger.info(f"바로보기 링크 발견, onclick: {onclick_attr}, href: {href_attr}")
                    
                    # getExtension_path('49234', '1') 형식에서 매개변수 추출
                    if onclick_attr and 'getExtension_path' in onclick_attr:
                        match = re.search(r"getExtension_path\s*\(\s*['\"]([\d]+)['\"]?\s*,\s*['\"]([\d]+)['\"]", onclick_attr)
                        if match:
                            atch_file_no = match.group(1)
                            file_ord = match.group(2)
                            
                            # 날짜 정보 추출
                            date_match = re.search(r'\((\d{4})년\s*(\d{1,2})월말\s*기준\)', post['title'])
                            if date_match:
                                year = int(date_match.group(1))
                                month = int(date_match.group(2))
                                
                                return {
                                    'atch_file_no': atch_file_no,
                                    'file_ord': file_ord,
                                    'date': {'year': year, 'month': month},
                                    'post_info': post
                                }
                            
                            return {
                                'atch_file_no': atch_file_no,
                                'file_ord': file_ord,
                                'post_info': post
                            }
                    # 직접 다운로드 URL인 경우 처리
                    elif href_attr and any(ext in href_attr for ext in ['.xls', '.xlsx', '.pdf', '.hwp']):
                        logger.info(f"직접 다운로드 링크 발견: {href_attr}")
                        
                        # 날짜 정보 추출
                        date_match = re.search(r'\((\d{4})년\s*(\d{1,2})월말\s*기준\)', post['title'])
                        if date_match:
                            year = int(date_match.group(1))
                            month = int(date_match.group(2))
                            
                            return {
                                'download_url': href_attr,
                                'date': {'year': year, 'month': month},
                                'post_info': post
                            }
                
                # 바로보기 링크를 찾을 수 없는 경우
                logger.warning(f"바로보기 링크를 찾을 수 없음: {post['title']}")
                
                # 게시물 내용 추출 시도
                try:
                    # 다양한 선택자로 내용 찾기
                    content_selectors = [
                        "div.view_cont", 
                        ".view_content", 
                        ".bbs_content",
                        ".bbs_detail_content",
                        "div[class*='view'] div[class*='cont']"
                    ]
                    
                    content = ""
                    for selector in content_selectors:
                        try:
                            content_elem = driver.find_element(By.CSS_SELECTOR, selector)
                            content = content_elem.text if content_elem else ""
                            if content.strip():
                                logger.info(f"게시물 내용 추출 성공 (길이: {len(content)})")
                                break
                        except:
                            continue
                    
                    # 날짜 정보 추출
                    date_match = re.search(r'\((\d{4})년\s*(\d{1,2})월말\s*기준\)', post['title'])
                    if date_match:
                        year = int(date_match.group(1))
                        month = int(date_match.group(2))
                        
                        return {
                            'content': content if content else "내용 없음",
                            'date': {'year': year, 'month': month},
                            'post_info': post
                        }
                    
                except Exception as content_err:
                    logger.warning(f"게시물 내용 추출 중 오류: {str(content_err)}")
                
                # 날짜 정보 추출
                date_match = re.search(r'\((\d{4})년\s*(\d{1,2})월말\s*기준\)', post['title'])
                if date_match:
                    year = int(date_match.group(1))
                    month = int(date_match.group(2))
                    
                    return {
                        'content': "내용 없음",
                        'date': {'year': year, 'month': month},
                        'post_info': post
                    }
                
                return None
                
            except Exception as e:
                logger.error(f"바로보기 링크 파라미터 추출 중 오류: {str(e)}")
                
                # 오류 발생 시에도 날짜 정보 추출 시도
                date_match = re.search(r'\((\d{4})년\s*(\d{1,2})월말\s*기준\)', post['title'])
                if date_match:
                    year = int(date_match.group(1))
                    month = int(date_match.group(2))
                    
                    return {
                        'content': f"오류 발생: {str(e)}",
                        'date': {'year': year, 'month': month},
                        'post_info': post
                    }
                    
                return None
                
        except TimeoutException:
            if attempt < max_retries - 1:
                logger.warning(f"페이지 로드 타임아웃, 재시도 {attempt+1}/{max_retries}")
                time.sleep(retry_delay * 2)  # 대기 시간 증가
            else:
                logger.error(f"{max_retries}번 시도 후 페이지 로드 실패")
                
                # AJAX 방식 시도
                ajax_result = try_ajax_access(driver, post)
                if ajax_result:
                    return ajax_result
                    
                # 날짜 정보 추출 시도
                date_match = re.search(r'\((\d{4})년\s*(\d{1,2})월말\s*기준\)', post['title'])
                if date_match:
                    year = int(date_match.group(1))
                    month = int(date_match.group(2))
                    
                    return {
                        'content': "페이지 로드 타임아웃",
                        'date': {'year': year, 'month': month},
                        'post_info': post
                    }
                
                return None
        except Exception as e:
            logger.error(f"게시물 상세 정보 접근 중 오류: {str(e)}")
            
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

def update_sheet(spreadsheet, sheet_name, df, date_str=None, post_info=None, options=None):
    """
    통합된 시트 업데이트 함수 - 다양한 모드와 재시도 로직 포함
    
    Args:
        spreadsheet: gspread Spreadsheet 객체
        sheet_name: 업데이트할 시트 이름
        df: pandas DataFrame - 업데이트할 데이터
        date_str: 날짜 문자열 (열 헤더로 사용)
        post_info: 게시물 정보 (선택 사항)
        options: 업데이트 옵션 딕셔너리
            - mode: 'append'(기존 시트에 추가), 'replace'(시트 대체)
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
    if isinstance(df, pd.DataFrame):
        df = validate_and_clean_dataframe(df.copy())
        if df.empty:
            logger.warning(f"데이터 정제 후 빈 DataFrame, 업데이트 중단")
            return False
    else:
        logger.error(f"지원되지 않는 데이터 타입: {type(df)}")
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
            else:
                logger.error(f"지원되지 않는 업데이트 모드: {mode}")
                return False
            
            if success:
                logger.info(f"시트 '{sheet_name}' 업데이트 성공 (모드: {mode})")
                return True
            else:
                # 부분 실패 - 재시도
                retry_count += 1
                logger.warning(f"시트 업데이트 부분 실패, 재시도 중... ({retry_count}/{max_retries})")
                time.sleep(2 * retry_count)  # 지수 백오프
        except gspread.exceptions.APIError as api_err:
            retry_count += 1
            last_error = api_err
            
            if "RESOURCE_EXHAUSTED" in str(api_err) or "RATE_LIMIT_EXCEEDED" in str(api_err):
                # API 제한 - 더 오래 대기 후 재시도
                wait_time = 5 + (3 * retry_count)
                logger.warning(f"API 속도 제한, {wait_time}초 대기 후 재시도 ({retry_count}/{max_retries})")
                time.sleep(wait_time)
            else:
                logger.error(f"API 오류: {str(api_err)}, 재시도 ({retry_count}/{max_retries})")
                time.sleep(2 * retry_count)
        except Exception as e:
            retry_count += 1
            last_error = e
            logger.error(f"시트 업데이트 중 오류: {str(e)}, 재시도 ({retry_count}/{max_retries})")
            time.sleep(2 * retry_count)
    
    # 모든 재시도 실패
    logger.error(f"시트 '{sheet_name}' 업데이트 실패 (최대 재시도 횟수 초과): {str(last_error)}")
    return False

def _replace_sheet(spreadsheet, sheet_name, df, date_str, post_info, batch_size, add_metadata, format_header):
    """
    시트를 완전히 대체하는 모드 처리
    
    Args:
        spreadsheet: gspread Spreadsheet 객체
        sheet_name: 업데이트할 시트 이름
        df: pandas DataFrame - 업데이트할 데이터
        date_str: 날짜 문자열
        post_info: 게시물 정보 (선택 사항)
        batch_size: 배치 크기
        add_metadata: 메타데이터 추가 여부
        format_header: 헤더 서식 설정 여부
        
    Returns:
        bool: 성공 여부
    """
    try:
        # 기존 시트 삭제
        try:
            existing_sheet = spreadsheet.worksheet(sheet_name)
            spreadsheet.del_worksheet(existing_sheet)
            logger.info(f"기존 워크시트 삭제: {sheet_name}")
            time.sleep(2)
        except gspread.exceptions.WorksheetNotFound:
            logger.info(f"기존 워크시트 없음: {sheet_name}")
        
        # 새 시트 생성
        rows = max(df.shape[0] + 20, 100)  # 여유 공간 추가
        cols = max(df.shape[1] + 10, 26)  # 최소 A-Z까지
        worksheet = spreadsheet.add_worksheet(title=sheet_name, rows=rows, cols=cols)
        logger.info(f"새 워크시트 생성: {sheet_name} (행: {rows}, 열: {cols})")
        
        # 전체 데이터 업데이트
        if df.shape[0] > batch_size:
            # 대용량 데이터는 배치 처리
            _update_in_batches(worksheet, df, batch_size)
        else:
            # 작은 데이터는 한 번에 업데이트
            header_values = [df.columns.tolist()]
            data_values = df.values.tolist()
            all_values = header_values + data_values
            
            update_range = f'A1:{chr(64 + df.shape[1])}{df.shape[0] + 1}'
            worksheet.update(update_range, all_values)
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
    """
    기존 시트에 열 추가하는 모드 처리
    
    Args:
        spreadsheet: gspread Spreadsheet 객체
        sheet_name: 업데이트할 시트 이름
        df: pandas DataFrame - 업데이트할 데이터
        date_str: 날짜 문자열 (열 헤더로 사용)
        post_info: 게시물 정보 (선택 사항)
        batch_size: 배치 크기
        
    Returns:
        bool: 성공 여부
    """
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
            _process_batch_updates(worksheet, cell_updates, batch_size)
            logger.info(f"{len(cell_updates)}개 셀 업데이트 완료 (새 항목: {new_items_count}개)")
            return True
        else:
            logger.warning(f"업데이트할 셀이 없습니다")
            return True  # 업데이트할 것이 없는 것도 성공으로 간주
    except Exception as e:
        logger.error(f"시트 추가 모드 처리 중 오류: {str(e)}")
        return False

def _update_in_batches(worksheet, df, batch_size=100):
    """
    대용량 DataFrame 분할 업데이트
    
    Args:
        worksheet: gspread Worksheet 객체
        df: pandas DataFrame - 업데이트할 데이터
        batch_size: 배치 크기
        
    Returns:
        bool: 성공 여부
    """
    try:
        # 헤더 먼저 업데이트
        worksheet.update('A1:1', [df.columns.tolist()])
        time.sleep(2)
        
        # 데이터 행 배치 업데이트
        for i in range(0, df.shape[0], batch_size):
            end_idx = min(i + batch_size, df.shape[0])
            batch_range = f'A{i+2}:{chr(64 + df.shape[1])}{end_idx+1}'
            batch_data = df.iloc[i:end_idx].values.tolist()
            
            worksheet.update(batch_range, batch_data)
            logger.info(f"배치 {i+1}~{end_idx} 업데이트 완료")
            time.sleep(2)
        
        return True
    except Exception as e:
        logger.error(f"배치 업데이트 중 오류: {str(e)}")
        raise  # 상위 함수에서 재시도 처리
        
def _process_batch_updates(worksheet, updates, batch_size=10):
    """
    셀 업데이트 배치 처리
    
    Args:
        worksheet: gspread Worksheet 객체
        updates: 업데이트할 셀 목록
        batch_size: 배치 크기
        
    Returns:
        bool: 성공 여부
    """
    try:
        for i in range(0, len(updates), batch_size):
            batch = updates[i:i+batch_size]
            worksheet.batch_update(batch)
            logger.info(f"일괄 업데이트 {i+1}~{min(i+batch_size, len(updates))} 완료")
            time.sleep(1)
        return True
    except Exception as e:
        logger.error(f"일괄 업데이트 중 오류: {str(e)}")
        raise  # 상위 함수에서 재시도 처리

def _add_metadata_to_sheet(worksheet, df, date_str, post_info):
    """
    시트에 메타데이터 추가
    
    Args:
        worksheet: gspread Worksheet 객체
        df: pandas DataFrame - 업데이트된 데이터
        date_str: 날짜 문자열
        post_info: 게시물 정보 (선택 사항)
        
    Returns:
        bool: 성공 여부
    """
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
        logger.info(f"메타 정보 추가 완료")
        return True
    except Exception as e:
        logger.warning(f"메타데이터 추가 중 오류: {str(e)}")
        return False

# 이전 함수들과의 호환성을 위한 래퍼 함수들
def update_single_sheet(spreadsheet, sheet_name, df, date_str, post_info=None):
    """
    기존 시트에 열 추가하는 방식으로 시트 업데이트 (이전 함수와의 호환성을 위한 래퍼)
    
    Args:
        spreadsheet: gspread Spreadsheet 객체
        sheet_name: 업데이트할 시트 이름
        df: pandas DataFrame - 업데이트할 데이터
        date_str: 날짜 문자열 (열 헤더로 사용)
        post_info: 게시물 정보 (선택 사항)
        
    Returns:
        bool: 업데이트 성공 여부
    """
    return update_sheet(spreadsheet, sheet_name, df, date_str, post_info, {'mode': 'append'})

def update_single_sheet_raw(spreadsheet, sheet_name, df, date_str, post_info=None):
    """
    시트를 완전히 대체하는 방식으로 시트 업데이트 (이전 함수와의 호환성을 위한 래퍼)
    
    Args:
        spreadsheet: gspread Spreadsheet 객체
        sheet_name: 업데이트할 시트 이름
        df: pandas DataFrame - 업데이트할 데이터
        date_str: 날짜 문자열
        post_info: 게시물 정보 (선택 사항)
        
    Returns:
        bool: 업데이트 성공 여부
    """
    return update_sheet(spreadsheet, sheet_name, df, date_str, post_info, {'mode': 'replace'})

def update_single_sheet_with_retry(spreadsheet, sheet_name, df, date_str, max_retries=3):
    """
    재시도 로직이 포함된 시트 업데이트 (이전 함수와의 호환성을 위한 래퍼)
    
    Args:
        spreadsheet: gspread Spreadsheet 객체
        sheet_name: 업데이트할 시트 이름
        df: pandas DataFrame - 업데이트할 데이터
        date_str: 날짜 문자열 (열 헤더로 사용)
        max_retries: 최대 재시도 횟수
        
    Returns:
        bool: 업데이트 성공 여부
    """
    return update_sheet(spreadsheet, sheet_name, df, date_str, None, 
                       {'mode': 'append', 'max_retries': max_retries})

def update_google_sheets(client, data):
    """
    리팩토링된 함수: Google Sheets 업데이트 - 데이터 검증 및 시트 업데이트 처리
    
    Args:
        client: gspread client 인스턴스
        data: Dictionary containing data and metadata to update
        
    Returns:
        bool: True if update was successful, False otherwise
    """
    if not client or not data:
        logger.error("Google Sheets update failed: client or data missing")
        return False
    
    try:
        # 게시물 정보 추출
        post_info = data['post_info']
        
        # 날짜 정보 추출
        if 'date' in data:
            year = data['date']['year']
            month = data['date']['month']
        else:
            # 제목에서 날짜 정보 추출 (향상된 정규식)
            date_match = re.search(r'\(\s*(\d{4})년\s*(\d{1,2})월말\s*기준\)', post_info['title'])
            if not date_match:
                logger.error(f"Failed to extract date from title: {post_info['title']}")
                return False
                
            year = int(date_match.group(1))
            month = int(date_match.group(2))
        
        # 날짜 문자열 포맷
        date_str = f"{year}년 {month}월"
        report_type = determine_report_type(post_info['title'])
        
        logger.info(f"Updating Google Sheets for: {date_str} - {report_type}")
        
        # 스프레드시트 열기 (재시도 로직 포함)
        spreadsheet = open_spreadsheet_with_retry(client)
        if not spreadsheet:
            logger.error("Failed to open spreadsheet after multiple attempts")
            return False
        
        # 데이터 유형에 따른 처리
        success = False
        
        if 'sheets' in data:
            # 여러 시트 데이터가 있는 경우
            sheets_data = data['sheets']
            success = update_multiple_sheets(spreadsheet, sheets_data, date_str, report_type, post_info)
        elif 'dataframe' in data:
            # 단일 데이터프레임인 경우
            df = data['dataframe']
            sheet_name = clean_sheet_name_for_gsheets(report_type)
            
            # 리팩토링된 update_sheet 함수 사용 (raw 모드로 업데이트)
            success = update_sheet(
                spreadsheet=spreadsheet,
                sheet_name=sheet_name,
                df=df,
                date_str=date_str,
                post_info=post_info,
                options={'mode': 'replace'}  # 기존 시트 대체 모드
            )
            
            if success:
                logger.info(f"성공적으로 시트 업데이트: {sheet_name}")
        else:
            logger.error("No data to update: neither 'sheets' nor 'dataframe' found in data")
            return False
        
        return success
    
    except Exception as e:
        logger.error(f"Error updating Google Sheets: {str(e)}")
        return False

def update_multiple_sheets(spreadsheet, sheets_data, date_str, report_type, post_info=None):
    """
    여러 시트 업데이트 처리 (리팩토링)
    
    Args:
        spreadsheet: gspread Spreadsheet 객체
        sheets_data: 시트 이름을 키로, DataFrame을 값으로 하는 딕셔너리
        date_str: 날짜 문자열
        report_type: 보고서 유형
        post_info: 게시물 정보 (선택 사항)
        
    Returns:
        bool: 업데이트 성공 여부
    """
    if not sheets_data:
        logger.error("No sheets data to update")
        return False
        
    success_count = 0
    total_sheets = len(sheets_data)
    
    # 요약 시트 생성
    try:
        summary_sheet_name = f"요약_{clean_sheet_name_for_gsheets(report_type)}"
        
        # 요약 정보 수집
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
            
            # 요약 시트 업데이트 (append 모드)
            success = update_sheet(
                spreadsheet=spreadsheet,
                sheet_name=summary_sheet_name,
                df=summary_df,
                date_str=date_str,
                post_info=post_info,
                options={'mode': 'append'}
            )
            
            if success:
                logger.info(f"성공적으로 요약 시트 업데이트: {summary_sheet_name}")
    except Exception as summary_err:
        logger.warning(f"요약 시트 생성 중 오류: {str(summary_err)}")
    
    # 통합 데이터 시트 생성 (여러 시트가 있는 경우)
    if len(sheets_data) > 1:
        try:
            all_data = []
            for sheet_name, df in sheets_data.items():
                if df is None or df.empty:
                    continue
                
                df_copy = df.copy()
                df_copy['데이터출처'] = sheet_name
                all_data.append(df_copy)
            
            if all_data:
                # 모든 데이터프레임 합치기
                combined_df = pd.concat(all_data, ignore_index=True)
                
                # '데이터출처' 열을 앞으로 이동
                cols = combined_df.columns.tolist()
                if '데이터출처' in cols:
                    cols.remove('데이터출처')
                    cols = ['데이터출처'] + cols
                    combined_df = combined_df[cols]
                
                # 통합 시트 업데이트 (Raw 모드)
                combined_sheet_name = f"전체데이터_{clean_sheet_name_for_gsheets(report_type)}_Raw"
                success = update_sheet(
                    spreadsheet=spreadsheet,
                    sheet_name=combined_sheet_name,
                    df=combined_df,
                    date_str=date_str,
                    post_info=post_info,
                    options={'mode': 'replace'}
                )
                
                if success:
                    logger.info(f"전체 데이터 통합 시트 생성 성공: {combined_sheet_name}")
        except Exception as combined_err:
            logger.warning(f"통합 데이터 시트 생성 중 오류: {str(combined_err)}")
    
    # 개별 시트 처리
    for sheet_name, df in sheets_data.items():
        try:
            # 빈 데이터프레임 건너뛰기
            if df is None or df.empty:
                logger.warning(f"빈 데이터프레임 건너뜀: {sheet_name}")
                continue
                
            # 시트 이름 정리
            clean_sheet_name = clean_sheet_name_for_gsheets(sheet_name)
            
            # Raw 접미사 추가
            raw_sheet_name = f"{clean_sheet_name}_Raw"
            
            # 데이터 품질 확인
            df = validate_and_clean_dataframe(df)
            if df.empty:
                logger.warning(f"데이터프레임 {raw_sheet_name}이 정제 후 비어 있습니다")
                continue
                
            # 시트 업데이트 (raw 모드)
            success = update_sheet(
                spreadsheet=spreadsheet,
                sheet_name=raw_sheet_name,
                df=df,
                date_str=date_str,
                post_info=post_info,
                options={'mode': 'replace'}
            )
            
            if success:
                success_count += 1
                logger.info(f"시트 업데이트 성공: {raw_sheet_name}")
            else:
                logger.warning(f"시트 업데이트 실패: {raw_sheet_name}")
                
        except Exception as sheet_err:
            logger.error(f"시트 {sheet_name} 처리 중 오류: {str(sheet_err)}")
    
    logger.info(f"{success_count}/{total_sheets} 시트 업데이트 완료")
    return success_count > 0

def cleanup_date_specific_sheets(spreadsheet):
    """
    날짜별 요약 및 데이터 시트를 정리하는 함수 (리팩토링)
    영구적인 _Raw 및 _통합 시트만 유지하고 날짜 특정 시트는 제거합니다.
    
    Args:
        spreadsheet: gspread Spreadsheet 객체
        
    Returns:
        int: 제거된 시트 수
    """
    try:
        # 모든 워크시트 가져오기
        all_worksheets = spreadsheet.worksheets()
        sheets_to_remove = []
        
        # 날짜별 시트 패턴 찾기
        date_patterns = [
            r'요약_.*_\d{4}년\s*\d{1,2}월',  # "요약_무선통신서비스 가입 현황_2025년 1월" 형식 매칭
            r'.*\d{4}년\s*\d{1,2}월.*'       # 년/월 표기가 있는 시트 매칭
        ]
        
        # 유지할 시트 패턴 - _Raw나 _통합으로 끝나는 시트
        keep_patterns = [r'.*_Raw$', r'.*_통합$']
        
        for worksheet in all_worksheets:
            title = worksheet.title
            
            # 유지할 시트인지 확인
            is_keeper = False
            for pattern in keep_patterns:
                if re.match(pattern, title):
                    is_keeper = True
                    break
                    
            # 유지할 시트가 아니면 날짜별 패턴과 일치하는지 확인
            if not is_keeper:
                for pattern in date_patterns:
                    if re.match(pattern, title):
                        sheets_to_remove.append(worksheet)
                        break
        
        # 삭제 예정인 시트 목록 기록
        logger.info(f"삭제 예정인 날짜별 시트: {len(sheets_to_remove)}개")
        for ws in sheets_to_remove:
            logger.info(f"삭제 예정: {ws.title}")
        
        # 시트 삭제
        remove_count = 0
        for ws in sheets_to_remove:
            try:
                spreadsheet.del_worksheet(ws)
                logger.info(f"시트 삭제 완료: {ws.title}")
                remove_count += 1
                # API 속도 제한 방지를 위한 대기
                time.sleep(1)
            except Exception as del_err:
                logger.error(f"시트 {ws.title} 삭제 중 오류: {str(del_err)}")
        
        return remove_count
        
    except Exception as e:
        logger.error(f"날짜별 시트 정리 중 오류: {str(e)}")
        return 0

def update_consolidated_sheets(client, data_updates):
    """
    Raw 시트의 최신 열을 통합 시트에 복사하는 리팩토링 함수
    
    Args:
        client: gspread client 인스턴스
        data_updates: 데이터 업데이트 정보의 리스트
        
    Returns:
        int: 업데이트된 통합 시트 수
    """
    if not client or not data_updates:
        logger.error("통합 시트 업데이트에 필요한 클라이언트 또는 데이터가 없습니다")
        return 0
        
    try:
        # 스프레드시트 열기
        spreadsheet = open_spreadsheet_with_retry(client)
        if not spreadsheet:
            logger.error("스프레드시트를 열지 못했습니다")
            return 0
            
        # 모든 워크시트 목록 가져오기
        all_worksheets = spreadsheet.worksheets()
        worksheet_map = {ws.title: ws for ws in all_worksheets}
        logger.info(f"{len(worksheet_map)}개 워크시트 발견")
        
        # Raw 시트와 해당 통합 시트 쌍 찾기
        raw_sheet_pairs = []
        for title in worksheet_map.keys():
            if title.endswith('_Raw'):
                base_name = title[:-4]  # '_Raw' 제거
                consolidated_name = f"{base_name}_통합"
                
                if consolidated_name in worksheet_map:
                    raw_sheet_pairs.append((title, consolidated_name))
                    logger.info(f"시트 쌍 발견: {title} -> {consolidated_name}")
        
        if not raw_sheet_pairs:
            logger.warning("Raw-통합 시트 쌍을 찾을 수 없습니다")
            return 0
        
        # 최신 날짜 확인
        latest_date = _get_latest_date_from_updates(data_updates)
        if not latest_date:
            logger.warning("업데이트에서 날짜 정보를 찾을 수 없습니다")
            return 0
            
        logger.info(f"사용할 날짜 열: {latest_date}")
        
        # 각 통합 시트 업데이트
        updated_count = 0
        
        for raw_name, consolidated_name in raw_sheet_pairs:
            try:
                # 워크시트 객체 가져오기
                raw_ws = worksheet_map[raw_name]
                consolidated_ws = worksheet_map[consolidated_name]
                
                # Raw 시트에서 데이터 가져오기
                raw_data = raw_ws.get_all_values()
                consolidated_data = consolidated_ws.get_all_values()
                
                if not raw_data:
                    logger.warning(f"Raw 시트 {raw_name}이 비어 있습니다")
                    continue
                    
                # Raw 시트의 데이터 검사
                logger.info(f"Raw 시트 {raw_name} 데이터 샘플: {raw_data[0][:5] if raw_data and len(raw_data[0]) > 0 else '(없음)'}")
                if len(raw_data) > 1:
                    logger.info(f"첫 번째 데이터 행: {raw_data[1][:5] if len(raw_data[1]) > 0 else '(없음)'}")
                
                # Raw 시트에서 마지막 데이터 열 찾기
                raw_headers = raw_data[0]
                last_col_idx = _find_last_data_column(raw_data)
                
                if last_col_idx <= 0:
                    logger.warning(f"{raw_name}에서 데이터 열을 찾을 수 없습니다")
                    continue
                    
                # 마지막 열 확인
                logger.info(f"{raw_name}의 마지막 데이터 열: {last_col_idx} ({raw_headers[last_col_idx] if last_col_idx < len(raw_headers) else 'unknown'})")
                
                # 통합 시트 초기화 (필요한 경우)
                if not consolidated_data or len(consolidated_data[0]) == 0:
                    consolidated_ws.update_cell(1, 1, "기준일자")
                    consolidated_data = [["기준일자"]]
                    time.sleep(2)
                
                # 통합 시트에 날짜 열이 있는지 확인
                consolidated_headers = consolidated_data[0]
                if latest_date in consolidated_headers:
                    date_col_idx = consolidated_headers.index(latest_date) + 1
                    logger.info(f"기존 날짜 열 발견: {date_col_idx}")
                else:
                    date_col_idx = len(consolidated_headers) + 1
                    consolidated_ws.update_cell(1, date_col_idx, latest_date)
                    logger.info(f"새 날짜 열 추가: {date_col_idx}")
                    time.sleep(2)
                
                # 데이터 업데이트 준비
                updates = []
                row_keys = {}  # 키와 행 인덱스 매핑
                
                # 통합 시트의 기존 항목 가져오기
                existing_keys = []
                if len(consolidated_data) > 1:
                    for i in range(1, len(consolidated_data)):
                        if len(consolidated_data[i]) > 0 and consolidated_data[i][0]:
                            existing_keys.append(consolidated_data[i][0])
                            row_keys[consolidated_data[i][0]] = i + 1  # +1 for 1-indexed rows
                
                # Raw 시트의 각 행 처리
                for i in range(1, len(raw_data)):
                    if i < len(raw_data) and len(raw_data[i]) > 0:
                        key = raw_data[i][0]
                        if not key or key.lower() in ('none', 'nan'):
                            continue
                            
                        # 마지막 열에서 값 가져오기
                        value = raw_data[i][last_col_idx] if last_col_idx < len(raw_data[i]) else ""
                        
                        # 통합 시트에서 행 결정
                        if key in row_keys:
                            row_idx = row_keys[key]
                        else:
                            row_idx = len(existing_keys) + 2  # +2 for header and 1-indexed
                            existing_keys.append(key)
                            row_keys[key] = row_idx
                            
                            # 키를 첫 번째 열에 추가
                            updates.append({
                                'range': f'A{row_idx}',
                                'values': [[key]]
                            })
                        
                        # 값을 날짜 열에 추가
                        updates.append({
                            'range': f'{chr(64 + date_col_idx)}{row_idx}',
                            'values': [[value]]
                        })
                
                # 업데이트 실행
                if updates:
                    logger.info(f"통합 시트 {consolidated_name}에 {len(updates)}개 업데이트 적용")
                    _apply_batch_updates(consolidated_ws, updates)
                    updated_count += 1
                    
                    # 업데이트 타임스탬프 추가
                    try:
                        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        footer_row = len(existing_keys) + 5
                        
                        consolidated_ws.update_cell(footer_row, 1, "Last Updated")
                        consolidated_ws.update_cell(footer_row, 2, timestamp)
                        logger.info(f"타임스탬프 추가: {consolidated_name}")
                    except Exception as ts_err:
                        logger.warning(f"타임스탬프 추가 실패: {str(ts_err)}")
                else:
                    logger.warning(f"{consolidated_name}에 업데이트할 내용이 없습니다")
                    
            except Exception as sheet_err:
                logger.error(f"시트 {raw_name}/{consolidated_name} 처리 중 오류: {str(sheet_err)}")
        
        logger.info(f"통합 업데이트 완료: {updated_count}개 시트 업데이트됨")
        return updated_count
        
    except Exception as e:
        logger.error(f"통합 업데이트 실패: {str(e)}")
        return 0

def _find_last_data_column(data):
    """
    Raw 시트에서 마지막 데이터 열을 찾는 도우미 함수
    
    Args:
        data: 시트 데이터 (2D 리스트)
        
    Returns:
        int: 마지막 데이터 열의 인덱스
    """
    if not data or len(data) < 2:
        return -1
        
    # 헤더 행 가져오기
    headers = data[0]
    
    # 뒤에서부터 데이터 열 찾기
    for col_idx in range(len(headers) - 1, 0, -1):
        # 열에 데이터가 있는지 확인
        has_data = False
        for row_idx in range(1, min(10, len(data))):  # 첫 10개 행만 확인
            if col_idx < len(data[row_idx]) and data[row_idx][col_idx] and \
               data[row_idx][col_idx].lower() not in ('none', 'nan', '-'):
                has_data = True
                break
                
        if has_data:
            return col_idx
            
    return -1  # 데이터 열을 찾지 못함

def _get_latest_date_from_updates(data_updates):
    """
    데이터 업데이트에서 최신 날짜 추출
    
    Args:
        data_updates: 데이터 업데이트 정보 리스트
        
    Returns:
        str: 날짜 문자열 또는 None
    """
    for update in data_updates:
        if 'date' in update:
            year = update['date']['year']
            month = update['date']['month']
            return f"{year}년 {month}월"
        elif 'post_info' in update:
            post_info = update['post_info']
            title = post_info.get('title', '')
            match = re.search(r'\((\d{4})년\s*(\d{1,2})월말\s*기준\)', title)
            if match:
                year = match.group(1)
                month = match.group(2)
                return f"{year}년 {month}월"
                
    # 날짜를 찾지 못한 경우 현재 날짜 사용
    now = datetime.now()
    return f"{now.year}년 {now.month}월"

def _apply_batch_updates(worksheet, updates, batch_size=10):
    """
    워크시트에 일괄 업데이트 적용 (API 속도 제한 고려)
    
    Args:
        worksheet: gspread Worksheet 객체
        updates: 업데이트 목록
        batch_size: 배치 크기
        
    Returns:
        bool: 성공 여부
    """
    if not updates:
        return True
        
    try:
        logger.info(f"{len(updates)}개 업데이트 배치 처리 시작")
        
        for i in range(0, len(updates), batch_size):
            batch = updates[i:i+batch_size]
            try:
                worksheet.batch_update(batch)
                logger.info(f"배치 {i//batch_size + 1}/{(len(updates)-1)//batch_size + 1} 완료")
                time.sleep(2)  # API 속도 제한 방지
            except gspread.exceptions.APIError as api_err:
                if "RESOURCE_EXHAUSTED" in str(api_err) or "RATE_LIMIT_EXCEEDED" in str(api_err):
                    logger.warning(f"API 속도 제한 발생: {str(api_err)}")
                    
                    # 더 긴 대기 시간 후 개별 업데이트 시도
                    time.sleep(5)
                    for update in batch:
                        try:
                            worksheet.batch_update([update])
                            time.sleep(3)  # 각 업데이트마다 충분히 대기
                        except Exception as single_err:
                            logger.error(f"개별 업데이트 실패: {str(single_err)}")
                else:
                    raise
        
        return True
    except Exception as e:
        logger.error(f"배치 업데이트 오류: {str(e)}")
        return False

def open_spreadsheet_with_retry(client, max_retries=3, retry_delay=2):
    """
    재시도 로직이 포함된 Google 스프레드시트 열기 함수 (리팩토링)
    
    Args:
        client: gspread client 인스턴스
        max_retries: 최대 재시도 횟수
        retry_delay: 재시도 간 대기 시간(초)
        
    Returns:
        gspread.Spreadsheet 객체 또는 실패 시 None
    """
    spreadsheet = None
    retry_count = 0
    
    while retry_count < max_retries and not spreadsheet:
        try:
            # ID로 먼저 시도
            if CONFIG['spreadsheet_id']:
                try:
                    spreadsheet = client.open_by_key(CONFIG['spreadsheet_id'])
                    logger.info(f"Successfully opened spreadsheet by ID: {CONFIG['spreadsheet_id']}")
                    return spreadsheet
                except Exception as id_err:
                    logger.warning(f"Failed to open spreadsheet by ID: {str(id_err)}")
            
            # 이름으로 시도
            try:
                spreadsheet = client.open(CONFIG['spreadsheet_name'])
                logger.info(f"Successfully opened spreadsheet by name: {CONFIG['spreadsheet_name']}")
                return spreadsheet
            except gspread.exceptions.SpreadsheetNotFound:
                # 없으면 새 스프레드시트 생성
                logger.info(f"Spreadsheet not found, creating new one: {CONFIG['spreadsheet_name']}")
                spreadsheet = client.create(CONFIG['spreadsheet_name'])
                # 생성 후 ID 업데이트
                CONFIG['spreadsheet_id'] = spreadsheet.id
                logger.info(f"Created new spreadsheet with ID: {spreadsheet.id}")
                return spreadsheet
                
        except gspread.exceptions.APIError as api_err:
            retry_count += 1
            
            if "RESOURCE_EXHAUSTED" in str(api_err) or "RATE_LIMIT_EXCEEDED" in str(api_err):
                wait_time = 2 ** retry_count  # 지수 백오프
                logger.warning(f"API rate limit hit, waiting {wait_time} seconds before retry")
                time.sleep(wait_time)
            else:
                logger.error(f"Google Sheets API error: {str(api_err)}")
                if retry_count < max_retries:
                    time.sleep(retry_delay)
                else:
                    return None
                    
        except Exception as e:
            logger.error(f"Error opening spreadsheet: {str(e)}")
            retry_count += 1
            if retry_count < max_retries:
                time.sleep(retry_delay)
            else:
                return None
    
    return None

def clean_sheet_name_for_gsheets(sheet_name):
    """
    Google Sheets에서 사용 가능한 시트 이름으로 정리
    
    Args:
        sheet_name: 원본 시트 이름
        
    Returns:
        str: 정리된 시트 이름
    """
    # 유효하지 않은 문자 제거
    clean_name = re.sub(r'[\\/*\[\]:]', '_', str(sheet_name))
    
    # 길이 제한 (Google Sheets는 100자로 제한)
    if len(clean_name) > 100:
        clean_name = clean_name[:97] + '...'
        
    # 빈 이름이 아닌지 확인
    if not clean_name:
        clean_name = 'Sheet'
        
    return clean_name

def validate_and_clean_dataframe(df):
    """
    Google Sheets 업데이트 전 DataFrame 검증 및 정리
    
    Args:
        df: pandas DataFrame
        
    Returns:
        pandas.DataFrame: 정리된 DataFrame
    """
    try:
        if df is None or df.empty:
            return pd.DataFrame()
            
        # 원본 수정 방지를 위한 복사본 생성
        df_clean = df.copy()
        
        # NaN 값을 빈 문자열로 변환
        df_clean = df_clean.fillna('')
        
        # 모든 값을 문자열로 변환 (일관성 유지)
        for col in df_clean.columns:
            df_clean[col] = df_clean[col].astype(str)
            
            # 숫자 값의 포맷 정리
            # 열이 숫자처럼 보이면 포맷 정리
            if df_clean[col].str.replace(',', '').str.replace('.', '').str.isdigit().mean() > 0.7:
                try:
                    numeric_values = pd.to_numeric(df_clean[col].str.replace(',', ''))
                    # 큰 숫자는 쉼표 포맷 적용
                    df_clean[col] = numeric_values.apply(lambda x: f"{x:,}" if abs(x) >= 1000 else str(x))
                except:
                    pass
        
        # 완전히 빈 행 제거
        df_clean = df_clean.replace('', np.nan)
        df_clean = df_clean.dropna(how='all').reset_index(drop=True)
        
        # 완전히 빈 열 제거
        df_clean = df_clean.loc[:, ~df_clean.isna().all()]
        
        # NaN을 다시 빈 문자열로 변환
        df_clean = df_clean.fillna('')
        
        # 열 이름 정리
        df_clean.columns = [str(col).strip() for col in df_clean.columns]
        
        # 중복 열 이름 처리
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
        
        return df_clean
        
    except Exception as e:
        logger.error(f"DataFrame 검증 중 오류: {str(e)}")
        return pd.DataFrame()  # 오류 시 빈 DataFrame 반환

def determine_report_type(title):
    """
    게시물 제목에서 보고서 유형 결정
    
    Args:
        title: 게시물 제목
        
    Returns:
        str: 보고서 유형
    """
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

def create_improved_placeholder_dataframe(post_info, file_params=None):
    """
    데이터 추출 실패 시 더 나은 플레이스홀더 DataFrame 생성
    
    Args:
        post_info: 게시물 정보 딕셔너리
        file_params: 파일 파라미터 (선택 사항)
        
    Returns:
        pandas.DataFrame: 플레이스홀더 DataFrame
    """
    try:
        # 날짜 정보 추출
        date_match = re.search(r'\((\d{4})년\s*(\d{1,2})월말\s*기준\)', post_info['title'])
        
        year = date_match.group(1) if date_match else "Unknown"
        month = date_match.group(2) if date_match else "Unknown"
        
        # 보고서 유형 확인
        report_type = determine_report_type(post_info['title'])
        
        # 추출 상태 결정
        extraction_status = "데이터 추출 실패"
        
        # 진단 정보 (제공된 경우)
        diagnostic_info = "알 수 없는 오류"
        
        if file_params:
            if 'atch_file_no' in file_params and 'file_ord' in file_params:
                extraction_status = "문서 뷰어 접근 실패"
                diagnostic_info = f"atch_file_no={file_params['atch_file_no']}, file_ord={file_params['file_ord']}"
            elif 'content' in file_params:
                extraction_status = "HTML 콘텐츠 분석 실패"
                content_preview = file_params['content'][:100] + "..." if len(file_params['content']) > 100 else file_params['content']
                diagnostic_info = f"Content preview: {content_preview}"
            elif 'ajax_data' in file_params:
                extraction_status = "AJAX 데이터 분석 실패"
                diagnostic_info = "AJAX response received but could not be parsed"
            elif 'download_url' in file_params:
                extraction_status = "직접 다운로드 URL 접근 실패"
                diagnostic_info = f"URL: {file_params['download_url']}"
        
        # 플레이스홀더 DataFrame 생성
        df = pd.DataFrame({
            '구분': [f'{year}년 {month}월 통계'],
            '보고서 유형': [report_type],
            '상태': [extraction_status],
            '진단 정보': [diagnostic_info],
            '링크': [post_info.get('url', '링크 없음')],
            '추출 시도 시간': [datetime.now().strftime('%Y-%m-%d %H:%M:%S')]
        })
        
        logger.info(f"Enhanced placeholder DataFrame created: {year}년 {month}월 {report_type}")
        return df
            
    except Exception as e:
        logger.error(f"Placeholder DataFrame creation error: {str(e)}")
        # 최소한의 DataFrame 생성 (오류 정보 포함)
        return pd.DataFrame({
            '구분': ['오류 발생'],
            '업데이트 상태': ['데이터프레임 생성 실패'],
            '비고': [f'오류: {str(e)}']
        })
