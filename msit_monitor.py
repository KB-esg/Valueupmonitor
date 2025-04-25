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
# Find the CONFIG dictionary (around line 36)
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

def parse_page_content(driver, page_num=1, days_range=None, start_date=None, end_date=None, reverse_order=False):
    """
    페이지 내용을 파싱하는 통합 함수. 날짜 범위 또는 days_range를 기반으로 게시물 필터링
    
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
            (By.CSS_SELECTOR, ".board_list .toggle"),
            (By.CSS_SELECTOR, "table.board_list tr")
        ]
        
        loaded = False
        for by_type, selector in selectors:
            try:
                WebDriverWait(driver, 15).until(  # 대기 시간 증가
                    EC.presence_of_element_located((by_type, selector))
                )
                loaded = True
                logger.info(f"페이지 로드 감지됨: {selector}")
                break
            except TimeoutException:
                continue
        
        if not loaded:
            logger.error("페이지 로드 시간 초과")
            result_info['current_page_complete'] = False
            result_info['continue_to_next_page'] = True
            result_info['messages'].append("페이지 로드 시간 초과")
            return [], [], result_info
        
        # 페이지가 로드되면 약간의 지연 시간 추가
        time.sleep(3)  # 더 긴 지연 시간
        
        # 스크롤을 천천히 내려 모든 요소 로드
        try:
            # 스크롤을 부드럽게 내리기
            execute_javascript(driver, """
                function smoothScroll() {
                    const height = document.body.scrollHeight;
                    const step = Math.floor(height / 10);
                    let i = 0;
                    const timer = setInterval(function() {
                        window.scrollBy(0, step);
                        i++;
                        if (i >= 10) clearInterval(timer);
                    }, 100);
                }
                smoothScroll();
            """, description="페이지 스크롤")
            time.sleep(2)  # 스크롤 완료 대기
            
            # 페이지 맨 위로 돌아가기
            driver.execute_script("window.scrollTo(0, 0);")
            time.sleep(1)
        except Exception as scroll_err:
            logger.warning(f"스크롤 중 오류: {str(scroll_err)}")
        
        # 페이지 소스 저장 (디버깅용)
        try:
            with open(f'page_{page_num}_source.html', 'w', encoding='utf-8') as f:
                f.write(driver.page_source)
            logger.info(f"현재 페이지 소스 저장 완료: page_{page_num}_source.html")
        except Exception as save_err:
            logger.warning(f"페이지 소스 저장 중 오류: {str(save_err)}")
        
        # 스크린샷 저장
        take_screenshot(driver, f"parsed_page_{page_num}")
        
        # BeautifulSoup으로 파싱
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        
        # 게시물 선택자 (다양한 사이트 레이아웃 지원)
        post_selectors = [
            "div.toggle:not(.thead)",
            "table.board_list tr:not(.thead)",
            ".board_list li",
            ".board_list .post-item"
        ]
        
        posts = []
        for selector in post_selectors:
            posts = soup.select(selector)
            if posts:
                logger.info(f"{len(posts)}개 게시물 항목 발견 (선택자: {selector})")
                result_info['total_posts'] = len(posts)
                break
        
        if not posts:
            # DOM에서 직접 시도
            try:
                logger.warning("BeautifulSoup으로 게시물을 찾을 수 없음, Selenium으로 직접 시도")
                direct_posts = []
                for selector in post_selectors:
                    direct_posts = driver.find_elements(By.CSS_SELECTOR, selector)
                    if direct_posts:
                        logger.info(f"Selenium으로 {len(direct_posts)}개 게시물 항목 발견 (선택자: {selector})")
                        result_info['total_posts'] = len(direct_posts)
                        break
                        
                if direct_posts:
                    # Selenium 요소를 사용하여 정보 추출
                    for item in direct_posts:
                        try:
                            # 헤더 행 건너뛰기
                            if 'thead' in item.get_attribute('class') or item.tag_name == 'th':
                                continue
                                
                            # 날짜 추출
                            date_sel = [".date", "div.date", "td.date", ".post-date"]
                            date_elem = None
                            for sel in date_sel:
                                try:
                                    date_elem = item.find_element(By.CSS_SELECTOR, sel)
                                    if date_elem:
                                        break
                                except:
                                    continue
                                    
                            if not date_elem:
                                continue
                                
                            date_str = date_elem.text.strip()
                            if not date_str or date_str == '등록일':
                                continue
                                
                            logger.info(f"날짜 문자열 발견: {date_str}")
                            
                            # 게시물 날짜 파싱
                            post_date = parse_post_date(date_str)
                            if not post_date:
                                logger.warning(f"날짜 파싱 실패: {date_str}, 건너뜀")
                                continue
                            
                            # 날짜 범위 확인하여 게시물 필터링 여부 결정
                            include_post = True
                            
                            # 날짜 정보 업데이트
                            if result_info['oldest_date_found'] is None or post_date < result_info['oldest_date_found']:
                                result_info['oldest_date_found'] = post_date
                                
                            if result_info['newest_date_found'] is None or post_date > result_info['newest_date_found']:
                                result_info['newest_date_found'] = post_date
                                
                            # 시작 날짜보다 이전인지 확인
                            if start_date_obj and post_date < start_date_obj:
                                include_post = False
                                logger.debug(f"시작 날짜 이전 게시물: {post_date} < {start_date_obj}")
                                
                                # 역순 탐색 시에는 현재 페이지의 나머지 게시물만 건너뛰고 다음 페이지로 계속 진행
                                if reverse_order:
                                    result_info['skip_remaining_in_page'] = True
                                    logger.info(f"날짜 범위 이전 게시물({date_str}) 발견, 현재 페이지 나머지 건너뛰기")
                                    result_info['messages'].append(f"날짜 범위 이전 게시물({date_str}) 발견, 현재 페이지 나머지 건너뛰기")
                                    break  # 현재 페이지 루프 종료
                                # 정순 탐색 시에는 모든 페이지 탐색 중단
                                else:
                                    result_info['continue_to_next_page'] = False
                                    logger.info(f"날짜 범위 이전 게시물({date_str}) 발견, 이후 페이지 탐색 중단")
                                    result_info['messages'].append(f"날짜 범위 이전 게시물({date_str}) 발견, 이후 페이지 탐색 중단")
                                
                            # 종료 날짜 이후인지 확인
                            if end_date_obj and post_date > end_date_obj:
                                include_post = False
                                logger.debug(f"종료 날짜 이후 게시물: {post_date} > {end_date_obj}")
                            
                            # 필터링 조건에 맞지 않으면 다음 게시물로
                            if not include_post:
                                continue
                                
                            # 필터링 조건을 통과한 게시물 처리
                            result_info['filtered_posts'] += 1
                            
                            # 제목 추출
                            title_sel = ["p.title", ".title", "td.title", ".subject a", "a.nttInfoBtn"]
                            title_elem = None
                            for sel in title_sel:
                                try:
                                    title_elem = item.find_element(By.CSS_SELECTOR, sel)
                                    if title_elem:
                                        break
                                except:
                                    continue
                                    
                            if not title_elem:
                                continue
                                
                            title = title_elem.text.strip()
                            
                            # 제목 요소의 href 또는 클릭 속성에서 ID 추출
                            post_id = None
                            onclick = title_elem.get_attribute('onclick')
                            if onclick:
                                match = re.search(r"fn_detail\((\d+)\)", onclick)
                                if match:
                                    post_id = match.group(1)
                                    
                            if not post_id:
                                # 부모 요소 또는 조상 요소에서 ID 추출 시도
                                parent = item
                                for _ in range(3):  # 최대 3단계 상위까지 확인
                                    parent_onclick = parent.get_attribute('onclick')
                                    if parent_onclick and 'fn_detail' in parent_onclick:
                                        match = re.search(r"fn_detail\((\d+)\)", parent_onclick)
                                        if match:
                                            post_id = match.group(1)
                                            break
                                    try:
                                        parent = parent.find_element(By.XPATH, "..")
                                    except:
                                        break
                                        
                            # 게시물 URL 생성
                            post_url = get_post_url(post_id) if post_id else None
                            
                            # 부서 정보 추출
                            dept_sel = ["dd[id*='td_CHRG_DEPT_NM']", ".dept", "td.dept", ".department"]
                            dept_elem = None
                            for sel in dept_sel:
                                try:
                                    dept_elem = item.find_element(By.CSS_SELECTOR, sel)
                                    if dept_elem:
                                        break
                                except:
                                    continue
                                    
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
                            
                            # 통신 통계 게시물인지 확인
                            if is_telecom_stats_post(title):
                                logger.info(f"통신 통계 게시물 발견: {title}")
                                telecom_stats_posts.append(post_info)
                                
                        except Exception as direct_err:
                            logger.error(f"직접 추출 중 오류: {str(direct_err)}")
                            continue
                else:
                    logger.warning("게시물을 찾을 수 없음")
                    result_info['messages'].append("게시물을 찾을 수 없음")
                    return [], [], result_info
                    
            except Exception as direct_attempt_err:
                logger.error(f"직접 파싱 시도 중 오류: {str(direct_attempt_err)}")
                result_info['messages'].append(f"직접 파싱 시도 중 오류: {str(direct_attempt_err)}")
                return [], [], result_info
        else:
            # BeautifulSoup으로 찾은 게시물 처리
            for item in posts:
                try:
                    # 헤더 행 건너뛰기
                    if 'thead' in item.get('class', []) or item.name == 'th':
                        continue
                    
                    # 날짜 정보 추출 (여러 선택자 시도)
                    date_selectors = [
                        "div.date[aria-label='등록일']",
                        "div.date",
                        ".date",
                        "td.date",
                        ".post-date"
                    ]
                    
                    date_elem = None
                    for selector in date_selectors:
                        date_elem = item.select_one(selector)
                        if date_elem:
                            break
                            
                    if not date_elem:
                        logger.debug("날짜 요소를 찾을 수 없음, 건너뜀")
                        continue
                        
                    date_str = date_elem.text.strip()
                    if not date_str or date_str == '등록일':
                        continue
                    
                    logger.info(f"날짜 문자열 발견: {date_str}")
                    
                    # 게시물 날짜 파싱
                    post_date = parse_post_date(date_str)
                    if not post_date:
                        logger.warning(f"날짜 파싱 실패: {date_str}, 건너뜀")
                        continue
                    
                    # 날짜 범위 확인하여 게시물 필터링 여부 결정
                    include_post = True
                    
                    # 날짜 정보 업데이트
                    if result_info['oldest_date_found'] is None or post_date < result_info['oldest_date_found']:
                        result_info['oldest_date_found'] = post_date
                        
                    if result_info['newest_date_found'] is None or post_date > result_info['newest_date_found']:
                        result_info['newest_date_found'] = post_date
                    
                    # 시작 날짜보다 이전인지 확인
                    if start_date_obj and post_date < start_date_obj:
                        include_post = False
                        logger.debug(f"시작 날짜 이전 게시물: {post_date} < {start_date_obj}")
                        
                        # 역순 탐색 시에는 현재 페이지의 나머지 게시물만 건너뛰고 다음 페이지로 계속 진행
                        if reverse_order:
                            result_info['skip_remaining_in_page'] = True
                            logger.info(f"날짜 범위 이전 게시물({date_str}) 발견, 현재 페이지 나머지 건너뛰기")
                            result_info['messages'].append(f"날짜 범위 이전 게시물({date_str}) 발견, 현재 페이지 나머지 건너뛰기")
                            break  # 현재 페이지 루프 종료
                        # 정순 탐색 시에는 모든 페이지 탐색 중단
                        else:
                            result_info['continue_to_next_page'] = False
                            logger.info(f"날짜 범위 이전 게시물({date_str}) 발견, 이후 페이지 탐색 중단")
                            result_info['messages'].append(f"날짜 범위 이전 게시물({date_str}) 발견, 이후 페이지 탐색 중단")
                    
                    # 종료 날짜 이후인지 확인
                    if end_date_obj and post_date > end_date_obj:
                        include_post = False
                        logger.debug(f"종료 날짜 이후 게시물: {post_date} > {end_date_obj}")
                    
                    # 필터링 조건에 맞지 않으면 다음 게시물로
                    if not include_post:
                        continue
                        
                    # 필터링 조건을 통과한 게시물 처리
                    result_info['filtered_posts'] += 1
                    
                    # 제목 및 게시물 ID 추출
                    title_selectors = [
                        "p.title",
                        ".title",
                        "td.title",
                        ".subject a",
                        "a.nttInfoBtn"
                    ]
                    
                    title_elem = None
                    for selector in title_selectors:
                        title_elem = item.select_one(selector)
                        if title_elem:
                            break
                            
                    if not title_elem:
                        logger.debug("제목 요소를 찾을 수 없음, 건너뜀")
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

def parse_post_date(date_str):
    """
    다양한 형식의 날짜 문자열을 날짜 객체로 변환
    
    Args:
        date_str: 날짜 문자열
        
    Returns:
        date: 파싱된 datetime.date 객체 또는 None
    """
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
                logger.warning(f"날짜 값이 유효하지 않음: {year}-{month}-{day}")
        
        logger.warning(f"알 수 없는 날짜 형식: {date_str}")
        return None
        
    except Exception as e:
        logger.error(f"날짜 파싱 오류: {str(e)}")
        return None

def has_next_page(driver):
    """다음 페이지가 있는지 확인"""
    try:
        # 여러 가능한 선택자 시도
        selectors = [
            "a.page-link[aria-current='page']",
            "a.on[href*='pageIndex']",
            ".pagination .active a"
        ]
        
        current_page = None
        for selector in selectors:
            elements = driver.find_elements(By.CSS_SELECTOR, selector)
            if elements:
                try:
                    current_page = int(elements[0].text)
                    break
                except ValueError:
                    continue
        
        if not current_page:
            logger.warning("현재 페이지 번호를 찾을 수 없음")
            return False
            
        # 다음 페이지 링크 확인
        next_page_selectors = [
            f"a.page-link[href*='pageIndex={current_page + 1}']",
            f"a[href*='pageIndex={current_page + 1}']",
            ".pagination a.next"
        ]
        
        for selector in next_page_selectors:
            next_page_links = driver.find_elements(By.CSS_SELECTOR, selector)
            if next_page_links:
                return True
                
        return False
        
    except Exception as e:
        logger.error(f"다음 페이지 확인 중 에러: {str(e)}")
        return False

def go_to_next_page(driver):
    """다음 페이지로 이동"""
    try:
        # 현재 페이지 번호 확인
        selectors = [
            "a.page-link[aria-current='page']",
            "a.on[href*='pageIndex']",
            ".pagination .active a"
        ]
        
        current_page = None
        current_page_element = None
        
        for selector in selectors:
            elements = driver.find_elements(By.CSS_SELECTOR, selector)
            if elements:
                try:
                    current_page = int(elements[0].text)
                    current_page_element = elements[0]
                    break
                except ValueError:
                    continue
        
        if not current_page:
            logger.warning("현재 페이지 번호를 찾을 수 없음")
            return False
            
        # 다음 페이지 링크 확인 및 클릭
        next_page_selectors = [
            f"a.page-link[href*='pageIndex={current_page + 1}']",
            f"a[href*='pageIndex={current_page + 1}']",
            ".pagination a.next"
        ]
        
        for selector in next_page_selectors:
            next_page_links = driver.find_elements(By.CSS_SELECTOR, selector)
            if next_page_links:
                # 현재 페이지 콘텐츠 저장
                try:
                    current_content = driver.find_element(By.CSS_SELECTOR, "div.board_list").get_attribute("innerHTML")
                except:
                    current_content = ""
                
                # 다음 페이지 클릭
                next_page_links[0].click()
                
                # 페이지 변경 대기
                wait = WebDriverWait(driver, 10)
                try:
                    # 콘텐츠 변경 대기
                    if current_content:
                        wait.until(lambda d: d.find_element(By.CSS_SELECTOR, "div.board_list").get_attribute("innerHTML") != current_content)
                    else:
                        # 다른 방식으로 페이지 변경 감지
                        wait.until(EC.staleness_of(current_page_element))
                    
                    logger.info(f"페이지 {current_page}에서 {current_page+1}로 이동 성공")
                    return True
                except TimeoutException:
                    logger.warning("페이지 콘텐츠 변경 감지 실패, 로딩 대기 중...")
                    time.sleep(3)  # 추가 대기
                    
                    # 페이지 번호 변경 확인
                    for selector in selectors:
                        new_page_elements = driver.find_elements(By.CSS_SELECTOR, selector)
                        if new_page_elements and new_page_elements[0].text != str(current_page):
                            logger.info(f"페이지 번호 변경 감지: {current_page} → {new_page_elements[0].text}")
                            return True
                    
                    logger.warning("페이지 변경 확인 실패")
                    return False
        
        logger.warning("다음 페이지 링크를 찾을 수 없음")
        return False
        
    except Exception as e:
        logger.error(f"다음 페이지 이동 중 에러: {str(e)}")
        return False

def navigate_to_specific_page(driver, target_page):
    """
    특정 페이지로 직접 이동하는 함수 (클릭 방식 사용)
    
    Args:
        driver: Selenium WebDriver 인스턴스
        target_page: 이동하려는 페이지 번호
        
    Returns:
        bool: 페이지 이동 성공 여부
    """
    try:
        # 현재 페이지 파악
        current_page = get_current_page(driver)
        logger.info(f"현재 페이지: {current_page}, 목표 페이지: {target_page}")
        
        if current_page == target_page:
            logger.info(f"이미 목표 페이지({target_page})에 있습니다.")
            return True
        
        # 페이지네이션 영역 찾기
        try:
            page_nav = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.ID, "pageNavi"))
            )
        except TimeoutException:
            logger.error("페이지 네비게이션을 찾을 수 없습니다.")
            return False
        
        # 직접 페이지 링크 찾기
        page_links = page_nav.find_elements(By.CSS_SELECTOR, "a.page-link")
        
        # 페이지 번호와 링크 매핑
        page_map = {}
        for link in page_links:
            try:
                page_text = link.text.strip()
                if page_text.isdigit():
                    page_num = int(page_text)
                    page_map[page_num] = link
            except Exception as e:
                continue
        
        # 목표 페이지 링크가 있으면 직접 클릭
        if target_page in page_map:
            logger.info(f"페이지 {target_page} 링크 발견, 직접 클릭 시도")
            link = page_map[target_page]
            
            # JavaScript로 클릭 (더 안정적)
            driver.execute_script("arguments[0].click();", link)
            
            # 페이지 변경 대기
            wait_for_page_change(driver, current_page)
            
            # 페이지 변경 확인
            new_page = get_current_page(driver)
            logger.info(f"페이지 이동 결과: {current_page} → {new_page}")
            return new_page == target_page
        
        # 목표 페이지가 현재 페이지네이션에 없는 경우 확인
        # 처음/이전/다음/마지막 네비게이션 링크 확인
        if target_page > current_page:
            # 다음 페이지 링크 찾기
            next_link = page_nav.find_elements(By.CSS_SELECTOR, "a.next, a.page-navi.next")
            if next_link:
                logger.info("다음 페이지 링크 클릭")
                driver.execute_script("arguments[0].click();", next_link[0])
                wait_for_page_change(driver, current_page)
                
                # 재귀적으로 다시 시도
                return navigate_to_specific_page(driver, target_page)
        else:
            # 이전 페이지 링크 찾기
            prev_link = page_nav.find_elements(By.CSS_SELECTOR, "a.prev, a.page-navi.prev")
            if prev_link:
                logger.info("이전 페이지 링크 클릭")
                driver.execute_script("arguments[0].click();", prev_link[0])
                wait_for_page_change(driver, current_page)
                
                # 재귀적으로 다시 시도
                return navigate_to_specific_page(driver, target_page)
        
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
            success = go_to_adjacent_page(driver, next_page)
            if not success:
                logger.error(f"페이지 {current}에서 {next_page}로 이동 실패")
                return False
            
            current = next_page
            logger.info(f"현재 페이지: {current}")
            
            # 과도한 요청 방지
            time.sleep(2)
        
        return current == target_page
        
    except Exception as e:
        logger.error(f"페이지 {target_page}로 이동 중 오류: {str(e)}")
        return False

def get_current_page(driver):
    """
    현재 페이지 번호를 가져오는 함수
    
    Args:
        driver: Selenium WebDriver 인스턴스
        
    Returns:
        int: 현재 페이지 번호 (기본값 1)
    """
    try:
        # 현재 활성화된 페이지 링크 찾기
        page_nav = WebDriverWait(driver, 5).until(
            EC.presence_of_element_located((By.ID, "pageNavi"))
        )
        
        # 여러 선택자 시도
        selectors = [
            "a.page-link[aria-current='page']",  # aria-current 속성이 있는 경우
            "a.on[href*='pageIndex']",           # 'on' 클래스가 있는 경우
            "a.page-link.active",                # active 클래스가 있는 경우
            ".pagination .active a"              # 다른 구조
        ]
        
        for selector in selectors:
            elements = page_nav.find_elements(By.CSS_SELECTOR, selector)
            if elements:
                try:
                    return int(elements[0].text.strip())
                except ValueError:
                    continue
        
        # 모든 페이지 링크 검사 (스타일로 현재 페이지 유추)
        page_links = page_nav.find_elements(By.CSS_SELECTOR, "a.page-link")
        for link in page_links:
            # 스타일 또는 클래스로 현재 페이지 확인
            is_current = False
            
            # 클래스 확인
            classes = link.get_attribute("class").split()
            if "active" in classes or "on" in classes or "current" in classes:
                is_current = True
            
            # 글꼴 두께 확인
            font_weight = link.value_of_css_property("font-weight")
            if font_weight in ["700", "bold"]:
                is_current = True
                
            # 배경색 확인
            bg_color = link.value_of_css_property("background-color")
            if bg_color != "rgba(0, 0, 0, 0)" and bg_color != "transparent":
                is_current = True
            
            if is_current:
                try:
                    return int(link.text.strip())
                except ValueError:
                    continue
        
        # URL에서 페이지 번호 추출 시도
        url = driver.current_url
        match = re.search(r'pageIndex=(\d+)', url)
        if match:
            return int(match.group(1))
            
        # 모든 방법으로 찾지 못한 경우 기본값 1 반환
        logger.warning("현재 페이지 번호를 찾지 못했습니다. 기본값 1을 사용합니다.")
        return 1
        
    except Exception as e:
        logger.error(f"현재 페이지 번호 확인 중 오류: {str(e)}")
        return 1

def wait_for_page_change(driver, previous_page):
    """
    페이지 변경을 기다리는 함수
    
    Args:
        driver: Selenium WebDriver 인스턴스
        previous_page: 이전 페이지 번호
        
    Returns:
        bool: 페이지 변경 성공 여부
    """
    try:
        # 페이지 로딩 대기
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CLASS_NAME, "board_list"))
        )
        
        # 스크립트 실행 완료 대기
        time.sleep(2)
        
        # 현재 페이지 번호 확인
        max_attempts = 5
        for attempt in range(max_attempts):
            current_page = get_current_page(driver)
            if current_page != previous_page:
                logger.info(f"페이지 변경 감지: {previous_page} → {current_page}")
                return True
                
            logger.debug(f"페이지 변경 대기 중... ({attempt+1}/{max_attempts})")
            time.sleep(1)
        
        logger.warning(f"페이지 변경 타임아웃: 아직 페이지 {previous_page}에 있습니다.")
        return False
        
    except Exception as e:
        logger.error(f"페이지 변경 대기 중 오류: {str(e)}")
        return False

def go_to_adjacent_page(driver, page_num):
    """
    인접한 페이지로 이동하는 함수
    
    Args:
        driver: Selenium WebDriver 인스턴스
        page_num: 이동하려는 페이지 번호
        
    Returns:
        bool: 페이지 이동 성공 여부
    """
    try:
        current_page = get_current_page(driver)
        
        # 현재 페이지와 같으면 이동 필요 없음
        if current_page == page_num:
            return True
            
        # 인접한 페이지가 아니면 오류
        if abs(current_page - page_num) != 1:
            logger.error(f"인접한 페이지가 아닙니다: {current_page} → {page_num}")
            return False
        
        # 페이지네이션 영역 찾기
        page_nav = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "pageNavi"))
        )
        
        # 목표 페이지 링크 찾기
        page_links = page_nav.find_elements(By.CSS_SELECTOR, "a.page-link")
        
        for link in page_links:
            try:
                if int(link.text.strip()) == page_num:
                    # JavaScript로 클릭 (더 안정적)
                    driver.execute_script("arguments[0].click();", link)
                    
                    # 페이지 변경 대기
                    return wait_for_page_change(driver, current_page)
            except ValueError:
                continue
        
        # 다음/이전 버튼으로 이동
        if page_num > current_page:
            next_buttons = page_nav.find_elements(By.CSS_SELECTOR, "a.next, a.page-navi.next")
            if next_buttons:
                driver.execute_script("arguments[0].click();", next_buttons[0])
                return wait_for_page_change(driver, current_page)
        else:
            prev_buttons = page_nav.find_elements(By.CSS_SELECTOR, "a.prev, a.page-navi.prev")
            if prev_buttons:
                driver.execute_script("arguments[0].click();", prev_buttons[0])
                return wait_for_page_change(driver, current_page)
        
        logger.error(f"페이지 {page_num}으로 이동할 수 있는 링크를 찾을 수 없습니다.")
        return False
        
    except Exception as e:
        logger.error(f"인접 페이지 {page_num}으로 이동 중 오류: {str(e)}")
        return False


def save_html_for_debugging(driver, name_prefix, include_iframe=True):
    """
    Save HTML content for debugging purposes.
    
    Args:
        driver: Selenium WebDriver instance
        name_prefix: Prefix for the file name
        include_iframe: Whether to also save iframe content if available
        
    Returns:
        None
    """
    timestamp = int(time.time())
    
    try:
        # Save main page HTML
        main_html = driver.page_source
        html_path = f"{name_prefix}_{timestamp}_main.html"
        
        with open(html_path, 'w', encoding='utf-8') as f:
            f.write(main_html)
        
        logger.info(f"Saved main page HTML: {html_path}")
        
        # Save iframe content if requested
        if include_iframe:
            try:
                # Find all iframes
                iframes = driver.find_elements(By.TAG_NAME, "iframe")
                
                for i, iframe in enumerate(iframes):
                    try:
                        # Switch to iframe
                        driver.switch_to.frame(iframe)
                        
                        # Get iframe HTML
                        iframe_html = driver.page_source
                        iframe_path = f"{name_prefix}_{timestamp}_iframe_{i+1}.html"
                        
                        with open(iframe_path, 'w', encoding='utf-8') as f:
                            f.write(iframe_html)
                        
                        logger.info(f"Saved iframe {i+1} HTML: {iframe_path}")
                        
                        # Switch back to main content
                        driver.switch_to.default_content()
                    except Exception as iframe_err:
                        logger.warning(f"Error saving iframe {i+1} HTML: {str(iframe_err)}")
                        try:
                            driver.switch_to.default_content()
                        except:
                            pass
            except Exception as iframes_err:
                logger.warning(f"Error finding/processing iframes: {str(iframes_err)}")
    
    except Exception as e:
        logger.error(f"Error saving HTML for debugging: {str(e)}")

def take_screenshot(driver, name, crop_area=None):
    """특정 페이지의 스크린샷 저장 (선택적 영역 잘라내기)"""
    try:
        screenshots_dir = Path("./screenshots")
        screenshots_dir.mkdir(exist_ok=True)
        
        screenshot_path = f"screenshots/{name}_{int(time.time())}.png"
        driver.save_screenshot(screenshot_path)
        
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
                    
                    # 크롭된 이미지 경로 반환
                    return crop_path
                # 요소 기준 잘라내기 (crop_area는 WebElement)
                elif hasattr(crop_area, 'location') and hasattr(crop_area, 'size'):
                    element = crop_area
                    location = element.location
                    size = element.size
                    
                    left = location['x']
                    top = location['y']
                    right = location['x'] + size['width']
                    bottom = location['y'] + size['height']
                    
                    cropped_img = img.crop((left, top, right, bottom))
                    
                    # 크롭된 이미지 저장
                    crop_path = screenshot_path.replace('.png', '_element_crop.png')
                    cropped_img.save(crop_path)
                    logger.info(f"요소 기준 크롭된 스크린샷 저장: {crop_path}")
                    
                    # 크롭된 이미지 경로 반환
                    return crop_path
            except ImportError:
                logger.warning("PIL 라이브러리가 설치되지 않아 크롭 불가")
            except Exception as crop_err:
                logger.warning(f"이미지 크롭 중 오류: {str(crop_err)}")
        
        logger.info(f"스크린샷 저장: {screenshot_path}")
        return screenshot_path
    except Exception as e:
        logger.error(f"스크린샷 저장 중 오류: {str(e)}")
        return None

def extract_data_from_screenshot(screenshot_path):
    """
    스크린샷에서 표 데이터를 추출하는 개선된 함수.
    이미지 전처리, 표 구조 감지, OCR 개선 등 포함.
    
    Args:
        screenshot_path (str): 스크린샷 파일 경로
        
    Returns:
        list: 추출된 DataFrame 리스트
    """
    try:
        import cv2
        import numpy as np
        import pandas as pd
        import pytesseract
        from PIL import Image, ImageEnhance
        
        logger.info(f"이미지 파일에서 표 데이터 추출 시작: {screenshot_path}")
        
        # 이미지 로드
        image = cv2.imread(screenshot_path)
        if image is None:
            logger.error(f"이미지를 로드할 수 없습니다: {screenshot_path}")
            return []
        
        # 원본 크기 저장
        original_height, original_width = image.shape[:2]
        logger.info(f"원본 이미지 크기: {original_width}x{original_height} 픽셀")
        
        # 이미지가 너무 크면 크기 조정
        max_dimension = 3000
        if max(original_height, original_width) > max_dimension:
            scale = max_dimension / max(original_height, original_width)
            new_width = int(original_width * scale)
            new_height = int(original_height * scale)
            image = cv2.resize(image, (new_width, new_height))
            logger.info(f"이미지 크기 조정: {new_width}x{new_height} 픽셀")
        
        # 1. 이미지 전처리 - 여러 방법으로 시도
        preprocessed_images = {}
        
        # 그레이스케일 변환
        gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
        preprocessed_images['gray'] = gray
        
        # 노이즈 제거
        denoised = cv2.fastNlMeansDenoising(gray, None, h=10, templateWindowSize=7, searchWindowSize=21)
        preprocessed_images['denoised'] = denoised
        
        # 이진화 - 여러 방법 적용
        # Otsu 이진화
        _, binary_otsu = cv2.threshold(denoised, 0, 255, cv2.THRESH_BINARY_INV + cv2.THRESH_OTSU)
        preprocessed_images['binary_otsu'] = binary_otsu
        
        # 적응형 이진화
        binary_adaptive = cv2.adaptiveThreshold(
            denoised, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C, cv2.THRESH_BINARY_INV, 11, 2
        )
        preprocessed_images['binary_adaptive'] = binary_adaptive
        
        # 2. 표 구조 감지
        # 이진 이미지에서 선 감지
        horizontal_kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (40, 1))
        vertical_kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (1, 40))
        
        # 수평선 감지
        horizontal_lines = cv2.morphologyEx(binary_adaptive, cv2.MORPH_OPEN, horizontal_kernel, iterations=2)
        horizontal_lines = cv2.dilate(horizontal_lines, horizontal_kernel, iterations=2)
        
        # 수직선 감지
        vertical_lines = cv2.morphologyEx(binary_adaptive, cv2.MORPH_OPEN, vertical_kernel, iterations=2)
        vertical_lines = cv2.dilate(vertical_lines, vertical_kernel, iterations=2)
        
        # 모든 선 합치기
        all_lines = cv2.add(horizontal_lines, vertical_lines)
        preprocessed_images['lines'] = all_lines
        
        # 침식 및 팽창을 통한 선 강화
        kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (3, 3))
        tables_structure = cv2.dilate(all_lines, kernel, iterations=2)
        preprocessed_images['tables_structure'] = tables_structure
        
        # 표 경계 감지
        contours, _ = cv2.findContours(tables_structure, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
        
        # 텍스트 추출용 이미지 준비
        ocr_image = cv2.bitwise_not(denoised)  # 검은 배경에 흰색 텍스트로 변환
        
        # 디버깅용 이미지 저장
        cv2.imwrite(f"{screenshot_path}_debug_gray.png", gray)
        cv2.imwrite(f"{screenshot_path}_debug_binary.png", binary_adaptive)
        cv2.imwrite(f"{screenshot_path}_debug_lines.png", all_lines)
        cv2.imwrite(f"{screenshot_path}_debug_tables.png", tables_structure)
        
        # 3. 감지된 테이블 처리
        all_dataframes = []
        
        if contours:
            logger.info(f"{len(contours)}개의 테이블 경계 후보 감지")
            
            # 이미지 면적의 1% 이상인 경계만 필터링
            min_area = (original_width * original_height) * 0.01
            table_contours = [cnt for cnt in contours if cv2.contourArea(cnt) > min_area]
            
            if table_contours:
                logger.info(f"크기 필터링 후 {len(table_contours)}개 테이블 경계 남음")
                
                # 각 테이블 영역 처리
                for i, contour in enumerate(table_contours):
                    # 경계 상자 얻기
                    x, y, w, h = cv2.boundingRect(contour)
                    
                    # 테이블 영역 추출
                    table_img = ocr_image[y:y+h, x:x+w]
                    
                    # 테이블 구조 이미지 저장
                    table_img_path = f"{screenshot_path}_table_{i+1}.png"
                    cv2.imwrite(table_img_path, table_img)
                    
                    # 테이블 이미지에서 텍스트 추출
                    try:
                        # PIL로 이미지 향상 처리
                        pil_img = Image.open(table_img_path)
                        enhancer = ImageEnhance.Contrast(pil_img)
                        enhanced_img = enhancer.enhance(2.0)  # 대비 증가
                        
                        # 향상된 이미지 저장
                        enhanced_path = f"{screenshot_path}_table_{i+1}_enhanced.png"
                        enhanced_img.save(enhanced_path)
                        
                        # 표 구조 감지
                        df = extract_table_structure(enhanced_path, i+1)
                        if df is not None and not df.empty:
                            logger.info(f"테이블 {i+1}: 표 구조 감지로 {df.shape[0]}행 {df.shape[1]}열 추출")
                            all_dataframes.append(df)
                        else:
                            # 표 구조 감지에 실패하면 직접 OCR
                            logger.info(f"테이블 {i+1}: 표 구조 감지 실패, 전체 OCR 시도")
                            text = pytesseract.image_to_string(enhanced_img, lang='kor+eng', config='--psm 6')
                            
                            # OCR 텍스트에서 표 구조 추출
                            table_data = parse_text_to_table(text)
                            if table_data and len(table_data) > 1:  # 헤더 + 데이터 행
                                headers = table_data[0]
                                data = table_data[1:]
                                df = pd.DataFrame(data, columns=headers)
                                logger.info(f"테이블 {i+1}: OCR 텍스트 파싱으로 {df.shape[0]}행 {df.shape[1]}열 추출")
                                all_dataframes.append(df)
                    except Exception as table_err:
                        logger.warning(f"테이블 {i+1} 처리 중 오류: {str(table_err)}")
        
        # 4. 표 구조 감지에 실패하면 전체 이미지에서 데이터 추출 시도
        if not all_dataframes:
            logger.info("테이블 구조 감지 실패, 전체 이미지 처리")
            
            # 전체 이미지 대비 향상
            pil_img = Image.open(screenshot_path)
            enhancer = ImageEnhance.Contrast(pil_img)
            enhanced_img = enhancer.enhance(2.0)
            enhanced_path = f"{screenshot_path}_full_enhanced.png"
            enhanced_img.save(enhanced_path)
            
            # 1) 테서렉트 표 인식 시도
            try:
                # --psm 6: 균일한 텍스트 블록으로 처리
                # -c tessedit_create_tsv=1: TSV 형식으로 출력
                tsv_output = pytesseract.image_to_data(enhanced_img, lang='kor+eng', config='--psm 6 -c tessedit_create_tsv=1', output_type=pytesseract.Output.DATAFRAME)
                
                # 테서렉트 TSV 데이터에서 행/열 구조 추출
                if not tsv_output.empty:
                    # 신뢰도 임계값 이상인 텍스트만 사용
                    tsv_output = tsv_output[tsv_output['conf'] > 30]
                    
                    # 공백이 아닌 텍스트만 사용
                    tsv_output = tsv_output[tsv_output['text'].str.strip() != '']
                    
                    # Y 좌표 기준으로 텍스트 그룹화 (같은 Y는 같은 행)
                    y_tolerance = 10  # Y 좌표 허용 오차
                    tsv_output['line_group'] = (tsv_output['top'].diff() > y_tolerance).cumsum()
                    
                    rows_data = []
                    line_groups = tsv_output.groupby('line_group')
                    
                    for _, group in line_groups:
                        # X 좌표로 정렬하여 왼쪽에서 오른쪽으로 텍스트 배치
                        sorted_group = group.sort_values('left')
                        row_text = sorted_group['text'].tolist()
                        if row_text:
                            rows_data.append(row_text)
                    
                    if len(rows_data) > 1:  # 헤더 + 데이터 행
                        headers = rows_data[0]
                        data = rows_data[1:]
                        
                        # 헤더가 빈 경우 자동 생성
                        if not any(headers):
                            headers = [f"Column_{i}" for i in range(len(data[0]))]
                        
                        # 행 길이 일치시키기
                        max_cols = len(headers)
                        for i in range(len(data)):
                            if len(data[i]) < max_cols:
                                data[i].extend([''] * (max_cols - len(data[i])))
                            elif len(data[i]) > max_cols:
                                data[i] = data[i][:max_cols]
                        
                        df = pd.DataFrame(data, columns=headers)
                        logger.info(f"테서렉트 TSV에서 {df.shape[0]}행 {df.shape[1]}열 추출")
                        all_dataframes.append(df)
            except Exception as tsv_err:
                logger.warning(f"테서렉트 TSV 처리 중 오류: {str(tsv_err)}")
            
            # 2) 테서렉트 호크 레이아웃 분석 시도
            if not all_dataframes:
                try:
                    # --psm 4: 열이 있는 텍스트로 인식
                    # --psm 6: 균일한 텍스트 블록으로 처리
                    for psm in [4, 6]:
                        hocr_config = f'--psm {psm} -c tessedit_create_hocr=1'
                        hocr = pytesseract.image_to_pdf_or_hocr(enhanced_img, lang='kor+eng', config=hocr_config, extension='hocr')
                        hocr_text = hocr.decode('utf-8')
                        
                        # HOCR 데이터에서 표 구조 추출 시도
                        df = parse_hocr_to_table(hocr_text)
                        if df is not None and not df.empty:
                            logger.info(f"HOCR(PSM {psm})에서 {df.shape[0]}행 {df.shape[1]}열 추출")
                            all_dataframes.append(df)
                            break
                except Exception as hocr_err:
                    logger.warning(f"HOCR 처리 중 오류: {str(hocr_err)}")
            
            # 3) 일반 텍스트 추출 및 표 구조 추론
            if not all_dataframes:
                try:
                    text = pytesseract.image_to_string(enhanced_img, lang='kor+eng', config='--psm 6')
                    table_data = parse_text_to_table(text)
                    
                    if table_data and len(table_data) > 1:  # 헤더 + 데이터 행
                        headers = table_data[0]
                        data = table_data[1:]
                        df = pd.DataFrame(data, columns=headers)
                        logger.info(f"일반 OCR에서 {df.shape[0]}행 {df.shape[1]}열 추출")
                        all_dataframes.append(df)
                except Exception as text_err:
                    logger.warning(f"일반 텍스트 처리 중 오류: {str(text_err)}")
        
        # 5. 추출된 데이터프레임 정제
        refined_dataframes = []
        for idx, df in enumerate(all_dataframes):
            try:
                if df.empty:
                    continue
                
                # 빈 행/열 제거
                df = df.replace('', np.nan)
                df = df.dropna(how='all', axis=0).dropna(how='all', axis=1)
                df = df.reset_index(drop=True)
                
                # NaN을 빈 문자열로 변환
                df = df.fillna('')
                
                # 중복 행 제거
                df = df.drop_duplicates().reset_index(drop=True)
                
                # 숫자 데이터 정수/실수 변환
                for col in df.columns:
                    try:
                        # 숫자 형식인지 확인 (쉼표, 소수점 등 처리)
                        numeric_series = df[col].str.replace(',', '').str.replace(' ', '')
                        if numeric_series.str.match(r'^-?\d+\.?\d*$').mean() > 0.5:  # 50% 이상이 숫자 패턴과 일치
                            df[col] = pd.to_numeric(numeric_series, errors='coerce')
                    except:
                        pass  # 숫자 변환 실패 시 문자열 유지
                
                # 첫 행이 헤더인지 확인
                first_row_as_header = all_headers_valid(df)
                if first_row_as_header and len(df) > 1:
                    new_headers = df.iloc[0].tolist()
                    df = df.iloc[1:].reset_index(drop=True)
                    df.columns = [str(h) if h else f"Column_{i}" for i, h in enumerate(new_headers)]
                
                # 열 이름 정리
                df.columns = [str(col) for col in df.columns]
                
                # 데이터프레임에 의미 있는 데이터가 있는지 확인
                if df.shape[0] >= 2 and df.shape[1] >= 2:
                    refined_dataframes.append(df)
                    logger.info(f"데이터프레임 {idx+1} 정제 완료: {df.shape[0]}행 {df.shape[1]}열")
            except Exception as refine_err:
                logger.warning(f"데이터프레임 {idx+1} 정제 중 오류: {str(refine_err)}")
        
        if not refined_dataframes:
            logger.warning("정제된 데이터프레임이 없음, 텍스트 전용 추출 시도")
            df = extract_text_without_table_structure(screenshot_path)
            if df is not None and not df.empty:
                refined_dataframes.append(df)
        
        logger.info(f"최종 추출된 데이터프레임: {len(refined_dataframes)}개")
        return refined_dataframes
        
    except Exception as e:
        logger.error(f"스크린샷에서 데이터 추출 중 오류: {str(e)}")
        # 기본 텍스트 추출 시도
        try:
            return [extract_text_without_table_structure(screenshot_path)]
        except:
            logger.error("기본 텍스트 추출도 실패")
            return []

def all_headers_valid(df):
    """
    첫 번째 행이 헤더로 유효한지 확인합니다.
    
    Args:
        df: pandas DataFrame
        
    Returns:
        bool: 첫 번째 행이 헤더로 유효하면 True
    """
    if df.empty or len(df) < 2:
        return False
        
    try:
        # 첫 번째 행에 숫자가 많으면 헤더가 아닐 가능성이 높음
        first_row = df.iloc[0]
        first_row_numeric = 0
        for value in first_row:
            if isinstance(value, (int, float)) or (isinstance(value, str) and value.replace('.', '').replace(',', '').isdigit()):
                first_row_numeric += 1
                
        # 50% 이상이 숫자면 헤더가 아닌 것으로 판단
        if first_row_numeric / len(first_row) > 0.5:
            return False
            
        # 다른 행과 형식이 다른지 확인
        other_rows_numeric = 0
        for i in range(1, min(5, len(df))):
            row = df.iloc[i]
            for value in row:
                if isinstance(value, (int, float)) or (isinstance(value, str) and value.replace('.', '').replace(',', '').isdigit()):
                    other_rows_numeric += 1
                    
        # 다른 행의 숫자 비율이 첫 행과 크게 다르면 헤더로 판단
        other_rows_ratio = other_rows_numeric / (min(5, len(df) - 1) * len(first_row)) if min(5, len(df) - 1) > 0 else 0
        if abs(first_row_numeric / len(first_row) - other_rows_ratio) > 0.3:
            return True
            
        # 첫 번째 행의 값이 짧은지 확인 (헤더는 보통 짧음)
        first_row_length = sum(len(str(v)) for v in first_row) / len(first_row)
        other_rows_length = 0
        count = 0
        
        for i in range(1, min(5, len(df))):
            row = df.iloc[i]
            for value in row:
                other_rows_length += len(str(value))
                count += 1
                
        other_rows_avg_length = other_rows_length / count if count > 0 else 0
        
        # 첫 행의 텍스트가 다른 행보다 훨씬 짧으면 헤더
        if first_row_length < other_rows_avg_length * 0.7:
            return True
            
        # 기본적으로 첫 행을 헤더로 간주하지 않음
        return False
        
    except Exception as e:
        logger.warning(f"헤더 유효성 검사 오류: {str(e)}")
        return False



def parse_text_to_table(text):
    """
    OCR로 인식된 텍스트에서 표 구조를 추출합니다.
    
    Args:
        text: OCR로 인식된 텍스트
        
    Returns:
        list: 행 데이터 리스트 또는 None
    """
    try:
        # 텍스트를 줄 단위로 분할
        lines = [line.strip() for line in text.split('\n') if line.strip()]
        
        if not lines:
            return None
            
        # 구분자 패턴 찾기
        possible_delimiters = ['\t', '  ', ' | ', '|', ';', ',']
        best_delimiter = None
        max_columns = 0
        
        for delimiter in possible_delimiters:
            # 각 구분자로 분할한 최대 열 수 계산
            columns_count = []
            for line in lines:
                columns = line.split(delimiter)
                if delimiter in '| ':  # 공백이나 파이프 구분자는 여러 개가 연속될 수 있음
                    columns = [col for col in columns if col.strip()]
                columns_count.append(len(columns))
            
            avg_columns = sum(columns_count) / len(columns_count) if columns_count else 0
            
            # 구분자로 분할했을 때 열 수가 일정해야 함
            if avg_columns > max_columns and avg_columns >= 2:
                consistency = 1 - (max(columns_count) - min(columns_count)) / max(max(columns_count), 1)
                if consistency > 0.5:  # 열 수가 일관적이면 (50% 이상 일치)
                    max_columns = avg_columns
                    best_delimiter = delimiter
        
        # 표로 볼 수 있는 구분자를 찾지 못한 경우
        if not best_delimiter:
            # 공백 기반 열 구분 시도 (위치 기반)
            return parse_positional_table(lines)
            
        # 최적의 구분자로 텍스트 파싱
        table_data = []
        for line in lines:
            columns = line.split(best_delimiter)
            if best_delimiter in '| ':  # 공백이나 파이프 구분자는 여러 개가 연속될 수 있음
                columns = [col for col in columns if col.strip()]
            table_data.append(columns)
        
        return table_data
        
    except Exception as e:
        logger.warning(f"텍스트 표 파싱 오류: {str(e)}")
        return None


def parse_positional_table(lines):
    """
    공백 위치를 기반으로 표 구조를 추출합니다.
    OCR된 텍스트에서 일정한 위치에 공백이 있는 경우 사용.
    
    Args:
        lines: 텍스트 라인 리스트
        
    Returns:
        list: 행 데이터 리스트 또는 None
    """
    try:
        if not lines or len(lines) < 2:
            return None
            
        # 모든 라인의 공백 위치 찾기
        space_positions = []
        max_line_length = max(len(line) for line in lines)
        
        for line in lines:
            positions = []
            for i, char in enumerate(line):
                if char == ' ' and i > 0 and i < len(line) - 1:
                    # 앞뒤 문자가 공백이 아닌 경우만 (연속 공백 방지)
                    if line[i-1] != ' ' and line[i+1] != ' ':
                        positions.append(i)
            space_positions.append(positions)
        
        # 모든 줄에서 공통적으로 나타나는 공백 위치 찾기
        common_positions = set(range(max_line_length))
        for positions in space_positions:
            position_set = set()
            for pos in positions:
                # 위치 주변 약간의 여유 허용 (OCR 오차 고려)
                for p in range(max(0, pos-2), min(max_line_length, pos+3)):
                    position_set.add(p)
            common_positions &= position_set
        
        # 공통 위치가 없으면 작업 종료
        if not common_positions:
            # 다른 전략: 길이 기반 아이템 추출
            return split_by_word_length(lines)
        
        # 공통 공백 위치 정렬
        split_positions = sorted(list(common_positions))
        
        # 너무 많은 분할 위치가 있으면 필터링
        if len(split_positions) > 10:
            # 위치 간의 간격이 너무 작으면 하나만 유지
            filtered_positions = [split_positions[0]]
            for pos in split_positions[1:]:
                if pos - filtered_positions[-1] > 3:  # 최소 3자 이상 간격
                    filtered_positions.append(pos)
            split_positions = filtered_positions
        
        # 분할 위치를 사용하여 각 행 분할
        table_data = []
        for line in lines:
            row_data = []
            start = 0
            
            for pos in split_positions:
                if pos > start:
                    cell = line[start:pos].strip()
                    if cell:
                        row_data.append(cell)
                    start = pos + 1
            
            # 마지막 셀
            if start < len(line):
                cell = line[start:].strip()
                if cell:
                    row_data.append(cell)
            
            if row_data:
                table_data.append(row_data)
        
        return table_data
        
    except Exception as e:
        logger.warning(f"위치 기반 표 파싱 오류: {str(e)}")
        return None

def split_by_word_length(lines):
    """
    단어 길이를 기준으로 텍스트를 표로 변환합니다.
    Heuristic: 첫 번째 행의 각 단어 길이를 기준으로 분할.
    
    Args:
        lines: 텍스트 라인 리스트
        
    Returns:
        list: 행 데이터 리스트 또는 None
    """
    try:
        if not lines or len(lines) < 2:
            return None
        
        # 첫 번째 행을 공백으로 분할
        first_row_words = lines[0].split()
        
        if len(first_row_words) < 2:
            return None
        
        # 공백만으로 각 행 분할
        table_data = []
        for line in lines:
            row_data = line.split()
            if row_data:
                table_data.append(row_data)
        
        return table_data
        
    except Exception as e:
        logger.warning(f"단어 길이 기반 표 파싱 오류: {str(e)}")
        return None

def parse_hocr_to_table(hocr_text):
    """
    HOCR 출력에서 표 구조를 추출합니다.
    
    Args:
        hocr_text: Tesseract HOCR 출력 텍스트
        
    Returns:
        pandas.DataFrame: 추출된 표 또는 None
    """
    try:
        import re
        import pandas as pd
        
        # HOCR에서 단어 추출
        word_pattern = re.compile(r'<span class=\'ocrx_word\'[^>]*title=\'bbox (\d+) (\d+) (\d+) (\d+)[^\']*\'[^>]*>([^<]+)</span>')
        words = word_pattern.findall(hocr_text)
        
        if not words:
            return None
        
        # 단어 정보 추출 (x1, y1, x2, y2, 텍스트)
        word_data = []
        for match in words:
            x1, y1, x2, y2, text = int(match[0]), int(match[1]), int(match[2]), int(match[3]), match[4]
            word_data.append((x1, y1, x2, y2, text))
        
        # y 좌표로 행 그룹화 (같은 y는 같은 행)
        y_tolerance = 10  # y 좌표 허용 오차
        word_data.sort(key=lambda w: w[1])  # y 좌표로 정렬
        
        rows = []
        current_row = [word_data[0]]
        current_y = word_data[0][1]
        
        for word in word_data[1:]:
            y = word[1]
            if abs(y - current_y) <= y_tolerance:
                current_row.append(word)
            else:
                # 각 행 내에서 단어를 x 좌표로 정렬
                current_row.sort(key=lambda w: w[0])
                rows.append(current_row)
                current_row = [word]
                current_y = y
        
        if current_row:
            current_row.sort(key=lambda w: w[0])
            rows.append(current_row)
        
        # 행 데이터 추출
        table_data = []
        for row in rows:
            row_text = [word[4] for word in row]
            table_data.append(row_text)
        
        if len(table_data) < 2:  # 헤더 + 데이터 행 필요
            return None
        
        # 행 길이 일치시키기
        max_cols = max(len(row) for row in table_data)
        for i in range(len(table_data)):
            if len(table_data[i]) < max_cols:
                table_data[i].extend([''] * (max_cols - len(table_data[i])))
        
        # DataFrame 생성
        headers = table_data[0]
        data = table_data[1:]
        
        # 빈 헤더 처리
        headers = [h if h else f"Column_{i}" for i, h in enumerate(headers)]
        
        df = pd.DataFrame(data, columns=headers)
        return df
        
    except Exception as e:
        logger.warning(f"HOCR 표 파싱 오류: {str(e)}")
        return None


def extract_table_structure(image_path, table_idx):
    """
    이미지에서 표 구조 추출을 시도합니다.
    셀 경계 감지 및 셀 내용 OCR을 수행합니다.
    
    Args:
        image_path: 이미지 파일 경로
        table_idx: 테이블 인덱스 (로깅용)
        
    Returns:
        pandas.DataFrame: 추출된 표 데이터 또는 None
    """
    try:
        import cv2
        import numpy as np
        import pytesseract
        
        # 이미지 로드
        image = cv2.imread(image_path)
        if image is None:
            return None
            
        gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
        
        # 적응형 이진화
        binary = cv2.adaptiveThreshold(
            gray, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C, cv2.THRESH_BINARY_INV, 11, 2
        )
        
        # 노이즈 제거
        kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (2, 2))
        binary = cv2.morphologyEx(binary, cv2.MORPH_OPEN, kernel, iterations=1)
        
        # 수직선 감지
        vertical_kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (1, 25))
        vertical_lines = cv2.morphologyEx(binary, cv2.MORPH_OPEN, vertical_kernel, iterations=2)
        
        # 수평선 감지
        horizontal_kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (25, 1))
        horizontal_lines = cv2.morphologyEx(binary, cv2.MORPH_OPEN, horizontal_kernel, iterations=2)
        
        # 선 합치기
        table_grid = cv2.add(vertical_lines, horizontal_lines)
        
        # 선 강화
        kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (3, 3))
        table_grid = cv2.dilate(table_grid, kernel, iterations=1)
        
        # 셀 경계 찾기
        contours, _ = cv2.findContours(table_grid, cv2.RETR_TREE, cv2.CHAIN_APPROX_SIMPLE)
        
        # 충분한 수의 셀이 있는지 확인
        if len(contours) < 10:
            return None
            
        # 셀 경계 정보 추출
        cells = []
        min_cell_area = (image.shape[0] * image.shape[1]) / 1000  # 이미지 크기에 비례한 최소 셀 크기
        
        for cnt in contours:
            area = cv2.contourArea(cnt)
            if area > min_cell_area:
                x, y, w, h = cv2.boundingRect(cnt)
                cells.append((x, y, w, h))
        
        # 셀 위치로 행/열 구조 추정
        cells.sort(key=lambda c: c[1])  # y 좌표로 정렬
        
        # 행 그룹화
        rows = []
        y_tolerance = image.shape[0] // 40  # 이미지 높이의 2.5%를 허용 오차로 사용
        
        current_row = [cells[0]] if cells else []
        current_y = cells[0][1] if cells else 0
        
        for cell in cells[1:]:
            y = cell[1]
            if abs(y - current_y) <= y_tolerance:
                current_row.append(cell)
            else:
                rows.append(current_row)
                current_row = [cell]
                current_y = y
        
        if current_row:
            rows.append(current_row)
        
        # 각 행 내에서 셀을 x 좌표로 정렬
        for row in rows:
            row.sort(key=lambda c: c[0])
        
        # 행/열 수 결정
        num_rows = len(rows)
        if num_rows < 2:  # 헤더와 최소 1개 데이터 행 필요
            return None
            
        # 열 수는 가장 많은 셀을 가진 행 기준
        num_cols = max(len(row) for row in rows)
        
        logger.info(f"테이블 {table_idx} 구조: {num_rows}행 x {num_cols}열")
        
        # 인식된 OCR 텍스트를 저장할 그리드
        table_data = []
        
        # 각 행 처리
        for row_idx, row in enumerate(rows):
            row_data = [''] * num_cols  # 빈 셀로 초기화
            
            for col_idx, (x, y, w, h) in enumerate(row):
                if col_idx >= num_cols:
                    continue
                    
                # 이미지에서 셀 영역 추출
                cell_img = gray[y:y+h, x:x+w]
                
                # 셀 이미지가 너무 작으면 건너뛰기
                if cell_img.size == 0 or w < 5 or h < 5:
                    continue
                
                # 셀 이미지 향상
                _, cell_thresh = cv2.threshold(cell_img, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
                
                # OCR 구성
                if row_idx == 0:
                    # 헤더용 구성
                    custom_config = r'--psm 6 --oem 3'
                elif col_idx == 0:
                    # 첫 번째 열(항목)용 구성
                    custom_config = r'--psm 6 --oem 3'
                else:
                    # 데이터 셀용 구성 (숫자에 최적화)
                    custom_config = r'--psm 7 --oem 3'
                
                # OCR 실행
                text = pytesseract.image_to_string(cell_thresh, lang='kor+eng', config=custom_config).strip()
                
                # 추출된 텍스트가 있으면 저장
                if text:
                    # 공백 및 개행 정리
                    text = ' '.join(text.split())
                    row_data[col_idx] = text
            
            # 유효한 데이터가 있는 행만 추가
            if any(cell for cell in row_data):
                table_data.append(row_data)
        
        # 충분한 데이터가 있는지 확인
        if len(table_data) < 2 or max(len(row) for row in table_data) < 2:
            return None
            
        # 첫 번째 행을 헤더로 사용
        headers = table_data[0]
        data = table_data[1:]
        
        # 빈 헤더 처리
        headers = [h if h else f"Column_{i}" for i, h in enumerate(headers)]
        
        # DataFrame 생성
        df = pd.DataFrame(data, columns=headers)
        return df
        
    except Exception as e:
        logger.warning(f"표 구조 추출 중 오류: {str(e)}")
        return None


def extract_text_without_table_structure(screenshot_path):
    """
    표 구조 없이 이미지에서 텍스트를 추출하는 개선된 함수
    
    Args:
        screenshot_path: 스크린샷 파일 경로
        
    Returns:
        pandas.DataFrame: 추출된 텍스트를 포함한 DataFrame
    """
    try:
        from PIL import Image, ImageEnhance
        import pytesseract
        import pandas as pd
        
        logger.info(f"표 구조 없이 텍스트 추출 시작: {screenshot_path}")
        
        # 이미지 로드 및 향상
        img = Image.open(screenshot_path)
        enhancer = ImageEnhance.Contrast(img)
        enhanced_img = enhancer.enhance(2.0)
        
        # OCR 실행 (여러 PSM 모드 시도)
        best_text = ""
        best_line_count = 0
        
        for psm in [6, 4, 3]:  # 다양한 페이지 분할 모드 시도
            text = pytesseract.image_to_string(enhanced_img, lang='kor+eng', config=f'--psm {psm}')
            lines = [line for line in text.split('\n') if line.strip()]
            
            if len(lines) > best_line_count:
                best_text = text
                best_line_count = len(lines)
        
        if not best_text:
            logger.warning("텍스트를 추출하지 못했습니다")
            return pd.DataFrame()
            
        # 텍스트를 줄 단위로 분할
        lines = [line.strip() for line in best_text.split('\n') if line.strip()]
        
        # 표 구조 추론
        table_data = parse_text_to_table(best_text)
        
        if table_data and len(table_data) > 1:
            # 첫 행을 헤더로 사용
            headers = table_data[0]
            data = table_data[1:]
            
            # 빈 헤더 처리
            headers = [h if h else f"Column_{i}" for i, h in enumerate(headers)]
            
            df = pd.DataFrame(data, columns=headers)
            logger.info(f"텍스트 파싱으로 표 구조 추출: {df.shape[0]}행 {df.shape[1]}열")
            return df
        
        # 표 구조를 찾지 못한 경우 두 열 구조로 변환
        col_name = "텍스트"
        df = pd.DataFrame({col_name: lines})
        
        # 행 번호 추가
        df.insert(0, "행", range(1, len(df) + 1))
        
        logger.info(f"텍스트만 추출: {df.shape[0]}행")
        return df
    
    except Exception as e:
        logger.error(f"텍스트 전용 추출 오류: {str(e)}")
        # 최소한의 데이터라도 반환
        return pd.DataFrame({"오류": [f"텍스트 추출 실패: {str(e)}"]})
        

def execute_javascript(driver, script, async_script=False, max_retries=3, description=""):
    """JavaScript 실행 유틸리티 메서드 (오류 처리 및 재시도 포함)"""
    retry_count = 0
    last_error = None
    
    while retry_count < max_retries:
        try:
            if async_script:
                return driver.execute_async_script(script)
            else:
                return driver.execute_script(script)
        except Exception as e:
            retry_count += 1
            last_error = e
            logger.warning(f"{description} JavaScript 실행 실패 (시도 {retry_count}/{max_retries}): {str(e)}")
            time.sleep(1)  # 재시도 전 대기
    
    logger.error(f"{description} JavaScript 최대 재시도 횟수 초과: {str(last_error)}")
    return None

def reset_browser_context(driver, delete_cookies=True, navigate_to_blank=True):
    """브라우저 컨텍스트 초기화 (쿠키 삭제 및 빈 페이지로 이동)"""
    try:
        # 쿠키 삭제
        if delete_cookies:
            driver.delete_all_cookies()
            logger.info("모든 쿠키 삭제 완료")
            
        # 빈 페이지로 이동
        if navigate_to_blank:
            driver.get("about:blank")
            logger.info("빈 페이지로 이동 완료")
            
        # 로컬 스토리지 및 세션 스토리지 클리어
        try:
            driver.execute_script("localStorage.clear(); sessionStorage.clear();")
            logger.info("로컬 스토리지 및 세션 스토리지 클리어")
        except Exception as js_err:
            logger.warning(f"스토리지 클리어 중 오류: {str(js_err)}")
            
        return True
    except Exception as e:
        logger.error(f"브라우저 컨텍스트 초기화 실패: {str(e)}")
        return False



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


def direct_access_view_link_params(driver, post):
    """직접 URL로 게시물 바로보기 링크 파라미터 접근"""
    try:
        if not post.get('post_id'):
            logger.error(f"직접 URL 접근 불가 {post['title']} - post_id 누락")
            return None
            
        logger.info(f"게시물 직접 URL 접근 시도: {post['title']}")
        
        # 게시물 상세 URL 구성
        post_url = f"https://www.msit.go.kr/bbs/view.do?sCode=user&mId=99&mPid=74&nttSeqNo={post['post_id']}"
        
        # 현재 URL 저장
        current_url = driver.current_url
        
        # 게시물 상세 페이지 접속
        driver.get(post_url)
        time.sleep(3)  # 페이지 로드 대기
        
        # 페이지 로드 확인
        try:
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CLASS_NAME, "view_head"))
            )
            logger.info(f"게시물 상세 페이지 로드 완료: {post['title']}")
        except TimeoutException:
            logger.warning(f"게시물 상세 페이지 로드 시간 초과: {post['title']}")
        
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
            
            # 날짜 정보 추출 (내용이 없어도 날짜는 반환)
            date_match = re.search(r'\((\d{4})년\s*(\d{1,2})월말\s*기준\)', post['title'])
            if date_match:
                year = int(date_match.group(1))
                month = int(date_match.group(2))
                
                return {
                    'content': "내용 없음",
                    'date': {'year': year, 'month': month},
                    'post_info': post
                }
                
        except Exception as link_err:
            logger.error(f"바로보기 링크 찾기 중 오류: {str(link_err)}")
            
            # 오류 발생 시에도 날짜 정보 추출 시도
            date_match = re.search(r'\((\d{4})년\s*(\d{1,2})월말\s*기준\)', post['title'])
            if date_match:
                year = int(date_match.group(1))
                month = int(date_match.group(2))
                
                return {
                    'content': f"오류 발생: {str(link_err)}",
                    'date': {'year': year, 'month': month},
                    'post_info': post
                }
        
        # 원래 페이지로 돌아가기
        driver.get(current_url)
        
        return None
        
    except Exception as e:
        logger.error(f"직접 URL 접근 중 오류: {str(e)}")
        
        try:
            # 원래 페이지로 돌아가기
            driver.get(current_url)
        except:
            pass
            
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
                    # 직접 URL 접근 방식으로 대체
                    logger.info("클릭 방식 실패, 직접 URL 접근 방식으로 대체")
                    return direct_access_view_link_params(driver, post)
            
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
                    
                    # 직접 URL 접근 방식으로 대체
                    return direct_access_view_link_params(driver, post)
            
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

def try_ajax_access(driver, post):
    """AJAX 방식으로 게시물 데이터 접근 시도"""
    if not post.get('post_id'):
        logger.error(f"AJAX 접근 불가 {post['title']} - post_id 누락")
        return None
        
    try:
        logger.info(f"AJAX 방식으로 게시물 데이터 접근 시도: {post['title']}")
        
        # AJAX 요청 실행
        script = f"""
            return new Promise((resolve, reject) => {{
                const xhr = new XMLHttpRequest();
                xhr.open('GET', '/bbs/ajaxView.do?sCode=user&mId=99&mPid=74&nttSeqNo={post['post_id']}', true);
                xhr.setRequestHeader('Content-Type', 'application/json');
                xhr.onload = function() {{
                    if (this.status >= 200 && this.status < 300) {{
                        resolve(xhr.responseText);
                    }} else {{
                        reject(xhr.statusText);
                    }}
                }};
                xhr.onerror = function() {{
                    reject(xhr.statusText);
                }};
                xhr.send();
            }});
        """
        
        try:
            result = driver.execute_async_script(script)
            logger.info(f"AJAX 호출 결과: {result[:100] if result else '결과 없음'}...")  # 처음 100자만 로깅
            
            if not result:
                logger.warning("AJAX 호출 결과가 없습니다")
                return None
                
            # JSON 파싱 및 데이터 추출
            try:
                data = json.loads(result)
                logger.info(f"AJAX 데이터 파싱 성공: {str(data)[:200]}")
                
                # 날짜 정보 추출
                date_match = re.search(r'\((\d{4})년\s*(\d{1,2})월말\s*기준\)', post['title'])
                if date_match:
                    year = int(date_match.group(1))
                    month = int(date_match.group(2))
                    
                    return {
                        'ajax_data': data,
                        'date': {'year': year, 'month': month},
                        'post_info': post
                    }
                
                return {
                    'ajax_data': data,
                    'post_info': post
                }
            except json.JSONDecodeError:
                logger.warning("AJAX 응답을 JSON으로 파싱할 수 없습니다")
                
                # 날짜 정보 추출
                date_match = re.search(r'\((\d{4})년\s*(\d{1,2})월말\s*기준\)', post['title'])
                if date_match:
                    year = int(date_match.group(1))
                    month = int(date_match.group(2))
                    
                    return {
                        'content': result[:1000],  # 긴 내용은 제한
                        'date': {'year': year, 'month': month},
                        'post_info': post
                    }
                
        except Exception as script_err:
            logger.warning(f"AJAX 스크립트 실행 오류: {str(script_err)}")
            
        # 대체 AJAX 엔드포인트 시도
        try:
            alternate_script = f"""
                return new Promise((resolve, reject) => {{
                    const xhr = new XMLHttpRequest();
                    xhr.open('GET', '/bbs/getPost.do?nttSeqNo={post['post_id']}', true);
                    xhr.setRequestHeader('Content-Type', 'application/json');
                    xhr.onload = function() {{
                        if (this.status >= 200 && this.status < 300) {{
                            resolve(xhr.responseText);
                        }} else {{
                            reject(xhr.statusText);
                        }}
                    }};
                    xhr.onerror = function() {{
                        reject(xhr.statusText);
                    }};
                    xhr.send();
                }});
            """
            
            result = driver.execute_async_script(alternate_script)
            logger.info(f"대체 AJAX 호출 결과: {result[:100] if result else '결과 없음'}...")
            
            if result:
                # 날짜 정보 추출
                date_match = re.search(r'\((\d{4})년\s*(\d{1,2})월말\s*기준\)', post['title'])
                if date_match:
                    year = int(date_match.group(1))
                    month = int(date_match.group(2))
                    
                    return {
                        'content': result[:1000],  # 긴 내용은 제한
                        'date': {'year': year, 'month': month},
                        'post_info': post
                    }
        except Exception as alt_err:
            logger.warning(f"대체 AJAX 시도 오류: {str(alt_err)}")
            
        return None
        
    except Exception as e:
        logger.error(f"AJAX 접근 시도 중 오류: {str(e)}")
        return None
        


def access_iframe_with_ocr_fallback(driver, file_params):
    """
    iframe 직접 접근 시도 후 실패시 OCR 추출 사용
    향상된 접근 방식과 다양한 추출 전략 포함
    
    Args:
        driver: Selenium WebDriver 인스턴스
        file_params: 파일 파라미터를 포함한 딕셔너리
        
    Returns:
        dict: 시트 이름을 키로, DataFrame을 값으로 하는 딕셔너리 또는 None
    """
    # 1. 기존 iframe 접근 방식 시도
    sheets_data = access_iframe_direct(driver, file_params)
    
    # 정상적으로 데이터 추출에 성공한 경우
    if sheets_data and any(not df.empty for df in sheets_data.values()):
        logger.info(f"iframe 직접 접근으로 데이터 추출 성공: {len(sheets_data)}개 시트 추출")
        
        # 데이터 품질 검증
        valid_sheets = {}
        for sheet_name, df in sheets_data.items():
            if df is not None and not df.empty:
                # 기본 품질 체크: 합리적인 데이터가 있는지 확인
                if df.shape[0] >= 2 and df.shape[1] >= 2:
                    valid_sheets[sheet_name] = df
                    logger.info(f"시트 {sheet_name} 검증 완료: {df.shape[0]}행 {df.shape[1]}열")
                else:
                    logger.warning(f"시트 {sheet_name}의 데이터가 불충분합니다: {df.shape}")
        
        # 유효한 시트가 있으면 반환
        if valid_sheets:
            return valid_sheets
        else:
            logger.warning("iframe 직접 접근 결과에서 유효한 시트를 찾을 수 없습니다")
    else:
        logger.warning("iframe 직접 접근 실패 또는 빈 데이터 반환")
    
    # 2. 전체 문서 전용 모드 활성화
    try:
        logger.info("전체 문서 보기 모드 활성화 시도")
        
        # 문서 뷰어 스케일 설정 (더 많은 콘텐츠 표시)
        scale_script = """
        try {
            // Synap 뷰어일 경우
            if (typeof localSynap !== 'undefined') {
                // 축소하여 전체 문서가 보이도록 함
                localSynap.setZoom(0.5);
                return "Synap viewer zoom adjusted";
            }
            
            // 일반 문서인 경우 CSS transform 사용
            document.body.style.transform = 'scale(0.5)';
            document.body.style.transformOrigin = 'top left';
            
            // 문서의 모든 스크롤 컨테이너 확장
            var containers = document.querySelectorAll('.scroll-container, [style*="overflow"]');
            for (var i = 0; i < containers.length; i++) {
                containers[i].style.height = 'auto';
                containers[i].style.maxHeight = 'none';
                containers[i].style.overflow = 'visible';
            }
            
            return "Document scale adjusted";
        } catch (e) {
            return "Error adjusting scale: " + e.message;
        }
        """
        result = driver.execute_script(scale_script)
        logger.info(f"스케일 조정 결과: {result}")
        
        # 잠시 대기하여 조정 적용
        time.sleep(1)
    except Exception as scale_err:
        logger.warning(f"문서 스케일 조정 실패: {str(scale_err)}")
    
    # 3. 전체 HTML 내용을 JavaScript로 추출 시도
    try:
        logger.info("전체 HTML 내용을 JavaScript로 추출 시도")
        entire_html = driver.execute_script("return document.documentElement.innerHTML;")
        if entire_html:
            # 디버깅용 HTML 저장
            with open(f"entire_html_{int(time.time())}.html", 'w', encoding='utf-8') as f:
                f.write(entire_html)
            
            # HTML에서 데이터 추출
            js_extracted_data = extract_data_from_html(entire_html)
            if js_extracted_data and any(not df.empty for df in js_extracted_data.values()):
                logger.info(f"전체 HTML에서 {len(js_extracted_data)}개 시트 추출 성공")
                return js_extracted_data
    except Exception as js_err:
        logger.warning(f"JavaScript HTML 추출 시도 실패: {str(js_err)}")
    
    # 4. JavaScript로 직접 표 데이터 추출
    try:
        logger.info("JavaScript로 표 데이터 직접 추출 시도")
        js_data = extract_with_javascript(driver)
        if js_data and any(not df.empty for df in js_data.values()):
            logger.info(f"JavaScript로 {len(js_data)}개 시트 추출 성공")
            return js_data
    except Exception as js_err:
        logger.warning(f"JavaScript 표 데이터 추출 시도 실패: {str(js_err)}")
    
    # 5. 대체 방식: SynapDocViewServer 내부 데이터구조 접근 시도
    try:
        logger.info("Synap 문서 뷰어 내부 데이터 접근 시도")
        synap_data_script = """
        try {
            if (typeof localSynap !== 'undefined') {
                // Synap 문서 뷰어 내부 데이터 접근
                var sheetIndex = window.sheetIndex || 0;
                var tableData = {};
                
                // 시트 데이터 직접 접근 시도
                if (typeof WM !== 'undefined' && WM.getSheets) {
                    var sheets = WM.getSheets();
                    if (sheets && sheets.length > 0) {
                        return {
                            success: true,
                            sheets: sheets.map(function(sheet) {
                                return {
                                    name: sheet.getName() || 'Sheet',
                                    data: sheet.getData ? sheet.getData() : null
                                };
                            })
                        };
                    }
                }
                
                // mainTable 요소에서 데이터 추출
                var mainTable = document.getElementById('mainTable');
                if (mainTable) {
                    var rows = mainTable.querySelectorAll('div[class*="tr"]');
                    if (!rows || rows.length === 0) {
                        rows = mainTable.children;
                    }
                    
                    var tableData = [];
                    for (var i = 0; i < rows.length; i++) {
                        var cells = rows[i].querySelectorAll('div[class*="td"]');
                        if (!cells || cells.length === 0) {
                            cells = rows[i].children;
                        }
                        
                        var rowData = [];
                        for (var j = 0; j < cells.length; j++) {
                            rowData.push(cells[j].textContent.trim());
                        }
                        
                        if (rowData.length > 0) {
                            tableData.push(rowData);
                        }
                    }
                    
                    if (tableData.length > 0) {
                        return {
                            success: true,
                            currentSheet: {
                                name: 'MainTable',
                                data: tableData
                            }
                        };
                    }
                }
                
                return {
                    success: false,
                    error: 'Could not find data in Synap viewer'
                };
            }
            
            return {
                success: false,
                error: 'Not a Synap viewer'
            };
        } catch (e) {
            return {
                success: false,
                error: e.message
            };
        }
        """
        
        synap_result = driver.execute_script(synap_data_script)
        if synap_result and synap_result.get('success', False):
            # 시트 데이터 처리
            sheets_data = {}
            
            if 'sheets' in synap_result:
                for sheet_info in synap_result['sheets']:
                    sheet_name = sheet_info.get('name', 'Unknown_Sheet')
                    sheet_data = sheet_info.get('data', [])
                    
                    if sheet_data and len(sheet_data) > 0:
                        # 첫 번째 행이 헤더, 나머지는 데이터로 처리
                        headers = sheet_data[0]
                        data = sheet_data[1:]
                        
                        # 헤더가 없으면 자동 생성
                        if not headers or len(headers) == 0:
                            max_cols = max(len(row) for row in data) if data else 0
                            headers = [f"Column_{i}" for i in range(max_cols)]
                        
                        # DataFrame 생성
                        df = pd.DataFrame(data, columns=headers)
                        sheets_data[sheet_name] = df
                        logger.info(f"Synap 내부 데이터에서 시트 '{sheet_name}' 추출: {df.shape[0]}행 {df.shape[1]}열")
            
            elif 'currentSheet' in synap_result:
                sheet_info = synap_result['currentSheet']
                sheet_name = sheet_info.get('name', 'Current_Sheet')
                sheet_data = sheet_info.get('data', [])
                
                if sheet_data and len(sheet_data) > 0:
                    # 첫 번째 행이 헤더, 나머지는 데이터로 처리
                    headers = sheet_data[0]
                    data = sheet_data[1:]
                    
                    # 헤더가 없으면 자동 생성
                    if not headers or len(headers) == 0:
                        max_cols = max(len(row) for row in data) if data else 0
                        headers = [f"Column_{i}" for i in range(max_cols)]
                    
                    # DataFrame 생성
                    df = pd.DataFrame(data, columns=headers)
                    sheets_data[sheet_name] = df
                    logger.info(f"Synap 현재 시트 데이터 추출: {df.shape[0]}행 {df.shape[1]}열")
            
            if sheets_data:
                logger.info(f"Synap 내부 데이터 접근으로 {len(sheets_data)}개 시트 추출 성공")
                return sheets_data
    except Exception as synap_err:
        logger.warning(f"Synap 내부 데이터 접근 실패: {str(synap_err)}")
    
    # 6. 창 크기 조정 (더 많은 내용이 보이도록)
    try:
        original_size = driver.get_window_size()
        logger.info(f"원래 창 크기: {original_size}")
        
        # 더 큰 창 크기로 조정
        driver.set_window_size(1920, 1080)
        time.sleep(1)  # 창 크기 조정 적용 대기
        
        # 전체 스크린샷 캡처
        full_page_path = f"full_page_{int(time.time())}.png"
        driver.save_screenshot(full_page_path)
        logger.info(f"확대된 창에서 전체 페이지 스크린샷 캡처: {full_page_path}")
        
        # 원래 HTML 컨텐츠를 다시 추출 시도
        try:
            entire_html = driver.execute_script("return document.documentElement.innerHTML;")
            js_extracted_data = extract_data_from_html(entire_html)
            if js_extracted_data and any(not df.empty for df in js_extracted_data.values()):
                logger.info(f"확대된 창에서 HTML 추출 성공: {len(js_extracted_data)}개 시트")
                return js_extracted_data
        except:
            pass
        
        # JavaScript 추출 다시 시도
        try:
            js_data = extract_with_javascript(driver)
            if js_data and any(not df.empty for df in js_data.values()):
                logger.info(f"확대된 창에서 JavaScript 추출 성공: {len(js_data)}개 시트")
                return js_data
        except:
            pass
        
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
    try:
        from PIL import Image, ImageEnhance, ImageFilter
        import cv2
        import pytesseract
        OCR_IMPORTS_AVAILABLE = True
    except ImportError:
        logger.warning("OCR 관련 라이브러리가 설치되지 않아 OCR 기능을 사용할 수 없습니다")
        return None
    
    # OCR 폴백
    logger.info("OCR 기반 추출로 폴백")
    
    try:
        # 디버깅용 HTML 저장
        if file_params.get('atch_file_no') and file_params.get('file_ord'):
            prefix = f"before_ocr_{file_params['atch_file_no']}_{file_params['file_ord']}"
        else:
            prefix = f"before_ocr_{int(time.time())}"
            
        save_html_for_debugging(driver, prefix)
        
        # 창 크기 최대화 (이미 설정했을 수 있지만 확실히 하기 위해)
        driver.maximize_window()
        time.sleep(1)
        
        # 전체 페이지 스크린샷 (기본)
        full_page_screenshot = f"ocr_full_page_{int(time.time())}.png"
        driver.save_screenshot(full_page_screenshot)
        logger.info(f"전체 페이지 스크린샷 캡처: {full_page_screenshot}")
        
        # 7. 고급 OCR 전략: 문서를 분할하여 스크린샷 캡처
        # 전체 페이지 높이 구하기
        try:
            page_height = int(driver.execute_script("return document.documentElement.scrollHeight"))
            page_width = int(driver.execute_script("return document.documentElement.scrollWidth"))
            view_height = int(driver.execute_script("return window.innerHeight"))
            view_width = int(driver.execute_script("return window.innerWidth"))
            
            logger.info(f"페이지 크기: {page_width}x{page_height}, 뷰포트 크기: {view_width}x{view_height}")
            
            # 매우 큰 테이블인 경우 문서 스케일 축소
            if page_height > 3 * view_height or page_width > 1.5 * view_width:
                logger.info("큰 문서 감지됨, 스케일 추가 축소")
                driver.execute_script("""
                    document.body.style.transform = 'scale(0.4)';
                    document.body.style.transformOrigin = 'top left';
                """)
                time.sleep(1)
                
                # 축소된 상태로 다시 스크린샷
                driver.save_screenshot(f"ocr_scaled_down_{int(time.time())}.png")
            
            # 각 시트 탭 클릭하여 모든 시트 캡처
            try:
                sheet_tabs = driver.find_elements(By.CSS_SELECTOR, ".sheet-list__sheet-tab")
                if sheet_tabs:
                    logger.info(f"{len(sheet_tabs)}개 시트 탭 발견, 각 시트 캡처 시도")
                    all_sheets_data = {}
                    
                    # 각 시트에 대해 처리
                    for i, tab in enumerate(sheet_tabs):
                        try:
                            # 시트 이름 추출
                            sheet_name = tab.text.strip()
                            if not sheet_name:
                                sheet_name = f"Sheet_{i+1}"
                                
                            logger.info(f"시트 탭 클릭: {sheet_name}")
                            
                            # 시트 탭 클릭
                            driver.execute_script("arguments[0].click();", tab)
                            time.sleep(2)  # 콘텐츠 로딩 대기
                            
                            # 스크린샷 캡처
                            sheet_screenshot = f"ocr_sheet_{sheet_name}_{int(time.time())}.png"
                            driver.save_screenshot(sheet_screenshot)
                            logger.info(f"시트 '{sheet_name}' 스크린샷 캡처: {sheet_screenshot}")
                            
                            # OCR 처리
                            sheet_data = extract_data_from_screenshot(sheet_screenshot)
                            if sheet_data and len(sheet_data) > 0:
                                for j, df in enumerate(sheet_data):
                                    if df is not None and not df.empty:
                                        all_sheets_data[f"{sheet_name}_Table{j+1}"] = df
                        except Exception as tab_err:
                            logger.warning(f"시트 탭 '{sheet_name}' 처리 중 오류: {str(tab_err)}")
                    
                    if all_sheets_data:
                        logger.info(f"모든 시트 탭에서 {len(all_sheets_data)}개 테이블 추출 성공")
                        return all_sheets_data
            except Exception as tabs_err:
                logger.warning(f"시트 탭 처리 중 오류: {str(tabs_err)}")
            
            # 한 번에 전체 표가 안 보이는 경우를 위한 분할 스크린샷
            if page_height > view_height:
                logger.info("페이지가 뷰포트보다 큼, 분할 스크린샷 시도")
                
                # 스크롤 포지션 계산 (50% 오버랩으로 분할)
                scroll_positions = []
                current_pos = 0
                while current_pos < page_height:
                    scroll_positions.append(current_pos)
                    current_pos += int(view_height * 0.5)  # 50% 오버랩
                
                # 마지막 위치 추가 (페이지 끝)
                if page_height - current_pos > 100:  # 의미 있는 콘텐츠가 있을 경우만
                    scroll_positions.append(page_height - view_height)
                
                logger.info(f"{len(scroll_positions)}개 스크롤 위치에서 분할 스크린샷 캡처 예정")
                
                # 각 위치에서 스크린샷 캡처
                split_screenshots = []
                for i, pos in enumerate(scroll_positions):
                    try:
                        # 스크롤 이동
                        driver.execute_script(f"window.scrollTo(0, {pos});")
                        time.sleep(0.5)  # 스크롤 후 잠시 대기
                        
                        # 스크린샷 캡처
                        split_path = f"ocr_split_{i+1}_{int(time.time())}.png"
                        driver.save_screenshot(split_path)
                        split_screenshots.append(split_path)
                        logger.info(f"분할 스크린샷 {i+1}/{len(scroll_positions)} 캡처: {split_path}")
                    except Exception as e:
                        logger.warning(f"분할 스크린샷 {i+1} 캡처 중 오류: {str(e)}")
                
                # 모든 분할 스크린샷에서 OCR 데이터 추출
                split_data = {}
                for i, ss_path in enumerate(split_screenshots):
                    try:
                        ocr_results = extract_data_from_screenshot(ss_path)
                        if ocr_results:
                            for j, df in enumerate(ocr_results):
                                if df is not None and not df.empty:
                                    split_data[f"Split_{i+1}_Table_{j+1}"] = df
                                    logger.info(f"분할 {i+1}, 테이블 {j+1} 추출: {df.shape[0]}행 {df.shape[1]}열")
                    except Exception as ocr_err:
                        logger.warning(f"분할 {i+1} OCR 처리 중 오류: {str(ocr_err)}")
                
                if split_data:
                    logger.info(f"분할 스크린샷에서 {len(split_data)}개 테이블 추출 성공")
                    return split_data
        except Exception as scroll_err:
            logger.warning(f"분할 스크린샷 처리 중 오류: {str(scroll_err)}")
        
        # 8. 콘텐츠 영역 기반 OCR
        all_sheets = {}
        content_areas = find_content_areas(driver)
        
        if content_areas:
            logger.info(f"타겟 OCR을 위한 {len(content_areas)}개의 잠재적 콘텐츠 영역 발견")
            
            for i, area in enumerate(content_areas):
                try:
                    # 특정 영역의 스크린샷 촬영
                    area_screenshot = capture_element_screenshot(driver, area, f"content_area_{i+1}")
                    
                    if area_screenshot:
                        # 향상된 OCR로 데이터 추출
                        ocr_data_list = extract_data_from_screenshot(area_screenshot)
                        
                        if ocr_data_list and any(not df.empty for df in ocr_data_list):
                            logger.info(f"콘텐츠 영역 {i+1}에서 성공적으로 데이터 추출")
                            for j, df in enumerate(ocr_data_list):
                                if df is not None and not df.empty:
                                    sheet_name = f"OCR_영역{i+1}_테이블{j+1}"
                                    all_sheets[sheet_name] = df
                                    logger.info(f"{sheet_name} 추가: {df.shape[0]}행 {df.shape[1]}열")
                except Exception as area_err:
                    logger.error(f"콘텐츠 영역 {i+1} 처리 중 오류: {str(area_err)}")
        
        # 9. 전체 페이지 OCR - 마지막 선택지
        if not all_sheets:
            logger.info("다른 방법으로 데이터를 찾지 못함, 전체 페이지 OCR 시도")
            
            # 추가 스크린샷
            enhanced_screenshot = f"ocr_enhanced_{int(time.time())}.png"
            driver.save_screenshot(enhanced_screenshot)
            
            # 전체 페이지에서 OCR 추출
            ocr_data_list = extract_data_from_screenshot(enhanced_screenshot)
            
            if ocr_data_list:
                for i, df in enumerate(ocr_data_list):
                    if df is not None and not df.empty:
                        sheet_name = f"OCR_전체페이지_테이블{i+1}"
                        all_sheets[sheet_name] = df
                        logger.info(f"전체 페이지 OCR에서 {sheet_name} 추가: {df.shape[0]}행 {df.shape[1]}열")
        
        # 시트가 있으면 반환
        if all_sheets:
            # 유효한 데이터로 보이는 시트만 필터링
            valid_sheets = {}
            for name, df in all_sheets.items():
                # 시트 품질 검사: 최소 행/열 수, 숫자 데이터 포함 여부
                if df.shape[0] >= 2 and df.shape[1] >= 2:
                    # 숫자 데이터 확인 (쉼표가 포함된 숫자 문자열 포함)
                    has_numeric = False
                    for col in df.columns:
                        try:
                            numeric_ratio = df[col].astype(str).str.replace(',', '').str.replace('.', '').str.isnumeric().mean()
                            if numeric_ratio > 0.3:  # 30% 이상이 숫자면 유효한 열로 간주
                                has_numeric = True
                                break
                        except:
                            continue
                    
                    if has_numeric:
                        valid_sheets[name] = df
                    else:
                        logger.warning(f"시트 {name}에 숫자 데이터가 부족하여 제외합니다")
                else:
                    logger.warning(f"시트 {name}의 크기가 너무 작아 제외합니다: {df.shape}")
            
            if valid_sheets:
                logger.info(f"OCR 데이터 검증 완료: {len(valid_sheets)}/{len(all_sheets)} 시트 유효")
                return valid_sheets
            
            # 검증에 실패해도 모든 시트 반환 (최소한의 데이터라도 제공)
            logger.warning("유효성 검증에 실패했지만 모든 OCR 데이터 반환")
            return all_sheets
        
        # 10. 텍스트 전용 OCR 추출 - 최후의 시도
        logger.info("표 구조 감지 실패, 텍스트 전용 OCR 시도")
        df = extract_text_without_table_structure(full_page_screenshot)
        
        if df is not None and not df.empty:
            all_sheets["OCR_텍스트전용"] = df
            logger.info(f"텍스트 전용 OCR 데이터 추가: {df.shape[0]}행 {df.shape[1]}열")
            return all_sheets
        
        logger.warning("모든 OCR 시도 실패")
        return None
        
    except Exception as e:
        logger.error(f"OCR 폴백 프로세스 중 오류: {str(e)}")
        return None

def extract_data_from_sheet_tabs(driver):
    """
    시트 탭 클릭을 통한 데이터 추출 개선 함수
    
    Args:
        driver: Selenium WebDriver 인스턴스
        
    Returns:
        dict: 시트 이름을 키로, DataFrame을 값으로 하는 딕셔너리
    """
    all_sheets_data = {}
    
    try:
        # 문서 뷰어 감지
        logger.info("문서 뷰어 감지 시도")
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.TAG_NAME, "iframe"))
        )
        
        # 시트 탭 찾기
        sheet_tabs = driver.find_elements(By.CSS_SELECTOR, ".sheet-list__sheet-tab")
        if not sheet_tabs:
            # 다른 선택자로 시트 탭 찾기
            sheet_tabs = driver.find_elements(By.CSS_SELECTOR, "div[role='tab'], .tab-item, li.tab")
            
        if sheet_tabs:
            logger.info(f"시트 탭 {len(sheet_tabs)}개 발견")
            
            for i, tab in enumerate(sheet_tabs):
                sheet_name = tab.text.strip() if tab.text.strip() else f"시트{i+1}"
                logger.info(f"시트 {i+1}/{len(sheet_tabs)} 처리 중: {sheet_name}")
                
                # 시트 탭 클릭 전 스크린샷
                take_screenshot(driver, f"before_tab_click_{sheet_name}")
                
                # 첫 번째가 아닌 시트는 클릭하여 전환
                if i > 0:
                    try:
                        # 자바스크립트로 탭 클릭 (더 안정적)
                        driver.execute_script("arguments[0].click();", tab)
                        logger.info(f"탭 '{sheet_name}' 클릭")
                        
                        # 충분한 대기 시간 추가 (중요)
                        time.sleep(5)  # 탭 변경 및 데이터 로드 대기
                    except Exception as click_err:
                        logger.error(f"시트 탭 클릭 실패 ({sheet_name}): {str(click_err)}")
                        continue
                        
                # 탭 클릭 후 스크린샷
                take_screenshot(driver, f"after_tab_click_{sheet_name}")
                
                # 여러 방법으로 데이터 추출 시도
                sheet_data = extract_data_with_multiple_methods(driver, sheet_name)
                
                if sheet_data is not None and not sheet_data.empty:
                    all_sheets_data[sheet_name] = sheet_data
                    logger.info(f"시트 '{sheet_name}'에서 데이터 추출 성공: {sheet_data.shape[0]}행 {sheet_data.shape[1]}열")
                else:
                    logger.warning(f"시트 '{sheet_name}'에서 데이터를 추출하지 못했습니다")
        else:
            logger.warning("시트 탭을 찾을 수 없습니다")
            
            # 단일 시트 처리
            sheet_data = extract_data_with_multiple_methods(driver, "기본 시트")
            if sheet_data is not None and not sheet_data.empty:
                all_sheets_data["기본 시트"] = sheet_data
                logger.info(f"단일 시트에서 데이터 추출 성공: {sheet_data.shape[0]}행 {sheet_data.shape[1]}열")
        
        return all_sheets_data
    
    except Exception as e:
        logger.error(f"시트 탭을 통한 데이터 추출 중 오류: {str(e)}")
        return all_sheets_data  # 부분적으로라도 추출된 데이터 반환

def extract_data_with_multiple_methods(driver, sheet_name):
    """
    여러 방법을 시도하여 현재 활성화된 시트에서 데이터 추출
    
    Args:
        driver: Selenium WebDriver 인스턴스
        sheet_name: 시트 이름
        
    Returns:
        pandas.DataFrame: 추출된 DataFrame 또는 None
    """
    try:
        # 1. mainTable 방식 시도
        logger.info(f"방법 1: '{sheet_name}'에서 mainTable 방식으로 데이터 추출 시도")
        
        # mainTable 요소 찾기
        main_table = None
        try:
            main_table = WebDriverWait(driver, 5).until(
                EC.presence_of_element_located((By.ID, "mainTable"))
            )
            logger.info(f"시트 '{sheet_name}'에서 mainTable 요소 찾음")
        except TimeoutException:
            logger.warning(f"시트 '{sheet_name}'에서 mainTable 요소를 찾을 수 없음")
        
        if main_table:
            try:
                # JavaScript로 테이블 데이터 추출 (메모리 효율적)
                table_data = driver.execute_script("""
                    try {
                        const mainTable = document.getElementById('mainTable');
                        if (!mainTable) return null;
                        
                        // 여러 방법으로 행 찾기
                        let rows = mainTable.querySelectorAll('div[class*="tr"]');
                        if (!rows || rows.length === 0) {
                            // 직접 자식 요소 시도
                            rows = Array.from(mainTable.children);
                        }
                        
                        if (!rows || rows.length === 0) return null;
                        
                        const tableData = [];
                        for (let i = 0; i < rows.length; i++) {
                            const row = rows[i];
                            
                            // 여러 방법으로 셀 찾기
                            let cells = row.querySelectorAll('div[class*="td"]');
                            if (!cells || cells.length === 0) {
                                // 직계 자식 요소 시도
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
                """)
                
                if isinstance(table_data, list) and table_data:
                    try:
                        # 첫 번째 행을 헤더로 가정
                        headers = table_data[0]
                        data = table_data[1:]
                        
                        if headers and data:
                            df = pd.DataFrame(data, columns=headers)
                            logger.info(f"시트 '{sheet_name}'에서 mainTable 방식으로 {df.shape[0]}행 {df.shape[1]}열 추출 성공")
                            return df
                    except Exception as df_err:
                        logger.warning(f"시트 '{sheet_name}'에서 DataFrame 변환 오류: {str(df_err)}")
            except Exception as js_err:
                logger.warning(f"시트 '{sheet_name}'에서 JavaScript 실행 오류: {str(js_err)}")
        
        # 2. 컨테이너 선택자 방식 시도
        logger.info(f"방법 2: '{sheet_name}'에서 컨테이너 선택자 방식으로 데이터 추출 시도")
        selectors = [
            "div[id='container']",
            ".grid-container",
            ".table-container",
            "div[class*='grid']",
            "div[class*='table']"
        ]
        
        for selector in selectors:
            try:
                containers = driver.find_elements(By.CSS_SELECTOR, selector)
                if containers:
                    logger.info(f"시트 '{sheet_name}'에서 '{selector}' 선택자로 {len(containers)}개 컨테이너 찾음")
                    
                    for container_idx, container in enumerate(containers):
                        try:
                            # 행 요소 찾기 (여러 선택자 시도)
                            row_selectors = [
                                "div[class*='tr']", 
                                "div[class*='row']",
                                "> div"  # 직계 자식 div
                            ]
                            
                            rows = []
                            for row_selector in row_selectors:
                                try:
                                    temp_rows = container.find_elements(By.CSS_SELECTOR, row_selector)
                                    if temp_rows and len(temp_rows) > 1:  # 최소 2행 필요 (헤더 + 데이터)
                                        rows = temp_rows
                                        logger.info(f"컨테이너 {container_idx+1}에서 '{row_selector}' 선택자로 {len(rows)}개 행 찾음")
                                        break
                                except:
                                    continue
                            
                            if not rows:
                                continue
                                
                            # 첫 번째 행에서 셀 선택자 결정
                            first_row = rows[0]
                            cell_selectors = [
                                "div[class*='td']", 
                                "div[class*='cell']",
                                "> div"  # 직계 자식 div
                            ]
                            
                            cell_selector = None
                            for cs in cell_selectors:
                                try:
                                    cells = first_row.find_elements(By.CSS_SELECTOR, cs)
                                    if cells and len(cells) > 0:
                                        cell_selector = cs
                                        break
                                except:
                                    continue
                            
                            if not cell_selector:
                                continue
                                
                            # 데이터 추출
                            table_data = []
                            for row in rows:
                                try:
                                    cells = row.find_elements(By.CSS_SELECTOR, cell_selector)
                                    row_data = [cell.text.strip() for cell in cells]
                                    if any(cell for cell in row_data):  # 빈 행 제외
                                        table_data.append(row_data)
                                except:
                                    continue
                            
                            if len(table_data) >= 2:  # 최소 헤더 + 1개 데이터 행
                                headers = table_data[0]
                                data = table_data[1:]
                                
                                # 헤더가 비어있으면 자동 생성
                                if not any(h for h in headers):
                                    col_count = max(len(row) for row in data)
                                    headers = [f"Column_{i+1}" for i in range(col_count)]
                                
                                # DataFrame 생성
                                df = pd.DataFrame(data, columns=headers)
                                logger.info(f"시트 '{sheet_name}'에서 컨테이너 선택자 방식으로 {df.shape[0]}행 {df.shape[1]}열 추출 성공")
                                return df
                        except Exception as container_err:
                            logger.warning(f"컨테이너 {container_idx+1} 처리 중 오류: {str(container_err)}")
            except Exception as selector_err:
                logger.debug(f"선택자 '{selector}' 처리 중 오류: {str(selector_err)}")
                continue
        
        # 3. iframe 내부 접근 시도
        logger.info(f"방법 3: '{sheet_name}'에서 iframe 내부 접근 시도")
        try:
            iframe = WebDriverWait(driver, 5).until(
                EC.presence_of_element_located((By.TAG_NAME, "iframe"))
            )
            
            # iframe으로 전환
            driver.switch_to.frame(iframe)
            logger.info(f"시트 '{sheet_name}'에서 iframe으로 전환 성공")
            
            # iframe 내에서 테이블 찾기
            tables = driver.find_elements(By.TAG_NAME, "table")
            
            if tables:
                logger.info(f"iframe 내에서 {len(tables)}개 테이블 찾음")
                
                # 가장 큰 테이블 선택
                largest_table = max(tables, key=lambda t: len(t.find_elements(By.TAG_NAME, "tr")))
                
                # 테이블 데이터 추출
                rows = largest_table.find_elements(By.TAG_NAME, "tr")
                
                table_data = []
                for row in rows:
                    cells = row.find_elements(By.TAG_NAME, "td")
                    if not cells:  # th도 확인
                        cells = row.find_elements(By.TAG_NAME, "th")
                        
                    row_data = [cell.text.strip() for cell in cells]
                    if any(cell for cell in row_data):  # 빈 행 제외
                        table_data.append(row_data)
                
                # 기본 컨텐츠로 돌아가기
                driver.switch_to.default_content()
                
                if len(table_data) >= 2:  # 최소 헤더 + 1개 데이터 행
                    headers = table_data[0]
                    data = table_data[1:]
                    
                    # 헤더가 비어있으면 자동 생성
                    if not any(h for h in headers):
                        col_count = max(len(row) for row in data)
                        headers = [f"Column_{i+1}" for i in range(col_count)]
                    
                    # DataFrame 생성
                    df = pd.DataFrame(data, columns=headers)
                    logger.info(f"시트 '{sheet_name}'에서 iframe 내부 테이블로 {df.shape[0]}행 {df.shape[1]}열 추출 성공")
                    return df
            else:
                # innerHTML에서 테이블 추출 시도
                html_content = driver.find_element(By.TAG_NAME, "html").get_attribute("innerHTML")
                
                # 기본 컨텐츠로 돌아가기
                driver.switch_to.default_content()
                
                # HTML에서 표 구조 추출
                if html_content:
                    tables_dict = extract_data_from_html(html_content)
                    if tables_dict and len(tables_dict) > 0:
                        # 가장 큰 테이블 선택
                        largest_df = None
                        max_size = 0
                        
                        for table_name, df in tables_dict.items():
                            if df.size > max_size:
                                max_size = df.size
                                largest_df = df
                        
                        if largest_df is not None and not largest_df.empty:
                            logger.info(f"시트 '{sheet_name}'에서 iframe HTML 파싱으로 {largest_df.shape[0]}행 {largest_df.shape[1]}열 추출 성공")
                            return largest_df
        except TimeoutException:
            logger.warning(f"시트 '{sheet_name}'에서 iframe을 찾을 수 없음")
            # 기본 컨텐츠로 복귀 시도
            try:
                driver.switch_to.default_content()
            except:
                pass
        except Exception as iframe_err:
            logger.warning(f"시트 '{sheet_name}'에서 iframe 처리 중 오류: {str(iframe_err)}")
            # 기본 컨텐츠로 복귀 시도
            try:
                driver.switch_to.default_content()
            except:
                pass
        
        # 4. JavaScript로 DOM 구조 자세히 분석
        logger.info(f"방법 4: '{sheet_name}'에서 JavaScript로 DOM 구조 분석")
        try:
            dom_info = driver.execute_script("""
                return (function() {
                    const result = {
                        potentialTables: []
                    };
                    
                    // 테이블처럼 보이는 구조 찾기
                    function findTableLikeStructures() {
                        const divs = document.querySelectorAll('div');
                        
                        for (const div of divs) {
                            // 자식 요소가 충분히 많은지
                            if (div.children.length < 2) continue;
                            
                            // 크기가 충분히 큰지
                            const rect = div.getBoundingClientRect();
                            if (rect.width < 100 || rect.height < 100) continue;
                            
                            // 자식 요소들이 유사한 구조를 가지는지
                            let similarChildren = 0;
                            const firstChildType = div.children[0].tagName;
                            for (const child of div.children) {
                                if (child.tagName === firstChildType) {
                                    similarChildren++;
                                }
                            }
                            
                            // 80% 이상의 자식이 같은 유형이면 테이블 후보
                            if (similarChildren / div.children.length > 0.8) {
                                // 각 자식의 자식 요소 수도 확인 (셀처럼 보이는지)
                                let cellCount = 0;
                                
                                for (const child of div.children) {
                                    if (child.children.length > 0) {
                                        cellCount += child.children.length;
                                    }
                                }
                                
                                if (cellCount > div.children.length) {
                                    // DOM 경로 구성
                                    let path = '';
                                    let current = div;
                                    while (current && current !== document.body) {
                                        const tag = current.tagName.toLowerCase();
                                        const id = current.id ? '#' + current.id : '';
                                        const classes = Array.from(current.classList).map(c => '.' + c).join('');
                                        path = tag + id + classes + ' > ' + path;
                                        current = current.parentElement;
                                    }
                                    
                                    // 정보 저장
                                    result.potentialTables.push({
                                        path: path.slice(0, -3),  // 마지막 ' > ' 제거
                                        childCount: div.children.length,
                                        cellCount: cellCount,
                                        dimensions: {
                                            width: rect.width,
                                            height: rect.height
                                        },
                                        id: div.id,
                                        classes: Array.from(div.classList)
                                    });
                                }
                            }
                        }
                    }
                    
                    findTableLikeStructures();
                    
                    // 추가 정보 수집
                    result.documentTitle = document.title;
                    result.bodyChildCount = document.body.children.length;
                    result.scripts = document.scripts.length;
                    result.iframeCount = document.querySelectorAll('iframe').length;
                    
                    return result;
                })();
            """)
            
            if dom_info and 'potentialTables' in dom_info and dom_info['potentialTables']:
                # 가능성 있는 테이블 구조 중 가장 큰 것 선택
                potential_tables = sorted(dom_info['potentialTables'], 
                                         key=lambda t: t['dimensions']['width'] * t['dimensions']['height'],
                                         reverse=True)
                
                logger.info(f"시트 '{sheet_name}'에서 {len(potential_tables)}개 테이블 구조 후보 발견")
                
                for table_info in potential_tables[:3]:  # 상위 3개만 시도
                    try:
                        # 선택자 구성
                        selector = table_info['path']
                        
                        # ID가 있으면 더 정확한 선택자 사용
                        if table_info['id']:
                            selector = f"#{table_info['id']}"
                        
                        logger.info(f"테이블 구조 후보 선택자: {selector}")
                        
                        # 해당 요소 찾기
                        table_element = driver.find_element(By.CSS_SELECTOR, selector)
                        
                        # 데이터 추출 시도
                        row_elements = table_element.find_elements(By.CSS_SELECTOR, "> div, > tr")
                        
                        if row_elements and len(row_elements) >= 2:
                            # 첫 번째 행에서 셀 선택자 파악
                            first_row = row_elements[0]
                            cell_selectors = ["> div", "> td", "> th", "*"]
                            
                            for cell_selector in cell_selectors:
                                try:
                                    cells = first_row.find_elements(By.CSS_SELECTOR, cell_selector)
                                    if cells and len(cells) > 1:
                                        # 테이블 데이터 추출
                                        table_data = []
                                        
                                        for row in row_elements:
                                            try:
                                                row_cells = row.find_elements(By.CSS_SELECTOR, cell_selector)
                                                row_data = [cell.text.strip() for cell in row_cells]
                                                if any(cell for cell in row_data):  # 빈 행 제외
                                                    table_data.append(row_data)
                                            except:
                                                continue
                                        
                                        if len(table_data) >= 2:  # 최소 헤더 + 1개 데이터 행
                                            # 열 길이 통일
                                            max_cols = max(len(row) for row in table_data)
                                            for i in range(len(table_data)):
                                                if len(table_data[i]) < max_cols:
                                                    table_data[i].extend([''] * (max_cols - len(table_data[i])))
                                            
                                            headers = table_data[0]
                                            data = table_data[1:]
                                            
                                            # 헤더가 비어있으면 자동 생성
                                            if not any(h for h in headers):
                                                headers = [f"Column_{i+1}" for i in range(max_cols)]
                                            
                                            # DataFrame 생성
                                            df = pd.DataFrame(data, columns=headers)
                                            logger.info(f"시트 '{sheet_name}'에서 DOM 구조 분석으로 {df.shape[0]}행 {df.shape[1]}열 추출 성공")
                                            return df
                                except:
                                    continue
                    except:
                        continue
        except Exception as js_err:
            logger.warning(f"시트 '{sheet_name}'에서 JavaScript DOM 분석 중 오류: {str(js_err)}")
        
        # 5. 스크린샷 OCR 시도 (마지막 수단)
        if CONFIG['ocr_enabled']:
            logger.info(f"방법 5: '{sheet_name}'에서 OCR 시도")
            try:
                # 현재 화면 스크린샷
                screenshot_path = f"ocr_{sheet_name}_{int(time.time())}.png"
                driver.save_screenshot(screenshot_path)
                
                # OCR로 데이터 추출
                ocr_dataframes = extract_data_from_screenshot(screenshot_path)
                
                if ocr_dataframes and len(ocr_dataframes) > 0:
                    # 가장 큰 DataFrame 선택
                    largest_df = max(ocr_dataframes, key=lambda df: df.size if df is not None and not df.empty else 0)
                    
                    if largest_df is not None and not largest_df.empty:
                        logger.info(f"시트 '{sheet_name}'에서 OCR로 {largest_df.shape[0]}행 {largest_df.shape[1]}열 추출 성공")
                        return largest_df
            except Exception as ocr_err:
                logger.warning(f"시트 '{sheet_name}'에서 OCR 시도 중 오류: {str(ocr_err)}")
        
        logger.warning(f"시트 '{sheet_name}'에서 모든 데이터 추출 방법이 실패했습니다")
        return None
        
    except Exception as e:
        logger.error(f"시트 '{sheet_name}'에서 데이터 추출 중 오류: {str(e)}")
        return None




def access_iframe_direct(driver, file_params):
    """
    iframe에 직접 접근하여 SynapDocViewServer의 데이터를 추출하는 개선된 함수
    
    Args:
        driver: Selenium WebDriver 인스턴스
        file_params: 파일 파라미터를 포함한 딕셔너리
        
    Returns:
        dict: 시트 이름을 키로, DataFrame을 값으로 하는 딕셔너리 또는 None
    """
    if not file_params or not file_params.get('atch_file_no') or not file_params.get('file_ord'):
        logger.error("파일 파라미터가 없습니다.")
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
            driver.get(view_url)
            time.sleep(5)  # 초기 대기
            
            # 현재 URL 확인
            current_url = driver.current_url
            logger.info(f"현재 URL: {current_url}")
           
            # 스크린샷 저장
            take_screenshot(driver, f"iframe_view_{atch_file_no}_{file_ord}_attempt_{attempt}")
            
            # 현재 페이지 스크린샷 저장 (디버깅용)
            try:
                driver.save_screenshot(f"document_view_{atch_file_no}_{file_ord}.png")
                logger.info(f"문서 뷰어 스크린샷 저장: document_view_{atch_file_no}_{file_ord}.png")
            except Exception as ss_err:
                logger.warning(f"스크린샷 저장 중 오류: {str(ss_err)}")
            
            # 시스템 점검 페이지 감지
            if "시스템 점검 안내" in driver.page_source:
                if attempt < max_retries - 1:
                    logger.warning("시스템 점검 중입니다. 나중에 다시 시도합니다.")
                    time.sleep(5)  # 더 오래 대기
                    continue
                else:
                    logger.warning("시스템 점검 중입니다. 문서를 열 수 없습니다.")
                    return None
            
            # SynapDocViewServer 탐색
            logger.info("SynapDocViewServer 내부 구조 탐색 시작")
            structure_info = explore_synap_doc_viewer(driver)
            
            # 구조 정보를 기반으로 데이터 추출
            if structure_info:
                extracted_data = extract_synap_data_using_structure_info(driver, structure_info)
                if extracted_data:
                    logger.info(f"SynapDocViewServer 구조 정보를 통해 {len(extracted_data)}개 시트 추출 성공")
                    
                    # DataFrame으로 변환
                    sheets_data = {}
                    for sheet_name, sheet_data in extracted_data.items():
                        headers = sheet_data.get('headers', [])
                        data = sheet_data.get('data', [])
                        
                        if headers and data:
                            try:
                                df = pd.DataFrame(data, columns=headers)
                                sheets_data[sheet_name] = df
                                logger.info(f"시트 '{sheet_name}'을 DataFrame으로 변환: {df.shape[0]}행 {df.shape[1]}열")
                            except Exception as df_err:
                                logger.warning(f"시트 '{sheet_name}' DataFrame 변환 오류: {str(df_err)}")
                    
                    if sheets_data:
                        return sheets_data
            
            # SynapDocViewServer 또는 문서 뷰어 감지
            if 'SynapDocViewServer' in current_url or 'doc.msit.go.kr' in current_url:
                logger.info("문서 뷰어 감지됨")
                
                # 현재 창 핸들 저장
                original_handle = driver.current_window_handle
                
                # 새 창이 열렸는지 확인
                window_handles = driver.window_handles
                if len(window_handles) > 1:
                    logger.info(f"새 창이 열렸습니다. 전환 시도...")
                    for handle in window_handles:
                        if handle != original_handle:
                            driver.switch_to.window(handle)
                            break
                
                # 여기에 새 함수 호출 코드 추가
                logger.info("향상된 시트 탭 기반 데이터 추출 시도...")
                sheets_data = extract_data_from_sheet_tabs(driver)
                if sheets_data and any(not df.empty for df in sheets_data.values()):
                    logger.info(f"향상된 시트 데이터 추출 방식으로 {len(sheets_data)}개 시트 추출 성공")
                    return sheets_data
                
                # 이하 기존 코드는 그대로 유지 (기존 시트 탭 처리 로직)
                # 1. 시트 탭 찾아서 처리
                sheet_tabs = driver.find_elements(By.CSS_SELECTOR, ".sheet-list__sheet-tab")
                if sheet_tabs:
                    logger.info(f"시트 탭 {len(sheet_tabs)}개 발견")
                    all_sheets = {}
                    
                    for i, tab in enumerate(sheet_tabs):
                        sheet_name = tab.text.strip() if tab.text.strip() else f"시트{i+1}"
                        logger.info(f"시트 {i+1}/{len(sheet_tabs)} 처리 중: {sheet_name}")
                        
                        # 첫 번째가 아닌 시트는 클릭하여 전환
                        if i > 0:
                            try:
                                # JavaScript로 클릭 (더 안정적)
                                driver.execute_script("arguments[0].click();", tab)
                                time.sleep(3)  # 시트 전환 대기
                            except Exception as click_err:
                                logger.error(f"시트 탭 클릭 실패 ({sheet_name}): {str(click_err)}")
                                continue
                        
                        try:
                            # mainTable 구조에서 직접 데이터 추출
                            table_data = driver.execute_script("""
                                try {
                                    const mainTable = document.getElementById('mainTable');
                                    if (!mainTable) return null;
                                    
                                    // 여러 방법으로 행 찾기
                                    let rows = mainTable.querySelectorAll('div[class*="tr"]');
                                    if (!rows || rows.length === 0) {
                                        // 직접 자식 요소 시도
                                        rows = Array.from(mainTable.children);
                                    }
                                    
                                    if (!rows || rows.length === 0) return null;
                                    
                                    const tableData = [];
                                    for (let i = 0; i < rows.length; i++) {
                                        const row = rows[i];
                                        
                                        // 여러 방법으로 셀 찾기
                                        let cells = row.querySelectorAll('div[class*="td"]');
                                        if (!cells || cells.length === 0) {
                                            // 직접 자식 요소 시도
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
                            """)
                            
                            if isinstance(table_data, list) and table_data:
                                # 첫 번째 행을 헤더로 가정
                                try:
                                    headers = table_data[0]
                                    data = table_data[1:]
                                    
                                    if headers and data:
                                        df = pd.DataFrame(data, columns=headers)
                                        all_sheets[sheet_name] = df
                                        logger.info(f"시트 '{sheet_name}'에서 데이터 추출 성공: {df.shape[0]}행 {df.shape[1]}열")
                                    else:
                                        logger.warning(f"시트 '{sheet_name}'에서 충분한 데이터가 없습니다")
                                except Exception as df_err:
                                    logger.warning(f"시트 '{sheet_name}' DataFrame 변환 오류: {str(df_err)}")
                            else:
                                logger.warning(f"시트 '{sheet_name}'에서 테이블 데이터를 추출하지 못했습니다")
                                
                                # 대체 방법: iframe 접근
                                try:
                                    # iframe 찾기
                                    iframe = WebDriverWait(driver, 10).until(
                                        EC.presence_of_element_located((By.ID, "innerWrap"))
                                    )
                                    
                                    # iframe으로 전환
                                    driver.switch_to.frame(iframe)
                                    
                                    # iframe HTML 가져오기
                                    iframe_html = driver.page_source
                                    
                                    # HTML에서 데이터 추출
                                    iframe_data = extract_data_from_html(iframe_html)
                                    
                                    # 기본 프레임으로 복귀
                                    driver.switch_to.default_content()
                                    
                                    if iframe_data:
                                        # 가장 큰 데이터프레임 선택
                                        largest_df = None
                                        max_size = 0
                                        
                                        for df_name, df in iframe_data.items():
                                            size = df.size
                                            if size > max_size:
                                                max_size = size
                                                largest_df = df
                                        
                                        if largest_df is not None:
                                            all_sheets[sheet_name] = largest_df
                                            logger.info(f"시트 '{sheet_name}'에서 iframe을 통해 데이터 추출 성공: {largest_df.shape[0]}행 {largest_df.shape[1]}열")
                                except Exception as iframe_err:
                                    logger.warning(f"시트 '{sheet_name}' iframe 접근 오류: {str(iframe_err)}")
                                    try:
                                        driver.switch_to.default_content()
                                    except:
                                        pass
                        except Exception as sheet_err:
                            logger.error(f"시트 '{sheet_name}' 처리 중 오류: {str(sheet_err)}")
                            try:
                                driver.switch_to.default_content()
                            except:
                                pass
                    
                    if all_sheets:
                        logger.info(f"총 {len(all_sheets)}개 시트에서 데이터 추출 완료")
                        return all_sheets
                    else:
                        logger.warning("어떤 시트에서도 데이터를 추출하지 못했습니다.")
                        if attempt < max_retries - 1:
                            logger.info(f"재시도 중... ({attempt+1}/{max_retries})")
                            continue
                else:
                    # 시트 탭이 없는 경우, 단일 문서 처리
                    logger.info("시트 탭 없음, 단일 iframe 또는 mainTable 처리 시도")
                    
                    # 먼저 mainTable에서 직접 추출 시도
                    table_data = driver.execute_script("""
                        try {
                            const mainTable = document.getElementById('mainTable');
                            if (!mainTable) return null;
                            
                            // 여러 방법으로 행 찾기
                            let rows = mainTable.querySelectorAll('div[class*="tr"]');
                            if (!rows || rows.length === 0) {
                                // 직접 자식 요소 시도
                                rows = Array.from(mainTable.children);
                            }
                            
                            if (!rows || rows.length === 0) return null;
                            
                            const tableData = [];
                            for (let i = 0; i < rows.length; i++) {
                                const row = rows[i];
                                
                                // 여러 방법으로 셀 찾기
                                let cells = row.querySelectorAll('div[class*="td"]');
                                if (!cells || cells.length === 0) {
                                    // 직접 자식 요소 시도
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
                    """)
                    
                    if isinstance(table_data, list) and table_data:
                        try:
                            # 첫 번째 행을 헤더로 가정
                            headers = table_data[0]
                            data = table_data[1:]
                            
                            df = pd.DataFrame(data, columns=headers)
                            logger.info(f"mainTable에서 직접 데이터 추출 성공: {df.shape[0]}행 {df.shape[1]}열")
                            return {"mainTable": df}
                        except Exception as df_err:
                            logger.warning(f"mainTable DataFrame 변환 오류: {str(df_err)}")
                    
                    # mainTable 추출 실패 시 iframe 접근 시도
                    try:
                        iframe = WebDriverWait(driver, 10).until(
                            EC.presence_of_element_located((By.ID, "innerWrap"))
                        )
                        
                        # iframe으로 전환
                        driver.switch_to.frame(iframe)
                        
                        # iframe HTML 가져오기
                        iframe_html = driver.page_source
                        
                        # HTML에서 데이터 추출
                        iframe_data = extract_data_from_html(iframe_html)
                        
                        # 기본 프레임으로 복귀
                        driver.switch_to.default_content()
                        
                        if iframe_data:
                            logger.info(f"iframe에서 {len(iframe_data)}개 시트 추출 성공")
                            return iframe_data
                        else:
                            logger.warning("iframe에서 데이터를 추출하지 못했습니다")
                            
                            # 마지막 시도로 전체 HTML 분석
                            html_data = extract_data_from_html(driver.page_source)
                            if html_data:
                                logger.info(f"전체 HTML에서 {len(html_data)}개 시트 추출 성공")
                                return html_data
                    except Exception as iframe_err:
                        logger.warning(f"iframe 접근 오류: {str(iframe_err)}")
                        try:
                            driver.switch_to.default_content()
                        except:
                            pass
                        
                        if attempt < max_retries - 1:
                            logger.info(f"재시도 중... ({attempt+1}/{max_retries})")
                            continue
            else:
                logger.info("SynapDocViewServer 미감지, 일반 HTML 페이지 처리")
                
                # 일반 HTML 페이지에서 테이블 추출
                try:
                    # HTML에서 데이터 추출
                    html_data = extract_data_from_html(driver.page_source)
                    if html_data:
                        logger.info(f"HTML에서 {len(html_data)}개 시트 추출 성공")
                        return html_data
                        
                    # pandas의 read_html 사용
                    tables = pd.read_html(driver.page_source)
                    
                    if tables:
                        sheets_data = {}
                        for i, table in enumerate(tables):
                            sheets_data[f"Table_{i+1}"] = table
                            
                        logger.info(f"pandas.read_html로 {len(sheets_data)}개 테이블 추출 성공")
                        return sheets_data
                    else:
                        logger.warning("페이지에서 테이블을 찾을 수 없습니다.")
                        
                        if attempt < max_retries - 1:
                            logger.info(f"재시도 중... ({attempt+1}/{max_retries})")
                            continue
                except Exception as html_err:
                    logger.warning(f"HTML 테이블 추출 중 오류: {str(html_err)}")
                    if attempt < max_retries - 1:
                        logger.info(f"재시도 중... ({attempt+1}/{max_retries})")
                        continue
        
        except Exception as e:
            logger.error(f"iframe 전환 및 데이터 추출 중 오류: {str(e)}")
            
            # 디버깅 정보 출력
            try:
                # HTML 미리보기 출력
                html_snippet = driver.page_source[:5000]
                logger.error(f"오류 발생 시 페이지 HTML (first 5000 characters):\n{html_snippet}")
                
                # <script> 태그 내용도 별도 출력
                soup = BeautifulSoup(driver.page_source, 'html.parser')
                script_tags = soup.find_all('script')
                if script_tags:
                    logger.error("오류 발생 시 <script> 태그 내용:")
                    for script in script_tags:
                        logger.error(script.prettify())
                
                # 기본 프레임으로 복귀
                driver.switch_to.default_content()
            except:
                pass
            
            if attempt < max_retries - 1:
                logger.info(f"재시도 중... ({attempt+1}/{max_retries})")
                time.sleep(3)
                continue
    
    return None

def verify_tab_change(driver, tab_name, timeout=10):
    """Verify that the tab has changed by checking various indicators"""
    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            # Check page title or header that might reflect current tab
            header_text = driver.execute_script("""
                const headers = document.querySelectorAll('.sheet-header, .page-header, h1, h2');
                for (let i = 0; i < headers.length; i++) {
                    if (headers[i].textContent.includes(arguments[0])) {
                        return true;
                    }
                }
                return false;
            """, tab_name)
            
            if header_text:
                return True
                
            # Check if tab appears selected
            tab_active = driver.execute_script("""
                const tabs = document.querySelectorAll('.sheet-list__sheet-tab');
                for (let i = 0; i < tabs.length; i++) {
                    if (tabs[i].textContent.includes(arguments[0]) && 
                        (tabs[i].classList.contains('active') || 
                         tabs[i].getAttribute('aria-selected') === 'true')) {
                        return true;
                    }
                }
                return false;
            """, tab_name)
            
            if tab_active:
                return True
                
            time.sleep(0.5)
        except:
            time.sleep(0.5)
    
    return False

def robust_tab_click(driver, tab, tab_name, max_attempts=3):
    """Try multiple approaches to click a tab and verify the switch"""
    for attempt in range(max_attempts):
        try:
            # Try different click methods
            if attempt == 0:
                driver.execute_script("arguments[0].click();", tab)
            elif attempt == 1:
                tab.click()  # Direct click
            else:
                # Try finding by text and clicking
                tabs = driver.find_elements(By.XPATH, f"//div[contains(@class, 'tab') and contains(text(), '{tab_name}')]")
                if tabs:
                    driver.execute_script("arguments[0].click();", tabs[0])
            
            # Wait for tab change to take effect
            time.sleep(2)
            
            # Verify the tab changed
            if verify_tab_change(driver, tab_name):
                return True
                
            # Try forcing a refresh if the tab didn't change
            force_tab_refresh(driver, tab_name)
            time.sleep(2)
            
        except Exception as e:
            logger.warning(f"Tab click attempt {attempt+1} failed: {str(e)}")
            time.sleep(1)
    
    return False

def force_tab_refresh(driver, tab_index):
    """Force a tab refresh by manipulating the iframe source if needed"""
    try:
        iframe = driver.find_element(By.ID, "innerWrap")
        current_src = iframe.get_attribute('src')
        
        # Some document viewers use URL parameters to specify tabs
        if '?' in current_src:
            base_url = current_src.split('?')[0]
            new_src = f"{base_url}?tab={tab_index}"
            driver.execute_script(f"document.getElementById('innerWrap').src = '{new_src}';")
            return True
    except:
        pass
    
    return False


def find_content_areas(driver):
    """
    페이지에서 타겟 OCR을 위한 잠재적 콘텐츠 영역 찾기.
    개선된 기능으로 테이블 영역을 더 효과적으로 찾아냅니다.
    
    Args:
        driver: Selenium WebDriver 인스턴스
        
    Returns:
        list: 잠재적 콘텐츠 영역을 나타내는 WebElement 객체 목록
    """
    try:
        content_areas = []
        
        # 1. SynapDocViewServer 특화 영역 찾기
        synap_specific_selectors = [
            "#mainTable",  # 메인 테이블
            ".sheet-content",  # 시트 콘텐츠
            "#doc-content",  # 문서 콘텐츠
            ".content-wrapper"  # 콘텐츠 래퍼
        ]
        
        for selector in synap_specific_selectors:
            try:
                elements = driver.find_elements(By.CSS_SELECTOR, selector)
                if elements:
                    for elem in elements:
                        # 요소가 표시되는지 확인
                        if elem.is_displayed() and elem.size['width'] > 50 and elem.size['height'] > 50:
                            content_areas.append(elem)
                            logger.info(f"Synap 특화 선택자 '{selector}'로 콘텐츠 영역 발견: 크기 {elem.size}")
            except Exception as synap_err:
                logger.debug(f"Synap 선택자 '{selector}' 검색 오류: {str(synap_err)}")
        
        # 2. iframe 내의 콘텐츠 찾기
        try:
            iframes = driver.find_elements(By.TAG_NAME, "iframe")
            if iframes:
                for iframe_index, iframe in enumerate(iframes):
                    try:
                        if iframe.is_displayed() and iframe.size['width'] > 100 and iframe.size['height'] > 100:
                            try:
                                # iframe으로 전환
                                driver.switch_to.frame(iframe)
                                
                                # iframe 내부에서 콘텐츠 찾기
                                tables = driver.find_elements(By.TAG_NAME, "table")
                                if tables:
                                    for table in tables:
                                        if table.is_displayed() and table.size['width'] > 50 and table.size['height'] > 50:
                                            # iframe의 body 추가
                                            iframe_body = driver.find_element(By.TAG_NAME, "body")
                                            if iframe_body not in content_areas:
                                                content_areas.append(iframe_body)
                                                logger.info(f"iframe {iframe_index+1}에서 테이블 콘텐츠 발견")
                                            break
                                else:
                                    # 테이블이 없으면 div 구조 확인
                                    main_divs = driver.find_elements(By.CSS_SELECTOR, "div[id*='table'], div[class*='table'], div[class*='grid']")
                                    if main_divs:
                                        for div in main_divs:
                                            if div.is_displayed() and div.size['width'] > 50 and div.size['height'] > 50:
                                                iframe_body = driver.find_element(By.TAG_NAME, "body")
                                                if iframe_body not in content_areas:
                                                    content_areas.append(iframe_body)
                                                    logger.info(f"iframe {iframe_index+1}에서 콘텐츠 발견")
                                                break
                                
                                # 메인 콘텐츠로 돌아가기
                                driver.switch_to.default_content()
                            except Exception as frame_err:
                                logger.debug(f"iframe {iframe_index+1} 콘텐츠 접근 오류: {str(frame_err)}")
                                try:
                                    driver.switch_to.default_content()
                                except:
                                    pass
                    except Exception as iframe_err:
                        logger.debug(f"iframe {iframe_index+1} 접근 오류: {str(iframe_err)}")
        except Exception as iframes_err:
            logger.debug(f"iframe 찾기 오류: {str(iframes_err)}")
            # 메인 콘텐츠로 돌아가기
            try:
                driver.switch_to.default_content()
            except:
                pass
        
        # 3. 일반적인 콘텐츠 영역 찾기
        content_selectors = [
            "div.content",
            "div.main-content",
            "div#content",
            "div[class*='content']",
            "div[class*='view']",
            "div[id*='content']",
            "div[id*='view']"
        ]
        
        for selector in content_selectors:
            try:
                elements = driver.find_elements(By.CSS_SELECTOR, selector)
                if elements:
                    for elem in elements:
                        if elem.is_displayed() and elem.size['width'] > 100 and elem.size['height'] > 100:
                            # 중복 확인
                            if not any(are_same_element(elem, existing) for existing in content_areas):
                                content_areas.append(elem)
                                logger.info(f"선택자: {selector}로 콘텐츠 영역 발견")
            except Exception as selector_err:
                logger.debug(f"선택자 '{selector}' 검색 오류: {str(selector_err)}")
        
        # 4. 테이블 직접 찾기
        try:
            tables = driver.find_elements(By.TAG_NAME, "table")
            if tables:
                for table in tables:
                    if table.is_displayed() and table.size['width'] > 100 and table.size['height'] > 50:
                        # 중복 확인
                        if not any(are_same_element(table, existing) for existing in content_areas):
                            content_areas.append(table)
                            logger.info(f"테이블 요소 발견: 크기 {table.size}")
        except Exception as table_err:
            logger.debug(f"테이블 찾기 오류: {str(table_err)}")
        
        # 5. 특별한 DIV 기반 테이블 찾기
        table_div_selectors = [
            "div.table",
            "div[class*='table']",
            "div.grid",
            "div[class*='grid']",
            ".tb",  # Synap 특수 클래스
            "div[style*='table']"
        ]
        
        for selector in table_div_selectors:
            try:
                elements = driver.find_elements(By.CSS_SELECTOR, selector)
                if elements:
                    for elem in elements:
                        if elem.is_displayed() and elem.size['width'] > 100 and elem.size['height'] > 50:
                            # 중복 확인
                            if not any(are_same_element(elem, existing) for existing in content_areas):
                                content_areas.append(elem)
                                logger.info(f"DIV 테이블 선택자 '{selector}'로 콘텐츠 영역 발견: 크기 {elem.size}")
            except Exception as div_err:
                logger.debug(f"DIV 테이블 선택자 '{selector}' 검색 오류: {str(div_err)}")
        
        # 6. JavaScript로 테이블 구조 가진 요소 탐지
        try:
            js_tables = driver.execute_script("""
                function findTableLikeElements() {
                    const results = [];
                    
                    // 테이블처럼 보이는 요소 찾기 (그리드 레이아웃)
                    function hasTableStructure(element) {
                        // 최소 2개 이상의 자식 요소 필요
                        if (!element || element.children.length < 2) {
                            return false;
                        }
                        
                        // 첫 번째 행의 셀 개수
                        const firstRowCells = element.children[0].children ? 
                            element.children[0].children.length : 0;
                        
                        // 최소 2개 이상의 셀 필요
                        if (firstRowCells < 2) {
                            return false;
                        }
                        
                        // 일정한 구조의 중첩 요소인지 확인
                        let consistentStructure = true;
                        for (let i = 1; i < Math.min(element.children.length, 5); i++) {
                            if (!element.children[i].children || 
                                Math.abs(element.children[i].children.length - firstRowCells) > 1) {
                                consistentStructure = false;
                                break;
                            }
                        }
                        
                        return consistentStructure;
                    }
                    
                    // 문서 내 모든 요소 확인
                    function scanElements(root) {
                        if (!root || !root.querySelectorAll) {
                            return;
                        }
                        
                        // 특정 크기 이상의 div 요소 확인
                        const divs = root.querySelectorAll('div');
                        for (let i = 0; i < divs.length; i++) {
                            const div = divs[i];
                            
                            // 최소 크기 확인
                            const rect = div.getBoundingClientRect();
                            if (rect.width < 200 || rect.height < 100) {
                                continue;
                            }
                            
                            // 테이블 구조인지 확인
                            if (hasTableStructure(div)) {
                                results.push(div);
                            }
                        }
                    }
                    
                    // 메인 문서 스캔
                    scanElements(document);
                    
                    // iframe 내부 스캔
                    const iframes = document.querySelectorAll('iframe');
                    for (let i = 0; i < iframes.length; i++) {
                        try {
                            const frameDoc = iframes[i].contentDocument || 
                                            iframes[i].contentWindow.document;
                            scanElements(frameDoc);
                        } catch (e) {
                            // 접근 권한 없음 - 무시
                        }
                    }
                    
                    return results;
                }
                
                return findTableLikeElements();
            """)
            
            if js_tables:
                for i, js_elem in enumerate(js_tables):
                    try:
                        # 중복 확인
                        if js_elem and js_elem.is_displayed() and not any(are_same_element(js_elem, existing) for existing in content_areas):
                            content_areas.append(js_elem)
                            logger.info(f"JavaScript로 테이블 구조 요소 {i+1} 발견")
                    except:
                        pass
        except Exception as js_err:
            logger.debug(f"JavaScript 테이블 구조 탐지 오류: {str(js_err)}")
        
        # 콘텐츠 영역이 없으면 본문 전체 반환
        if not content_areas:
            try:
                body = driver.find_element(By.TAG_NAME, "body")
                content_areas.append(body)
                logger.info("콘텐츠 영역을 찾지 못함, 전체 페이지 body 사용")
            except Exception as body_err:
                logger.warning(f"body 요소를 찾을 수 없음: {str(body_err)}")
        
        # 요소를 크기별로 정렬 (큰 것부터)
        content_areas.sort(key=lambda elem: 
            (elem.size['width'] * elem.size['height']) if elem.size['width'] > 0 and elem.size['height'] > 0 else 0, 
            reverse=True)
        
        # 중복 요소 제거 및 최종 목록 생성
        unique_areas = []
        for area in content_areas:
            if not any(are_same_element(area, existing) for existing in unique_areas):
                unique_areas.append(area)
        
        logger.info(f"총 {len(unique_areas)}개의 고유한 콘텐츠 영역 발견")
        return unique_areas
        
    except Exception as e:
        logger.error(f"콘텐츠 영역 찾기 오류: {str(e)}")
        return []

def are_same_element(elem1, elem2):
    """두 요소가 동일한지 확인"""
    try:
        # ID로 비교
        if elem1.id and elem1.id == elem2.id:
            return True
        
        # 크기와 위치로 비교
        try:
            loc1 = elem1.location
            loc2 = elem2.location
            size1 = elem1.size
            size2 = elem2.size
            
            # 위치와 크기가 유사하면 같은 요소로 간주
            return (abs(loc1['x'] - loc2['x']) < 10 and 
                    abs(loc1['y'] - loc2['y']) < 10 and 
                    abs(size1['width'] - size2['width']) < 20 and 
                    abs(size1['height'] - size2['height']) < 20)
        except:
            # 위치나 크기를 가져올 수 없으면 다른 요소로 간주
            return False
    except:
        return False



def capture_element_screenshot(driver, element, name_prefix):
    """
    특정 요소의 스크린샷을 개선된 방식으로 캡처합니다.
    
    Args:
        driver: Selenium WebDriver 인스턴스
        element: 캡처할 WebElement
        name_prefix: 스크린샷 파일 이름의 접두사
        
    Returns:
        str: 스크린샷 파일 경로 또는 캡처 실패 시 None
    """
    try:
        # 요소의 위치와 크기 가져오기
        location = element.location
        size = element.size
        
        # 0인 속성 체크 및 수정
        if size['width'] <= 0 or size['height'] <= 0:
            logger.warning(f"요소 크기가 유효하지 않음: 너비={size['width']}, 높이={size['height']}")
            
            # JavaScript로 실제 요소 크기 확인 시도
            try:
                js_rect = driver.execute_script("""
                    var rect = arguments[0].getBoundingClientRect();
                    return {
                        top: rect.top,
                        left: rect.left,
                        width: rect.width,
                        height: rect.height
                    };
                """, element)
                
                if js_rect['width'] > 0 and js_rect['height'] > 0:
                    logger.info(f"JavaScript에서 요소 크기 복구: {js_rect}")
                    location = {'x': js_rect['left'], 'y': js_rect['top']}
                    size = {'width': js_rect['width'], 'height': js_rect['height']}
                else:
                    # 여전히 유효하지 않으면 기본값 사용
                    logger.warning("JavaScript에서도 유효한 요소 크기를 얻지 못함, 전체 페이지 스크린샷 사용")
                    temp_screenshot = f"temp_{name_prefix}_{int(time.time())}.png"
                    driver.save_screenshot(temp_screenshot)
                    return temp_screenshot
            except Exception as js_err:
                logger.warning(f"JavaScript 요소 크기 확인 중 오류: {str(js_err)}")
                temp_screenshot = f"temp_{name_prefix}_{int(time.time())}.png"
                driver.save_screenshot(temp_screenshot)
                return temp_screenshot
        
        # 요소가 화면 밖에 있는지 확인
        viewport_height = driver.execute_script("return window.innerHeight")
        viewport_width = driver.execute_script("return window.innerWidth")
        
        # 요소가 뷰포트 밖에 있으면 스크롤하여 보이게 함
        if (location['y'] < 0 or 
            location['y'] + size['height'] > viewport_height or 
            location['x'] < 0 or 
            location['x'] + size['width'] > viewport_width):
            
            logger.info(f"요소가 뷰포트 밖에 있어 스크롤로 조정: 위치={location}, 크기={size}, 뷰포트={viewport_width}x{viewport_height}")
            
            # 요소가 보이도록 스크롤
            driver.execute_script("arguments[0].scrollIntoView({block: 'center', inline: 'center'});", element)
            time.sleep(0.5)  # 스크롤 후 잠시 대기
            
            # 스크롤 후 위치와 크기 업데이트
            try:
                location = element.location
                size = element.size
            except:
                # 위치를 얻지 못하면 JavaScript로 시도
                js_rect = driver.execute_script("""
                    var rect = arguments[0].getBoundingClientRect();
                    return {
                        top: rect.top,
                        left: rect.left,
                        width: rect.width,
                        height: rect.height
                    };
                """, element)
                
                location = {'x': js_rect['left'], 'y': js_rect['top']}
                size = {'width': js_rect['width'], 'height': js_rect['height']}
        
        # 전체 페이지 스크린샷
        temp_screenshot = f"temp_{name_prefix}_{int(time.time())}.png"
        driver.save_screenshot(temp_screenshot)
        
        # 페이지 스크롤 위치 고려
        scroll_x = driver.execute_script("return window.pageXOffset")
        scroll_y = driver.execute_script("return window.pageYOffset")
        
        # 요소 크기 확인 후 조정
        try:
            from PIL import Image
            
            # 스크린샷 열기
            img = Image.open(temp_screenshot)
            
            # 이미지 크기 얻기
            img_width, img_height = img.size
            
            # 요소 경계 계산 (스크롤 위치 고려)
            left = max(0, location['x'])
            top = max(0, location['y'])
            right = min(img_width, left + size['width'])
            bottom = min(img_height, top + size['height'])
            
            # 크기가 유효한지 확인 (최소 크기 요구)
            if right - left < 10 or bottom - top < 10:
                logger.warning(f"크기가 너무 작음 ({right-left}x{bottom-top}), 전체 이미지 사용")
                return temp_screenshot
            
            # 이미지 크롭
            cropped_img = img.crop((left, top, right, bottom))
            
            # 크롭된 이미지가 너무 작으면 원본 반환
            if cropped_img.width < 10 or cropped_img.height < 10:
                logger.warning(f"크롭된 이미지가 너무 작음 ({cropped_img.width}x{cropped_img.height}), 전체 이미지 사용")
                return temp_screenshot
            
            # 최종 이미지 저장
            element_screenshot = f"{name_prefix}_{int(time.time())}.png"
            cropped_img.save(element_screenshot)
            
            # 임시 스크린샷 정리
            try:
                os.remove(temp_screenshot)
            except:
                pass
                
            logger.info(f"요소 스크린샷 캡처: {element_screenshot} (크기: {cropped_img.width}x{cropped_img.height})")
            return element_screenshot
        except Exception as img_err:
            logger.warning(f"이미지 처리 중 오류: {str(img_err)}")
            return temp_screenshot
            
    except Exception as e:
        logger.error(f"요소 스크린샷 캡처 오류: {str(e)}")
        
        # 오류 발생 시 전체 페이지 스크린샷
        try:
            temp_screenshot = f"temp_{name_prefix}_{int(time.time())}.png"
            driver.save_screenshot(temp_screenshot)
            return temp_screenshot
        except:
            return None

def extract_from_document_viewer(driver):
    """
    Extract data specifically from document viewer interface.
    
    Args:
        driver: Selenium WebDriver instance
        
    Returns:
        dict: Dictionary of sheet names to pandas DataFrames, or None if extraction fails
    """
    all_sheets = {}
    
    try:
        # Wait for viewer to initialize
        time.sleep(5)  # Initial wait for viewer to load
        
        # Take screenshot for debugging
        screenshot_path = f"document_viewer_{int(time.time())}.png"
        driver.save_screenshot(screenshot_path)
        
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
                tabs = driver.find_elements(By.CSS_SELECTOR, selector)
                if tabs:
                    sheet_tabs = tabs
                    logger.info(f"Found {len(tabs)} sheet tabs with selector: {selector}")
                    break
            except:
                continue
        
        # Process multiple sheets if found
        if sheet_tabs and len(sheet_tabs) > 0:
            for i, tab in enumerate(sheet_tabs):
                sheet_name = tab.text.strip() if tab.text.strip() else f"Sheet_{i+1}"
                logger.info(f"Processing sheet {i+1}/{len(sheet_tabs)}: {sheet_name}")
                
                # Click on tab if not the first one (first is usually selected by default)
                if i > 0:
                    try:
                        tab.click()
                        time.sleep(3)  # Wait for sheet content to load
                    except Exception as click_err:
                        logger.warning(f"Failed to click sheet tab {i+1}: {str(click_err)}")
                        continue
                
                # Try different iframe selectors
                iframe_found = False
                iframe_selectors = ["#innerWrap", "#viewerFrame", "iframe", "[id*='frame']"]
                
                for selector in iframe_selectors:
                    try:
                        iframe = WebDriverWait(driver, 10).until(
                            EC.presence_of_element_located((By.CSS_SELECTOR, selector))
                        )
                        driver.switch_to.frame(iframe)
                        iframe_found = True
                        logger.info(f"Switched to iframe with selector: {selector}")
                        break
                    except:
                        continue
                
                if not iframe_found:
                    logger.warning(f"No iframe found for sheet {sheet_name}, taking screenshot for OCR")
                    # Capture screenshot for this sheet for potential OCR
                    driver.save_screenshot(f"sheet_{sheet_name}_{int(time.time())}.png")
                    continue
                
                # Get iframe content
                try:
                    iframe_html = driver.page_source
                    df = extract_table_from_html(iframe_html)
                    
                    # Switch back to main document
                    driver.switch_to.default_content()
                    
                    if df is not None and not df.empty:
                        all_sheets[sheet_name] = df
                        logger.info(f"Successfully extracted data from sheet '{sheet_name}': {df.shape[0]} rows, {df.shape[1]} columns")
                    else:
                        logger.warning(f"No table data found in sheet '{sheet_name}'")
                except Exception as content_err:
                    logger.error(f"Error extracting content from sheet '{sheet_name}': {str(content_err)}")
                    # Try to switch back to main document on error
                    try:
                        driver.switch_to.default_content()
                    except:
                        pass
        else:
            # No sheet tabs found, try to extract from a single sheet/iframe
            logger.info("No sheet tabs found, attempting single sheet extraction")
            
            # Try different iframe selectors
            iframe_found = False
            iframe_selectors = ["#innerWrap", "#viewerFrame", "iframe", "[id*='frame']"]
            
            for selector in iframe_selectors:
                try:
                    iframe = WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, selector))
                    )
                    driver.switch_to.frame(iframe)
                    iframe_found = True
                    logger.info(f"Switched to iframe with selector: {selector}")
                    break
                except:
                    continue
            
            if iframe_found:
                try:
                    iframe_html = driver.page_source
                    df = extract_table_from_html(iframe_html)
                    
                    # Switch back to main document
                    driver.switch_to.default_content()
                    
                    if df is not None and not df.empty:
                        all_sheets["Main_Sheet"] = df
                        logger.info(f"Successfully extracted data from single sheet: {df.shape[0]} rows, {df.shape[1]} columns")
                    else:
                        logger.warning("No table data found in single sheet")
                except Exception as content_err:
                    logger.error(f"Error extracting content from single sheet: {str(content_err)}")
                    try:
                        driver.switch_to.default_content()
                    except:
                        pass
            else:
                logger.warning("No iframe found, taking screenshot for OCR")
                driver.save_screenshot(f"single_sheet_{int(time.time())}.png")
        
        return all_sheets if all_sheets else None
        
    except Exception as e:
        logger.error(f"Error in document viewer extraction: {str(e)}")
        try:
            driver.switch_to.default_content()
        except:
            pass
        return None

def explore_synap_doc_viewer(driver, file_params=None):
    """
    SynapDocViewServer의 내부 구조를 탐색하고 분석하는 함수
    
    Args:
        driver: Selenium WebDriver 인스턴스
        file_params: 파일 접근 파라미터 (선택 사항)
        
    Returns:
        dict: 수집된 정보 및 분석 결과
    """
    results = {
        'timestamp': int(time.time()),
        'url': driver.current_url,
        'title': driver.title,
        'structure': {},
        'objects': {},
        'html_snippets': {},
        'potential_data': {}
    }
    
    try:
        # 1. 현재 페이지 정보 수집
        logger.info("SynapDocViewServer 탐색 시작...")
        logger.info(f"현재 URL: {driver.current_url}")
        logger.info(f"페이지 제목: {driver.title}")
        
        # 전체 HTML 저장 (디버깅용)
        html_path = f"synap_html_snapshot_{int(time.time())}.html"
        with open(html_path, 'w', encoding='utf-8') as f:
            f.write(driver.page_source)
        logger.info(f"HTML 스냅샷 저장됨: {html_path}")
        results['html_path'] = html_path
        
        # 2. JavaScript 환경 탐색
        js_env = driver.execute_script("""
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
        """)
        
        logger.info(f"JavaScript 환경 정보: {json.dumps(js_env, indent=2)}")
        results['js_env'] = js_env
        
        # 3. 주요 객체 구조 탐색
        if js_env.get('hasLocalSynap'):
            localSynap_info = driver.execute_script("""
                try {
                    if (typeof localSynap === 'undefined') return null;
                    
                    // 안전하게 객체 속성 추출
                    function safeGetProps(obj) {
                        if (!obj) return null;
                        
                        const props = {};
                        for (const key of Object.keys(obj)) {
                            try {
                                const val = obj[key];
                                const type = typeof val;
                                
                                if (type === 'function') {
                                    props[key] = 'function';
                                } else if (type === 'object') {
                                    props[key] = val === null ? 'null' : 
                                        Array.isArray(val) ? `array(${val.length})` : 'object';
                                } else if (['string', 'number', 'boolean'].includes(type)) {
                                    props[key] = String(val).substring(0, 100); // 문자열 제한
                                } else {
                                    props[key] = type;
                                }
                            } catch (e) {
                                props[key] = `[Error: ${e.message}]`;
                            }
                        }
                        return props;
                    }
                    
                    return {
                        properties: safeGetProps(localSynap),
                        methods: Object.getOwnPropertyNames(Object.getPrototypeOf(localSynap))
                            .filter(name => typeof localSynap[name] === 'function'),
                        // 중요 메소드 존재 여부
                        hasSetZoom: typeof localSynap.setZoom === 'function',
                        hasGetSheets: typeof localSynap.getSheets === 'function',
                        hasUpdateChartInfo: typeof localSynap.updateChartInfo === 'function'
                    };
                } catch (e) {
                    return { error: e.message };
                }
            """)
            
            logger.info(f"localSynap 객체 정보: {json.dumps(localSynap_info, indent=2)}")
            results['objects']['localSynap'] = localSynap_info
        
        # WM 객체 탐색
        if js_env.get('hasWM'):
            wm_info = driver.execute_script("""
                try {
                    if (typeof WM === 'undefined') return null;
                    
                    return {
                        methods: Object.getOwnPropertyNames(WM)
                            .filter(name => typeof WM[name] === 'function'),
                        hasGetSheets: typeof WM.getSheets === 'function',
                        currentSheetIndex: typeof sheetIndex !== 'undefined' ? sheetIndex : null
                    };
                } catch (e) {
                    return { error: e.message };
                }
            """)
            
            logger.info(f"WM 객체 정보: {json.dumps(wm_info, indent=2)}")
            results['objects']['WM'] = wm_info
            
            # WM.getSheets() 호출 시도
            if wm_info.get('hasGetSheets'):
                sheets_info = driver.execute_script("""
                    try {
                        const sheets = WM.getSheets();
                        if (!sheets) return { error: 'No sheets returned' };
                        
                        return {
                            count: sheets.length,
                            sheets: sheets.map((sheet, idx) => {
                                try {
                                    return {
                                        index: idx,
                                        name: sheet.getName ? sheet.getName() : `Sheet${idx+1}`,
                                        hasData: !!sheet.getData,
                                        rowCount: sheet.getRowCount ? sheet.getRowCount() : null,
                                        colCount: sheet.getColCount ? sheet.getColCount() : null
                                    };
                                } catch (e) {
                                    return { index: idx, error: e.message };
                                }
                            })
                        };
                    } catch (e) {
                        return { error: e.message };
                    }
                """)
                
                logger.info(f"시트 정보: {json.dumps(sheets_info, indent=2)}")
                results['objects']['sheets'] = sheets_info
                
                # 각 시트의 데이터 수집 시도
                if isinstance(sheets_info, dict) and 'sheets' in sheets_info:
                    for sheet_info in sheets_info.get('sheets', []):
                        sheet_idx = sheet_info.get('index')
                        sheet_name = sheet_info.get('name', f"Sheet_{sheet_idx}")
                        
                        # 시트 데이터 추출 시도
                        sheet_data = driver.execute_script(f"""
                            try {{
                                const sheets = WM.getSheets();
                                if (!sheets || !sheets[{sheet_idx}] || !sheets[{sheet_idx}].getData) 
                                    return null;
                                
                                const data = sheets[{sheet_idx}].getData();
                                if (!data || !Array.isArray(data)) return null;
                                
                                // 최대 10행 x 10열 제한 (로그 크기 제한)
                                return data.slice(0, 10).map(row => 
                                    Array.isArray(row) ? row.slice(0, 10) : row
                                );
                            }} catch (e) {{
                                return {{ error: e.message }};
                            }}
                        """)
                        
                        if sheet_data:
                            logger.info(f"시트 '{sheet_name}' 데이터 샘플: {sheet_data[:3] if isinstance(sheet_data, list) else sheet_data}")
                            results['potential_data'][f"sheet_{sheet_idx}"] = sheet_data
        
        # 4. DOM 구조 탐색
        # mainTable 구조 탐색
        if js_env.get('hasMainTable'):
            main_table_info = driver.execute_script("""
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
                    
                    // mainTable 내 모든 셀 개수
                    const allCells = mainTable.querySelectorAll('div[class*="td"]');
                    
                    // 첫 행의 셀 내용
                    const headerContents = [];
                    if (cells && cells.length > 0) {
                        for (let i = 0; i < Math.min(cells.length, 10); i++) {
                            headerContents.push(cells[i].textContent.trim());
                        }
                    }
                    
                    // 첫 행의 첫 셀과 마지막 셀의 내용
                    const firstCellContent = cells && cells[0] ? cells[0].textContent.trim() : null;
                    const lastCellContent = cells && cells[cells.length-1] ? 
                        cells[cells.length-1].textContent.trim() : null;
                    
                    return {
                        element: 'mainTable',
                        rowCount,
                        colCount,
                        totalCells: allCells.length,
                        class: mainTable.className,
                        headerContents,
                        firstCellContent,
                        lastCellContent,
                        style: mainTable.getAttribute('style')
                    };
                } catch (e) {
                    return { error: e.message };
                }
            """)
            
            logger.info(f"mainTable 구조 정보: {json.dumps(main_table_info, indent=2)}")
            results['structure']['mainTable'] = main_table_info
            
            # 테이블 데이터 샘플 추출
            table_data_sample = driver.execute_script("""
                try {
                    const mainTable = document.getElementById('mainTable');
                    if (!mainTable) return null;
                    
                    const rows = mainTable.querySelectorAll('div[class*="tr"]');
                    if (!rows || rows.length === 0) {
                        // 대체 방법: 모든 직계 자식 div를 행으로 간주
                        const childDivs = mainTable.querySelectorAll(':scope > div');
                        if (!childDivs || childDivs.length === 0) return null;
                        
                        // 최대 10개 행 처리
                        const tableData = [];
                        for (let i = 0; i < Math.min(childDivs.length, 10); i++) {
                            const row = childDivs[i];
                            const cells = row.querySelectorAll('div');
                            if (!cells || cells.length === 0) continue;
                            
                            const rowData = [];
                            for (let j = 0; j < Math.min(cells.length, 10); j++) {
                                rowData.push(cells[j].textContent.trim());
                            }
                            
                            if (rowData.length > 0) {
                                tableData.push(rowData);
                            }
                        }
                        
                        return tableData;
                    }
                    
                    // 최대 10개 행 처리
                    const tableData = [];
                    for (let i = 0; i < Math.min(rows.length, 10); i++) {
                        const row = rows[i];
                        const cells = row.querySelectorAll('div[class*="td"]');
                        if (!cells || cells.length === 0) continue;
                        
                        const rowData = [];
                        for (let j = 0; j < Math.min(cells.length, 10); j++) {
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
            """)
            
            if table_data_sample:
                logger.info(f"테이블 데이터 샘플: {table_data_sample[:3] if isinstance(table_data_sample, list) else table_data_sample}")
                results['potential_data']['table_sample'] = table_data_sample
        
        # 5. 시트 탭 정보 수집
        if js_env.get('hasSheetTabs'):
            sheet_tabs_info = driver.execute_script("""
                try {
                    const tabs = document.querySelectorAll('.sheet-list__sheet-tab');
                    if (!tabs || tabs.length === 0) return null;
                    
                    return {
                        count: tabs.length,
                        tabs: Array.from(tabs).map((tab, idx) => ({
                            index: idx,
                            text: tab.textContent.trim(),
                            isActive: tab.classList.contains('active') || 
                                     tab.classList.contains('sheet-list__sheet-tab--active'),
                            id: tab.id || null,
                            hasClickHandler: tab.onclick !== null || 
                                            tab.getAttribute('onclick') !== null
                        }))
                    };
                } catch (e) {
                    return { error: e.message };
                }
            """)
            
            logger.info(f"시트 탭 정보: {json.dumps(sheet_tabs_info, indent=2)}")
            results['structure']['sheetTabs'] = sheet_tabs_info
        
        # 6. iframe 탐색
        iframe_count = driver.execute_script("return document.querySelectorAll('iframe').length;")
        if iframe_count > 0:
            iframe_info = driver.execute_script("""
                try {
                    const iframes = document.querySelectorAll('iframe');
                    return Array.from(iframes).map((iframe, idx) => ({
                        index: idx,
                        id: iframe.id || null,
                        name: iframe.name || null,
                        src: iframe.src || null,
                        width: iframe.width || null,
                        height: iframe.height || null
                    }));
                } catch (e) {
                    return { error: e.message };
                }
            """)
            
            logger.info(f"iframe 정보: {json.dumps(iframe_info, indent=2)}")
            results['structure']['iframes'] = iframe_info
            
            # 각 iframe 내용 탐색
            for i in range(iframe_count):
                try:
                    driver.switch_to.frame(i)
                    
                    iframe_content_info = driver.execute_script("""
                        return {
                            title: document.title,
                            hasTable: !!document.querySelector('table'),
                            hasMainTable: !!document.getElementById('mainTable'),
                            bodyText: document.body.textContent.substring(0, 200) + '...'
                        };
                    """)
                    
                    logger.info(f"iframe {i} 내용: {json.dumps(iframe_content_info, indent=2)}")
                    results['structure'][f'iframe_{i}_content'] = iframe_content_info
                    
                    # 기본 컨텐츠로 돌아가기
                    driver.switch_to.default_content()
                except Exception as iframe_err:
                    logger.warning(f"iframe {i} 접근 오류: {str(iframe_err)}")
                    try:
                        driver.switch_to.default_content()
                    except:
                        pass
        
        # 7. 최종 HTML 구조 요약
        html_summary = driver.execute_script("""
            return {
                bodyChildren: document.body.children.length,
                divCount: document.querySelectorAll('div').length,
                tableCount: document.querySelectorAll('table').length,
                iframeCount: document.querySelectorAll('iframe').length,
                mainIds: Array.from(document.querySelectorAll('[id]')).map(el => el.id).slice(0, 20)
            };
        """)
        
        logger.info(f"HTML 구조 요약: {json.dumps(html_summary, indent=2)}")
        results['structure']['summary'] = html_summary
        
        return results
        
    except Exception as e:
        logger.error(f"SynapDocViewServer 탐색 중 오류: {str(e)}")
        results['error'] = str(e)
        return results


def extract_synap_data_using_structure_info(driver, structure_info):
    """
    SynapDocViewServer 구조 정보를 기반으로 데이터 추출
    
    Args:
        driver: Selenium WebDriver 인스턴스
        structure_info: SynapDocViewServer 구조 정보
        
    Returns:
        dict: 추출된 시트 데이터
    """
    extracted_data = {}
    
    try:
        js_env = structure_info.get('js_env', {})
        objects = structure_info.get('objects', {})
        
        # 1. WM.getSheets()가 있는 경우
        wm_info = objects.get('WM', {})
        sheets_info = objects.get('sheets', {})
        
        if wm_info.get('hasGetSheets') and isinstance(sheets_info.get('sheets'), list):
            logger.info("WM.getSheets() 메소드를 통한 데이터 추출 시도")
            
            sheets = sheets_info.get('sheets', [])
            for sheet in sheets:
                sheet_idx = sheet.get('index')
                sheet_name = sheet.get('name', f"Sheet_{sheet_idx}")
                
                sheet_data = driver.execute_script(f"""
                    try {{
                        const sheets = WM.getSheets();
                        if (!sheets || !sheets[{sheet_idx}]) return null;
                        
                        const sheet = sheets[{sheet_idx}];
                        if (!sheet.getData) return null;
                        
                        return sheet.getData();
                    }} catch (e) {{
                        return {{ error: e.message }};
                    }}
                """)
                
                if isinstance(sheet_data, list) and sheet_data:
                    # 헤더와 데이터 구분
                    headers = sheet_data[0] if sheet_data else []
                    data = sheet_data[1:] if len(sheet_data) > 1 else []
                    
                    extracted_data[sheet_name] = {
                        'headers': headers,
                        'data': data,
                        'source': 'WM.getSheets'
                    }
                    
                    logger.info(f"시트 '{sheet_name}' 데이터 추출 성공: {len(data)}행 {len(headers)}열")
            
            if extracted_data:
                return extracted_data
        
        # 2. mainTable에서 직접 추출
        main_table_info = structure_info.get('structure', {}).get('mainTable', {})
        
        if main_table_info and not isinstance(main_table_info, str):
            logger.info("mainTable에서 직접 데이터 추출 시도")
            
            table_data = driver.execute_script("""
                try {
                    const mainTable = document.getElementById('mainTable');
                    if (!mainTable) return null;
                    
                    // 다양한 방법으로 행 찾기
                    let rows = mainTable.querySelectorAll('div[class*="tr"]');
                    if (!rows || rows.length === 0) {
                        rows = mainTable.children;
                    }
                    
                    if (!rows || rows.length === 0) return null;
                    
                    const tableData = [];
                    for (let i = 0; i < rows.length; i++) {
                        const row = rows[i];
                        
                        // 다양한 방법으로 셀 찾기
                        let cells = row.querySelectorAll('div[class*="td"]');
                        if (!cells || cells.length === 0) {
                            cells = row.children;
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
            """)
            
            if isinstance(table_data, list) and table_data:
                sheet_name = "MainTable"
                
                # 첫 번째 행이 헤더인지 판단
                if len(table_data) > 1:
                    headers = table_data[0]
                    data = table_data[1:]
                    
                    extracted_data[sheet_name] = {
                        'headers': headers,
                        'data': data,
                        'source': 'mainTable_direct'
                    }
                    
                    logger.info(f"mainTable에서 데이터 추출 성공: {len(data)}행 {len(headers)}열")
                    return extracted_data
        
        # 3. 시트 탭이 있는 경우, 각 탭 클릭하여 데이터 추출
        sheet_tabs_info = structure_info.get('structure', {}).get('sheetTabs', {})
        if isinstance(sheet_tabs_info, dict) and sheet_tabs_info.get('tabs'):
            logger.info("시트 탭 클릭을 통한 데이터 추출 시도")
            
            tabs = sheet_tabs_info.get('tabs', [])
            for tab in tabs:
                tab_idx = tab.get('index')
                tab_text = tab.get('text', f"Tab_{tab_idx}")
                
                # 탭 클릭
                try:
                    driver.execute_script(f"""
                        const tabs = document.querySelectorAll('.sheet-list__sheet-tab');
                        if (tabs && tabs[{tab_idx}]) {{
                            tabs[{tab_idx}].click();
                        }}
                    """)
                    
                    logger.info(f"탭 '{tab_text}' 클릭")
                    time.sleep(2)  # 탭 전환 대기
                    
                    # 현재 표시된 데이터 추출
                    tab_data = driver.execute_script("""
                        try {
                            const mainTable = document.getElementById('mainTable');
                            if (!mainTable) return null;
                            
                            // 다양한 방법으로 행 찾기
                            let rows = mainTable.querySelectorAll('div[class*="tr"]');
                            if (!rows || rows.length === 0) {
                                rows = mainTable.children;
                            }
                            
                            if (!rows || rows.length === 0) return null;
                            
                            const tableData = [];
                            for (let i = 0; i < rows.length; i++) {
                                const row = rows[i];
                                
                                // 다양한 방법으로 셀 찾기
                                let cells = row.querySelectorAll('div[class*="td"]');
                                if (!cells || cells.length === 0) {
                                    cells = row.children;
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
                    """)
                    
                    if isinstance(tab_data, list) and tab_data:
                        sheet_name = tab_text.strip()
                        
                        # 첫 번째 행이 헤더인지 판단
                        if len(tab_data) > 1:
                            headers = tab_data[0]
                            data = tab_data[1:]
                            
                            extracted_data[sheet_name] = {
                                'headers': headers,
                                'data': data,
                                'source': 'tab_click'
                            }
                            
                            logger.info(f"탭 '{tab_text}'에서 데이터 추출 성공: {len(data)}행 {len(headers)}열")
                
                except Exception as tab_err:
                    logger.warning(f"탭 '{tab_text}' 처리 중 오류: {str(tab_err)}")
        
        return extracted_data
            
    except Exception as e:
        logger.error(f"SynapDocViewServer 데이터 추출 중 오류: {str(e)}")
        return extracted_data

def extract_with_javascript(driver):
    """
    JavaScript를 사용하여 페이지의 테이블 데이터를 직접 추출합니다.
    DOM 구조에 관계없이 표 형식 데이터를 찾아내는 데 유용합니다.
    
    Args:
        driver: Selenium WebDriver 인스턴스
        
    Returns:
        dict: 시트 이름을 키로, DataFrame을 값으로 하는 딕셔너리
    """
    try:
        logger.info("JavaScript를 사용하여 테이블 데이터 추출 시도")
        
        # 다양한 테이블 구조를 추출하는 JavaScript 코드
        js_code = """
        function extractTables() {
            const results = [];
            
            // 1. 일반 HTML 테이블 추출
            const tables = document.querySelectorAll('table');
            for (let i = 0; i < tables.length; i++) {
                const table = tables[i];
                const tableData = {
                    id: 'HTML_Table_' + (i + 1),
                    headers: [],
                    rows: []
                };
                
                // 헤더 추출
                const headerRows = table.querySelectorAll('thead tr, tr:first-child');
                if (headerRows.length > 0) {
                    const headerCells = headerRows[0].querySelectorAll('th, td');
                    for (let j = 0; j < headerCells.length; j++) {
                        const cell = headerCells[j];
                        // colspan 처리
                        const colspan = parseInt(cell.getAttribute('colspan')) || 1;
                        const headerText = cell.textContent.trim() || 'Column_' + j;
                        
                        tableData.headers.push(headerText);
                        // 추가 colspan 컬럼 생성
                        for (let c = 1; c < colspan; c++) {
                            tableData.headers.push(headerText + '_' + c);
                        }
                    }
                }
                
                // 데이터 행 추출
                const dataRows = table.querySelectorAll('tbody tr, tr:not(:first-child)');
                for (let j = 0; j < dataRows.length; j++) {
                    const row = dataRows[j];
                    const rowData = [];
                    const cells = row.querySelectorAll('td, th');
                    
                    for (let k = 0; k < cells.length; k++) {
                        const cell = cells[k];
                        const colspan = parseInt(cell.getAttribute('colspan')) || 1;
                        const cellText = cell.textContent.trim();
                        
                        rowData.push(cellText);
                        // 추가 colspan 컬럼 생성
                        for (let c = 1; c < colspan; c++) {
                            rowData.push(cellText);
                        }
                    }
                    
                    if (rowData.length > 0) {
                        tableData.rows.push(rowData);
                    }
                }
                
                // 최소한의 데이터가 있는 경우만 추가
                if (tableData.rows.length > 0) {
                    results.push(tableData);
                }
            }
            
            // 2. 특수한 문서 뷰어에서 테이블 구조 추출 (SynapDocViewServer 등)
            // 주요 컨테이너 찾기
            const containers = [
                document.getElementById('mainTable'),
                document.getElementById('viewerFrame'),
                document.getElementById('innerWrap'),
                ...document.querySelectorAll('.mainTable, .viewerContainer, .docContent')
            ].filter(el => el !== null);
            
            for (let i = 0; i < containers.length; i++) {
                const container = containers[i];
                const tableData = {
                    id: 'Container_' + (i + 1),
                    headers: [],
                    rows: []
                };
                
                // DIV 기반 행 찾기
                let rows = container.querySelectorAll('div[class*="tr"], div[class*="row"]');
                if (rows.length === 0) {
                    // 클래스 없는 div도 시도
                    rows = Array.from(container.children).filter(el => el.tagName === 'DIV');
                }
                
                if (rows.length > 0) {
                    // 첫 번째 행을 헤더로 처리
                    const headerRow = rows[0];
                    const headerCells = headerRow.querySelectorAll('div[class*="td"], div[class*="cell"]');
                    
                    if (headerCells.length > 0) {
                        for (let j = 0; j < headerCells.length; j++) {
                            const headerText = headerCells[j].textContent.trim() || 'Column_' + j;
                            tableData.headers.push(headerText);
                        }
                        
                        // 데이터 행 추출 (첫번째 행 제외)
                        for (let j = 1; j < rows.length; j++) {
                            const dataRow = rows[j];
                            const dataCells = dataRow.querySelectorAll('div[class*="td"], div[class*="cell"]');
                            
                            if (dataCells.length > 0) {
                                const rowData = [];
                                for (let k = 0; k < dataCells.length; k++) {
                                    rowData.push(dataCells[k].textContent.trim());
                                }
                                tableData.rows.push(rowData);
                            }
                        }
                        
                        // 최소한의 데이터가 있는 경우만 추가
                        if (tableData.rows.length > 0) {
                            results.push(tableData);
                        }
                    }
                }
            }
            
            // 3. 그리드 형태의 컴포넌트 추출
            const grids = document.querySelectorAll('.grid, [role="grid"], [class*="grid"]');
            for (let i = 0; i < grids.length; i++) {
                const grid = grids[i];
                const tableData = {
                    id: 'Grid_' + (i + 1),
                    headers: [],
                    rows: []
                };
                
                // 헤더 행 찾기
                const headerRows = grid.querySelectorAll('.header, [role="rowheader"], [class*="header"]');
                if (headerRows.length > 0) {
                    const headerCells = headerRows[0].querySelectorAll('div, span');
                    for (let j = 0; j < headerCells.length; j++) {
                        const headerText = headerCells[j].textContent.trim() || 'Column_' + j;
                        tableData.headers.push(headerText);
                    }
                }
                
                // 데이터 행 찾기
                const dataRows = grid.querySelectorAll('.row, [role="row"], [class*="row"]');
                for (let j = 0; j < dataRows.length; j++) {
                    const row = dataRows[j];
                    if (row.closest('.header, [role="rowheader"], [class*="header"]')) {
                        continue; // 헤더 행 건너뛰기
                    }
                    
                    const rowData = [];
                    const cells = row.querySelectorAll('.cell, [role="cell"], [class*="cell"]');
                    for (let k = 0; k < cells.length; k++) {
                        rowData.push(cells[k].textContent.trim());
                    }
                    
                    if (rowData.length > 0) {
                        tableData.rows.push(rowData);
                    }
                }
                
                // 최소한의 데이터가 있는 경우만 추가
                if (tableData.headers.length > 0 && tableData.rows.length > 0) {
                    results.push(tableData);
                }
            }
            
            // 4. Synap Viewer 특수 처리
            try {
                // 시트 탭 확인
                const sheetTabs = document.querySelectorAll('.sheet-list__sheet-tab');
                if (sheetTabs.length > 0) {
                    // 현재 선택된 시트에서 데이터 추출
                    // 표시된 셀 추출
                    const cells = document.querySelectorAll('.cell-wrapper');
                    
                    // 셀 위치와 내용으로 그리드 구조 재구성
                    if (cells.length > 0) {
                        const cellMap = new Map();
                        let maxRow = 0;
                        let maxCol = 0;
                        
                        cells.forEach(cell => {
                            // 위치 가져오기 (스타일 또는 데이터 속성에서)
                            let row = parseInt(cell.getAttribute('data-row') || '0');
                            let col = parseInt(cell.getAttribute('data-col') || '0');
                            
                            // 스타일에서 위치 추출 시도
                            if (!row || !col) {
                                const style = cell.getAttribute('style') || '';
                                const rowMatch = style.match(/top:\\s*(\\d+)/);
                                const colMatch = style.match(/left:\\s*(\\d+)/);
                                
                                if (rowMatch) row = Math.floor(parseInt(rowMatch[1]) / 20);
                                if (colMatch) col = Math.floor(parseInt(colMatch[1]) / 100);
                            }
                            
                            maxRow = Math.max(maxRow, row);
                            maxCol = Math.max(maxCol, col);
                            
                            // 맵에 셀 추가
                            cellMap.set(`${row},${col}`, cell.textContent.trim());
                        });
                        
                        // 그리드 데이터로 테이블 생성
                        const tableData = {
                            id: 'SynapSheet',
                            headers: [],
                            rows: []
                        };
                        
                        // 첫 번째 행을 헤더로 가정
                        for (let c = 0; c <= maxCol; c++) {
                            const headerText = cellMap.get(`0,${c}`) || `Column_${c}`;
                            tableData.headers.push(headerText);
                        }
                        
                        // 나머지 행을 데이터로 처리
                        for (let r = 1; r <= maxRow; r++) {
                            const rowData = [];
                            for (let c = 0; c <= maxCol; c++) {
                                rowData.push(cellMap.get(`${r},${c}`) || '');
                            }
                            tableData.rows.push(rowData);
                        }
                        
                        if (tableData.rows.length > 0) {
                            results.push(tableData);
                        }
                    }
                }
            } catch (e) {
                console.error("Synap 뷰어 처리 중 오류:", e);
            }
            
            return results;
        }
        
        return extractTables();
        """
        
        # JavaScript 실행하여 데이터 추출
        table_data_list = driver.execute_script(js_code)
        
        if not table_data_list or len(table_data_list) == 0:
            logger.warning("JavaScript로 테이블 데이터를 추출하지 못했습니다")
            return None
        
        logger.info(f"JavaScript로 {len(table_data_list)}개 테이블 추출")
        
        # 추출된 데이터를 DataFrame으로 변환
        sheets_data = {}
        
        for table_data in table_data_list:
            table_id = table_data.get('id', f"Table_{len(sheets_data) + 1}")
            headers = table_data.get('headers', [])
            rows = table_data.get('rows', [])
            
            if not rows:
                logger.warning(f"테이블 {table_id}에 행 데이터가 없습니다")
                continue
            
            # 헤더가 없으면 자동 생성
            if not headers or len(headers) == 0:
                max_cols = max(len(row) for row in rows)
                headers = [f"Column_{i+1}" for i in range(max_cols)]
            
            # 헤더 길이에 맞게 행 데이터 조정
            processed_rows = []
            for row in rows:
                if len(row) < len(headers):
                    # 부족한 열 채우기
                    processed_row = row + [''] * (len(headers) - len(row))
                elif len(row) > len(headers):
                    # 초과 열 자르기
                    processed_row = row[:len(headers)]
                else:
                    processed_row = row
                
                processed_rows.append(processed_row)
            
            # DataFrame 생성
            try:
                df = pd.DataFrame(processed_rows, columns=headers)
                
                # 데이터 정제
                df = clean_dataframe(df)
                
                if not df.empty and df.shape[0] >= 1 and df.shape[1] >= 1:
                    sheets_data[table_id] = df
                    logger.info(f"테이블 {table_id} 추출 성공: {df.shape[0]}행 {df.shape[1]}열")
                else:
                    logger.warning(f"테이블 {table_id}의 정제된 DataFrame이 비어 있습니다")
            except Exception as df_err:
                logger.error(f"테이블 {table_id}의 DataFrame 생성 중 오류: {str(df_err)}")
        
        if not sheets_data:
            logger.warning("JavaScript로 추출된 유효한 테이블이 없습니다")
            return None
        
        return sheets_data
        
    except Exception as e:
        logger.error(f"JavaScript 데이터 추출 중 오류: {str(e)}")
        return None


def clean_dataframe(df):
    """
    Clean and normalize a DataFrame.
    
    Args:
        df: pandas DataFrame to clean
        
    Returns:
        pandas DataFrame: Cleaned DataFrame
    """
    if df is None or df.empty:
        return df
        
    try:
        # Remove completely empty rows and columns
        df = df.replace('', np.nan)
        df = df.dropna(how='all').reset_index(drop=True)
        df = df.loc[:, ~df.isna().all()]
        
        # Fill NaN back with empty strings for text processing
        df = df.fillna('')
        
        # Attempt numeric conversion for likely numeric columns
        for col in df.columns:
            # Skip columns that are clearly not numeric (long text)
            if df[col].astype(str).str.len().mean() > 20:
                continue
                
            # Clean strings that might be numeric
            cleaned = df[col].astype(str).str.replace(',', '').str.replace('%', '')
            
            # Check if column looks numeric
            if cleaned.str.match(r'^-?\d+\.?\d*').mean() > 0.7:  # If >70% match numeric pattern
                try:
                    df[col] = pd.to_numeric(cleaned, errors='coerce')
                    df[col] = df[col].fillna(0)  # Replace NaN from failed conversions
                except:
                    pass  # Keep as string if conversion fails
        
        # Remove duplicate rows
        df = df.drop_duplicates().reset_index(drop=True)
        
        return df
    except Exception as e:
        logger.warning(f"Error cleaning DataFrame: {str(e)}")
        return df


def extract_data_from_screenshot(screenshot_path):
    """
    Enhanced function to extract tabular data from screenshots with better image preprocessing
    and table structure recognition.
    
    Args:
        screenshot_path (str): Path to the screenshot file
        
    Returns:
        list: List of extracted DataFrames
    """
    try:
        import cv2
        import numpy as np
        import pandas as pd
        import pytesseract
        from PIL import Image, ImageEnhance
        
        logger.info(f"Starting enhanced OCR extraction from: {screenshot_path}")
        
        # Load image
        image = cv2.imread(screenshot_path)
        if image is None:
            logger.error(f"Failed to load image: {screenshot_path}")
            return []
        
        # Save original dimensions for reference
        original_height, original_width = image.shape[:2]
        logger.info(f"Original image dimensions: {original_width}x{original_height}")
        
        # Image preprocessing pipeline for better OCR
        processed_images = preprocess_image_for_ocr(image)
        
        # Table structure detection
        tables_info = detect_table_structure(processed_images['table_structure'])
        
        all_dataframes = []
        
        # If table structure is detected
        if tables_info and len(tables_info) > 0:
            logger.info(f"Detected {len(tables_info)} table structures")
            
            for i, table_info in enumerate(tables_info):
                logger.info(f"Processing table {i+1}/{len(tables_info)}")
                
                # Extract cells based on detected structure
                cells_data = extract_cells_from_table(processed_images['ocr_ready'], table_info)
                
                if cells_data and len(cells_data) > 0:
                    # Convert to DataFrame
                    df = create_dataframe_from_cells(cells_data)
                    if df is not None and not df.empty:
                        all_dataframes.append(df)
                        logger.info(f"Successfully created DataFrame from table {i+1}: {df.shape[0]} rows, {df.shape[1]} columns")
                    else:
                        logger.warning(f"Failed to create DataFrame from table {i+1}")
        
        # If no tables were detected or extracted successfully, fall back to general OCR
        if not all_dataframes:
            logger.info("No tables detected or extraction failed, falling back to general OCR")
            df = extract_text_without_table_structure(screenshot_path)
            if df is not None and not df.empty:
                all_dataframes.append(df)
        
        # Save debug images
        cv2.imwrite(f"{screenshot_path}_processed_binary.png", processed_images['binary'])
        cv2.imwrite(f"{screenshot_path}_processed_lines.png", processed_images['lines'])
        
        return all_dataframes
        
    except Exception as e:
        logger.error(f"Error in enhanced OCR extraction: {str(e)}")
        # Try basic extraction as fallback
        try:
            return [extract_text_without_table_structure(screenshot_path)]
        except:
            logger.error("Basic OCR extraction also failed")
            return []



def create_dataframe_from_cells(cell_data):
    """
    Create a pandas DataFrame from extracted cell data.
    
    Args:
        cell_data: 2D list of cell text
        
    Returns:
        pandas.DataFrame: DataFrame created from cell data
    """
    try:
        if not cell_data or len(cell_data) < 2:  # Need at least header and one data row
            logger.warning("Insufficient cell data for DataFrame creation")
            return None
            
        # First row as header
        header = cell_data[0]
        data_rows = cell_data[1:]
        
        # Clean headers
        clean_headers = []
        header_counts = {}
        
        for h in header:
            h_str = str(h).strip()
            if not h_str:  # Replace empty headers
                h_str = f"Column_{len(clean_headers)}"
                
            if h_str in header_counts:
                header_counts[h_str] += 1
                clean_headers.append(f"{h_str}_{header_counts[h_str]}")
            else:
                header_counts[h_str] = 0
                clean_headers.append(h_str)
        
        # Create DataFrame
        df = pd.DataFrame(data_rows, columns=clean_headers)
        
        # Clean data
        df = df.replace(r'^\s*$', '', regex=True)  # Replace whitespace-only cells
        
        # Try to convert numeric columns
        for col in df.columns:
            # Skip columns that are clearly not numeric (long text)
            if df[col].astype(str).str.len().mean() > 20:
                continue
                
            # Clean strings that might be numeric
            cleaned = df[col].astype(str).str.replace(',', '').str.replace('%', '')
            
            # Check if column looks numeric
            try:
                is_numeric = pd.to_numeric(cleaned, errors='coerce').notna().mean() > 0.7
                if is_numeric:
                    df[col] = pd.to_numeric(cleaned, errors='coerce')
            except:
                pass  # Keep as string if conversion fails
        
        # Remove empty rows and columns
        df = df.replace('', np.nan)
        df = df.dropna(how='all').reset_index(drop=True)
        df = df.loc[:, ~df.isna().all()]
        df = df.fillna('')  # Convert NaN back to empty string
        
        # Remove duplicate rows
        df = df.drop_duplicates().reset_index(drop=True)
        
        return df
        
    except Exception as e:
        logger.error(f"Error creating DataFrame from cells: {str(e)}")
        return None

def preprocess_image_for_ocr(image):
    """
    Apply multiple preprocessing techniques to optimize image for both table structure detection
    and text recognition.
    
    Args:
        image: OpenCV image (numpy array)
        
    Returns:
        dict: Dictionary of processed images for different purposes
    """
    try:
        # Make a copy to avoid modifying original
        original = image.copy()
        
        # Convert to grayscale if not already
        if len(image.shape) == 3:
            gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
        else:
            gray = image.copy()
        
        # Create results dictionary
        result = {
            'original': original,
            'grayscale': gray
        }
        
        # Resize if image is very large (for faster processing)
        height, width = gray.shape
        max_dimension = 2000  # Maximum allowed dimension
        
        if max(height, width) > max_dimension:
            scale_factor = max_dimension / max(height, width)
            new_width = int(width * scale_factor)
            new_height = int(height * scale_factor)
            gray = cv2.resize(gray, (new_width, new_height))
            logger.info(f"Resized image from {width}x{height} to {new_width}x{new_height}")
            result['resized'] = gray
        
        # Apply noise reduction
        gray_denoised = cv2.fastNlMeansDenoising(gray, None, h=10, templateWindowSize=7, searchWindowSize=21)
        result['denoised'] = gray_denoised
        
        # Create different binary versions with various thresholds
        
        # 1. Adaptive threshold (good for varying lighting conditions)
        binary_adaptive = cv2.adaptiveThreshold(
            gray_denoised, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C, cv2.THRESH_BINARY_INV, 11, 2
        )
        result['binary_adaptive'] = binary_adaptive
        
        # 2. Otsu's threshold (good for bimodal images)
        _, binary_otsu = cv2.threshold(gray_denoised, 0, 255, cv2.THRESH_BINARY_INV + cv2.THRESH_OTSU)
        result['binary_otsu'] = binary_otsu
        
        # 3. Standard binary threshold
        _, binary = cv2.threshold(gray_denoised, 150, 255, cv2.THRESH_BINARY_INV)
        result['binary'] = binary
        
        # Create a combined binary image optimized for line detection
        binary_for_lines = cv2.bitwise_or(binary_adaptive, binary_otsu)
        
        # Morphological operations to enhance lines
        kernel_line = cv2.getStructuringElement(cv2.MORPH_RECT, (3, 3))
        binary_line_enhanced = cv2.morphologyEx(binary_for_lines, cv2.MORPH_OPEN, kernel_line, iterations=1)
        binary_line_enhanced = cv2.dilate(binary_line_enhanced, kernel_line, iterations=1)
        result['line_enhanced'] = binary_line_enhanced
        
        # Detect horizontal and vertical lines
        # Vertical lines
        vertical_kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (1, height // 30))
        vertical_lines = cv2.erode(binary_line_enhanced, vertical_kernel, iterations=3)
        vertical_lines = cv2.dilate(vertical_lines, vertical_kernel, iterations=3)
        result['vertical_lines'] = vertical_lines
        
        # Horizontal lines
        horizontal_kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (width // 30, 1))
        horizontal_lines = cv2.erode(binary_line_enhanced, horizontal_kernel, iterations=3)
        horizontal_lines = cv2.dilate(horizontal_lines, horizontal_kernel, iterations=3)
        result['horizontal_lines'] = horizontal_lines
        
        # Combine lines
        lines = cv2.bitwise_or(vertical_lines, horizontal_lines)
        result['lines'] = lines
        
        # Create image for table structure detection
        table_structure = lines.copy()
        result['table_structure'] = table_structure
        
        # Create image optimized for OCR (remove lines to improve text recognition)
        # Invert binary for better OCR
        _, binary_inv = cv2.threshold(gray_denoised, 150, 255, cv2.THRESH_BINARY)
        
        # Dilate lines slightly to ensure they cover text that touches them
        dilated_lines = cv2.dilate(lines, np.ones((3, 3), np.uint8), iterations=1)
        
        # Remove lines from the inverted binary image
        ocr_ready = cv2.bitwise_and(binary_inv, binary_inv, mask=cv2.bitwise_not(dilated_lines))
        result['ocr_ready'] = ocr_ready
        
        return result
        
    except Exception as e:
        logger.error(f"Error in image preprocessing: {str(e)}")
        # Return simple preprocessing as fallback
        if len(image.shape) == 3:
            gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
        else:
            gray = image.copy()
            
        _, binary = cv2.threshold(gray, 150, 255, cv2.THRESH_BINARY_INV)
        
        return {
            'original': image,
            'grayscale': gray,
            'binary': binary,
            'ocr_ready': gray,
            'table_structure': binary,
            'lines': binary
        }


def detect_table_structure(image):
    """
    Detect table structure from the preprocessed image.
    
    Args:
        image: Binary image optimized for table structure detection
        
    Returns:
        list: List of detected tables with their structure information
    """
    try:
        # Find contours
        contours, hierarchy = cv2.findContours(image, cv2.RETR_CCOMP, cv2.CHAIN_APPROX_SIMPLE)
        
        if not contours:
            logger.warning("No contours found for table detection")
            return []
            
        # Get image dimensions
        height, width = image.shape
        min_area = (width * height) / 1000  # Minimum area threshold
        
        # Filter contours by area and shape
        cell_contours = []
        for cnt in contours:
            area = cv2.contourArea(cnt)
            if area < min_area:
                continue
                
            x, y, w, h = cv2.boundingRect(cnt)
            # Filter out contours that are too small or too large
            if w < 10 or h < 10 or w > width * 0.9 or h > height * 0.9:
                continue
                
            cell_contours.append((x, y, w, h))
        
        if not cell_contours:
            logger.warning("No suitable cell contours found")
            return []
            
        # Sort by y-coordinate to group into rows
        cell_contours.sort(key=lambda c: c[1])
        
        # Group contours into rows based on y-coordinate
        y_tolerance = height // 40  # Adjust based on image size
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
            
        # Sort each row by x-coordinate
        for i in range(len(rows)):
            rows[i].sort(key=lambda c: c[0])
            
        # Count rows and columns
        if not rows:
            logger.warning("No rows detected in table structure")
            return []
            
        num_rows = len(rows)
        num_cols = max(len(row) for row in rows)
        
        logger.info(f"Detected table structure: {num_rows} rows × {num_cols} columns")
        
        # Check if this looks like a valid table (at least 2 rows and 2 columns)
        if num_rows < 2 or num_cols < 2:
            logger.warning(f"Structure too small to be a proper table: {num_rows} rows × {num_cols} columns")
            return []
            
        # Return table structure
        return [{
            'rows': rows,
            'num_rows': num_rows,
            'num_cols': num_cols
        }]
        
    except Exception as e:
        logger.error(f"Error detecting table structure: {str(e)}")
        return []



def extract_cells_from_table(image, table_info):
    """
    Extract cell content from the detected table structure.
    
    Args:
        image: Image optimized for OCR
        table_info: Dictionary with table structure information
        
    Returns:
        list: 2D list with extracted cell text
    """
    try:
        rows = table_info['rows']
        num_rows = table_info['num_rows']
        num_cols = table_info['num_cols']
        
        # Create tesseract configuration for different cell types
        # Header configuration (optimized for text)
        header_config = r'-c preserve_interword_spaces=1 --psm 6'
        
        # Data configuration (optimized for numbers and short text)
        data_config = r'-c preserve_interword_spaces=1 --psm 6'
        
        # Configuration for cells likely containing numbers
        number_config = r'-c preserve_interword_spaces=1 --psm 7 -c tessedit_char_whitelist="0123456789,.-%() "'
        
        cell_data = []
        
        # Process each row
        for row_idx, row in enumerate(rows):
            row_data = [''] * num_cols  # Initialize with empty strings
            
            # Process each cell in the row
            for col_idx, (x, y, w, h) in enumerate(row):
                if col_idx >= num_cols:
                    continue  # Skip if column index exceeds the determined number of columns
                    
                # Extract cell region from the image
                cell_img = image[y:y+h, x:x+w]
                
                if cell_img.size == 0:
                    continue  # Skip empty cells
                    
                # Determine which OCR configuration to use
                if row_idx == 0:
                    config = header_config  # First row is likely header
                elif col_idx == 0:
                    config = header_config  # First column often contains text labels
                elif w < 100:
                    config = number_config  # Narrow columns often contain numbers
                else:
                    config = data_config  # Default configuration
                
                # Improve contrast for OCR
                # Create a PIL Image for contrast enhancement
                pil_img = Image.fromarray(cell_img)
                enhancer = ImageEnhance.Contrast(pil_img)
                enhanced_img = enhancer.enhance(2.0)  # Increase contrast
                
                # Apply OCR using appropriate configuration
                try:
                    text = pytesseract.image_to_string(
                        enhanced_img, lang='kor+eng', config=config
                    ).strip()
                    
                    # Clean the text
                    text = ' '.join(text.split())  # Remove excessive whitespace
                    
                    # Store in the row data
                    if text:
                        row_data[col_idx] = text
                except Exception as ocr_err:
                    logger.warning(f"OCR error for cell at row {row_idx}, col {col_idx}: {str(ocr_err)}")
            
            # Add row to cell data if it contains some text
            if any(cell for cell in row_data):
                cell_data.append(row_data)
        
        return cell_data
        
    except Exception as e:
        logger.error(f"Error extracting cells from table: {str(e)}")
        return []























def extract_table_from_html(html_content):
    """
    Enhanced function to extract table data from HTML content with better handling of
    colspan, rowspan, and complex table structures.
    
    Args:
        html_content (str): The HTML content containing tables
        
    Returns:
        pd.DataFrame or None: The extracted table as a DataFrame, or None if extraction fails
    """
    try:
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Look for tables with different approaches
        tables = []
        selectors = [
            'table', 
            'div.table', 
            '.grid-table',
            'div[role="table"]', 
            '.tableData',
            '.data-table'
        ]
        
        for selector in selectors:
            found_tables = soup.select(selector)
            if found_tables:
                tables.extend(found_tables)
                logger.info(f"Found {len(found_tables)} tables with selector: {selector}")
        
        if not tables:
            logger.warning("No tables found in HTML content")
            
            # Try looking for structured data that might be a table but not in <table> tags
            grid_elements = soup.select('div.grid, div.row, div.cell, div[class*="grid"], div[class*="row"], div[class*="cell"]')
            if grid_elements:
                logger.info(f"Found {len(grid_elements)} grid-like elements, attempting to reconstruct table")
                # This would require a custom parser for grid layouts
                # For now, we'll try pandas read_html as fallback
            
            # Try pandas read_html as a fallback
            try:
                logger.info("Attempting to use pandas read_html as fallback")
                tables_df = pd.read_html(html_content, flavor='bs4')
                if tables_df and len(tables_df) > 0:
                    largest_df = max(tables_df, key=lambda df: df.size)
                    logger.info(f"Successfully extracted table with pandas: {largest_df.shape}")
                    return largest_df
            except Exception as pandas_err:
                logger.warning(f"pandas read_html failed: {str(pandas_err)}")
            
            return None
        
        # Parse each table and select the best one
        parsed_tables = []
        
        for table_idx, table in enumerate(tables):
            logger.info(f"Processing table {table_idx+1}/{len(tables)}")
            
            # First, try to determine if this is a data table or a layout table
            # Data tables typically have th elements or regular structure
            if not table.find_all(['th', 'td']):
                logger.info(f"Table {table_idx+1} appears to be a layout table without cells, skipping")
                continue
            
            # Check if table has reasonable data density
            cells = table.find_all(['td', 'th'])
            empty_cells = sum(1 for cell in cells if not cell.get_text(strip=True))
            if cells and empty_cells / len(cells) > 0.7:  # More than 70% empty
                logger.info(f"Table {table_idx+1} appears to be mostly empty ({empty_cells}/{len(cells)} empty cells), skipping")
                continue
                
            # Advanced table parsing with better rowspan/colspan handling
            try:
                table_data = parse_complex_table(table)
                if table_data and len(table_data) > 0:
                    # Check if table has a good amount of data
                    row_count = len(table_data)
                    col_count = max(len(row) for row in table_data) if table_data else 0
                    data_density = sum(len(str(cell).strip()) for row in table_data for cell in row) / (row_count * col_count) if row_count * col_count > 0 else 0
                    
                    logger.info(f"Table {table_idx+1}: {row_count} rows, {col_count} columns, data density: {data_density:.2f}")
                    parsed_tables.append((row_count, col_count, data_density, table_data))
            except Exception as parse_err:
                logger.warning(f"Error parsing table {table_idx+1}: {str(parse_err)}")
        
        if not parsed_tables:
            logger.warning("No valid tables were successfully parsed")
            return None
        
        # Select best table based on combined criteria: rows, columns, and data density
        # We normalize and weight each factor
        best_table = None
        best_score = -1
        
        for row_count, col_count, data_density, table_data in parsed_tables:
            # Simple scoring: prefer tables with more rows, more columns, and higher data density
            # Adjust weights as needed
            row_weight, col_weight, density_weight = 0.4, 0.3, 0.3
            max_rows = max(t[0] for t in parsed_tables)
            max_cols = max(t[1] for t in parsed_tables)
            max_density = max(t[2] for t in parsed_tables)
            
            # Normalize scores
            row_score = row_count / max_rows if max_rows > 0 else 0
            col_score = col_count / max_cols if max_cols > 0 else 0
            density_score = data_density / max_density if max_density > 0 else 0
            
            total_score = (row_weight * row_score) + (col_weight * col_score) + (density_weight * density_score)
            
            if total_score > best_score:
                best_score = total_score
                best_table = table_data
        
        if not best_table or len(best_table) < 2:  # Need at least header + 1 data row
            logger.warning("No suitable table data found or table too small")
            return None
        
        # Process the best table into a DataFrame
        # First row is assumed to be header
        header = best_table[0]
        data_rows = best_table[1:]
        
        # Clean and normalize data rows
        clean_data = []
        for row in data_rows:
            # Skip entirely empty rows
            if not any(cell.strip() if isinstance(cell, str) else cell for cell in row):
                continue
                
            # Normalize row length to match header length
            if len(row) < len(header):
                row = row + [""] * (len(header) - len(row))
            elif len(row) > len(header):
                row = row[:len(header)]
                
            clean_data.append(row)
        
        # Handle duplicate headers by adding suffixes
        unique_headers = []
        header_counts = {}
        
        for h in header:
            h_str = str(h).strip()
            if not h_str:  # Replace empty headers
                h_str = f"Column_{len(unique_headers)}"
                
            if h_str in header_counts:
                header_counts[h_str] += 1
                unique_headers.append(f"{h_str}_{header_counts[h_str]}")
            else:
                header_counts[h_str] = 0
                unique_headers.append(h_str)
        
        # Create DataFrame
        df = pd.DataFrame(clean_data, columns=unique_headers)
        
        # Post-process: clean data types, remove duplicate rows
        for col in df.columns:
            # Try to convert numerical columns
            try:
                # Check if column looks like it contains numbers
                # First, clean the strings (remove commas, etc)
                if df[col].dtype == 'object':
                    cleaned = df[col].astype(str).str.replace(',', '').str.replace('%', '')
                    # If more than 50% of non-empty values can be converted to float, try conversion
                    non_empty = cleaned[cleaned != '']
                    if len(non_empty) > 0:
                        convertible = sum(is_numeric(val) for val in non_empty) / len(non_empty)
                        if convertible > 0.5:
                            df[col] = pd.to_numeric(cleaned, errors='coerce')
            except Exception as type_err:
                logger.debug(f"Type conversion failed for column {col}: {str(type_err)}")
        
        # Remove completely empty rows and columns
        df = df.dropna(how='all').reset_index(drop=True)
        df = df.loc[:, ~df.isna().all()]
        
        # Remove duplicate rows
        df = df.drop_duplicates().reset_index(drop=True)
        
        logger.info(f"Successfully created DataFrame from table: {df.shape[0]} rows, {df.shape[1]} columns")
        return df
        
    except Exception as e:
        logger.error(f"Error extracting table from HTML: {str(e)}")
        return None

def parse_complex_table(table):
    """
    Advanced table parser that handles rowspan and colspan attributes.
    
    Args:
        table: BeautifulSoup table element
        
    Returns:
        list: 2D list containing table data with merged cells properly handled
    """
    rows = table.find_all('tr')
    if not rows:
        return []
        
    # First, determine the total columns by examining all rows
    max_cols = 0
    for row in rows:
        cells = row.find_all(['td', 'th'])
        # Count total columns considering colspan
        cols = sum(int(cell.get('colspan', 1)) for cell in cells)
        max_cols = max(max_cols, cols)
    
    # Initialize the 2D grid with empty strings
    # We don't know exact dimensions yet due to rowspan, so we'll extend as needed
    grid = []
    
    # Keep track of cells extended by rowspan
    rowspan_tracker = [0] * max_cols  # How many more rows each column extends down
    
    for row_idx, row in enumerate(rows):
        # Initialize new row in the grid
        if row_idx >= len(grid):
            grid.append([''] * max_cols)
        
        # Decrement rowspan tracker and carry values down
        for col_idx in range(max_cols):
            if rowspan_tracker[col_idx] > 0:
                # This cell is covered by a rowspan from above
                rowspan_tracker[col_idx] -= 1
                # If we're adding a new row due to rowspan, extend the grid
                if row_idx + rowspan_tracker[col_idx] >= len(grid):
                    grid.append([''] * max_cols)
                # Get value from the cell above
                if row_idx > 0:
                    grid[row_idx][col_idx] = grid[row_idx-1][col_idx]
        
        # Process cells in this row
        cells = row.find_all(['td', 'th'])
        col_idx = 0
        
        for cell in cells:
            # Skip columns that are already filled due to rowspan
            while col_idx < max_cols and rowspan_tracker[col_idx] > 0:
                col_idx += 1
            
            # If we've run out of columns, break
            if col_idx >= max_cols:
                break
            
            # Get cell content
            content = cell.get_text(strip=True)
            
            # Get rowspan and colspan
            try:
                rowspan = int(cell.get('rowspan', 1))
            except ValueError:
                rowspan = 1
                
            try:
                colspan = int(cell.get('colspan', 1))
            except ValueError:
                colspan = 1
            
            # Fill in the current cell and cells to the right (colspan)
            for c in range(colspan):
                if col_idx + c < max_cols:
                    grid[row_idx][col_idx + c] = content
            
            # Update rowspan tracker for future rows
            if rowspan > 1:
                for c in range(colspan):
                    if col_idx + c < max_cols:
                        rowspan_tracker[col_idx + c] = rowspan - 1
                
                # Ensure grid has enough rows for the rowspan
                while len(grid) < row_idx + rowspan:
                    grid.append([''] * max_cols)
                
                # Fill in cells below (rowspan)
                for r in range(1, rowspan):
                    for c in range(colspan):
                        if row_idx + r < len(grid) and col_idx + c < max_cols:
                            grid[row_idx + r][col_idx + c] = content
            
            # Move to the next column, considering colspan
            col_idx += colspan
    
    return grid

def is_numeric(value):
    """
    Check if a string value can be converted to a number.
    
    Args:
        value: The string value to check
        
    Returns:
        bool: True if the value can be converted to a number, False otherwise
    """
    if not value or not isinstance(value, str):
        return False
        
    # Strip whitespace and handle empty strings
    value = value.strip()
    if not value:
        return False
        
    # Try to convert to float
    try:
        float(value)
        return True
    except ValueError:
        # Handle special cases like percentages, thousands separators
        try:
            # Remove common non-numeric characters
            cleaned = value.replace(',', '').replace('%', '').replace(' ', '')
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

# 시트 이름 생성 수정 부분 
def update_multiple_sheets(spreadsheet, sheets_data, date_str, report_type, post_info=None):
    """
    Update multiple sheets in the spreadsheet.
    Modified to prevent creating date-specific summary sheets.
    
    Args:
        spreadsheet: gspread Spreadsheet object
        sheets_data: Dictionary mapping sheet names to DataFrames
        date_str: Date string for the column header
        report_type: Type of report
        post_info: Optional post information dictionary
        
    Returns:
        bool: True if update was successful, False otherwise
    """
    if not sheets_data:
        logger.error("No sheets data to update")
        return False
        
    success_count = 0
    total_sheets = len(sheets_data)
    
    # Create a summary sheet first (with fixed name, no date)
    try:
        summary_sheet_name = f"요약_{report_type}"
        summary_sheet_name = clean_sheet_name_for_gsheets(summary_sheet_name)
        
        # Extract info to create summary sheet
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
        
        # If we have summary data, create a summary DataFrame
        if summary_data['데이터시트']:
            summary_df = pd.DataFrame(summary_data)
            
            # Add post information
            if post_info:
                summary_df['게시물 제목'] = post_info.get('title', '')
                summary_df['게시물 URL'] = post_info.get('url', '')
                summary_df['게시물 날짜'] = post_info.get('date', '')
            
            # Update summary sheet
            success = update_single_sheet(spreadsheet, summary_sheet_name, summary_df, date_str, post_info)
            if success:
                logger.info(f"성공적으로 요약 시트 업데이트: {summary_sheet_name}")
                
    except Exception as summary_err:
        logger.warning(f"요약 시트 생성 중 오류: {str(summary_err)}")
    
    # Update each individual sheet
    # First, get a list of existing worksheets to avoid too many API calls
    existing_worksheets = []
    try:
        existing_worksheets = [ws.title for ws in spreadsheet.worksheets()]
        logger.info(f"현재 스프레드시트에 {len(existing_worksheets)}개 워크시트가 있습니다.")
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
        keep_patterns = [r'.*_Raw$', r'.*_통합$']
        
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
        
        # Ask for confirmation if running interactively
        # (Skip this check if running as automation)
        remove_count = 0
        
        # Remove the sheets
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


def update_single_sheet(spreadsheet, sheet_name, df, date_str, post_info=None):
    """이전 함수와의 호환성을 위한 래퍼"""
    return update_sheet(spreadsheet, sheet_name, df, date_str, post_info, {'mode': 'append'})

# 새로운 함수: Raw 업데이트를 위한 시트 처리 함수
def update_single_sheet_raw(spreadsheet, sheet_name, df, date_str, post_info=None):
    """이전 함수와의 호환성을 위한 래퍼"""
    return update_sheet(spreadsheet, sheet_name, df, date_str, post_info, {'mode': 'replace'})

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
    
def clean_sheet_name_for_gsheets(sheet_name):
    """
    Clean sheet name to be valid for Google Sheets.
    
    Args:
        sheet_name: Original sheet name
        
    Returns:
        str: Cleaned sheet name
    """
    # Replace invalid characters
    clean_name = re.sub(r'[\\/*\[\]:]', '_', str(sheet_name))
    
    # Limit length (Google Sheets has a 100 character limit)
    if len(clean_name) > 100:
        clean_name = clean_name[:97] + '...'
        
    # Ensure it's not empty
    if not clean_name:
        clean_name = 'Sheet'
        
    return clean_name



def validate_and_clean_dataframe(df):
    """
    Validate and clean a DataFrame before updating Google Sheets.
    
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
            if df_clean[col].str.replace(',', '').str.replace('.', '').str.isdigit().mean() > 0.7:
                try:
                    numeric_values = pd.to_numeric(df_clean[col].str.replace(',', ''))
                    # Format large numbers with commas
                    df_clean[col] = numeric_values.apply(lambda x: f"{x:,}" if abs(x) >= 1000 else str(x))
                except:
                    pass
        
        # Remove rows that are completely empty
        df_clean = df_clean.replace('', np.nan)
        df_clean = df_clean.dropna(how='all').reset_index(drop=True)
        
        # Remove columns that are completely empty
        df_clean = df_clean.loc[:, ~df_clean.isna().all()]
        
        # Convert NaN back to empty strings
        df_clean = df_clean.fillna('')
        
        # Clean column headers
        df_clean.columns = [str(col).strip() for col in df_clean.columns]
        
        # Handle duplicate column names
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
        logger.error(f"Error validating DataFrame: {str(e)}")
        return pd.DataFrame()  # Return empty DataFrame on error


def open_spreadsheet_with_retry(client, max_retries=3, retry_delay=2):
    """
    Open Google Spreadsheet with retry logic.
    
    Args:
        client: gspread client instance
        max_retries: Maximum number of retry attempts
        retry_delay: Delay between retries (seconds)
        
    Returns:
        Spreadsheet object or None if failed
    """
    spreadsheet = None
    retry_count = 0
    
    while retry_count < max_retries and not spreadsheet:
        try:
            # Try by ID first
            if CONFIG['spreadsheet_id']:
                try:
                    spreadsheet = client.open_by_key(CONFIG['spreadsheet_id'])
                    logger.info(f"Successfully opened spreadsheet by ID: {CONFIG['spreadsheet_id']}")
                    return spreadsheet
                except Exception as id_err:
                    logger.warning(f"Failed to open spreadsheet by ID: {str(id_err)}")
            
            # Try by name
            try:
                spreadsheet = client.open(CONFIG['spreadsheet_name'])
                logger.info(f"Successfully opened spreadsheet by name: {CONFIG['spreadsheet_name']}")
                return spreadsheet
            except gspread.exceptions.SpreadsheetNotFound:
                # Create new spreadsheet
                logger.info(f"Spreadsheet not found, creating new one: {CONFIG['spreadsheet_name']}")
                spreadsheet = client.create(CONFIG['spreadsheet_name'])
                # Update config with new ID
                CONFIG['spreadsheet_id'] = spreadsheet.id
                logger.info(f"Created new spreadsheet with ID: {spreadsheet.id}")
                return spreadsheet
                
        except gspread.exceptions.APIError as api_err:
            retry_count += 1
            
            if "RESOURCE_EXHAUSTED" in str(api_err) or "RATE_LIMIT_EXCEEDED" in str(api_err):
                wait_time = 2 ** retry_count  # Exponential backoff
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

# 모드별 처리 함수들 (private helper 함수들)
def _replace_sheet(spreadsheet, sheet_name, df, date_str, post_info, batch_size, add_metadata, format_header):
    """시트를 완전히 대체하는 모드 처리"""
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

def _update_sheet_cells(spreadsheet, sheet_name, df, date_str, post_info, batch_size):
    """특정 셀만 업데이트하는 모드 처리"""
    # 이 함수는 필요에 따라 구현
    # 기존 _append_to_sheet와 유사하지만 특정 셀만 대상으로 함
    pass

def _update_in_batches(worksheet, df, batch_size=100):
    """대용량 DataFrame 분할 업데이트"""
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
    """셀 업데이트 배치 처리"""
    for i in range(0, len(updates), batch_size):
        batch = updates[i:i+batch_size]
        worksheet.batch_update(batch)
        logger.info(f"일괄 업데이트 {i+1}~{min(i+batch_size, len(updates))} 완료")
        time.sleep(1)
    return True

def _add_metadata_to_sheet(worksheet, df, date_str, post_info):
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
        logger.info(f"메타 정보 추가 완료")
        return True
    except Exception as e:
        logger.warning(f"메타데이터 추가 중 오류: {str(e)}")
        return False

def update_sheet_from_dataframe(worksheet, df, col_idx):
    """데이터프레임으로 워크시트 업데이트 (배치 처리)"""
    try:
        # 기존 항목 (첫 번째 열) 가져오기
        existing_items = worksheet.col_values(1)[1:]  # 헤더 제외
        
        if df.shape[0] > 0:
            # 데이터프레임에서 항목과 값 추출
            # 첫 번째 열은 항목, 두 번째 열은 값으로 가정
            if df.shape[1] >= 2:
                df = df.fillna('')  # NaN 값 처리
                item_col = df.columns[0]
                value_col = df.columns[1]
                
                new_items = df[item_col].astype(str).tolist()
                values = df[value_col].astype(str).tolist()
                
                # 배치 업데이트 준비
                cell_updates = []
                new_rows = []
                
                for i, (item, value) in enumerate(zip(new_items, values)):
                    if item and item.strip():  # 빈 항목 제외
                        # 항목이 이미 존재하는지 확인
                        if item in existing_items:
                            row_idx = existing_items.index(item) + 2  # 헤더와, 0-인덱스 보정
                        else:
                            # 새 항목은 끝에 추가
                            row_idx = len(existing_items) + 2
                            new_rows.append(item)  # 새 행 추적
                            
                            # 항목 업데이트
                            cell_updates.append({
                                'range': f'A{row_idx}',
                                'values': [[item]]
                            })
                            existing_items.append(item)
                        
                        # 값 업데이트
                        value_to_update = "" if pd.isna(value) else value
                        cell_updates.append({
                            'range': f'{chr(64 + col_idx)}{row_idx}',
                            'values': [[value_to_update]]
                        })
                
                # 일괄 업데이트 실행 (API 호출 제한 방지를 위한 분할)
                if cell_updates:
                    batch_size = 10  # 한 번에 처리할 업데이트 수
                    for i in range(0, len(cell_updates), batch_size):
                        batch = cell_updates[i:i+batch_size]
                        try:
                            worksheet.batch_update(batch)
                            logger.info(f"일괄 업데이트 {i+1}~{min(i+batch_size, len(cell_updates))} 완료")
                            time.sleep(2)  # API 속도 제한 방지
                        except gspread.exceptions.APIError as api_err:
                            if "RESOURCE_EXHAUSTED" in str(api_err) or "RATE_LIMIT_EXCEEDED" in str(api_err):
                                logger.warning(f"API 속도 제한 발생: {str(api_err)}")
                                time.sleep(10)  # 더 긴 대기
                                # 더 작은 배치로 재시도
                                for update in batch:
                                    try:
                                        worksheet.batch_update([update])
                                        time.sleep(3)
                                    except Exception as single_err:
                                        logger.error(f"단일 업데이트 실패: {str(single_err)}")
                            else:
                                logger.error(f"일괄 업데이트 실패: {str(api_err)}")
                    
                    logger.info(f"{len(cell_updates)}개 셀 업데이트 완료 (새 항목: {len(new_rows)}개)")
                    
        return True
        
    except Exception as e:
        logger.error(f"데이터프레임으로 워크시트 업데이트 중 오류: {str(e)}")
        return False


def update_single_sheet_with_retry(spreadsheet, sheet_name, df, date_str, max_retries=3):
    """이전 함수와의 호환성을 위한 래퍼"""
    return update_sheet(spreadsheet, sheet_name, df, date_str, None, {'mode': 'append', 'max_retries': max_retries})



def update_sheet_data_batched(worksheet, df, date_col_idx, batch_size=10):
    """
    Update sheet data using optimized batched updates.
    
    Args:
        worksheet: gspread Worksheet object
        df: pandas DataFrame with data
        date_col_idx: Column index for the date column
        batch_size: Number of cells to update in each batch
        
    Returns:
        bool: True if update was successful, False otherwise
    """
    try:
        # Get existing items (first column)
        existing_items = worksheet.col_values(1)[1:]  # Skip header
        
        # Prepare updates
        cell_updates = []
        
        # First column is used as the key
        if df.shape[1] >= 1:
            key_col = df.columns[0]
            value_col = df.columns[1] if df.shape[1] >= 2 else key_col
            
            # Get items and values
            items = df[key_col].astype(str).tolist()
            values = df[value_col].astype(str).tolist()
            
            # Build update operations
            for i, (item, value) in enumerate(zip(items, values)):
                if not item or not item.strip():
                    continue  # Skip empty items
                    
                # Find row index for this item
                if item in existing_items:
                    row_idx = existing_items.index(item) + 2  # +2 for header and 0-index
                else:
                    # Add new item at the end
                    row_idx = len(existing_items) + 2
                    existing_items.append(item)
                    
                    # Add item to first column
                    cell_updates.append({
                        'range': f'A{row_idx}',
                        'values': [[item]]
                    })
                
                # Add value to date column
                cell_updates.append({
                    'range': f'{chr(64 + date_col_idx)}{row_idx}',
                    'values': [[value]]
                })
        
        # Execute batched updates
        if cell_updates:
            success = execute_batched_updates(worksheet, cell_updates, batch_size)
            if success:
                logger.info(f"Successfully updated {len(cell_updates)} cells")
                return True
            else:
                logger.warning("Batched updates failed")
                return False
        else:
            logger.warning("No cell updates to perform")
            return True  # No updates needed is still a "success"
            
    except Exception as e:
        logger.error(f"Error in batched sheet update: {str(e)}")
        return False


def execute_batched_updates(worksheet, updates, batch_size):
    """
    Execute batched updates with rate limit handling.
    
    Args:
        worksheet: gspread Worksheet object
        updates: List of update operations
        batch_size: Number of operations per batch
        
    Returns:
        bool: True if all updates were successful
    """
    try:
        for i in range(0, len(updates), batch_size):
            batch = updates[i:i+batch_size]
            
            try:
                worksheet.batch_update(batch)
                logger.info(f"Batch update {i+1}-{i+len(batch)} succeeded ({len(batch)} operations)")
                
                # Add delay to avoid rate limits
                time.sleep(1 + (len(batch) // 5))  # Adjust delay based on batch size
                
            except gspread.exceptions.APIError as api_err:
                if "RESOURCE_EXHAUSTED" in str(api_err) or "RATE_LIMIT_EXCEEDED" in str(api_err):
                    logger.warning(f"Rate limit hit during batch {i+1}-{i+len(batch)}, switching to individual updates")
                    
                    # Fall back to individual updates with longer delays
                    for update in batch:
                        try:
                            worksheet.batch_update([update])
                            time.sleep(2)  # Longer delay for individual updates
                        except Exception as single_err:
                            logger.error(f"Individual update failed: {str(single_err)}")
                else:
                    logger.error(f"Batch update API error: {str(api_err)}")
                    return False
                    
        return True
        
    except Exception as e:
        logger.error(f"Error executing batched updates: {str(e)}")
        return False

def extract_data_from_html(html_content):
    """
    HTML 콘텐츠에서 표 데이터를 추출하는 개선된 함수
    SynapDocViewServer의 특수한 표 형식에 특화됨
    
    Args:
        html_content (str): HTML 문자열
        
    Returns:
        dict: 시트 이름을 키로, DataFrame을 값으로 하는 딕셔너리
    """
    try:
        soup = BeautifulSoup(html_content, 'html.parser')
        all_sheets = {}
        
        # Synap 문서 뷰어 확인
        synap_viewer = False
        if 'SynapDocViewServer' in html_content or 'Synap Document Viewer' in html_content:
            synap_viewer = True
            logger.info("Synap 문서 뷰어 감지됨, 특수 처리 적용")
        
        # 1. mainTable 찾기 (Synap 문서 뷰어에서 주로 사용)
        main_table = soup.find('div', id='mainTable')
        if main_table:
            logger.info("mainTable 요소 찾음")
            
            # 시트 제목 추출 시도 (페이지 제목이나 시트 탭에서)
            sheet_name = "기본 시트"
            sheet_tabs = soup.find_all('div', class_='sheet-list__sheet-tab')
            if sheet_tabs:
                active_tab = next((tab for tab in sheet_tabs if 'active' in tab.get('class', [])), None)
                if active_tab:
                    sheet_name = active_tab.text.strip()
                    logger.info(f"활성 시트 탭 발견: {sheet_name}")
            
            # tr 클래스를 가진 div 찾기 (행 요소)
            rows = main_table.find_all('div', class_=lambda c: c and ('tr' in c.lower()))
            
            if not rows:
                # 다른 방법으로 행 요소 찾기 (Synap 뷰어는 특수한 구조를 사용)
                rows = main_table.find_all('div', recursive=False)
                logger.info(f"대체 방법으로 {len(rows)}개 행 찾음")
            
            if rows:
                logger.info(f"mainTable에서 {len(rows)}개 행 찾음")
                table_data = []
                
                # 첫 번째 행이 비어있는지 확인
                first_row_empty = True
                if rows and rows[0]:
                    cells = rows[0].find_all('div', class_=lambda c: c and ('td' in c.lower()))
                    if not cells:
                        cells = rows[0].find_all('div', recursive=False)
                    
                    if cells:
                        for cell in cells:
                            if cell.text.strip():
                                first_row_empty = False
                                break
                
                # 첫 번째 행이 헤더인지 확인
                header_row = 0 if not first_row_empty else 1
                
                # 헤더 추출
                headers = []
                if rows and len(rows) > header_row:
                    header_cells = rows[header_row].find_all('div', class_=lambda c: c and ('td' in c.lower()))
                    if not header_cells:
                        header_cells = rows[header_row].find_all('div', recursive=False)
                    
                    for cell in header_cells:
                        text = cell.text.strip()
                        if not text:
                            text = f"Column_{len(headers)}"
                        headers.append(text)
                
                if not headers:
                    # 헤더가 없으면 첫 번째 행이 헤더가 아닐 수 있음
                    header_row = -1
                    
                    # 임시 헤더 생성
                    if rows and rows[0]:
                        cells = rows[0].find_all('div', class_=lambda c: c and ('td' in c.lower()))
                        if not cells:
                            cells = rows[0].find_all('div', recursive=False)
                        
                        headers = [f"Column_{i}" for i in range(len(cells))]
                
                # 데이터 행 추출
                for i, row in enumerate(rows):
                    if i == header_row:
                        continue  # 헤더 행 건너뛰기
                        
                    cells = row.find_all('div', class_=lambda c: c and ('td' in c.lower()))
                    if not cells:
                        cells = row.find_all('div', recursive=False)
                    
                    row_data = []
                    for cell in cells:
                        text = cell.text.strip()
                        row_data.append(text)
                    
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
                    logger.info(f"mainTable에서 DataFrame 생성: {df.shape[0]}행 {df.shape[1]}열")
        
        # 2. 일반 HTML 테이블 추출
        tables = soup.find_all('table')
        if tables:
            logger.info(f"HTML에서 {len(tables)}개의 <table> 태그를 찾았습니다")
            
            for table_idx, table in enumerate(tables):
                sheet_name = f"Table_{table_idx+1}"
                
                # 헤더 추출
                headers = []
                header_rows = table.find_all('tr', limit=1)
                if header_rows:
                    header_cells = header_rows[0].find_all(['th', 'td'])
                    for cell in header_cells:
                        # colspan 처리
                        colspan = int(cell.get('colspan', 1))
                        text = cell.text.strip()
                        if not text:
                            text = f"Column_{len(headers)}"
                        
                        headers.append(text)
                        # 추가 열 생성 (colspan > 1인 경우)
                        for i in range(1, colspan):
                            headers.append(f"{text}_{i}")
                
                # 데이터 행 추출
                rows = table.find_all('tr')
                table_data = []
                
                for row_idx, row in enumerate(rows):
                    if row_idx == 0 and headers:
                        continue  # 헤더 행 건너뛰기
                    
                    cells = row.find_all(['td', 'th'])
                    row_data = []
                    
                    for cell in cells:
                        # colspan 처리
                        colspan = int(cell.get('colspan', 1))
                        text = cell.text.strip()
                        
                        row_data.append(text)
                        # 추가 셀 생성 (colspan > 1인 경우)
                        for i in range(1, colspan):
                            row_data.append(text)
                    
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
                    logger.info(f"테이블 {sheet_name} 데이터 추출: {df.shape[0]}행 {df.shape[1]}열")
        
        # 3. DIV 기반 그리드 구조 찾기 (다양한 선택자 시도)
        container_selectors = [
            'div[id="container"]',
            'div.container',
            'div.content',
            'div.grid',
            'div[class*="table"]'
        ]
        
        for selector in container_selectors:
            containers = soup.select(selector)
            
            if containers:
                logger.info(f"컨테이너 선택자 '{selector}'로 {len(containers)}개 컨테이너 찾음")
                
                for container_idx, container in enumerate(containers):
                    # 행 요소 찾기
                    row_selectors = [
                        'div[class*="tr"]', 
                        'div[class*="row"]',
                        'div:not([class])'  # 클래스 없는 div도 시도
                    ]
                    
                    rows = []
                    for row_selector in row_selectors:
                        temp_rows = container.select(row_selector)
                        if temp_rows:
                            rows = temp_rows
                            logger.info(f"행 선택자 '{row_selector}'로 {len(rows)}개 행 찾음")
                            break
                    
                    if rows:
                        logger.info(f"컨테이너 {container_idx+1}에서 {len(rows)}개 행을 찾았습니다")
                        
                        # 데이터 처리를 위한 변수
                        section_name = f"Section_{container_idx+1}"
                        headers = []
                        table_data = []
                        
                        # 첫 번째 행이 헤더인지 확인
                        first_row = rows[0]
                        
                        # 셀 찾기
                        cell_selectors = [
                            'div[class*="td"]', 
                            'div[class*="cell"]',
                            'div'  # 모든 div
                        ]
                        
                        header_cells = []
                        for cell_selector in cell_selectors:
                            temp_cells = first_row.select(cell_selector)
                            if temp_cells:
                                header_cells = temp_cells
                                logger.info(f"헤더 셀 선택자 '{cell_selector}'로 {len(header_cells)}개 셀 찾음")
                                break
                        
                        # 헤더 추출
                        for cell in header_cells:
                            text = cell.text.strip()
                            if not text:
                                text = f"Column_{len(headers)}"
                            headers.append(text)
                        
                        # 데이터 행 추출
                        for i, row in enumerate(rows[1:], 1):  # 첫 번째 행 제외
                            cells = []
                            for cell_selector in cell_selectors:
                                temp_cells = row.select(cell_selector)
                                if temp_cells:
                                    cells = temp_cells
                                    break
                            
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
                            all_sheets[section_name] = df
                            logger.info(f"섹션 {section_name} 데이터 추출: {df.shape[0]}행 {df.shape[1]}열")
        
        # 4. 만약 아무 것도 찾지 못한 경우, Synap 문서 뷰어에서 스크립트 태그에서 데이터 추출 시도
        if not all_sheets and synap_viewer:
            logger.info("일반적인 방법으로 데이터를 찾지 못함, 스크립트 태그에서 데이터 추출 시도")
            
            # 스크립트 태그에서 데이터 추출 시도
            script_tags = soup.find_all('script')
            
            for script in script_tags:
                script_content = script.string
                if script_content:
                    # 데이터가 포함된 스크립트 태그 찾기
                    data_patterns = [
                        r'var\s+jsonData\s*=\s*(\{.*?\});',
                        r'var\s+data\s*=\s*(\{.*?\});',
                        r'var\s+cellData\s*=\s*(\{.*?\});',
                        r'var\s+sheetData\s*=\s*(\{.*?\});'
                    ]
                    
                    for pattern in data_patterns:
                        match = re.search(pattern, script_content, re.DOTALL)
                        if match:
                            logger.info(f"스크립트 태그에서 데이터 패턴 '{pattern}' 발견")
                            try:
                                # JSON으로 변환 시도
                                data_str = match.group(1)
                                # 자바스크립트 JSON을 파이썬 JSON으로 변환
                                data_str = data_str.replace("'", '"')
                                data = json.loads(data_str)
                                
                                # 데이터에서 테이블 구조 재구성
                                if isinstance(data, dict) and 'rows' in data:
                                    rows_data = data['rows']
                                    columns = data.get('columns', [{'name': f"Column_{i}"} for i in range(len(rows_data[0]) if rows_data else 0)])
                                    
                                    headers = [col.get('name', f"Column_{i}") for i, col in enumerate(columns)]
                                    
                                    df = pd.DataFrame(rows_data, columns=headers)
                                    all_sheets['Script_Data'] = df
                                    logger.info(f"스크립트 데이터에서 DataFrame 생성: {df.shape[0]}행 {df.shape[1]}열")
                                    break
                            except Exception as json_err:
                                logger.warning(f"JSON 데이터 파싱 중 오류: {str(json_err)}")
        
        # 5. 데이터 정제
        refined_sheets = {}
        for name, df in all_sheets.items():
            if df.empty:
                continue
            
            # 데이터 타입 변환 시도 (숫자 및 날짜)
            for col in df.columns:
                # 숫자 변환 시도
                try:
                    # 쉼표 제거 후 숫자 변환 시도
                    df[col] = df[col].astype(str)
                    numeric_values = df[col].str.replace(',', '').str.replace(' ', '').str.replace('%', '')
                    is_numeric = pd.to_numeric(numeric_values, errors='coerce')
                    
                    # 50% 이상이 숫자면 변환
                    if is_numeric.notna().mean() > 0.5:
                        df[col] = is_numeric
                except Exception as type_err:
                    pass
            
            # NaN 값을 빈 문자열로 변환
            df = df.fillna('')
            
            # 중복 행 제거
            df = df.drop_duplicates().reset_index(drop=True)
            
            refined_sheets[name] = df
        
        if not refined_sheets:
            logger.warning("추출된 유효한 데이터가 없습니다.")
            return None
        
        logger.info(f"총 {len(refined_sheets)}개 시트 추출 완료")
        return refined_sheets
        
    except Exception as e:
        logger.error(f"HTML에서 데이터 추출 중 오류: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return None


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
                else:
                    date_match = re.search(r'\((\d{4})년\s*(\d{1,2})월말\s*기준\)', post_info['title'])
                    if date_match:
                        year = date_match.group(1)
                        month = date_match.group(2)
                    else:
                        year = "알 수 없음"
                        month = "알 수 없음"
                
                date_str = f"{year}년 {month}월"
                report_type = determine_report_type(post_info['title'])
                
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
        if data_updates and CONFIG['spreadsheet_id']:
            spreadsheet_url = f"https://docs.google.com/spreadsheets/d/{CONFIG['spreadsheet_id']}"
            message += f"📋 [스프레드시트 보기]({spreadsheet_url})\n\n"
        
        # 현재 시간 추가
        kr_time = datetime.now() + timedelta(hours=9)
        message += f"🕒 *업데이트 시간: {kr_time.strftime('%Y-%m-%d %H:%M')} (KST)*"
        
        # 메시지 분할 (텔레그램 제한)
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


async def run_monitor(days_range=4, check_sheets=True, start_page=1, end_page=5, start_date=None, end_date=None, reverse_order=True):
    """
    Enhanced monitoring function with reverse page exploration.
    
    Args:
        days_range: Number of days to look back for new posts
        check_sheets: Whether to update Google Sheets
        start_page: Starting page number to parse
        end_page: Ending page number to parse
        start_date: Optional start date string (YYYY-MM-DD)
        end_date: Optional end date string (YYYY-MM-DD)
        reverse_order: Whether to explore pages in reverse order (highest to lowest)
        
    Returns:
        None
    """
    driver = None
    gs_client = None
    
    try:
        # Start time recording
        start_time = time.time()
        
        if reverse_order:
            logger.info(f"=== MSIT 통신 통계 모니터링 시작 (days_range={days_range}, 페이지={end_page}~{start_page} 역순, check_sheets={check_sheets}) ===")
        else:
            logger.info(f"=== MSIT 통신 통계 모니터링 시작 (days_range={days_range}, 페이지={start_page}~{end_page}, check_sheets={check_sheets}) ===")

        # Create screenshot directory
        screenshots_dir = Path("./screenshots")
        screenshots_dir.mkdir(exist_ok=True)

        # Initialize WebDriver with enhanced stealth settings
        driver = setup_driver()
        logger.info("WebDriver 초기화 완료")
        
        # Initialize Google Sheets client
        if check_sheets and CONFIG['gspread_creds']:
            gs_client = setup_gspread_client()
            if gs_client:
                logger.info("Google Sheets 클라이언트 초기화 완료")
            else:
                logger.warning("Google Sheets 클라이언트 초기화 실패")
        
        # Enhance WebDriver stealth settings
        try:
            # Reset User-Agent
            user_agents = [
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.71 Safari/537.36",
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.1 Safari/605.1.15",
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:96.0) Gecko/20100101 Firefox/96.0"
            ]
            selected_ua = random.choice(user_agents)
            execute_javascript(driver, f'Object.defineProperty(navigator, "userAgent", {{get: function() {{return "{selected_ua}";}}}});', description="User-Agent 재설정")
            logger.info(f"JavaScript로 User-Agent 재설정: {selected_ua}")
            
            # Avoid webdriver detection
            execute_javascript(driver, """
                Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
                if (window.navigator.permissions) {
                    window.navigator.permissions.query = (parameters) => {
                        return Promise.resolve({state: 'prompt', onchange: null});
                    };
                }
            """, description="웹드라이버 감지 회피")
        except Exception as setup_err:
            logger.warning(f"웹드라이버 강화 설정 중 오류: {str(setup_err)}")
        
        # Access landing page
        try:
            # Navigate to landing page
            landing_url = CONFIG['landing_url']
            driver.get(landing_url)
            
            # Wait for page to load
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.ID, "skip_nav"))
            )
            logger.info("랜딩 페이지 접속 완료 - 쿠키 및 세션 정보 획득")
            
            # Random delay (simulate natural user behavior)
            time.sleep(random.uniform(2, 4))
            
            # Save screenshot
            driver.save_screenshot("landing_page.png")
            logger.info("랜딩 페이지 스크린샷 저장 완료")
            
            # Simulate scrolling
            execute_javascript(driver, """
                function smoothScroll() {
                    const height = document.body.scrollHeight;
                    const step = Math.floor(height / 10);
                    let i = 0;
                    const timer = setInterval(function() {
                        window.scrollBy(0, step);
                        i++;
                        if (i >= 10) clearInterval(timer);
                    }, 100);
                }
                smoothScroll();
            """, description="랜딩 페이지 스크롤")
            time.sleep(2)
            
            # Find and click statistics link
            stats_link_selectors = [
                "a[href*='mId=99'][href*='mPid=74']",
                "//a[contains(text(), '통계정보')]",
                "//a[contains(text(), '통계')]"
            ]
            
            stats_link_found = False
            for selector in stats_link_selectors:
                try:
                    if selector.startswith("//"):
                        # XPath selector
                        links = driver.find_elements(By.XPATH, selector)
                    else:
                        # CSS selector
                        links = driver.find_elements(By.CSS_SELECTOR, selector)
                    
                    if links:
                        stats_link = links[0]
                        logger.info(f"통계정보 링크 발견 (선택자: {selector}), 클릭 시도")
                        
                        # Screenshot (before click)
                        driver.save_screenshot("before_stats_click.png")
                        
                        # Click using JavaScript (more reliable)
                        driver.execute_script("arguments[0].click();", stats_link)
                        
                        # Wait for URL change
                        WebDriverWait(driver, 15).until(
                            lambda d: '/bbs/list.do' in d.current_url
                        )
                        stats_link_found = True
                        logger.info(f"통계정보 페이지로 이동 완료: {driver.current_url}")
                        break
                except Exception as link_err:
                    logger.warning(f"통계정보 링크 클릭 실패 (선택자: {selector}): {str(link_err)}")
            
            if not stats_link_found:
                logger.warning("통계정보 링크를 찾을 수 없음, 직접 URL로 접속")
                driver.get(CONFIG['stats_url'])
                
                # Wait for page to load
                try:
                    WebDriverWait(driver, 15).until(
                        EC.presence_of_element_located((By.CLASS_NAME, "board_list"))
                    )
                    logger.info("통계정보 페이지 직접 접속 성공")
                except TimeoutException:
                    logger.warning("통계정보 페이지 로드 시간 초과, 계속 진행")
            
        except Exception as e:
            logger.error(f"랜딩 또는 통계정보 버튼 클릭 중 오류 발생, fallback으로 직접 접속: {str(e)}")
            
            # Reset browser context
            reset_browser_context(driver)
            
            # Access stats page directly
            driver.get(CONFIG['stats_url'])
            
            try:
                WebDriverWait(driver, 15).until(
                    EC.presence_of_element_located((By.CLASS_NAME, "board_list"))
                )
                logger.info("통계정보 페이지 직접 접속 성공 (오류 후 재시도)")
            except TimeoutException:
                logger.warning("통계정보 페이지 로드 시간 초과 (오류 후 재시도), 계속 진행")
        
        logger.info("MSIT 웹사이트 접근 완료")
        
        # Save screenshot (for debugging)
        try:
            driver.save_screenshot("stats_page.png")
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
        continue_to_next_page = True  # 다음 페이지로 계속 진행할지 여부
        
        # 각 페이지 탐색
        for page_num in page_sequence:
            if not continue_to_next_page:
                logger.info(f"이전 페이지에서 날짜 범위 조건으로 검색 중단 설정됨")
                break
                
            logger.info(f"페이지 {page_num} 탐색 시도...")
            
            # 특정 페이지로 이동
            page_navigation_success = navigate_to_specific_page(driver, page_num)
            
            if not page_navigation_success:
                logger.warning(f"페이지 {page_num}으로 이동 실패, 다음 페이지로 진행")
                continue
            
            logger.info(f"페이지 {page_num} 파싱 중...")
            
            # 새로운 통합 파싱 함수 사용
            page_posts, stats_posts, result_info = parse_page_content(
                driver, 
                page_num=page_num, 
                days_range=days_range,
                start_date=start_date, 
                end_date=end_date,
                reverse_order=reverse_order
            )
            
            # 파싱 결과 로깅
            logger.info(f"페이지 {page_num} 파싱 결과: {len(page_posts)}개 게시물, {len(stats_posts)}개 통신 통계")
            if result_info['messages']:
                for msg in result_info['messages']:
                    logger.info(f"페이지 {page_num} 메시지: {msg}")
            
            # 다음 페이지 진행 여부 결정
            continue_to_next_page = result_info['continue_to_next_page']
            
            # 날짜 정보 로깅
            if result_info['oldest_date_found']:
                logger.info(f"페이지 {page_num} 가장 오래된 날짜: {result_info['oldest_date_found']}")
            if result_info['newest_date_found']:
                logger.info(f"페이지 {page_num} 가장 최근 날짜: {result_info['newest_date_found']}")
            
            # 전체 결과에 추가
            all_posts.extend(page_posts)
            telecom_stats_posts.extend(stats_posts)
            
            # 페이지 사이 대기
            time.sleep(2)
        
        # Process telecom statistics posts with enhanced extraction
        data_updates = []
        
        if gs_client and telecom_stats_posts and check_sheets:
            logger.info(f"{len(telecom_stats_posts)}개 통신 통계 게시물 처리 중")
            
            for i, post in enumerate(telecom_stats_posts):
                try:
                    logger.info(f"게시물 {i+1}/{len(telecom_stats_posts)} 처리 중: {post['title']}")
                    
                    # Extract view link parameters with enhanced error handling
                    file_params = find_view_link_params(driver, post)
                    
                    if not file_params:
                        logger.warning(f"바로보기 링크 파라미터 추출 실패: {post['title']}")
                        continue
                    
                    # ENHANCED: Detailed logging of extraction parameters
                    param_log = {k: v for k, v in file_params.items() if k != 'post_info'}
                    logger.info(f"Document extraction parameters: {json.dumps(param_log, default=str)}")
                    
                    # Direct access with OCR fallback - CORE IMPROVEMENT
                    if 'atch_file_no' in file_params and 'file_ord' in file_params:
                        # ENHANCED: Clear logging of extraction process
                        logger.info(f"문서 추출 시작 - atch_file_no: {file_params['atch_file_no']}, file_ord: {file_params['file_ord']}")
                        
                        # 먼저 SynapDocViewServer 구조 탐색 시도
                        structure_info = explore_synap_doc_viewer(driver, file_params)
                        logger.info(f"SynapDocViewServer 구조 탐색 완료: {len(structure_info.keys())} 항목 수집")
                        
                        # 구조 정보 기반 데이터 추출 시도
                        if structure_info and not 'error' in structure_info:
                            extracted_data = extract_synap_data_using_structure_info(driver, structure_info)
                            
                            # DataFrame으로 변환
                            sheets_data = {}
                            if extracted_data:
                                for sheet_name, sheet_data in extracted_data.items():
                                    headers = sheet_data.get('headers', [])
                                    data = sheet_data.get('data', [])
                                    
                                    if headers and data:
                                        try:
                                            df = pd.DataFrame(data, columns=headers)
                                            sheets_data[sheet_name] = df
                                            logger.info(f"시트 '{sheet_name}'을 DataFrame으로 변환: {df.shape[0]}행 {df.shape[1]}열")
                                        except Exception as df_err:
                                            logger.warning(f"시트 '{sheet_name}' DataFrame 변환 오류: {str(df_err)}")
                            
                            if sheets_data and any(not df.empty for df in sheets_data.values()):
                                logger.info(f"구조 탐색으로 데이터 추출 성공: {len(sheets_data)}개 시트")
                            else:
                                # 구조 분석 실패 시 기존 직접 접근 함수 사용
                                logger.info("구조 정보 기반 추출 실패, 직접 접근 시도")
                                sheets_data = access_iframe_direct(driver, file_params)
                        else:
                            # 구조 분석이 없는 경우 직접 접근 함수 사용
                            sheets_data = access_iframe_direct(driver, file_params)
                        
                        # 데이터 추출 성공 여부에 따라 OCR 폴백 결정
                        if not sheets_data or not any(not df.empty for df in sheets_data.values()):
                            logger.info("직접 접근 실패, OCR 폴백 시도")
                            sheets_data = access_iframe_with_ocr_fallback(driver, file_params)
                        
                        if sheets_data and any(not df.empty for df in sheets_data.values()):
                            # Log detailed information about extracted data
                            sheet_names = list(sheets_data.keys())
                            sheet_sizes = {name: sheets_data[name].shape for name in sheet_names}
                            logger.info(f"데이터 추출 성공: {len(sheet_names)}개 시트")
                            logger.info(f"시트 크기: {sheet_sizes}")
                            
                            # Prepare update data
                            update_data = {
                                'sheets': sheets_data,
                                'post_info': post
                            }
                            
                            if 'date' in file_params:
                                update_data['date'] = file_params['date']
                            
                            # Update Google Sheets with enhanced update function
                            success = update_google_sheets(gs_client, update_data)
                            if success:
                                logger.info(f"Google Sheets 업데이트 성공: {post['title']}")
                                data_updates.append(update_data)
                            else:
                                logger.warning(f"Google Sheets 업데이트 실패: {post['title']}")
                        else:
                            logger.warning(f"모든 방법으로 데이터 추출 실패: {post['title']}")
                            
                            # ENHANCED: Create a better placeholder with diagnostic info
                            placeholder_df = create_improved_placeholder_dataframe(post, file_params)
                            
                            if not placeholder_df.empty:
                                update_data = {
                                    'dataframe': placeholder_df,
                                    'post_info': post
                                }
                                
                                if 'date' in file_params:
                                    update_data['date'] = file_params['date']
                                
                                success = update_google_sheets(gs_client, update_data)
                                if success:
                                    logger.info(f"대체 데이터로 업데이트 성공: {post['title']}")
                                    data_updates.append(update_data)
                    
                    # Handle content-only case
                    elif 'content' in file_params:
                        logger.info(f"게시물 내용으로 처리 중: {post['title']}")
                        
                        # Create placeholder dataframe
                        placeholder_df = create_improved_placeholder_dataframe(post, file_params)
                        if not placeholder_df.empty:
                            update_data = {
                                'dataframe': placeholder_df,
                                'post_info': post
                            }
                            
                            if 'date' in file_params:
                                update_data['date'] = file_params['date']
                            
                            success = update_google_sheets(gs_client, update_data)
                            if success:
                                logger.info(f"내용 기반 데이터로 업데이트 성공: {post['title']}")
                                data_updates.append(update_data)
                    
                    # Handle AJAX data case
                    elif 'ajax_data' in file_params:
                        logger.info(f"AJAX 데이터로 처리 중: {post['title']}")
                        
                        # Create placeholder dataframe with AJAX info
                        placeholder_df = create_improved_placeholder_dataframe(post, file_params)
                        placeholder_df['AJAX 데이터'] = ['있음']
                        
                        if not placeholder_df.empty:
                            update_data = {
                                'dataframe': placeholder_df,
                                'post_info': post
                            }
                            
                            if 'date' in file_params:
                                update_data['date'] = file_params['date']
                            
                            success = update_google_sheets(gs_client, update_data)
                            if success:
                                logger.info(f"AJAX 데이터로 업데이트 성공: {post['title']}")
                                data_updates.append(update_data)
                    
                    # Handle direct download URL case
                    elif 'download_url' in file_params:
                        logger.info(f"직접 다운로드 URL 처리 중: {post['title']}")
                        
                        # Create placeholder with URL info
                        placeholder_df = create_improved_placeholder_dataframe(post, file_params)
                        
                        if not placeholder_df.empty:
                            update_data = {
                                'dataframe': placeholder_df,
                                'post_info': post
                            }
                            
                            if 'date' in file_params:
                                update_data['date'] = file_params['date']
                            
                            success = update_google_sheets(gs_client, update_data)
                            if success:
                                logger.info(f"URL 정보로 업데이트 성공: {post['title']}")
                                data_updates.append(update_data)
                    
                    # Prevent rate limiting
                    time.sleep(2)
                
                except Exception as e:
                    logger.error(f"게시물 처리 중 오류: {str(e)}")
                    
                    # ENHANCED: More robust error recovery
                    try:
                        # 오류 스크린샷 저장
                        error_screenshot = f"error_{int(time.time())}.png"
                        driver.save_screenshot(error_screenshot)
                        logger.info(f"오류 스크린샷 저장: {error_screenshot}")
                        
                        # 페이지 소스 저장
                        with open(f"error_source_{int(time.time())}.html", 'w', encoding='utf-8') as f:
                            f.write(driver.page_source)
                        
                        # Reset browser context without losing cookies
                        reset_browser_context(driver, delete_cookies=False)
                        logger.info("게시물 처리 오류 후 브라우저 컨텍스트 초기화")
                        
                        # Return to statistics page
                        driver.get(CONFIG['stats_url'])
                        WebDriverWait(driver, 10).until(
                            EC.presence_of_element_located((By.CLASS_NAME, "board_list"))
                        )
                        logger.info("통계 페이지로 복귀 성공")
                    except Exception as recovery_err:
                        logger.error(f"오류 복구 실패: {str(recovery_err)}")
                        
                        # 브라우저 완전 재설정 시도
                        try:
                            driver.quit()
                            driver = setup_driver()
                            driver.get(CONFIG['stats_url'])
                            logger.info("브라우저 완전 재설정 성공")
                        except Exception as reset_err:
                            logger.error(f"브라우저 재설정 실패: {str(reset_err)}")
        
        # 수정: 데이터 업데이트 후 통합 시트 업데이트 처리
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
        
        # ADD THIS CODE RIGHT HERE - before sending Telegram notification:
        if gs_client and data_updates and CONFIG.get('cleanup_old_sheets', False):
            logger.info("Cleaning up date-specific sheets...")
            try:
                # Use the same spreadsheet object if already available, or open it again
                spreadsheet = open_spreadsheet_with_retry(gs_client)
                
                if spreadsheet:
                    removed_count = cleanup_date_specific_sheets(spreadsheet)
                    logger.info(f"Removed {removed_count} date-specific sheets")
            except Exception as cleanup_err:
                logger.error(f"Error during sheet cleanup: {str(cleanup_err)}")
        
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
                    text=f"📊 MSIT 통신 통계 모니터링: 최근 {days_range}일 내 새 게시물이 없습니다. ({datetime.now().strftime('%Y-%m-%d %H:%M')})"
                )
    
    except Exception as e:
        logger.error(f"모니터링 중 오류 발생: {str(e)}")
        
        # 오류 처리 향상
        try:
            # Save error screenshot
            if driver:
                try:
                    driver.save_screenshot("error_screenshot.png")
                    logger.info("오류 발생 시점 스크린샷 저장 완료")
                except Exception as ss_err:
                    logger.error(f"오류 스크린샷 저장 실패: {str(ss_err)}")
            
            # 스택 트레이스 저장
            import traceback
            error_trace = traceback.format_exc()
            logger.error(f"상세 오류 정보: {error_trace}")
            
            # 진단 정보 수집
            try:
                if driver:
                    js_info = driver.execute_script("return {url: document.URL, readyState: document.readyState, title: document.title};")
                    logger.info(f"페이지 진단 정보: {json.dumps(js_info)}")
            except:
                pass
            
            # Send error notification
            bot = telegram.Bot(token=CONFIG['telegram_token'])
            error_post = {
                'title': f"모니터링 오류: {str(e)}",
                'date': datetime.now().strftime('%Y. %m. %d'),
                'department': 'System Error'
            }
            await send_telegram_message([error_post])
            logger.info("오류 알림 전송 완료")
        except Exception as telegram_err:
            logger.error(f"오류 알림 전송 중 추가 오류: {str(telegram_err)}")
    
    finally:
        # Clean up resources
        if driver:
            driver.quit()
            logger.info("WebDriver 종료")
        
        logger.info("=== MSIT 통신 통계 모니터링 종료 ===")

def update_consolidated_sheets(client, data_updates):
    """
    개선된 통합 시트 업데이트 함수.
    Raw 시트의 모든 행과 계층 구조를 통합 시트에 복제하면서 과거 데이터를 보존합니다.
    
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
        
        # 최신 날짜 가져오기
        latest_date = None
        for update in data_updates:
            if 'date' in update:
                year = update['date']['year']
                month = update['date']['month']
                latest_date = f"{year}년 {month}월"
                break
            elif 'post_info' in update:
                post_info = update['post_info']
                title = post_info.get('title', '')
                match = re.search(r'\((\d{4})년\s*(\d{1,2})월말\s*기준\)', title)
                if match:
                    year = match.group(1)
                    month = match.group(2)
                    latest_date = f"{year}년 {month}월"
                    break
        
        if not latest_date:
            latest_date = datetime.now().strftime("%Y년 %m월")
            
        logger.info(f"통합 시트 날짜 컬럼: {latest_date}")
        
        # 각 통합 시트 업데이트
        updated_count = 0
        
        for raw_name, consol_name in raw_sheets:
            try:
                # Raw 시트 가져오기
                raw_ws = worksheet_map.get(raw_name)
                if not raw_ws:
                    logger.warning(f"Raw 시트를 찾을 수 없음: {raw_name}")
                    continue
                
                # 통합 시트 찾기 또는 생성
                if consol_name in worksheet_map:
                    consol_ws = worksheet_map[consol_name]
                    logger.info(f"기존 통합 시트 사용: {consol_name}")
                else:
                    # 새 통합 시트 생성
                    try:
                        consol_ws = spreadsheet.add_worksheet(title=consol_name, rows=1000, cols=100)
                        logger.info(f"새 통합 시트 생성됨: {consol_name}")
                        # 통합 시트에 초기 헤더 추가
                        consol_ws.update_cell(1, 1, "기준일자")
                        time.sleep(1)  # API 제한 방지
                    except Exception as create_err:
                        logger.error(f"통합 시트 생성 실패 {consol_name}: {str(create_err)}")
                        continue
                
                # Raw 시트 데이터 가져오기 (전체 데이터)
                try:
                    raw_data = raw_ws.get_all_values()
                    logger.info(f"Raw 시트 '{raw_name}'에서 {len(raw_data)}행 데이터 가져옴")
                except Exception as raw_err:
                    logger.error(f"Raw 시트 데이터 가져오기 실패: {str(raw_err)}")
                    continue
                
                if not raw_data or len(raw_data) < 2:  # 최소 헤더 + 1행 필요
                    logger.warning(f"Raw 시트 '{raw_name}'에 충분한 데이터가 없음")
                    continue
                
                # 통합 시트 데이터 가져오기
                try:
                    consol_data = consol_ws.get_all_values()
                    logger.info(f"통합 시트 '{consol_name}'에서 {len(consol_data)}행 데이터 가져옴")
                except Exception as consol_err:
                    logger.error(f"통합 시트 데이터 가져오기 실패: {str(consol_err)}")
                    consol_data = []
                
                # Raw 시트 헤더 가져오기
                raw_headers = raw_data[0] if raw_data else []
                
                # 데이터 열 범위와 식별자 열 범위 결정
                # 기본값: 계층구조는 0-4열, 데이터는 5열부터
                id_cols_count = min(5, len(raw_headers))
                id_cols_range = range(0, id_cols_count)
                
                # 마지막 데이터 열 찾기
                last_col_idx = -1
                for col_idx in range(len(raw_headers)-1, id_cols_count-1, -1):
                    # 헤더가 유효한지 확인
                    if col_idx < len(raw_headers) and raw_headers[col_idx] and raw_headers[col_idx].lower() not in ('nan', 'none', ''):
                        # 데이터가 있는지 확인
                        has_data = False
                        for row_idx in range(1, len(raw_data)):
                            if col_idx < len(raw_data[row_idx]):
                                cell_value = raw_data[row_idx][col_idx]
                                if cell_value and cell_value.strip() and cell_value.lower() not in ('nan', 'none', ''):
                                    has_data = True
                                    break
                        
                        if has_data:
                            last_col_idx = col_idx
                            break
                
                if last_col_idx < id_cols_count:
                    last_col_idx = len(raw_headers) - 1
                    logger.warning(f"유효한 마지막 데이터 열을 찾을 수 없음, 마지막 열({last_col_idx})을 사용")
                
                # 마지막 열의 헤더 확인
                last_col_header = raw_headers[last_col_idx] if last_col_idx < len(raw_headers) else "Unknown"
                logger.info(f"마지막 데이터 열: {last_col_idx} ('{last_col_header}')")
                
                # 통합 시트 헤더 처리
                consol_headers = []
                if consol_data and len(consol_data) > 0:
                    consol_headers = consol_data[0]
                
                # 1. 통합 시트 데이터를 메모리에 구조화해서 저장
                structured_consol_data = {}
                
                # 기존 통합 시트의 헤더
                date_headers = []
                date_col_indices = {}  # 날짜 헤더 -> 열 인덱스 매핑
                
                if consol_headers:
                    for i, header in enumerate(consol_headers[id_cols_count:], id_cols_count):
                        date_headers.append(header)
                        date_col_indices[header] = i
                
                # 최신 날짜가 이미 존재하는지 확인
                if latest_date in date_headers:
                    logger.info(f"'{latest_date}' 열이 이미 존재함: 열 {date_col_indices[latest_date]}")
                else:
                    # 새 날짜 열 추가
                    date_headers.append(latest_date)
                    date_col_indices[latest_date] = id_cols_count + len(date_headers) - 1
                    logger.info(f"새 날짜 열 '{latest_date}' 추가")
                
                # 기존 통합 시트에서 데이터 로드
                if consol_data and len(consol_data) > 1:
                    for row_idx in range(1, len(consol_data)):
                        if row_idx >= len(consol_data):
                            continue
                            
                        # 행이 너무 짧으면 스킵
                        if len(consol_data[row_idx]) < id_cols_count:
                            continue
                            
                        # 계층 식별자 생성 (여러 열 조합)
                        id_vals = []
                        for col_idx in id_cols_range:
                            if col_idx < len(consol_data[row_idx]):
                                val = consol_data[row_idx][col_idx].strip()
                                id_vals.append(f"col{col_idx}={val}")
                            else:
                                id_vals.append(f"col{col_idx}=")
                                
                        row_id = "|".join(id_vals)
                        
                        # 데이터 저장 (날짜별 값)
                        if row_id not in structured_consol_data:
                            structured_consol_data[row_id] = {
                                'id_vals': consol_data[row_idx][:id_cols_count],
                                'date_vals': {}
                            }
                            
                        # 각 날짜 열의 값 저장
                        for date_header, col_idx in date_col_indices.items():
                            if col_idx < len(consol_data[row_idx]):
                                structured_consol_data[row_id]['date_vals'][date_header] = consol_data[row_idx][col_idx]
                
                # 2. Raw 시트 데이터를 기반으로 새 구조 생성
                # 새 헤더 생성 (계층 구조 + 날짜 헤더)
                new_headers = []
                
                # 계층 구조 헤더 추가
                for col_idx in id_cols_range:
                    if col_idx < len(raw_headers):
                        new_headers.append(raw_headers[col_idx])
                    else:
                        new_headers.append(f"Column_{col_idx}")
                
                # 날짜 헤더 추가
                for date_header in date_headers:
                    new_headers.append(date_header)
                
                # 3. Raw 시트 데이터를 토대로 새 행 생성
                # 새로 추가될 데이터 행 준비
                new_rows = [new_headers]  # 헤더 행부터 시작
                
                # Raw 시트의 모든 행 처리
                for row_idx in range(1, len(raw_data)):
                    if row_idx >= len(raw_data):
                        continue
                        
                    # 계층 구조 추출
                    id_vals = []
                    
                    for col_idx in id_cols_range:
                        if col_idx < len(raw_data[row_idx]):
                            id_vals.append(raw_data[row_idx][col_idx])
                        else:
                            id_vals.append("")
                    
                    # 계층 식별자 생성
                    id_parts = []
                    for col_idx, val in enumerate(id_vals):
                        id_parts.append(f"col{col_idx}={val.strip()}")
                    row_id = "|".join(id_parts)
                    
                    # 새 행 데이터 준비
                    new_row = id_vals.copy()  # 계층 구조 먼저 복사
                    
                    # 기존 데이터가 있는지 확인
                    existing_data = structured_consol_data.get(row_id, {'id_vals': id_vals, 'date_vals': {}})
                    
                    # 각 날짜 열에 대한 값 추가
                    for date_header in date_headers:
                        # 최신 날짜이면 Raw 데이터에서 값 가져오기
                        if date_header == latest_date:
                            if last_col_idx < len(raw_data[row_idx]):
                                new_row.append(raw_data[row_idx][last_col_idx])
                            else:
                                new_row.append("")
                        else:
                            # 기존 날짜 열이면 통합 시트에서 값 가져오기
                            new_row.append(existing_data['date_vals'].get(date_header, ""))
                    
                    # 새 행 추가
                    new_rows.append(new_row)
                    
                    # 이미 처리된 행 표시
                    structured_consol_data.pop(row_id, None)
                
                # 4. 통합 시트에는 있지만 Raw 시트에는 없는 행 추가
                for row_id, data in structured_consol_data.items():
                    id_vals = data['id_vals']
                    
                    # 빈 행 스킵
                    if not any(val.strip() for val in id_vals):
                        continue
                        
                    # 새 행 준비
                    new_row = id_vals.copy()
                    
                    # 각 날짜 열에 대한 값 추가
                    for date_header in date_headers:
                        # 최신 날짜에는 빈 값 설정
                        if date_header == latest_date:
                            new_row.append("")
                        else:
                            # 기존 날짜 열의 값 복사
                            new_row.append(data['date_vals'].get(date_header, ""))
                    
                    # 새 행 추가
                    new_rows.append(new_row)
                
                # 5. 최종 데이터로 통합 시트 업데이트
                try:
                    # 기존 시트 내용 모두 지우기
                    consol_ws.clear()
                    logger.info(f"통합 시트 '{consol_name}'의 기존 데이터 삭제")
                    time.sleep(2)  # API 제한 방지
                    
                    # 새 데이터로 일괄 업데이트
                    consol_ws.update('A1', new_rows)
                    logger.info(f"통합 시트 '{consol_name}'에 {len(new_rows)}행 업데이트 완료")
                    
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
                        logger.info(f"메타데이터 업데이트 완료: 행 {meta_row}")
                    except Exception as meta_err:
                        logger.warning(f"메타데이터 업데이트 실패: {str(meta_err)}")
                        
                    updated_count += 1
                    
                except Exception as update_err:
                    logger.error(f"통합 시트 업데이트 실패: {str(update_err)}")
                    
                    # 업데이트 실패 시 대체 방법으로 시도
                    try:
                        logger.info("대체 방법으로 행별 업데이트 시도")
                        
                        # 헤더 먼저 업데이트
                        consol_ws.update('A1', [new_headers])
                        logger.info("헤더 업데이트 완료")
                        time.sleep(2)
                        
                        # 행별로 업데이트
                        batch_size = 10  # 작은 배치 크기로 안정성 높임
                        for i in range(1, len(new_rows), batch_size):
                            end_idx = min(i + batch_size, len(new_rows))
                            batch_range = f'A{i+1}:{chr(64 + len(new_headers))}{end_idx}'
                            batch_data = new_rows[i:end_idx]
                            
                            try:
                                consol_ws.update(batch_range, batch_data)
                                logger.info(f"행 {i+1}-{end_idx} 업데이트 완료")
                                time.sleep(2)  # API 제한 방지
                            except Exception as batch_err:
                                logger.error(f"행 {i+1}-{end_idx} 업데이트 실패: {str(batch_err)}")
                                
                                # 개별 행 업데이트로 대체
                                for j, row_data in enumerate(batch_data, i+1):
                                    try:
                                        row_range = f'A{j}:{chr(64 + len(row_data))}{j}'
                                        consol_ws.update(row_range, [row_data])
                                        logger.info(f"개별 행 {j} 업데이트 완료")
                                        time.sleep(1)
                                    except Exception as row_err:
                                        logger.error(f"개별 행 {j} 업데이트 실패: {str(row_err)}")
                        
                        updated_count += 1
                    except Exception as alt_err:
                        logger.error(f"대체 업데이트 방법도 실패: {str(alt_err)}")
                
            except Exception as sheet_err:
                logger.error(f"시트 '{raw_name}/{consol_name}' 처리 중 오류: {str(sheet_err)}")
                import traceback
                logger.error(traceback.format_exc())
        
        logger.info(f"통합 시트 업데이트 완료: {updated_count}개 시트 업데이트됨")
        return updated_count
        
    except Exception as e:
        logger.error(f"통합 시트 업데이트 실패: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return 0


def create_improved_placeholder_dataframe(post_info, file_params=None):
    """
    Enhanced function to create more informative placeholder DataFrames.
    
    Args:
        post_info: Dictionary with post information
        file_params: Optional dictionary with file parameters
        
    Returns:
        pandas.DataFrame: Placeholder DataFrame with diagnostic information
    """
    try:
        # Extract date information
        date_match = re.search(r'\((\d{4})년\s*(\d{1,2})월말\s*기준\)', post_info['title'])
        
        year = date_match.group(1) if date_match else "Unknown"
        month = date_match.group(2) if date_match else "Unknown"
        
        # Get report type
        report_type = determine_report_type(post_info['title'])
        
        # Determine extraction status
        extraction_status = "데이터 추출 실패"
        
        # Add diagnostic information if available
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
        
        # Create DataFrame with improved diagnostic information
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
        # Create minimal DataFrame with error info
        return pd.DataFrame({
            '구분': ['오류 발생'],
            '업데이트 상태': ['데이터프레임 생성 실패'],
            '비고': [f'오류: {str(e)}']
        })


def retry_with_backoff(func, max_retries=3, base_delay=2, *args, **kwargs):
    """
    지수 백오프 방식으로 함수 재시도
    
    Args:
        func: 실행할 함수
        max_retries: 최대 재시도 횟수
        base_delay: 기본 지연 시간 (초)
        *args, **kwargs: 함수에 전달할 인자들
        
    Returns:
        함수의 반환값
        
    Raises:
        마지막 예외를 다시 발생시킴
    """
    last_exception = None
    for attempt in range(max_retries):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            last_exception = e
            wait_time = base_delay * (2 ** attempt)  # 지수 백오프
            logger.warning(f"시도 {attempt+1}/{max_retries} 실패: {str(e)}, {wait_time}초 후 재시도")
            time.sleep(wait_time)
    
    logger.error(f"{max_retries}번 시도 후 실패: {str(last_exception)}")
    raise last_exception

        
def process_in_chunks(large_data, chunk_size=1000, process_func=None):
    """
    대용량 데이터를 청크로 나누어 처리
    
    Args:
        large_data: 대용량 데이터 (리스트 또는 유사한 시퀀스)
        chunk_size: 청크 크기
        process_func: 각 청크를 처리할 함수
        
    Returns:
        전체 처리 결과 리스트
    """
    if process_func is None:
        def process_func(x): return x
        
    results = []
    
    try:
        for i in range(0, len(large_data), chunk_size):
            chunk = large_data[i:i+chunk_size]
            
            # 청크 처리
            chunk_result = process_func(chunk)
            
            if chunk_result:
                if isinstance(chunk_result, list):
                    results.extend(chunk_result)
                else:
                    results.append(chunk_result)
            
            # 명시적 메모리 해제
            chunk = None
            chunk_result = None
            
            # 가비지 컬렉션 강제 실행
            import gc
            gc.collect()
            
            # 잠시 대기 (시스템에 부담 감소)
            time.sleep(0.1)
            
        return results
    except Exception as e:
        logger.error(f"청크 처리 중 오류: {str(e)}")
        return results  # 부분적으로라도 처리된 결과 반환

def collect_diagnostic_info(driver, error=None):
    """
    드라이버 상태와 페이지에 대한 진단 정보 수집
    
    Args:
        driver: Selenium WebDriver 인스턴스
        error: 발생한 예외 객체 (선택 사항)
        
    Returns:
        dict: 수집된 진단 정보
    """
    info = {
        'timestamp': datetime.now().isoformat(),
        'error': str(error) if error else None
    }
    
    try:
        # 기본 페이지 정보
        info['url'] = driver.current_url
        info['title'] = driver.title
        
        # JavaScript 진단 정보
        js_info = driver.execute_script("""
            return {
                readyState: document.readyState,
                url: document.URL,
                referrer: document.referrer,
                domain: document.domain,
                iframesCount: document.querySelectorAll('iframe').length,
                tablesCount: document.querySelectorAll('table').length,
                mainTableExists: !!document.getElementById('mainTable'),
                viewportHeight: window.innerHeight,
                viewportWidth: window.innerWidth,
                hasJQuery: typeof jQuery !== 'undefined',
                hasSynap: typeof localSynap !== 'undefined'
            };
        """)
        info['page_info'] = js_info
        
        # DOM 상태 확인
        dom_info = driver.execute_script("""
            return {
                bodyChildCount: document.body ? document.body.children.length : 0,
                headChildCount: document.head ? document.head.children.length : 0,
                scriptsCount: document.scripts ? document.scripts.length : 0,
                formsCount: document.forms ? document.forms.length : 0
            };
        """)
        info['dom_info'] = dom_info
        
        # iframe 정보
        iframe_info = []
        iframes = driver.find_elements(By.TAG_NAME, "iframe")
        for i, iframe in enumerate(iframes):
            try:
                iframe_info.append({
                    'index': i,
                    'id': iframe.get_attribute('id'),
                    'name': iframe.get_attribute('name'),
                    'src': iframe.get_attribute('src'),
                    'width': iframe.get_attribute('width'),
                    'height': iframe.get_attribute('height'),
                    'is_displayed': iframe.is_displayed()
                })
            except:
                iframe_info.append({'index': i, 'error': 'Could not get attributes'})
        
        info['iframes'] = iframe_info
        
        # 오류가 있는 경우 스크린샷 저장
        if error:
            try:
                screenshot_path = f"error_screenshot_{int(time.time())}.png"
                driver.save_screenshot(screenshot_path)
                info['screenshot_path'] = screenshot_path
            except Exception as ss_err:
                info['screenshot_error'] = str(ss_err)
            
            # 페이지 소스 저장
            try:
                source_path = f"error_source_{int(time.time())}.html"
                with open(source_path, 'w', encoding='utf-8') as f:
                    f.write(driver.page_source)
                info['source_path'] = source_path
            except Exception as src_err:
                info['source_error'] = str(src_err)
                
            # 스택 트레이스 저장
            import traceback
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


async def main():
    """메인 함수: 환경 변수 처리 및 모니터링 실행"""
    # 향상된 로깅 설정
    global logger
    logger = setup_enhanced_logging()
    
    # GitHub Actions 환경 감지
    is_github_actions = os.environ.get('GITHUB_ACTIONS', 'false').lower() == 'true'
    if is_github_actions:
        logger.info("GitHub Actions 환경에서 실행 중")
    
    # 1. 검토 기간 설정
    try:
        # 시작 날짜와 종료 날짜 (YYYY-MM-DD 형식)
        start_date_str = os.environ.get('START_DATE', '')
        end_date_str = os.environ.get('END_DATE', '')
        
        if start_date_str and end_date_str:
            # 날짜 형식 검증
            try:
                start_date = datetime.strptime(start_date_str, '%Y-%m-%d').date()
                end_date = datetime.strptime(end_date_str, '%Y-%m-%d').date()
                
                # 날짜 범위 확인
                if start_date > end_date:
                    logger.error(f"시작 날짜({start_date_str})가 종료 날짜({end_date_str})보다 나중입니다.")
                    logger.info("기본 날짜 범위(최근 4일)를 사용합니다.")
                    # 기본값 사용
                    days_range = int(os.environ.get('DAYS_RANGE', '4'))
                    start_date_str = None
                    end_date_str = None
                else:
                    # 현재 날짜와의 차이로 days_range 계산
                    today = datetime.now().date()
                    days_range = (today - start_date).days
                    
                    logger.info(f"검토 기간 설정: {start_date_str} ~ {end_date_str} (days_range: {days_range}일)")
            except ValueError as date_err:
                logger.error(f"날짜 형식 오류: {str(date_err)}")
                logger.info("기본 날짜 범위(최근 4일)를 사용합니다.")
                # 기본값 사용
                days_range = int(os.environ.get('DAYS_RANGE', '4'))
                start_date_str = None
                end_date_str = None
        else:
            # 날짜가 입력되지 않은 경우 days_range 사용
            days_range = int(os.environ.get('DAYS_RANGE', '4'))
            logger.info(f"검토 기간 미설정, 기본값 사용: days_range={days_range}일")
            start_date_str = None
            end_date_str = None
    except Exception as e:
        logger.error(f"검토 기간 설정 중 오류: {str(e)}")
        logger.info("기본 날짜 범위(최근 4일)를 사용합니다.")
        days_range = int(os.environ.get('DAYS_RANGE', '4'))
        start_date_str = None
        end_date_str = None
    
    # 2. 페이지 범위 설정
    try:
        start_page_str = os.environ.get('START_PAGE', '1')
        end_page_str = os.environ.get('END_PAGE', '5')
        
        # 페이지 번호 검증
        try:
            start_page = int(start_page_str)
            end_page = int(end_page_str)
            
            # 페이지 범위 확인
            if start_page <= 0 or end_page <= 0:
                logger.error("페이지 번호는 1 이상이어야 합니다.")
                logger.info("기본 페이지 범위(1~5)를 사용합니다.")
                start_page = 1
                end_page = 5
            elif start_page > end_page:
                logger.error(f"시작 페이지({start_page})가 종료 페이지({end_page})보다 큽니다.")
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
    except Exception as e:
        logger.error(f"페이지 범위 설정 중 오류: {str(e)}")
        logger.info("기본 페이지 범위(1~5)를 사용합니다.")
        start_page = 1
        end_page = 5
    
    # 3. 기타 환경 변수
    try:
        # Google Sheets 업데이트 여부
        check_sheets_str = os.environ.get('CHECK_SHEETS', 'true').lower()
        check_sheets = check_sheets_str in ('true', 'yes', '1', 'y')
        
        # 통합 시트 업데이트 설정
        update_consolidation_str = os.environ.get('UPDATE_CONSOLIDATION', 'true').lower()
        update_consolidation = update_consolidation_str in ('true', 'yes', '1', 'y')
        logger.info(f"통합 시트 업데이트: {update_consolidation}")
        
        # 스프레드시트 이름
        spreadsheet_name = os.environ.get('SPREADSHEET_NAME', 'MSIT 통신 통계')
        
        # OCR 설정 확인
        ocr_enabled_str = os.environ.get('OCR_ENABLED', 'true').lower()
        ocr_enabled = ocr_enabled_str in ('true', 'yes', '1', 'y')

         # ADD THIS SECTION - read cleanup option
        cleanup_sheets_str = os.environ.get('CLEANUP_OLD_SHEETS', 'false').lower()
        cleanup_old_sheets = cleanup_sheets_str in ('true', 'yes', '1', 'y')

       
        # 최대 API 요청 간격
        api_request_wait = int(os.environ.get('API_REQUEST_WAIT', '2'))
        
        # 재시도 설정
        max_retries = int(os.environ.get('MAX_RETRIES', '3'))
        page_load_timeout = int(os.environ.get('PAGE_LOAD_TIMEOUT', '30'))
        
        # 역순 탐색 (기본 활성화)
        reverse_order_str = os.environ.get('REVERSE_ORDER', 'true').lower()
        reverse_order = reverse_order_str in ('true', 'yes', '1', 'y')
        
        # 환경 설정 로그
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

        
        # Log the configuration
        logger.info(f"환경 설정 - Google Sheets 업데이트: {check_sheets}, OCR: {ocr_enabled}, 시트 정리: {cleanup_old_sheets}")
    
    
    except Exception as config_err:
        logger.error(f"환경 설정 처리 중 오류: {str(config_err)}")
        return
    
    # OCR 라이브러리 확인 (기존 코드)
    if CONFIG['ocr_enabled']:
        try:
            import pytesseract
            from PIL import Image, ImageEnhance, ImageFilter
            import cv2
            
            # Tesseract 경로 설정
            pytesseract_cmd = os.environ.get('PYTESSERACT_CMD', 'tesseract')
            pytesseract.pytesseract.tesseract_cmd = pytesseract_cmd
            logger.info(f"Tesseract 경로 설정: {pytesseract_cmd}")
            
            # Tesseract 가용성 확인
            try:
                version = pytesseract.get_tesseract_version()
                logger.info(f"Tesseract OCR 버전: {version}")
            except Exception as tess_err:
                logger.warning(f"Tesseract OCR 설치 확인 실패: {str(tess_err)}")
                if is_github_actions:
                    # GitHub Actions에서는 tesseract 설치 여부 확인
                    import subprocess
                    try:
                        result = subprocess.run(['which', 'tesseract'], capture_output=True, text=True)
                        if result.stdout:
                            logger.info(f"Tesseract 실행 파일 경로: {result.stdout.strip()}")
                        else:
                            logger.warning("Tesseract 실행 파일을 찾을 수 없음")
                            CONFIG['ocr_enabled'] = False
                    except Exception as which_err:
                        logger.warning(f"Tesseract 경로 확인 실패: {str(which_err)}")
                        CONFIG['ocr_enabled'] = False
                else:
                    CONFIG['ocr_enabled'] = False
            
            logger.info("OCR 관련 라이브러리 로드 성공")
        except ImportError as import_err:
            logger.warning(f"OCR 라이브러리 가져오기 실패: {str(import_err)}")
            CONFIG['ocr_enabled'] = False
    
    # 모니터링 실행
    try:
        await run_monitor(
            days_range=days_range,
            check_sheets=check_sheets,
            start_page=start_page,
            end_page=end_page,
            start_date=start_date_str,
            end_date=end_date_str,
            reverse_order=reverse_order
        )
    except Exception as e:
        logging.error(f"메인 함수 오류: {str(e)}", exc_info=True)
        
        # 치명적 오류 시 텔레그램 알림 시도
        try:
            bot = telegram.Bot(token=CONFIG['telegram_token'])
            # 오류 메시지 제한 (너무 길면 자름)
            error_msg = str(e)
            if len(error_msg) > 500:
                error_msg = error_msg[:500] + "..."
                
            await bot.send_message(
                chat_id=int(CONFIG['chat_id']),
                text=f"⚠️ *MSIT 모니터링 치명적 오류*\n\n{error_msg}\n\n시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
                parse_mode='Markdown'
            )
        except Exception as telegram_err:
            logger.error(f"텔레그램 메시지 전송 중 추가 오류: {str(telegram_err)}")


if __name__ == "__main__":
    asyncio.run(main())
