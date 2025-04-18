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
from selenium.webdriver.common.action_chains import ActionChains

from bs4 import BeautifulSoup
import telegram
import gspread
from oauth2client.service_account import ServiceAccountCredentials

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
    'ocr_enabled': os.environ.get('OCR_ENABLED', 'true').lower() in ('true', 'yes', '1', 'y')
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
                
                # 다음 페이지 클릭 (ActionChains 사용)
                next_link = next_page_links[0]
                
                try:
                    # 자연스러운 클릭 시뮬레이션
                    actions = ActionChains(driver)
                    actions.move_to_element(next_link)
                    actions.pause(random.uniform(0.3, 0.7))  # 자연스러운 일시 정지
                    actions.click()
                    actions.perform()
                    logger.info("ActionChains를 통한 다음 페이지 클릭 실행")
                except Exception as action_err:
                    logger.warning(f"ActionChains 클릭 실패: {str(action_err)}")
                    # JavaScript로 클릭
                    driver.execute_script("arguments[0].click();", next_link)
                    logger.info("JavaScript를 통한 다음 페이지 클릭 실행")
                
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

def parse_page(driver, page_num=1, days_range=4):
    """현재 페이지에서 관련 게시물 파싱 (개선된 버전)"""
    all_posts = []
    telecom_stats_posts = []
    continue_search = True
    
    try:
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
            return [], [], False
        
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
                            
                            # 게시물이 날짜 범위 내에 있는지 확인
                            if not is_in_date_range(date_str, days=days_range):
                                logger.info(f"날짜 범위 밖의 게시물: {date_str}, 이후 게시물 검색 중단")
                                continue_search = False
                                break
                                
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
                    return [], [], False
            except Exception as direct_attempt_err:
                logger.error(f"직접 파싱 시도 중 오류: {str(direct_attempt_err)}")
                return [], [], False
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
                    
                    # 게시물이 날짜 범위 내에 있는지 확인
                    if not is_in_date_range(date_str, days=days_range):
                        logger.info(f"날짜 범위 밖의 게시물: {date_str}, 이후 게시물 검색 중단")
                        continue_search = False
                        break
                    
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
        
        return all_posts, telecom_stats_posts, continue_search
        
    except Exception as e:
        logger.error(f"페이지 파싱 중 에러: {str(e)}")
        return [], [], False

def find_view_link_params(driver, post):
    """게시물에서 바로보기 링크 파라미터 찾기 (클릭 방식 우선 - 개선된 사용자 시뮬레이션)"""
    if not post.get('post_id'):
        logger.error(f"게시물 접근 불가 {post['title']} - post_id 누락")
        return None
    
    logger.info(f"게시물 열기 시도: {post['title']}")
    
    # 현재 URL 저장
    current_url = driver.current_url
    
    # 게시물 목록 페이지로 돌아가기
    try:
        driver.get(CONFIG['stats_url'])
        
        # 더 자연스러운 로딩 대기
        try:
            # 페이지 로드 확인을 위한 여러 요소 시도
            for selector in ["div.board_list", "table.board_list", ".bbs_list"]:
                try:
                    WebDriverWait(driver, 15).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, selector))
                    )
                    logger.info(f"게시물 목록 로드 확인됨: {selector}")
                    break
                except TimeoutException:
                    continue
            
            # 자연스러운 스크롤 시뮬레이션
            driver.execute_script("""
                window.scrollTo(0, 0);
                
                // 부드러운 스크롤
                function smoothScroll(duration) {
                    const scrollHeight = Math.max(
                        document.body.scrollHeight, document.documentElement.scrollHeight,
                        document.body.offsetHeight, document.documentElement.offsetHeight,
                        document.body.clientHeight, document.documentElement.clientHeight
                    );
                    const scrollStep = Math.round(scrollHeight / 20);
                    let scrollCount = 0;
                    const scrollInterval = setInterval(() => {
                        window.scrollBy(0, scrollStep);
                        scrollCount++;
                        if(scrollCount >= 20) clearInterval(scrollInterval);
                    }, duration/20);
                }
                
                smoothScroll(1500);
            """)
            time.sleep(1.5)  # 스크롤 대기
            
            # 다시 맨 위로 스크롤
            driver.execute_script("window.scrollTo(0, 0);")
            time.sleep(0.8)  # 짧은 대기
            
        except Exception as scroll_err:
            logger.warning(f"페이지 로드 후 스크롤 중 오류: {str(scroll_err)}")
            
        # 사용자 같은 지연
        time.sleep(random.uniform(1.5, 3.0))
        
    except Exception as e:
        logger.error(f"게시물 목록 페이지 접근 실패: {str(e)}")
        return direct_access_view_link_params(driver, post)
    
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
            
            # 링크 클릭 시뮬레이션 강화 - ActionChains 사용
            try:
                # 요소가 화면에 보이도록 스크롤
                driver.execute_script("arguments[0].scrollIntoView({block: 'center', behavior: 'smooth'});", post_link)
                time.sleep(0.8)  # 스크롤 대기
                
                # 마우스 움직임 시뮬레이션 및 클릭
                actions = ActionChains(driver)
                actions.move_to_element(post_link)
                actions.pause(random.uniform(0.5, 1.0))  # 자연스러운 일시 중지
                actions.click()
                actions.perform()
                
                logger.info("ActionChains로 자연스러운 클릭 실행")
            except Exception as action_err:
                logger.warning(f"ActionChains 클릭 실패: {str(action_err)}")
                
                # 일반 JavaScript 클릭으로 대체
                try:
                    driver.execute_script("arguments[0].click();", post_link)
                    logger.info("JavaScript를 통한 클릭 실행")
                except Exception as js_click_err:
                    logger.warning(f"JavaScript 클릭 실패: {str(js_click_err)}")
                    # 마지막 시도로 일반 클릭
                    post_link.click()
                    logger.info("일반 클릭 실행")
            
            # 페이지 전환 대기
            wait_time = 15  # 더 길게 대기
            try:
                WebDriverWait(driver, wait_time).until(
                    lambda d: d.current_url != CONFIG['stats_url']
                )
                logger.info(f"페이지 URL 변경 감지됨: {driver.current_url}")
                
                # 자연스러운 로딩 대기
                time.sleep(random.uniform(2.0, 3.0))
            except TimeoutException:
                logger.warning(f"URL 변경 감지 실패 ({wait_time}초 대기 후)")
            
            # 상세 페이지 대기
            wait_elements = [
                (By.CLASS_NAME, "view_head"),
                (By.CLASS_NAME, "view_cont"),
                (By.CSS_SELECTOR, ".bbs_wrap .view"),
                (By.XPATH, "//div[contains(@class, 'view')]"),
                (By.CSS_SELECTOR, "div.view_box"),
                (By.CSS_SELECTOR, "div.view")
            ]
            
            element_found = False
            for by_type, selector in wait_elements:
                try:
                    WebDriverWait(driver, wait_time).until(
                        EC.presence_of_element_located((by_type, selector))
                    )
                    logger.info(f"상세 페이지 로드 완료: {selector} 요소 발견")
                    element_found = True
                    break
                except TimeoutException:
                    continue
            
            if not element_found:
                logger.warning("상세 페이지 로드 실패")
                
                # 페이지 소스 확인하여 디버깅
                try:
                    page_source = driver.page_source
                    if "시스템 점검" in page_source or "정비" in page_source:
                        logger.warning("시스템 점검 페이지 감지됨")
                        take_screenshot(driver, f"system_maintenance_{post['post_id']}")
                        
                        if attempt < max_retries - 1:
                            logger.info(f"시스템 점검 페이지 감지, 재시도 중... ({attempt+1}/{max_retries})")
                            # 브라우저 상태 초기화
                            driver.delete_all_cookies()
                            driver.get(CONFIG['landing_url'])
                            time.sleep(5)
                            driver.get(CONFIG['stats_url'])
                            time.sleep(3)
                            continue
                    
                    # 작은 HTML 샘플 저장 (디버깅용)
                    with open(f"debug_page_source_{post['post_id']}.html", "w", encoding="utf-8") as f:
                        f.write(page_source[:10000])  # 처음 10000자만 저장
                    logger.info(f"디버깅용 HTML 샘플 저장: debug_page_source_{post['post_id']}.html")
                    
                except Exception as debug_err:
                    logger.warning(f"디버깅 정보 저장 중 오류: {str(debug_err)}")
                
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
                    
                    # 스크린샷 캡처 (클릭 전)
                    take_screenshot(driver, f"before_view_click_{post['post_id']}")
                    
                    # 바로보기 링크 클릭 (새 창이 열릴 수 있음)
                    original_window = driver.current_window_handle
                    original_handles = driver.window_handles
                    
                    # ActionChains로 자연스럽게 클릭
                    try:
                        # 요소가 보이도록 스크롤
                        driver.execute_script("arguments[0].scrollIntoView({block: 'center', behavior: 'smooth'});", view_link)
                        time.sleep(0.8)
                        
                        # 자연스러운 클릭
                        actions = ActionChains(driver)
                        actions.move_to_element(view_link)
                        actions.pause(random.uniform(0.3, 0.7))
                        actions.click()
                        actions.perform()
                        
                        logger.info("바로보기 링크 ActionChains로 클릭")
                    except Exception as view_action_err:
                        logger.warning(f"바로보기 ActionChains 클릭 실패: {str(view_action_err)}")
                        
                        # JavaScript 클릭으로 대체
                        try:
                            driver.execute_script("arguments[0].click();", view_link)
                            logger.info("바로보기 JavaScript로 클릭")
                        except Exception as view_js_err:
                            logger.warning(f"바로보기 JavaScript 클릭 실패: {str(view_js_err)}")
                            # 직접 클릭
                            view_link.click()
                            logger.info("바로보기 일반 클릭")
                    
                    # 새 창이 열렸는지 확인 (5초 대기)
                    time.sleep(5)
                    new_handles = driver.window_handles
                    
                    if len(new_handles) > len(original_handles):
                        # 새 창으로 전환
                        logger.info("새 창 감지됨, 전환 중...")
                        new_window = [handle for handle in new_handles if handle not in original_handles][0]
                        driver.switch_to.window(new_window)
                        
                        # 새 창 스크린샷
                        take_screenshot(driver, f"new_window_{post['post_id']}")
                        
                        # getExtension_path 매개변수 추출 (URL에서)
                        current_url = driver.current_url
                        logger.info(f"현재 URL: {current_url}")
                        
                        # 파일 매개변수 추출 시도
                        # 파일 매개변수 추출 시도
                        param_patterns = [
                            r"atchFileNo=(\d+)",
                            r"fileOrdr=(\d+)"
                        ]

                        
                        params = {}
                        for pattern in param_patterns:
                            match = re.search(pattern, current_url)
                            if match:
                                param_name = pattern.split('=')[0].strip('()')
                                params[param_name] = match.group(1)
                        
                        # 원래 창으로 돌아가기
                        driver.close()
                        driver.switch_to.window(original_window)
                        
                        if 'atchFileNo' in params and 'fileOrdr' in params:
                            logger.info(f"URL에서 파일 매개변수 추출: {params}")
                            
                            # 날짜 정보 추출
                            date_match = re.search(r'\((\d{4})년\s*(\d{1,2})월말\s*기준\)', post['title'])
                            if date_match:
                                year = int(date_match.group(1))
                                month = int(date_match.group(2))
                                
                                return {
                                    'atch_file_no': params['atchFileNo'],
                                    'file_ord': params['fileOrdr'],
                                    'date': {'year': year, 'month': month},
                                    'post_info': post
                                }
                            
                            return {
                                'atch_file_no': params['atchFileNo'],
                                'file_ord': params['fileOrdr'],
                                'post_info': post
                            }
                    
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
                
                # 모든 iframe 탐색
                iframe_elements = driver.find_elements(By.TAG_NAME, "iframe")
                if iframe_elements:
                    logger.info(f"{len(iframe_elements)}개 iframe 발견, 탐색 시도...")
                    
                    # 모든 iframe 검사
                    iframe_results = {}
                    for i, iframe in enumerate(iframe_elements):
                        try:
                            iframe_id = iframe.get_attribute("id") or f"iframe_{i}"
                            iframe_src = iframe.get_attribute("src") or "unknown_source"
                            logger.info(f"iframe {i+1}/{len(iframe_elements)} 확인: id={iframe_id}, src={iframe_src}")
                            
                            # iframe으로 전환
                            driver.switch_to.frame(iframe)
                            
                            # iframe 내용 확인
                            iframe_html = driver.page_source
                            
                            # 테이블이 있는지 확인
                            if "<table" in iframe_html:
                                logger.info(f"iframe {iframe_id}에서 테이블 발견")
                                
                                # 스크린샷 캡처
                                iframe_screenshot = take_screenshot(driver, f"iframe_{iframe_id}_{post['post_id']}")
                                
                                # iframe 정보 저장
                                iframe_results[iframe_id] = {
                                    "src": iframe_src,
                                    "has_table": True,
                                    "screenshot": iframe_screenshot
                                }
                            
                            # 기본 컨텐츠로 돌아가기
                            driver.switch_to.default_content()
                            
                        except Exception as iframe_err:
                            logger.warning(f"iframe {i+1} 접근 중 오류: {str(iframe_err)}")
                            # 오류 발생 시 기본 컨텐츠로 복귀
                            try:
                                driver.switch_to.default_content()
                            except:
                                pass
                    
                    # iframe 결과가 있으면 반환
                    if iframe_results:
                        logger.info(f"{len(iframe_results)}개 iframe에서 정보 발견")
                        
                        # 날짜 정보 추출
                        date_match = re.search(r'\((\d{4})년\s*(\d{1,2})월말\s*기준\)', post['title'])
                        if date_match:
                            year = int(date_match.group(1))
                            month = int(date_match.group(2))
                            
                            return {
                                'iframe_results': iframe_results,
                                'date': {'year': year, 'month': month},
                                'post_info': post
                            }
                        
                        return {
                            'iframe_results': iframe_results,
                            'post_info': post
                        }
                
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
                    
                    # 내용 캡처
                    if content:
                        # 내용 스크린샷
                        for selector in content_selectors:
                            try:
                                content_elem = driver.find_element(By.CSS_SELECTOR, selector)
                                if content_elem:
                                    content_screenshot = take_screenshot(driver, f"content_{post['post_id']}", content_elem)
                                    logger.info(f"내용 스크린샷 저장: {content_screenshot}")
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

def access_iframe_direct(driver, file_params):
    """iframe에 직접 접근하여 데이터 추출 (개선된 버전)"""
    if not file_params or not file_params.get('atch_file_no') or not file_params.get('file_ord'):
        logger.error("파일 파라미터가 없습니다.")
        return None
    
    atch_file_no = file_params['atch_file_no']
    file_ord = file_params['file_ord']
    
    # 바로보기 URL 구성
    view_url = f"https://www.msit.go.kr/bbs/documentView.do?atchFileNo={atch_file_no}&fileOrdr={file_ord}"
    logger.info(f"바로보기 URL: {view_url}")
    
    # 사용자 에이전트 변경 및 cookies/localStorage 확인
    try:
        # 쿠키 정보 확인 (디버깅용)
        cookies = driver.get_cookies()
        logger.info(f"현재 쿠키 개수: {len(cookies)}")
        
        # LocalStorage와 SessionStorage 확인
        local_storage = driver.execute_script("return Object.keys(localStorage);")
        session_storage = driver.execute_script("return Object.keys(sessionStorage);")
        logger.info(f"LocalStorage 항목: {len(local_storage)}, SessionStorage 항목: {len(session_storage)}")
        
        # 최신 브라우저 지문 설정
        browser_fingerprint_script = """
            // 난수 생성
            Math.random = (function() {
                var seed = 123456789;
                return function() {
                    seed = (seed * 9301 + 49297) % 233280;
                    return seed / 233280;
                };
            })();
            
            // 캔버스 지문 변경
            HTMLCanvasElement.prototype.getContext = (function(origFn) {
                return function(type, attributes) {
                    const context = origFn.call(this, type, attributes);
                    if (type === '2d') {
                        const originalGetImageData = context.getImageData;
                        context.getImageData = function() {
                            const imageData = originalGetImageData.apply(this, arguments);
                            // 미세하게 변경
                            for (let i = 0; i < 10; i++) {
                                const idx = Math.floor(Math.random() * imageData.data.length);
                                imageData.data[idx] = (imageData.data[idx] + 1) % 256;
                            }
                            return imageData;
                        };
                    }
                    return context;
                };
            })(HTMLCanvasElement.prototype.getContext);
            
            // 플러그인 데이터 일반화
            Object.defineProperty(navigator, 'plugins', {
                get: function() {
                    const fakeMimeTypes = [
                        { type: 'application/pdf', suffixes: 'pdf', description: 'Portable Document Format' },
                        { type: 'video/mp4', suffixes: 'mp4', description: 'MPEG 4 Video' },
                        { type: 'application/x-shockwave-flash', suffixes: 'swf', description: 'Shockwave Flash' }
                    ];
                    
                    const fakePlugins = [
                        { name: 'Chrome PDF Viewer', filename: 'internal-pdf-viewer', description: 'Portable Document Format' },
                        { name: 'Chrome PDF Plugin', filename: 'internal-pdf-plugin', description: 'Portable Document Format' }
                    ];
                    
                    return {
                        length: fakePlugins.length,
                        item: function(index) { return fakePlugins[index] || null; },
                        namedItem: function(name) {
                            return fakePlugins.find(p => p.name === name) || null;
                        },
                        refresh: function() {},
                        ...fakePlugins
                    };
                }
            });
        """
        driver.execute_script(browser_fingerprint_script)
        logger.info("브라우저 지문 위장 스크립트 적용 완료")
        
    except Exception as prep_err:
        logger.warning(f"브라우저 준비 중 오류 (무시됨): {str(prep_err)}")
    
    # 여러 번 재시도
    max_retries = 3
    for attempt in range(max_retries):
        try:
            # 바로보기 URL에 직접 접근하기 전에 중간 페이지를 통해 세션 흐름 유지
            try:
                # 항상 랜딩 페이지를 먼저 방문하여 세션과 쿠키를 초기화
                if attempt > 0:  # 첫 번째 시도가 아닌 경우에만 
                    driver.get(CONFIG['landing_url'])
                    WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located((By.TAG_NAME, "body"))
                    )
                    logger.info("랜딩 페이지 재방문 완료")
                    time.sleep(random.uniform(1.5, 3.0))  # 랜덤 지연
                    
                    # 통계 페이지 방문
                    driver.get(CONFIG['stats_url'])
                    WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located((By.CLASS_NAME, "board_list"))
                    )
                    logger.info("게시판 페이지 방문 완료")
                    time.sleep(random.uniform(1.0, 2.0))  # 랜덤 지연
            except Exception as pre_nav_err:
                logger.warning(f"사전 네비게이션 중 오류 (계속 진행): {str(pre_nav_err)}")
            
            # 자연스러운 방식으로 페이지 로드
            # 일부 사이트는 너무 빠른 페이지 로드를 봇으로 간주할 수 있음
            driver.execute_script("""
                // 페이지 로드 속도 조절
                window.originalFetch = window.fetch;
                window.fetch = function() {
                    return new Promise((resolve, reject) => {
                        window.originalFetch.apply(this, arguments)
                            .then(response => {
                                // 의도적으로 응답 지연 (50-200ms)
                                setTimeout(() => resolve(response), Math.floor(Math.random() * 150) + 50);
                            })
                            .catch(err => reject(err));
                    });
                };
                
                // XMLHttpRequest 지연 추가
                var originalOpen = XMLHttpRequest.prototype.open;
                XMLHttpRequest.prototype.open = function() {
                    this.addEventListener('load', function() {
                        // 인위적 지연
                        var delayTime = Math.floor(Math.random() * 100) + 30;
                        var startTime = Date.now();
                        while(Date.now() - startTime < delayTime) {
                            // 인위적 CPU 사용
                            Math.random() * Math.random();
                        }
                    });
                    originalOpen.apply(this, arguments);
                };
            """)
            
            # 페이지 로드
            logger.info(f"바로보기 URL로 접속 시도 ({attempt+1}/{max_retries}): {view_url}")
            driver.get(view_url)
            
            # 자연스러운 로딩 시간 (랜덤)
            load_wait = random.uniform(4.0, 7.0)
            logger.info(f"페이지 로딩 대기 중... ({load_wait:.1f}초)")
            time.sleep(load_wait)
            
            # 현재 URL 확인
            current_url = driver.current_url
            logger.info(f"현재 URL: {current_url}")
           
            # 스크린샷 저장
            take_screenshot(driver, f"iframe_view_{atch_file_no}_{file_ord}_attempt_{attempt}")
            
            # 페이지 소스 저장 (디버깅용)
            try:
                with open(f"page_source_{atch_file_no}_{file_ord}.html", "w", encoding="utf-8") as f:
                    f.write(driver.page_source)
                logger.info(f"페이지 소스 저장 완료: page_source_{atch_file_no}_{file_ord}.html")
            except Exception as save_err:
                logger.warning(f"페이지 소스 저장 중 오류: {str(save_err)}")
            
            # 현재 페이지 스크린샷 저장 (디버깅용)
            try:
                driver.save_screenshot(f"document_view_{atch_file_no}_{file_ord}.png")
                logger.info(f"문서 뷰어 스크린샷 저장: document_view_{atch_file_no}_{file_ord}.png")
            except Exception as ss_err:
                logger.warning(f"스크린샷 저장 중 오류: {str(ss_err)}")
            
            # 시스템 점검 또는 에러 페이지 감지
            error_patterns = ["시스템 점검 안내", "정비", "오류", "이용에 불편", "죄송합니다", "다시 시도", "Error"]
            error_detected = False
            
            for pattern in error_patterns:
                if pattern in driver.page_source:
                    error_detected = True
                    logger.warning(f"오류 페이지 감지됨: '{pattern}' 문구 발견")
                    break
            
            if error_detected:
                if attempt < max_retries - 1:
                    # 더 긴 지연 후 재시도
                    logger.warning(f"오류 페이지 감지. 재시도 전 쿠키 및 캐시 재설정 후 {(attempt+1)*5}초 대기")
                    
                    # 브라우저 상태 재설정
                    driver.delete_all_cookies()
                    driver.execute_script("localStorage.clear(); sessionStorage.clear();")
                    
                    # 랜딩 페이지 방문하여 정상 상태 복원
                    try:
                        driver.get(CONFIG['landing_url'])
                        time.sleep((attempt+1) * 5)  # 점진적으로 더 오래 대기
                    except:
                        pass
                        
                    continue
                else:
                    logger.warning("최대 재시도 횟수에 도달했습니다. 대체 접근법 시도")
                    return try_alternative_approaches(driver, file_params)
            
            # SynapDocViewServer 또는 문서 뷰어 감지
            viewer_detected = False
            viewer_patterns = ['SynapDocViewServer', 'doc.msit.go.kr', 'documentView', 'hwpView', 'xlsView']
            
            for pattern in viewer_patterns:
                if pattern in current_url or pattern in driver.page_source:
                    viewer_detected = True
                    logger.info(f"문서 뷰어 감지됨: '{pattern}'")
                    break
            
            if viewer_detected:
                logger.info("문서 뷰어로 식별된 페이지 처리 중")
                
                # 새 창이 열렸는지 먼저 확인
                all_window_handles = driver.window_handles
                
                if len(all_window_handles) > 1:
                    original_handle = driver.current_window_handle
                    logger.info(f"여러 창 감지됨: {len(all_window_handles)}개")
                    
                    # 새 창으로 전환 (마지막으로 열린 창 선택)
                    for handle in reversed(all_window_handles):
                        if handle != original_handle:
                            driver.switch_to.window(handle)
                            logger.info(f"새 창으로 전환 완료 (handle: {handle})")
                            # 로딩 대기
                            time.sleep(3)
                            break
                
                # 스크롤 시뮬레이션 (문서 전체 로드 유도)
                try:
                    driver.execute_script("""
                        function smoothScroll() {
                            const height = Math.max(
                                document.body.scrollHeight, 
                                document.documentElement.scrollHeight
                            );
                            const scrollSteps = 10;
                            const stepSize = height / scrollSteps;
                            
                            for (let i = 0; i < scrollSteps; i++) {
                                setTimeout(() => {
                                    window.scrollTo({
                                        top: stepSize * (i + 1),
                                        behavior: 'smooth'
                                    });
                                }, i * 300);
                            }
                            
                            // 맨 위로 다시 스크롤
                            setTimeout(() => {
                                window.scrollTo({
                                    top: 0,
                                    behavior: 'smooth'
                                });
                            }, scrollSteps * 300 + 500);
                        }
                        smoothScroll();
                    """)
                    logger.info("문서 로딩을 위해 부드러운 스크롤 수행")
                    time.sleep(4)  # 스크롤이 완료될 때까지 대기
                except Exception as scroll_err:
                    logger.warning(f"스크롤 중 오류: {str(scroll_err)}")
                
                # 다양한 방식의 iframe 탐색
                iframe_elements = []
                
                # 1. 일반적인 방법 (ID로 찾기)
                iframe_selectors = ["#innerWrap", "#viewerFrame", "#hwpCtrl", "#docViewFrame", "iframe[id]"]
                
                for selector in iframe_selectors:
                    try:
                        iframe = driver.find_elements(By.CSS_SELECTOR, selector)
                        if iframe:
                            iframe_elements.extend(iframe)
                            logger.info(f"선택자 '{selector}'로 {len(iframe)}개 iframe 발견")
                    except Exception as iframe_err:
                        logger.warning(f"iframe 탐색 중 오류 ({selector}): {str(iframe_err)}")
                
                # 2. 모든 iframe 찾기
                try:
                    all_iframes = driver.find_elements(By.TAG_NAME, "iframe")
                    if all_iframes:
                        logger.info(f"총 {len(all_iframes)}개 iframe 발견")
                        # 이미 찾은 iframe과 중복 제거
                        for iframe in all_iframes:
                            if iframe not in iframe_elements:
                                iframe_elements.append(iframe)
                except Exception as all_iframe_err:
                    logger.warning(f"모든 iframe 탐색 중 오류: {str(all_iframe_err)}")
                
                if iframe_elements:
                    logger.info(f"총 {len(iframe_elements)}개 iframe 처리 시도")
                    
                    # 시트 탭 먼저 확인
                    sheet_tabs = driver.find_elements(By.CSS_SELECTOR, ".sheet-list__sheet-tab, .sheet-tab, [class*='sheet'][class*='tab']")
                    
                    if sheet_tabs:
                        logger.info(f"시트 탭 {len(sheet_tabs)}개 발견")
                        all_sheets = {}
                        
                        # iframe 처리 함수 호출
                        return process_sheet_tabs(driver, sheet_tabs, iframe_elements)
                    else:
                        # 시트 탭 없음, 모든 iframe 직접 처리
                        logger.info(f"시트 탭 없음, {len(iframe_elements)}개 iframe 직접 처리")
                        return process_all_iframes(driver, iframe_elements)
                else:
                    logger.warning("iframe을 찾을 수 없습니다. 일반 HTML 테이블 추출 시도")
                    
                    # 일반 HTML에서 테이블 추출 시도
                    try:
                        tables = pd.read_html(driver.page_source)
                        if tables:
                            largest_table = max(tables, key=lambda t: t.size)
                            logger.info(f"일반 HTML에서 테이블 추출 성공: {largest_table.shape}")
                            return {"기본_테이블": largest_table}
                        else:
                            logger.warning("HTML에서 테이블을 찾을 수 없습니다.")
                            
                            # OCR 시도 (마지막 수단)
                            if CONFIG['ocr_enabled'] and attempt == max_retries - 1:
                                full_screenshot = capture_full_table(driver, f"no_table_document_{atch_file_no}_{file_ord}.png")
                                ocr_data = extract_data_from_screenshot(full_screenshot)
                                if ocr_data and len(ocr_data) > 0:
                                    return {"OCR_추출": ocr_data[0]}
                    except Exception as html_table_err:
                        logger.error(f"HTML 테이블 추출 중 오류: {str(html_table_err)}")
            else:
                logger.info("문서 뷰어가 아닌 일반 HTML 페이지 처리")
                
                # 일반 HTML 테이블 추출 시도
                try:
                    tables = pd.read_html(driver.page_source)
                    if tables:
                        largest_table = max(tables, key=lambda t: t.size)
                        logger.info(f"HTML에서 테이블 추출 성공: {largest_table.shape}")
                        return {"HTML_테이블": largest_table}
                    else:
                        logger.warning("HTML에서 테이블을 찾을 수 없습니다")
                        
                        if CONFIG['ocr_enabled'] and attempt == max_retries - 1:
                            full_screenshot = capture_full_table(driver, f"html_page_{atch_file_no}_{file_ord}.png")
                            ocr_data = extract_data_from_screenshot(full_screenshot)
                            if ocr_data and len(ocr_data) > 0:
                                return {"OCR_추출": ocr_data[0]}
                except Exception as html_err:
                    logger.error(f"HTML 테이블 추출 중 오류: {str(html_err)}")
            
            # 현재 시도에서 데이터 추출 실패
            if attempt < max_retries - 1:
                logger.info(f"데이터 추출 실패. 쿠키 및 세션 초기화 후 재시도 ({attempt+1}/{max_retries})")
                
                # 브라우저 상태 재설정
                driver.delete_all_cookies()
                driver.execute_script("localStorage.clear(); sessionStorage.clear();")
                
                # 다음 시도를 위해 대기
                time.sleep((attempt + 1) * 3)  # 점진적으로 대기 시간 증가
                continue
            else:
                # 마지막 방법: OCR 추출
                if CONFIG['ocr_enabled']:
                    logger.info("최종 방법: 전체 페이지 OCR 추출 시도")
                    try:
                        screenshot_path = capture_full_table(driver, f"final_attempt_{atch_file_no}_{file_ord}.png")
                        ocr_data = extract_data_from_screenshot(screenshot_path)
                        if ocr_data and len(ocr_data) > 0:
                            return {"OCR_최종추출": ocr_data[0]}
                        else:
                            logger.warning("OCR 추출도 실패")
                            return None
                    except Exception as final_ocr_err:
                        logger.error(f"최종 OCR 시도 중 오류: {str(final_ocr_err)}")
                        return None
                else:
                    logger.warning("모든 데이터 추출 방법 실패, OCR 비활성화됨")
                    return None
                
        except Exception as e:
            logger.error(f"iframe 접근 중 오류: {str(e)}")
            
            if attempt < max_retries - 1:
                # 다음 시도 전에 브라우저 상태 재설정
                try:
                    driver.delete_all_cookies()
                    driver.execute_script("localStorage.clear(); sessionStorage.clear();")
                    driver.get("about:blank")  # 빈 페이지로 이동
                    time.sleep(5)  # 5초 대기
                except:
                    pass
                
                continue
            else:
                # 최종 대안: 스크린샷 OCR
                if CONFIG['ocr_enabled']:
                    try:
                        # 현재 상태의 스크린샷 캡처
                        error_screenshot = f"error_state_{atch_file_no}_{file_ord}.png"
                        driver.save_screenshot(error_screenshot)
                        logger.info(f"오류 상태 스크린샷 저장: {error_screenshot}")
                        
                        # OCR 시도
                        ocr_data = extract_data_from_screenshot(error_screenshot)
                        if ocr_data and len(ocr_data) > 0:
                            return {"OCR_오류복구": ocr_data[0]}
                    except Exception as ocr_err:
                        logger.error(f"최종 OCR 시도 중 추가 오류: {str(ocr_err)}")
                
                return None
    
    return None

def process_sheet_tabs(driver, sheet_tabs, iframe_elements):
    """시트 탭이 있는 경우 각 시트에서 데이터 추출"""
    all_sheets = {}
    
    for i, tab in enumerate(sheet_tabs):
        try:
            sheet_name = tab.text.strip() if tab.text.strip() else f"시트{i+1}"
            logger.info(f"시트 {i+1}/{len(sheet_tabs)} 처리 중: {sheet_name}")
            
            # 첫 번째가 아닌 시트는 클릭하여 전환
            if i > 0:
                try:
                    # 자연스러운 클릭 시뮬레이션
                    actions = ActionChains(driver)
                    actions.move_to_element(tab)
                    actions.pause(0.5)  # 자연스러운 일시 중지
                    actions.click()
                    actions.perform()
                    
                    logger.info(f"시트 탭 '{sheet_name}' 클릭 완료")
                    time.sleep(3.0)  # 시트 전환 대기
                except Exception as click_err:
                    logger.error(f"시트 탭 클릭 실패 ({sheet_name}): {str(click_err)}")
                    continue
            
            # 클릭 후 새로운 iframe 업데이트/확인
            try:
                # 가끔 시트 변경시 iframe도 변경될 수 있음
                refreshed_iframes = driver.find_elements(By.TAG_NAME, "iframe")
                if len(refreshed_iframes) != len(iframe_elements):
                    logger.info(f"iframe 수 변경 감지: {len(iframe_elements)} -> {len(refreshed_iframes)}")
                    iframe_elements = refreshed_iframes
            except:
                pass
            
            # 각 iframe 순회
            sheet_df = None
            for j, iframe in enumerate(iframe_elements):
                try:
                    iframe_id = iframe.get_attribute("id") or iframe.get_attribute("name") or f"iframe_{j+1}"
                    logger.info(f"iframe {j+1}/{len(iframe_elements)} 처리 중: {iframe_id}")
                    
                    # iframe으로 전환
                    driver.switch_to.frame(iframe)
                    logger.info(f"iframe {iframe_id}로 전환 성공")
                    
                    # 약간 대기
                    time.sleep(1.0)
                    
                    # 페이지 소스 가져오기
                    iframe_html = driver.page_source
                    
                    # 테이블 추출
                    iframe_df = extract_table_from_html(iframe_html)
                    
                    # 기본 프레임으로 복귀
                    driver.switch_to.default_content()
                    
                    if iframe_df is not None and not iframe_df.empty:
                        sheet_df = iframe_df
                        logger.info(f"시트 '{sheet_name}', iframe {iframe_id}에서 데이터 추출 성공: {iframe_df.shape}")
                        break  # 데이터를 찾았으므로 다음 iframe 확인 불필요
                    else:
                        logger.info(f"시트 '{sheet_name}', iframe {iframe_id}에서 테이블 데이터 없음")
                except Exception as iframe_err:
                    logger.error(f"시트 '{sheet_name}', iframe {iframe_id} 처리 중 오류: {str(iframe_err)}")
                    # 오류 발생 시 기본 프레임으로 복귀
                    try:
                        driver.switch_to.default_content()
                    except:
                        pass
            
            if sheet_df is not None:
                all_sheets[sheet_name] = sheet_df
                logger.info(f"시트 '{sheet_name}'에서 데이터 추출 성공: {sheet_df.shape}")
            else:
                logger.warning(f"시트 '{sheet_name}'에서 테이블 추출 실패")
                
                # OCR 시도
                if CONFIG['ocr_enabled']:
                    try:
                        # 전체 표 캡처 시도
                        ocr_screenshot = capture_full_table(driver, f"sheet_{sheet_name}_{int(time.time())}.png")
                        
                        if ocr_screenshot:
                            # OCR로 데이터 추출
                            ocr_data = extract_data_from_screenshot(ocr_screenshot)
                            if ocr_data and len(ocr_data) > 0:
                                all_sheets[sheet_name] = ocr_data[0]
                                logger.info(f"시트 '{sheet_name}'에서 OCR로 데이터 추출 성공")
                    except Exception as ocr_err:
                        logger.error(f"OCR 처리 중 오류: {str(ocr_err)}")
        except Exception as sheet_err:
            logger.error(f"시트 '{sheet_name}' 처리 중 오류: {str(sheet_err)}")
    
    if all_sheets:
        logger.info(f"총 {len(all_sheets)}개 시트에서 데이터 추출 완료")
        return all_sheets
    else:
        logger.warning("어떤 시트에서도 데이터를 추출하지 못했습니다.")
        # OCR로 마지막 시도
        if CONFIG['ocr_enabled']:
            full_screenshot = capture_full_table(driver, f"full_document_{int(time.time())}.png")
            ocr_data = extract_data_from_screenshot(full_screenshot)
            if ocr_data and len(ocr_data) > 0:
                return {"OCR_추출": ocr_data[0]}
        return None


def process_all_iframes(driver, iframe_elements):
    """모든 iframe을 처리하여 데이터 추출"""
    combined_results = {}
    success_count = 0
    
    for i, iframe in enumerate(iframe_elements):
        try:
            iframe_id = iframe.get_attribute("id") or iframe.get_attribute("name") or f"iframe_{i+1}"
            logger.info(f"iframe {i+1}/{len(iframe_elements)} 처리 중: {iframe_id}")
            
            # iframe 내용 처리
            iframe_df = None
            
            try:
                # iframe 스위치
                driver.switch_to.frame(iframe)
                logger.info(f"iframe {iframe_id}로 전환 성공")
                
                # 페이지 소스 가져오기
                iframe_html = driver.page_source
                
                # 테이블 추출
                iframe_df = extract_table_from_html(iframe_html)
                
                # 기본 프레임으로 복귀
                driver.switch_to.default_content()
            except Exception as iframe_switch_err:
                logger.error(f"iframe {iframe_id} 처리 중 오류: {str(iframe_switch_err)}")
                try:
                    driver.switch_to.default_content()
                except:
                    pass
            
            if iframe_df is not None and not iframe_df.empty:
                combined_results[f"iframe_{iframe_id}"] = iframe_df
                success_count += 1
                logger.info(f"iframe {iframe_id}에서 데이터 추출 성공: {iframe_df.shape}")
            else:
                logger.warning(f"iframe {iframe_id}에서 데이터 추출 실패")
                
                # OCR 시도
                if CONFIG['ocr_enabled']:
                    try:
                        # iframe 스위치
                        driver.switch_to.frame(iframe)
                        
                        # 스크린샷 캡처
                        iframe_screenshot = f"iframe_{iframe_id}_{int(time.time())}.png"
                        driver.save_screenshot(iframe_screenshot)
                        
                        # 기본 프레임으로 복귀
                        driver.switch_to.default_content()
                        
                        # OCR 처리
                        ocr_data = extract_data_from_screenshot(iframe_screenshot)
                        if ocr_data and len(ocr_data) > 0:
                            combined_results[f"OCR_iframe_{iframe_id}"] = ocr_data[0]
                            success_count += 1
                            logger.info(f"iframe {iframe_id}에서 OCR로 데이터 추출 성공")
                    except Exception as ocr_err:
                        logger.error(f"iframe {iframe_id} OCR 처리 중 오류: {str(ocr_err)}")
                        try:
                            driver.switch_to.default_content()
                        except:
                            pass
        except Exception as iframe_err:
            logger.error(f"iframe {i+1} 처리 중 오류: {str(iframe_err)}")
    
    if success_count > 0:
        logger.info(f"{success_count}개 iframe에서 데이터 추출 성공")
        return combined_results
    else:
        logger.warning("어떤 iframe에서도 데이터를 추출하지 못했습니다.")
        
        # 마지막 시도로 OCR 사용
        if CONFIG['ocr_enabled']:
            try:
                full_screenshot = capture_full_table(driver, f"full_document_{int(time.time())}.png")
                ocr_data = extract_data_from_screenshot(full_screenshot)
                if ocr_data and len(ocr_data) > 0:
                    return {"OCR_추출": ocr_data[0]}
            except Exception as ocr_err:
                logger.error(f"OCR 추출 중 오류: {str(ocr_err)}")
        
        return None


def try_alternative_approaches(driver, file_params):
    """직접 접근 실패 시 대체 접근 방식 시도"""
    logger.info("대체 접근 방식 시도 중...")
    
    # 방법 1: 리페러(Referer) 헤더를 설정하여 접근
    try:
        # 메인 페이지로 접근 후 세션 확보
        driver.get(CONFIG['landing_url'])
        time.sleep(3)
        
        # Referer 헤더 설정 스크립트 실행
        referer_script = """
            // 리페러 헤더 재정의
            Object.defineProperty(document, 'referrer', {
                get: function() { return "https://www.msit.go.kr/bbs/list.do?sCode=user&mPid=74&mId=99"; }
            });
            
            // 오리진 헤더 재정의 (필요한 경우)
            try {
                Object.defineProperty(window.location, 'origin', {
                    get: function() { return "https://www.msit.go.kr"; }
                });
            } catch(e) {}
        """
        driver.execute_script(referer_script)
        logger.info("Referer 헤더 설정 완료")
        
        # 바로보기 URL 구성
        view_url = f"https://www.msit.go.kr/bbs/documentView.do?atchFileNo={file_params['atch_file_no']}&fileOrdr={file_params['file_ord']}"
        driver.get(view_url)
        time.sleep(5)
        
        # iframe 확인
        iframes = driver.find_elements(By.TAG_NAME, "iframe")
        if iframes:
            logger.info(f"Referer 방식으로 {len(iframes)}개 iframe 발견")
            return process_all_iframes(driver, iframes)
        else:
            logger.warning("Referer 방식으로 iframe을 찾을 수 없음")
    except Exception as referer_err:
        logger.error(f"Referer 방식 시도 중 오류: {str(referer_err)}")
    
    # 방법 2: 쿠키 조작
    try:
        # 브라우저 초기화
        driver.delete_all_cookies()
        driver.get("about:blank")
        
        # 메인 페이지 방문하여 세션 쿠키 획득
        driver.get(CONFIG['landing_url'])
        time.sleep(2)
        
        # 필요한 경우 추가 쿠키 설정
        driver.add_cookie({
            'name': 'allowAccess', 
            'value': 'true',
            'domain': '.msit.go.kr',
            'path': '/'
        })
        
        driver.add_cookie({
            'name': 'sessionActive', 
            'value': 'true',
            'domain': '.msit.go.kr',
            'path': '/'
        })
        
        logger.info("임의 쿠키 추가 완료")
        
        # 바로보기 URL 접근
        view_url = f"https://www.msit.go.kr/bbs/documentView.do?atchFileNo={file_params['atch_file_no']}&fileOrdr={file_params['file_ord']}"
        driver.get(view_url)
        time.sleep(5)
        
        # iframe 확인
        iframes = driver.find_elements(By.TAG_NAME, "iframe")
        if iframes:
            logger.info(f"쿠키 조작 방식으로 {len(iframes)}개 iframe 발견")
            return process_all_iframes(driver, iframes)
        else:
            logger.warning("쿠키 조작 방식으로 iframe을 찾을 수 없음")
    except Exception as cookie_err:
        logger.error(f"쿠키 조작 방식 시도 중 오류: {str(cookie_err)}")
    
    # 방법 3: 최후의 수단 - OCR
    if CONFIG['ocr_enabled']:
        try:
            logger.info("최후의 수단: 전체 페이지 OCR 시도")
            driver.get(CONFIG['landing_url'])  # 접근 가능한 페이지로 이동
            time.sleep(2)
            driver.get(view_url)  # 다시 바로보기 URL 시도
            time.sleep(3)
            
            # 현재 페이지 스크린샷
            screenshot_path = f"alternative_approach_final_{file_params['atch_file_no']}_{file_params['file_ord']}.png"
            driver.save_screenshot(screenshot_path)
            
            # OCR 처리
            ocr_data = extract_data_from_screenshot(screenshot_path)
            if ocr_data and len(ocr_data) > 0:
                return {"OCR_최종시도": ocr_data[0]}
        except Exception as final_ocr_err:
            logger.error(f"최종 OCR 시도 중 오류: {str(final_ocr_err)}")
    
    return None


def capture_full_table(driver, screenshot_path="full_table.png"):
    """전체 표가 보이도록 스크린샷을 캡처하는 함수 (OCR용 최적화)
    
    Args:
        driver: Selenium WebDriver 인스턴스
        screenshot_path: 스크린샷 저장 경로
        
    Returns:
        str 또는 list: 캡처된 스크린샷 경로(들)
    """
    import time
    import logging
    from selenium.webdriver.common.by import By
    
    logger = logging.getLogger('msit_monitor')
    
    try:
        logger.info("전체 표 캡처 시작")
        
        # 원본 창 크기 저장
        original_size = driver.get_window_size()
        
        # 표 요소 찾기 (여러 선택자 시도)
        table_selectors = [
            "table", 
            ".board_list", 
            ".view_cont table", 
            "div[class*='table']", 
            "div.innerWrap table",
            "#innerWrap table",
            ".table-responsive",
            "[class*=table]",
            "[class*=data]"
        ]
        
        table_element = None
        for selector in table_selectors:
            try:
                tables = driver.find_elements(By.CSS_SELECTOR, selector)
                if tables:
                    # 가장 큰 테이블 선택 (일반적으로 주요 데이터 테이블)
                    table_element = max(tables, key=lambda t: t.size['width'] * t.size['height'])
                    logger.info(f"테이블 요소 발견: {selector}")
                    break
            except Exception as e:
                logger.warning(f"선택자 '{selector}'로 테이블 찾기 실패: {str(e)}")
        
        if not table_element:
            logger.warning("테이블 요소를 찾을 수 없음, 페이지 내 모든 요소 검사")
            
            # 특정 요소 클래스 이름에서 "table" 또는 "data" 포함여부 검사
            try:
                all_elems = driver.find_elements(By.CSS_SELECTOR, "*")
                potential_tables = []
                
                for elem in all_elems:
                    try:
                        class_name = elem.get_attribute("class") or ""
                        tag_name = elem.tag_name
                        element_text = elem.text
                        
                        # 테이블 관련 요소 식별
                        if (tag_name == "table" or 
                            "table" in class_name.lower() or 
                            "grid" in class_name.lower() or
                            "data" in class_name.lower() or
                            (tag_name in ["div", "section"] and len(element_text) > 100 and 
                             any(char.isdigit() for char in element_text))):
                            
                            size = elem.size
                            if size['width'] > 50 and size['height'] > 50:  # 너무 작은 요소 제외
                                potential_tables.append((elem, size['width'] * size['height']))
                    except:
                        continue
                
                # 크기가 큰 순서로 정렬
                potential_tables.sort(key=lambda x: x[1], reverse=True)
                
                if potential_tables:
                    table_element = potential_tables[0][0]
                    logger.info(f"잠재적 테이블 요소 발견: {table_element.tag_name}")
            except Exception as find_err:
                logger.warning(f"잠재적 테이블 요소 찾기 실패: {str(find_err)}")
            
            # iframe 내부 확인
            if not table_element:
                try:
                    iframe_selectors = ["iframe", "#innerWrap", "iframe#innerWrap"]
                    for selector in iframe_selectors:
                        iframes = driver.find_elements(By.CSS_SELECTOR, selector)
                        if iframes:
                            logger.info(f"iframe 발견: {selector}, 내부 확인 중")
                            driver.switch_to.frame(iframes[0])
                            
                            # iframe 내부에서 테이블 찾기
                            for selector in table_selectors:
                                try:
                                    tables = driver.find_elements(By.CSS_SELECTOR, selector)
                                    if tables:
                                        table_element = max(tables, key=lambda t: t.size['width'] * t.size['height'])
                                        logger.info(f"iframe 내부에서 테이블 발견: {selector}")
                                        break
                                except Exception as e:
                                    continue
                            
                            # iframe에서 표를 찾지 못하면 기본 프레임으로 복귀
                            if not table_element:
                                driver.switch_to.default_content()
                            else:
                                break
                except Exception as iframe_err:
                    logger.warning(f"iframe 접근 중 오류: {str(iframe_err)}")
                    try:
                        driver.switch_to.default_content()
                    except:
                        pass
        
        # 스크린샷 전에 javascript로 보이는 모든 테이블 강조 (OCR 개선)
        try:
            driver.execute_script("""
                // 모든 테이블 및 테이블 셀 강조
                var tables = document.getElementsByTagName('table');
                for (var i = 0; i < tables.length; i++) {
                    tables[i].style.border = '2px solid black';
                    
                    var cells = tables[i].getElementsByTagName('td');
                    for (var j = 0; j < cells.length; j++) {
                        cells[j].style.border = '1px solid black';
                        cells[j].style.padding = '4px';
                        cells[j].style.textAlign = 'center';
                        
                        // 텍스트 강조
                        if (cells[j].textContent.trim()) {
                            cells[j].style.fontWeight = 'bold';
                            cells[j].style.color = '#000000';
                        }
                    }
                    
                    // 헤더 셀 강조
                    var headers = tables[i].getElementsByTagName('th');
                    for (var k = 0; k < headers.length; k++) {
                        headers[k].style.border = '1px solid black';
                        headers[k].style.backgroundColor = '#f0f0f0';
                        headers[k].style.fontWeight = 'bold';
                        headers[k].style.color = '#000000';
                        headers[k].style.padding = '4px';
                    }
                }
                
                // div나 span 기반 가상 테이블 강조
                var divTables = document.querySelectorAll('div[class*="table"], div[class*="grid"], div[class*="data"]');
                for (var t = 0; t < divTables.length; t++) {
                    divTables[t].style.border = '2px solid black';
                    divTables[t].style.padding = '4px';
                    
                    // 자식 요소들 강조
                    var rows = divTables[t].querySelectorAll('div[class*="row"], div[class*="line"]');
                    for (var r = 0; r < rows.length; r++) {
                        rows[r].style.border = '1px solid #888';
                        rows[r].style.margin = '2px 0';
                        rows[r].style.padding = '4px';
                    }
                    
                    // 셀 요소들 강조
                    var cells = divTables[t].querySelectorAll('div[class*="cell"], span[class*="cell"], div[class*="col"], span[class*="col"]');
                    for (var c = 0; c < cells.length; c++) {
                        cells[c].style.border = '1px solid #aaa';
                        cells[c].style.padding = '3px';
                        cells[c].style.margin = '1px';
                        cells[c].style.fontWeight = 'bold';
                        cells[c].style.color = '#000000';
                    }
                }
            """)
            logger.info("테이블 요소 강조 완료 (OCR 향상용)")
        except Exception as enhance_err:
            logger.warning(f"테이블 강조 실패: {str(enhance_err)}")
        
        # 전체 페이지 캡처 (표 요소를 찾지 못한 경우)
        if not table_element:
            logger.warning("표 요소를 찾지 못했습니다. 전체 페이지 캡처를 진행합니다.")
            
            # 전체 페이지 높이 구하기
            total_height = driver.execute_script("return document.body.scrollHeight")
            
            # 필요한 경우 창 크기 조정
            driver.set_window_size(1920, min(total_height + 100, 10000))  # 여유 공간 추가, 최대 값 제한
            
            # 스크롤하면서 페이지가 완전히 로드되도록 함
            driver.execute_script("window.scrollTo(0, 0);")
            time.sleep(1)
            
            # 점진적으로 스크롤하여 모든 콘텐츠가 로드되도록 함
            scroll_step = min(500, total_height // 10)
            current_position = 0
            
            while current_position < total_height:
                next_position = min(current_position + scroll_step, total_height)
                driver.execute_script(f"window.scrollTo(0, {next_position});")
                time.sleep(0.5)
                current_position = next_position
            
            # 맨 위로 스크롤 복귀
            driver.execute_script("window.scrollTo(0, 0);")
            time.sleep(1)
            
            # 전체 페이지 스크린샷
            driver.save_screenshot(screenshot_path)
            logger.info(f"전체 페이지 스크린샷 저장: {screenshot_path}")
            
            # OCR 최적화를 위한 이미지 처리
            enhanced_path = optimize_image_for_ocr(screenshot_path)
            if enhanced_path:
                logger.info(f"OCR 최적화 이미지 저장: {enhanced_path}")
                result_paths = [screenshot_path, enhanced_path]
            else:
                result_paths = screenshot_path
            
            # 원래 창 크기로 복원
            driver.set_window_size(original_size['width'], original_size['height'])
            
            return result_paths
        
        # 표 요소가 있는 경우
        logger.info(f"발견된 표 크기: {table_element.size}")
        
        # 표 위치 및 크기 가져오기
        table_location = table_element.location
        table_size = table_element.size
        
        # 표 좌표 계산
        table_x = table_location['x']
        table_y = table_location['y']
        table_width = table_size['width']
        table_height = table_size['height']
        
        # 표가 너무 크면 전체 페이지 높이를 조정
        window_height = max(table_y + table_height + 100, 1080)  # 최소 1080px
        
        # 창 크기 조정
        driver.set_window_size(max(table_width + 200, 1920), min(window_height, 10000))  # 여유 공간 추가, 최대 값 제한
        
        # 표가 보이도록 스크롤
        driver.execute_script(f"window.scrollTo(0, {max(0, table_y - 100)});")
        time.sleep(2)  # 스크롤 및 렌더링 대기
        
        # 페이지 전체 스크린샷
        driver.save_screenshot(screenshot_path)
        logger.info(f"표 포함 스크린샷 저장: {screenshot_path}")
        
        # 표 영역만 크롭 (OCR 정확도 향상)
        try:
            from PIL import Image, ImageEnhance
            
            img = Image.open(screenshot_path)
            
            # 스크롤 후 위치 재계산 (필요하면)
            current_scroll = driver.execute_script("return window.pageYOffset;")
            cropped_y = table_y - current_scroll
            
            # 표 영역만 크롭
            if cropped_y >= 0 and table_width > 0 and table_height > 0:
                # 약간의 여백 추가 (테이블 전체 포함 보장)
                padding = 20
                crop_box = (
                    max(0, table_x - padding),
                    max(0, cropped_y - padding),
                    min(img.width, table_x + table_width + padding),
                    min(img.height, cropped_y + table_height + padding)
                )
                
                cropped_img = img.crop(crop_box)
                
                # 이미지 품질 향상 (OCR 정확도 개선)
                # 대비 강화
                enhancer = ImageEnhance.Contrast(cropped_img)
                enhanced_img = enhancer.enhance(1.5)
                
                # 선명도 증가
                enhancer = ImageEnhance.Sharpness(enhanced_img)
                enhanced_img = enhancer.enhance(1.3)
                
                # 밝기 조정 (살짝 밝게)
                enhancer = ImageEnhance.Brightness(enhanced_img)
                enhanced_img = enhancer.enhance(1.1)
                
                # 이미지 크기 조정 (OCR 개선을 위해 확대)
                if cropped_img.width < 1000 or cropped_img.height < 800:
                    scale_factor = min(1500 / cropped_img.width, 1200 / cropped_img.height)
                    if scale_factor > 1:
                        new_width = int(cropped_img.width * scale_factor)
                        new_height = int(cropped_img.height * scale_factor)
                        enhanced_img = enhanced_img.resize((new_width, new_height), Image.LANCZOS)
                        logger.info(f"이미지 확대: {cropped_img.width}x{cropped_img.height} -> {new_width}x{new_height}")
                
                # 크롭 및 향상된 이미지 저장
                cropped_path = screenshot_path.replace('.png', '_cropped.png')
                enhanced_img.save(cropped_path)
                logger.info(f"표 영역 크롭 및 향상된 이미지 저장: {cropped_path}")
                
                # 추가 처리된 이미지 생성 (OCR 최적화)
                enhanced_ocr_path = optimize_image_for_ocr(cropped_path)
                
                # 원본, 크롭, 최적화 이미지 모두 반환
                result_paths = [screenshot_path, cropped_path]
                if enhanced_ocr_path:
                    result_paths.append(enhanced_ocr_path)
                return result_paths
            else:
                logger.warning(f"유효하지 않은 크롭 영역: x={table_x}, y={cropped_y}, w={table_width}, h={table_height}")
                
                # 일반 스크린샷만 최적화
                enhanced_path = optimize_image_for_ocr(screenshot_path)
                if enhanced_path:
                    return [screenshot_path, enhanced_path]
                return screenshot_path
        except Exception as crop_err:
            logger.warning(f"이미지 크롭 및 처리 중 오류: {str(crop_err)}")
            return screenshot_path
        
    except Exception as e:
        logger.error(f"전체 표 캡처 중 오류: {str(e)}")
        
        # 오류 발생해도 일반 스크린샷은 시도
        try:
            driver.save_screenshot(screenshot_path)
            logger.info(f"오류 발생 후 일반 스크린샷 저장: {screenshot_path}")
            return screenshot_path
        except:
            logger.error("스크린샷 저장 실패")
            return None
    finally:
        # 항상 원래 창 크기로 복원
        try:
            driver.set_window_size(original_size['width'], original_size['height'])
        except:
            pass
        
        # iframe에서 나오기
        try:
            driver.switch_to.default_content()
        except:
            pass


def optimize_image_for_ocr(image_path):
    """OCR을 위한 이미지 최적화 처리"""
    try:
        from PIL import Image, ImageEnhance, ImageFilter
        import cv2
        import numpy as np
        
        # 이미지 로드 
        img = cv2.imread(image_path)
        if img is None:
            logger.warning(f"이미지를 로드할 수 없습니다: {image_path}")
            return None
        
        # 그레이스케일 변환
        gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
        
        # 노이즈 제거
        denoised = cv2.fastNlMeansDenoising(gray, None, 10, 7, 21)
        
        # 적응형 임계값 적용
        binary = cv2.adaptiveThreshold(
            denoised, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C,
            cv2.THRESH_BINARY, 11, 2
        )
        
        # 모폴로지 연산으로 텍스트 향상
        kernel = np.ones((1, 1), np.uint8)
        morph = cv2.morphologyEx(binary, cv2.MORPH_CLOSE, kernel)
        
        # 결과 저장
        enhanced_path = image_path.replace('.png', '_enhanced.png')
        cv2.imwrite(enhanced_path, morph)
        
        # 추가적인 PIL 처리 (색상 인식 개선)
        img_pil = Image.open(enhanced_path)
        
        # 대비 향상
        enhancer = ImageEnhance.Contrast(img_pil)
        enhanced_img = enhancer.enhance(2.0)
        
        # 선명도 조정
        enhancer = ImageEnhance.Sharpness(enhanced_img)
        enhanced_img = enhancer.enhance(2.0)
        
        # 저장
        enhanced_img.save(enhanced_path)
        
        logger.info(f"OCR 최적화 이미지 저장: {enhanced_path}")
        return enhanced_path
    except Exception as e:
        logger.warning(f"이미지 최적화 중 오류: {str(e)}")
        return None

def try_ajax_access(driver, post):
    """AJAX 방식으로 게시물 데이터 접근 시도 (개선된 버전)"""
    if not post.get('post_id'):
        logger.error(f"AJAX 접근 불가 {post['title']} - post_id 누락")
        return None
        
    try:
        logger.info(f"AJAX 방식으로 게시물 데이터 접근 시도: {post['title']}")
        
        # 여러 AJAX 엔드포인트 시도
        ajax_endpoints = [
            f"/bbs/ajaxView.do?sCode=user&mId=99&mPid=74&nttSeqNo={post['post_id']}",
            f"/bbs/getPost.do?nttSeqNo={post['post_id']}",
            f"/bbs/api/posts/{post['post_id']}",
            f"/bbs/boardDetail.do?nttSeqNo={post['post_id']}"
        ]
        
        for endpoint in ajax_endpoints:
            try:
                # AJAX 요청 실행 (헤더 추가)
                script = f"""
                    return new Promise((resolve, reject) => {{
                        const xhr = new XMLHttpRequest();
                        xhr.open('GET', '{endpoint}', true);
                        xhr.setRequestHeader('Content-Type', 'application/json');
                        xhr.setRequestHeader('X-Requested-With', 'XMLHttpRequest');
                        xhr.setRequestHeader('Referer', 'https://www.msit.go.kr/bbs/list.do?sCode=user&mPid=74&mId=99');
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
                
                result = driver.execute_async_script(script)
                logger.info(f"AJAX 호출 결과 ({endpoint}): {result[:100] if result else '결과 없음'}...")
                
                if not result:
                    logger.warning(f"AJAX 호출 결과가 없습니다: {endpoint}")
                    continue
                    
                # 결과가 JSON인지 확인 
                try:
                    data = json.loads(result)
                    logger.info(f"AJAX 데이터 파싱 성공: {str(data)[:200] if len(str(data)) > 200 else str(data)}")
                    
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
                    # JSON이 아닌 경우 HTML 내용일 수 있음
                    logger.info("AJAX 응답을 JSON으로 파싱할 수 없습니다. HTML 또는 텍스트 데이터일 수 있습니다.")
                    
                    # HTML에서 테이블 추출 시도
                    try:
                        # BeautifulSoup으로 파싱
                        soup = BeautifulSoup(result, 'html.parser')
                        tables = soup.find_all('table')
                        
                        if tables:
                            logger.info(f"HTML 응답에서 {len(tables)}개 테이블 발견")
                            
                            # 테이블에서 데이터프레임 추출
                            df = extract_table_from_html(result)
                            if df is not None and not df.empty:
                                logger.info(f"HTML 테이블에서 데이터 추출 성공: {df.shape}")
                                
                                # 날짜 정보 추출
                                date_match = re.search(r'\((\d{4})년\s*(\d{1,2})월말\s*기준\)', post['title'])
                                if date_match:
                                    year = int(date_match.group(1))
                                    month = int(date_match.group(2))
                                    
                                    return {
                                        'dataframe': df,
                                        'date': {'year': year, 'month': month},
                                        'post_info': post
                                    }
                                
                                return {
                                    'dataframe': df,
                                    'post_info': post
                                }
                        
                        # 테이블을 찾지 못하면 일반 텍스트로 처리
                        content = soup.get_text(strip=True)
                        date_match = re.search(r'\((\d{4})년\s*(\d{1,2})월말\s*기준\)', post['title'])
                        if date_match:
                            year = int(date_match.group(1))
                            month = int(date_match.group(2))
                            
                            return {
                                'content': content[:1000],  # 긴 내용은 제한
                                'date': {'year': year, 'month': month},
                                'post_info': post
                            }
                    except Exception as html_err:
                        logger.warning(f"HTML 내용 처리 중 오류: {str(html_err)}")
                        
                    # 텍스트 결과로 처리
                    date_match = re.search(r'\((\d{4})년\s*(\d{1,2})월말\s*기준\)', post['title'])
                    if date_match:
                        year = int(date_match.group(1))
                        month = int(date_match.group(2))
                        
                        return {
                            'content': result[:1000],  # 긴 내용은 제한
                            'date': {'year': year, 'month': month},
                            'post_info': post
                        }
            except Exception as endpoint_err:
                logger.warning(f"AJAX 엔드포인트 시도 중 오류 ({endpoint}): {str(endpoint_err)}")
                continue
        
        # 모든 엔드포인트 시도 실패
        logger.warning("모든 AJAX 엔드포인트 시도 실패")
        return None
        
    except Exception as e:
        logger.error(f"AJAX 접근 시도 중 오류: {str(e)}")
        return None


def direct_access_view_link_params(driver, post):
    """게시물 URL로 직접 접근하여 데이터 파라미터 추출 (마지막 대안)"""
    if not post.get('post_id'):
        logger.error(f"직접 접근 불가 {post['title']} - post_id 누락")
        return None
    
    try:
        logger.info(f"게시물 URL로 직접 접근 시도: {post['title']}")
        
        # 게시물 URL
        post_url = get_post_url(post['post_id'])
        if not post_url:
            logger.error(f"게시물 URL을 생성할 수 없음: {post['title']}")
            return None
        
        # 브라우저 리페러(Referer) 설정
        driver.execute_script("""
            // 리페러 헤더 재정의
            Object.defineProperty(document, 'referrer', {
                get: function() { return "https://www.msit.go.kr/bbs/list.do?sCode=user&mPid=74&mId=99"; }
            });
        """)
        
        # 게시물 페이지 접근
        logger.info(f"게시물 URL: {post_url}")
        driver.get(post_url)
        
        # 자연스러운 로딩 대기
        time.sleep(random.uniform(3.0, 5.0))
        
        # 페이지 로드 확인
        try:
            # 다양한 선택자로 상세 페이지 확인
            for selector in ["div.view_head", "div.view_cont", ".bbs_wrap .view", "div.view"]:
                try:
                    WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, selector))
                    )
                    logger.info(f"게시물 페이지 로드 완료: {selector} 요소 발견")
                    break
                except TimeoutException:
                    continue
        except Exception as wait_err:
            logger.warning(f"게시물 페이지 로드 대기 중 오류: {str(wait_err)}")
        
        # 스크린샷 저장 (디버깅용)
        take_screenshot(driver, f"direct_access_{post['post_id']}")
        
        # 바로보기 링크 찾기
        try:
            # 다양한 선택자로 바로보기 링크 찾기
            view_links = []
            view_link_selectors = [
                "a.view[title='새창 열림']",
                "a[onclick*='getExtension_path']",
                "a:contains('바로보기')",
                "a.attach-file, a.file_link, a.download",
                "a[href*='.xls'], a[href*='.xlsx'], a[href*='.pdf'], a[href*='.hwp']"
            ]
            
            for selector in view_link_selectors:
                try:
                    if ':contains' in selector:
                        # 텍스트 내용으로 찾기 (jQuery 스타일)
                        text = selector.split("'")[1]
                        script = f"""
                            return Array.from(document.querySelectorAll('a')).filter(a => 
                                a.textContent.includes('{text}')
                            );
                        """
                        elements = driver.execute_script(script)
                    else:
                        # 일반 CSS 선택자
                        elements = driver.find_elements(By.CSS_SELECTOR, selector)
                    
                    if elements:
                        view_links.extend(elements)
                        logger.info(f"선택자 '{selector}'로 {len(elements)}개 바로보기 링크 발견")
                except Exception as link_err:
                    logger.warning(f"바로보기 링크 찾기 실패 ({selector}): {str(link_err)}")
            
            if view_links:
                view_link = view_links[0]  # 첫 번째 링크 사용
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
                
                # 새 창에서 링크 열어보기
                try:
                    original_handles = driver.window_handles
                    
                    # 링크 클릭 (새 창에서 열림)
                    view_link.click()
                    
                    # 새 창 감지 및 처리
                    time.sleep(3)
                    new_handles = driver.window_handles
                    
                    if len(new_handles) > len(original_handles):
                        # 새 창으로 전환
                        new_window = [handle for handle in new_handles if handle not in original_handles][0]
                        driver.switch_to.window(new_window)
                        
                        # URL에서 파라미터 추출
                        current_url = driver.current_url
                        logger.info(f"새 창 URL: {current_url}")
                        
                        atch_file_match = re.search(r"atchFileNo=(\d+)", current_url)
                        file_ord_match = re.search(r"fileOrdr=(\d+)", current_url)
                        
                        if atch_file_match and file_ord_match:
                            atch_file_no = atch_file_match.group(1)
                            file_ord = file_ord_match.group(1)
                            
                            # 창 닫고 원래 창으로 돌아가기
                            driver.close()
                            driver.switch_to.window(original_handles[0])
                            
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
                        else:
                            # 창 닫고 원래 창으로 돌아가기
                            driver.close()
                            driver.switch_to.window(original_handles[0])
                    
                except Exception as click_err:
                    logger.warning(f"새 창 처리 중 오류: {str(click_err)}")
                    # 원래 창으로 전환 시도
                    try:
                        if len(driver.window_handles) > len(original_handles):
                            for handle in driver.window_handles:
                                if handle not in original_handles:
                                    driver.switch_to.window(handle)
                                    driver.close()
                            driver.switch_to.window(original_handles[0])
                    except:
                        pass
            else:
                logger.warning("바로보기 링크를 찾을 수 없음")
        except Exception as link_err:
            logger.error(f"바로보기 링크 찾기 중 오류: {str(link_err)}")
        
        # 게시물 내용 추출 시도 (링크를 찾지 못한 경우)
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
            
            # 내용이 있는지 확인
            if content:
                # 내용 OCR 또는 텍스트 분석으로 파일 번호 추출 시도
                # 내용에서 숫자 패턴 추출 (atchFileNo, fileOrdr 추측)
                file_patterns = [
                    r"파일번호\s*[:\-=]\s*(\d+)",
                    r"문서\s*ID\s*[:\-=]\s*(\d+)",
                    r"문서\s*번호\s*[:\-=]\s*(\d+)",
                    r"첨부\s*[:\-=]\s*(\d+)"
                ]
                
                file_nums = []
                for pattern in file_patterns:
                    matches = re.findall(pattern, content)
                    file_nums.extend(matches)
                
                # 내용에서 추출된 숫자가 있으면 첫 번째와 두 번째를 파일 파라미터로 사용
                if len(file_nums) >= 2:
                    atch_file_no = file_nums[0]
                    file_ord = file_nums[1]
                    
                    logger.info(f"내용에서 파일 파라미터 추출: atchFileNo={atch_file_no}, fileOrdr={file_ord}")
                    
                    # 날짜 정보 추출
                    date_match = re.search(r'\((\d{4})년\s*(\d{1,2})월말\s*기준\)', post['title'])
                    if date_match:
                        year = int(date_match.group(1))
                        month = int(date_match.group(2))
                        
                        return {
                            'atch_file_no': atch_file_no,
                            'file_ord': file_ord,
                            'date': {'year': year, 'month': month},
                            'post_info': post,
                            'from_content': True
                        }
                
                # 날짜 정보 추출
                date_match = re.search(r'\((\d{4})년\s*(\d{1,2})월말\s*기준\)', post['title'])
                if date_match:
                    year = int(date_match.group(1))
                    month = int(date_match.group(2))
                    
                    return {
                        'content': content,
                        'date': {'year': year, 'month': month},
                        'post_info': post
                    }
            
            # 마지막 수단: 내용이 없거나 파일 파라미터를 추출하지 못한 경우, OCR 시도
            if CONFIG['ocr_enabled']:
                # 전체 페이지 스크린샷 캡처
                screenshot_path = capture_full_table(driver, f"direct_access_full_{post['post_id']}.png")
                
                # OCR 기반 데이터 추출
                ocr_data = extract_data_from_screenshot(screenshot_path)
                if ocr_data and len(ocr_data) > 0:
                    logger.info(f"OCR로 데이터 추출 성공: {ocr_data[0].shape}")
                    
                    # 날짜 정보 추출
                    date_match = re.search(r'\((\d{4})년\s*(\d{1,2})월말\s*기준\)', post['title'])
                    if date_match:
                        year = int(date_match.group(1))
                        month = int(date_match.group(2))
                        
                        return {
                            'dataframe': ocr_data[0],
                            'date': {'year': year, 'month': month},
                            'post_info': post,
                            'ocr_generated': True
                        }
        except Exception as content_err:
            logger.error(f"게시물 내용 추출 중 오류: {str(content_err)}")
        
        # 기본 정보만 반환 (날짜 정보 추출)
        date_match = re.search(r'\((\d{4})년\s*(\d{1,2})월말\s*기준\)', post['title'])
        if date_match:
            year = int(date_match.group(1))
            month = int(date_match.group(2))
            
            return {
                'content': "직접 접근으로 데이터를 추출할 수 없습니다.",
                'date': {'year': year, 'month': month},
                'post_info': post
            }
            
        return None
    except Exception as e:
        logger.error(f"직접 접근 중 오류: {str(e)}")
        
        # 날짜 정보라도 추출
        date_match = re.search(r'\((\d{4})년\s*(\d{1,2})월말\s*기준\)', post['title'])
        if date_match:
            year = int(date_match.group(1))
            month = int(date_match.group(2))
            
            return {
                'content': f"오류: {str(e)}",
                'date': {'year': year, 'month': month},
                'post_info': post
            }
        
        return None


def extract_data_from_screenshot(screenshot_path):
    """스크린샷에서 표 형태의 데이터를 추출하는 함수 (개선된 OCR)
    
    Args:
        screenshot_path (str 또는 list): 스크린샷 파일 경로(들)
        
    Returns:
        list: 추출된 데이터프레임 목록
    """
    import os
    import numpy as np
    import pandas as pd
    from PIL import Image, ImageEnhance, ImageFilter
    import logging
    
    logger = logging.getLogger('msit_monitor')
    
    # 경로가 리스트인 경우 (원본과 크롭 이미지가 모두 있는 경우)
    if isinstance(screenshot_path, list) and len(screenshot_path) > 0:
        logger.info(f"여러 스크린샷 처리 중: {len(screenshot_path)}개")
        results = []
        
        # 크롭/향상된 이미지 우선 처리 (더 나은 OCR 결과 기대)
        # 크롭된 이미지는 일반적으로 파일명에 'cropped'나 'enhanced'를 포함
        sorted_paths = sorted(screenshot_path, 
                             key=lambda x: 0 if 'cropped' in x or 'enhanced' in x else 
                                          (1 if '_ocr' in x else 2))
        
        for path in sorted_paths:
            if os.path.exists(path):
                result = extract_single_screenshot(path)
                if result and len(result) > 0:
                    results.extend(result)
                    # 괜찮은 결과를 얻었으면 중단 (최적화된 이미지에서 결과를 얻은 경우)
                    if len(result[0].columns) > 2 and len(result[0]) > 3:
                        logger.info(f"OCR: {path}에서 충분한 품질의 결과 획득, 추가 처리 중단")
                        break
        
        # 결과가 있으면 반환
        if results:
            return results
        
        # 없으면 원본 이미지만 다시 처리
        screenshot_path = screenshot_path[0] if len(screenshot_path) > 0 else None
    
    # 단일 경로 처리
    if isinstance(screenshot_path, str) and os.path.exists(screenshot_path):
        return extract_single_screenshot(screenshot_path)
    
    logger.error(f"유효한 스크린샷 경로가 아닙니다: {screenshot_path}")
    return []


def extract_single_screenshot(screenshot_path):
    """단일 스크린샷에서 표 데이터 추출 (개선된 OCR)
    
    Args:
        screenshot_path (str): 스크린샷 파일 경로
        
    Returns:
        list: 추출된 데이터프레임 목록
    """
    import os
    import numpy as np
    import pandas as pd
    import cv2
    import logging
    
    logger = logging.getLogger('msit_monitor')
    
    try:
        # 이미지 처리를 위한 패키지 동적 로드
        try:
            import pytesseract
            from PIL import Image, ImageEnhance, ImageFilter
        except ImportError:
            logger.error("OCR 라이브러리가 설치되지 않았습니다. pip install pytesseract pillow opencv-python")
            return []
        
        logger.info(f"이미지 파일에서 표 데이터 추출 시작: {screenshot_path}")
        
        # 이미지 로드 및 전처리
        image = cv2.imread(screenshot_path)
        if image is None:
            logger.error(f"이미지를 로드할 수 없습니다: {screenshot_path}")
            return []
        
        # 이미지 크기 확인
        height, width, _ = image.shape
        logger.info(f"이미지 크기: {width}x{height}")
        
        # 이미지가 너무 큰 경우 크기 조정 (메모리 문제 방지)
        max_dimension = 3000
        if width > max_dimension or height > max_dimension:
            scale_factor = max_dimension / max(width, height)
            new_width = int(width * scale_factor)
            new_height = int(height * scale_factor)
            image = cv2.resize(image, (new_width, new_height))
            logger.info(f"이미지 크기 조정: {new_width}x{new_height}")
        
        # 아래 두 접근 방식 병렬 시도
        # 1. 표 구조 감지 기반 접근법
        table_structure_result = extract_with_table_structure(image, screenshot_path)
        
        # 2. 일반 OCR 접근법
        general_ocr_result = extract_text_without_table_structure(image, screenshot_path)
        
        # 결과 비교 및 선택
        if table_structure_result and len(table_structure_result) > 0 and not table_structure_result[0].empty:
            df_table = table_structure_result[0]
            if general_ocr_result and len(general_ocr_result) > 0 and not general_ocr_result[0].empty:
                df_ocr = general_ocr_result[0]
                
                # 두 결과 중 더 좋은 것 선택 (열과 행 개수로 판단)
                if (len(df_table.columns) >= len(df_ocr.columns) and len(df_table) >= len(df_ocr)):
                    logger.info(f"표 구조 기반 결과 선택: {len(df_table.columns)}열 x {len(df_table)}행")
                    return table_structure_result
                elif (len(df_ocr.columns) > len(df_table.columns) and len(df_ocr) > len(df_table)):
                    logger.info(f"일반 OCR 기반 결과 선택: {len(df_ocr.columns)}열 x {len(df_ocr)}행")
                    return general_ocr_result
                else:
                    # 판단이 어려운 경우 두 결과 모두 반환
                    logger.info(f"두 결과 모두 반환")
                    return table_structure_result + general_ocr_result
            else:
                logger.info(f"표 구조 감지 결과만 반환: {len(df_table.columns)}열 x {len(df_table)}행")
                return table_structure_result
        elif general_ocr_result and len(general_ocr_result) > 0 and not general_ocr_result[0].empty:
            df_ocr = general_ocr_result[0]
            logger.info(f"일반 OCR 결과만 반환: {len(df_ocr.columns)}열 x {len(df_ocr)}행")
            return general_ocr_result
        
        # 둘 다 실패한 경우
        logger.warning("두 방식 모두 표 추출 실패")
        return []
        
    except Exception as e:
        logger.error(f"표 데이터 추출 중 오류: {str(e)}")
        return []


def extract_with_table_structure(image, screenshot_path):
    """표 구조 감지를 활용한 데이터 추출"""
    import cv2
    import numpy as np
    import pandas as pd
    import pytesseract
    from PIL import Image, ImageEnhance
    import logging
    
    logger = logging.getLogger('msit_monitor')
    
    try:
        logger.info("표 구조 감지 기반 OCR 시작")
        
        # 그레이스케일 변환
        gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
        
        # 이미지 향상 (대비 증가)
        _, thresh = cv2.threshold(gray, 150, 255, cv2.THRESH_BINARY_INV)
        
        # 노이즈 제거
        kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (3, 3))
        opening = cv2.morphologyEx(thresh, cv2.MORPH_OPEN, kernel, iterations=1)
        
        # 테이블 경계 감지를 위한 전처리
        dilated = cv2.dilate(opening, kernel, iterations=3)
        
        # 표를 구성하는 선 감지
        # 수직선 감지
        vertical_kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (1, np.array(gray).shape[0] // 50))
        vertical_lines = cv2.erode(dilated, vertical_kernel, iterations=3)
        vertical_lines = cv2.dilate(vertical_lines, vertical_kernel, iterations=5)
        
        # 수평선 감지
        horizontal_kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (np.array(gray).shape[1] // 50, 1))
        horizontal_lines = cv2.erode(dilated, horizontal_kernel, iterations=3)
        horizontal_lines = cv2.dilate(horizontal_lines, horizontal_kernel, iterations=5)
        
        # 수직선과 수평선 병합
        table_mask = cv2.bitwise_or(vertical_lines, horizontal_lines)
        
        # 처리된 이미지 저장 (디버깅용)
        cv2.imwrite(f"{screenshot_path}_processed.png", table_mask)
        
        # 셀 경계 찾기
        contours, _ = cv2.findContours(table_mask, cv2.RETR_TREE, cv2.CHAIN_APPROX_SIMPLE)
        
        # 테이블 구조가 없는 경우 False 반환
        if len(contours) < 10:  # 충분한 셀이 없는 경우
            logger.info("표 구조를 감지하지 못했습니다.")
            return None
        
        # 감지된 셀을 정렬하여 테이블 구조 복원
        # 먼저 충분히 큰 셀만 필터링
        min_cell_area = (image.shape[0] * image.shape[1]) / 1000  # 이미지 크기에 비례한 최소 셀 크기
        cell_contours = [cnt for cnt in contours if cv2.contourArea(cnt) > min_cell_area]
        
        # 셀이 충분하지 않으면 False 반환
        if len(cell_contours) < 5:
            logger.info(f"감지된 셀이 너무 적습니다 ({len(cell_contours)})")
            return None
        
        # 셀의 바운딩 박스 추출 및 정렬
        bounding_boxes = []
        for cnt in cell_contours:
            x, y, w, h = cv2.boundingRect(cnt)
            bounding_boxes.append((x, y, w, h))
        
        # 셀 위치에 따라 행과 열로 그룹화
        # 첫 번째 단계: y 좌표로 행 그룹화
        y_tolerance = image.shape[0] // 40  # 높이의 2.5% 이내면 같은 행으로 간주
        rows = []
        bounding_boxes.sort(key=lambda b: b[1])  # y 좌표로 정렬
        
        current_row = [bounding_boxes[0]]
        current_y = bounding_boxes[0][1]
        
        for box in bounding_boxes[1:]:
            if abs(box[1] - current_y) <= y_tolerance:
                current_row.append(box)
            else:
                rows.append(current_row)
                current_row = [box]
                current_y = box[1]
        
        if current_row:
            rows.append(current_row)
        
        # 각 행 내에서 셀을 x 좌표로 정렬
        for i in range(len(rows)):
            rows[i].sort(key=lambda b: b[0])
        
        # 행과 열 수 결정
        if not rows:
            logger.warning("행 그룹화 실패")
            return None
        
        num_rows = len(rows)
        num_cols = max(len(row) for row in rows)
        
        logger.info(f"감지된 표 구조: {num_rows} 행 x {num_cols} 열")
        
        # OCR로 각 셀의 텍스트 추출
        table_data = []
        for i, row in enumerate(rows):
            row_data = [''] * num_cols  # 빈 셀로 초기화
            
            for j, (x, y, w, h) in enumerate(row):
                if j >= num_cols:  # 열 인덱스가 범위를 벗어나는 경우 건너뛰기
                    continue
                    
                # 이미지에서 셀 영역 추출
                cell_image = gray[y:y+h, x:x+w]
                
                # 셀 이미지가 너무 작으면 건너뛰기
                if cell_image.size == 0 or w < 10 or h < 10:
                    continue
                
                # 셀 이미지 강화 
                _, cell_thresh = cv2.threshold(cell_image, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
                
                # Tesseract OCR 구성 (한국어 포함)
                custom_config = r'-c preserve_interword_spaces=1 --oem 1 --psm 6 -l kor+eng'
                
                # 셀 내용이 주로 숫자인 경우 특수 구성
                if j > 0:  # 첫 번째 열이 아닌 경우 (보통 헤더는 텍스트, 값은 숫자)
                    custom_config = r'-c preserve_interword_spaces=1 --oem 1 --psm 6 -l kor+eng -c tessedit_char_whitelist="0123456789,.-% \'"' 
                
                # OCR 실행
                text = pytesseract.image_to_string(cell_thresh, config=custom_config).strip()
                
                # 공백 및 개행 정리
                text = ' '.join(text.split())
                
                # 추출된 텍스트가 있으면 저장
                if text:
                    row_data[j] = text
            
            # 의미 있는 데이터가 있는 행만 추가
            if any(cell.strip() for cell in row_data):
                table_data.append(row_data)
        
        # Pandas DataFrame 생성
        df = pd.DataFrame(table_data)
        
        # 첫 번째 행이 헤더인지 확인
        if len(table_data) > 1:
            # 데이터 정제
            df = df.replace(r'^\s*$', '', regex=True)  # 공백 셀 정리
            
            # 첫 행이 헤더인지 확인 (모든 값이 있고 숫자가 아닌 경우)
            first_row = df.iloc[0].fillna('')
            if all(first_row) and not any(cell.replace(',', '').replace('.', '').isdigit() for cell in first_row if cell):
                # 첫 행을 헤더로 설정
                headers = first_row.tolist()
                df = df.iloc[1:].reset_index(drop=True)
                df.columns = headers
        
        # 빈 열 제거
        df = df.loc[:, df.notna().any()]
        
        # 빈 행 제거
        df = df.loc[df.astype(str).apply(lambda x: x.str.strip().astype(bool).any(), axis=1)]
        
        # 결과 저장
        logger.info(f"표 데이터 추출 완료: {df.shape[0]}행 {df.shape[1]}열")
        return [df]
    
    except Exception as e:
        logger.error(f"표 구조 기반 데이터 추출 중 오류: {str(e)}")
        return None


def extract_text_without_table_structure(image, screenshot_path):
    """표 구조 없이 일반 OCR을 사용하여 텍스트 추출 및 표 형태로 변환"""
    import numpy as np
    import pandas as pd
    import pytesseract
    from PIL import Image, ImageEnhance
    import re
    import logging
    
    logger = logging.getLogger('msit_monitor')
    
    try:
        logger.info("일반 OCR로 텍스트 추출 시작")
        
        # PIL 이미지로 변환
        image_pil = Image.fromarray(cv2.cvtColor(image, cv2.COLOR_BGR2RGB))
        
        # 이미지 향상
        enhancer = ImageEnhance.Contrast(image_pil)
        enhanced_image = enhancer.enhance(1.5)  # 대비 증가
        
        # 선명도 조정
        enhancer = ImageEnhance.Sharpness(enhanced_image)
        enhanced_image = enhancer.enhance(1.8)
        
        # 다양한 설정으로 OCR 시도
        texts = []
        
        # 기본 설정 (전체 텍스트)
        custom_configs = [
            r'-l kor+eng --oem 1 --psm 6',  # 기본
            r'-l kor+eng --oem 1 --psm 1',  # 자동 페이지 분할
            r'-l kor+eng --oem 1 --psm 4',  # 단일 칼럼 텍스트
            r'-l kor+eng --oem 1 --psm 3',  # 전체 페이지
        ]
        
        for config in custom_configs:
            text = pytesseract.image_to_string(enhanced_image, config=config)
            if text.strip():
                texts.append(text.strip())
        
        # 가장 긴 결과 선택 (더 많은 내용 포함 가능성)
        if texts:
            text = max(texts, key=len)
            logger.info(f"추출된 텍스트 (처음 200자): {text[:200]}")
        else:
            logger.warning("OCR로 텍스트를 추출할 수 없습니다")
            return None
        
        # 줄 단위로 분리
        lines = [line.strip() for line in text.split('\n') if line.strip()]
        
        # 표 형태 데이터 추출 시도
        table_data = []
        
        # 표 구조 감지 (여러 구분자 시도)
        for line in lines:
            # 1. 일반적인 공백 패턴으로 분리
            parts = re.split(r'\s{2,}', line)
            if len(parts) >= 2:
                table_data.append(parts)
                continue
            
            # 2. 탭 문자로 분리
            parts = line.split('\t')
            if len(parts) >= 2:
                table_data.append(parts)
                continue
            
            # 3. 기타 구분자 시도
            for separator in ['|', ';', ',']:
                if separator in line:
                    parts = [p.strip() for p in line.split(separator)]
                    if len(parts) >= 2:
                        table_data.append(parts)
                        break
            
            # 구분자를 찾지 못한 경우 단일 열로 추가
            if not table_data or table_data[-1] != parts:
                table_data.append([line])
        
        # 데이터가 충분한지 확인
        if not table_data:
            logger.warning("추출된 표 데이터가 없습니다")
            return None
        
        # 열 수 표준화 (첫 번째 열 2개 이상 행의 최대 열 수 기준)
        multi_column_rows = [row for row in table_data if len(row) >= 2]
        if multi_column_rows:
            max_cols = max(len(row) for row in multi_column_rows)
        else:
            max_cols = max(len(row) for row in table_data)
        
        normalized_data = []
        for row in table_data:
            # 부족한 열은 빈 문자열로 채움
            normalized_data.append(row + [''] * (max_cols - len(row)))
        
        # DataFrame 생성
        df = pd.DataFrame(normalized_data)
        
        # 첫 행이 헤더인지 확인
        if len(normalized_data) > 1:
            first_row = df.iloc[0]
            if all(first_row.astype(str).str.strip() != ''):
                headers = first_row.tolist()
                df = df.iloc[1:].reset_index(drop=True)
                df.columns = headers
        
        # 빈 열 제거
        df = df.loc[:, ~df.isna().all()]
        df = df.loc[:, ~(df == '').all()]
        
        # 중복 행 제거
        df = df.drop_duplicates().reset_index(drop=True)
        
        # 각 셀 데이터 정리 (앞뒤 공백 제거)
        for col in df.columns:
            df[col] = df[col].astype(str).str.strip()
        
        # 숫자로 구성된 열 인식 및 데이터 타입 변환 시도
        for col in df.columns:
            # 문자열을 숫자로 변환 시도
            try:
                # 각 셀의 숫자 여부 확인 (천 단위 구분자, 소수점 처리)
                is_numeric = df[col].str.replace(',', '').str.replace('.', '', 1).str.isnumeric()
                if is_numeric.sum() / len(is_numeric) > 0.7:  # 70% 이상이 숫자인 경우
                    df[col] = df[col].str.replace(',', '')
                    df[col] = pd.to_numeric(df[col], errors='ignore')
            except:
                continue
        
        logger.info(f"일반 OCR로 데이터 추출 완료: {df.shape[0]}행 {df.shape[1]}열")
        return [df]
        
    except Exception as e:
        logger.error(f"일반 OCR 텍스트 추출 중 오류: {str(e)}")
        # 최소한의 빈 데이터프레임 반환
        return None

async def run_monitor(days_range=4, check_sheets=True):
    """모니터링 실행 (개선된 버전)"""
    driver = None
    gs_client = None
    
    try:
        # 시작 시간 기록
        start_time = time.time()
        logger.info(f"=== MSIT 통신 통계 모니터링 시작 (days_range={days_range}, check_sheets={check_sheets}) ===")

        # 스크린샷 디렉토리 생성
        screenshots_dir = Path("./screenshots")
        screenshots_dir.mkdir(exist_ok=True)

        # WebDriver 초기화
        driver = setup_driver()
        logger.info("WebDriver 초기화 완료")
        
        # Google Sheets 클라이언트 초기화
        if check_sheets and CONFIG['gspread_creds']:
            gs_client = setup_gspread_client()
            if gs_client:
                logger.info("Google Sheets 클라이언트 초기화 완료")
            else:
                logger.warning("Google Sheets 클라이언트 초기화 실패")
        
        # 웹드라이버 설정 강화
        try:
            # User-Agent 재설정
            user_agents = [
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.71 Safari/537.36",
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.1 Safari/605.1.15",
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:96.0) Gecko/20100101 Firefox/96.0"
            ]
            selected_ua = random.choice(user_agents)
            execute_javascript(driver, f'Object.defineProperty(navigator, "userAgent", {{get: function() {{return "{selected_ua}";}}}});', description="User-Agent 재설정")
            logger.info(f"JavaScript로 User-Agent 재설정: {selected_ua}")
            
            # 웹드라이버 감지 회피 강화
            execute_javascript(driver, """
                // 웹드라이버 속성 감지 회피
                Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
                
                // 권한 API 감지 회피
                if (window.navigator.permissions) {
                    window.navigator.permissions.query = (parameters) => {
                        return Promise.resolve({state: 'prompt', onchange: null});
                    };
                }
                
                // 추가 지문 위장
                Object.defineProperty(navigator, 'plugins', {
                    get: function() {
                        return [
                            {name: 'Chrome PDF Plugin', filename: 'internal-pdf-viewer'},
                            {name: 'Chrome PDF Viewer', filename: 'internal-pdf-viewer'},
                            {name: 'Native Client', filename: 'internal-nacl-plugin'}
                        ];
                    }
                });
                
                // 플랫폼 정보 위장
                Object.defineProperty(navigator, 'platform', {
                    get: function() { return 'Win32'; }
                });
                
                // 언어 설정
                Object.defineProperty(navigator, 'language', {
                    get: function() { return 'ko-KR'; }
                });
                
                // 하드웨어 동시실행 정보
                Object.defineProperty(navigator, 'hardwareConcurrency', {
                    get: function() { return 8; }
                });
            """, description="웹드라이버 감지 회피 강화")
        except Exception as setup_err:
            logger.warning(f"웹드라이버 강화 설정 중 오류: {str(setup_err)}")
        
        # 랜딩 페이지 접속 (세션 초기화용)
        try:
            # 랜딩 페이지 접속
            landing_url = CONFIG['landing_url']
            driver.get(landing_url)
            
            # 페이지 로드 대기
            for selector in ["#skip_nav", "body", "header"]:
                try:
                    WebDriverWait(driver, 15).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, selector))
                    )
                    logger.info(f"랜딩 페이지 로드 확인됨: {selector}")
                    break
                except TimeoutException:
                    continue
            
            logger.info("랜딩 페이지 접속 완료 - 쿠키 및 세션 정보 획득")
            
            # 자연스러운 사용자 행동 시뮬레이션
            # 랜덤 지연 (자연스러운 사용자 행동 시뮬레이션)
            time.sleep(random.uniform(2, 4))
            
            # 스크린샷 저장
            driver.save_screenshot("landing_page.png")
            logger.info("랜딩 페이지 스크린샷 저장 완료")
            
            # 스크롤 시뮬레이션
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
            
            # 통계정보 링크 찾기 및 클릭
            stats_link_selectors = [
                "a[href*='mId=99'][href*='mPid=74']",
                "//a[contains(text(), '통계정보')]",
                "//a[contains(text(), '통계')]"
            ]
            
            stats_link_found = False
            for selector in stats_link_selectors:
                try:
                    if selector.startswith("//"):
                        # XPath 선택자
                        links = driver.find_elements(By.XPATH, selector)
                    else:
                        # CSS 선택자
                        links = driver.find_elements(By.CSS_SELECTOR, selector)
                    
                    if links:
                        stats_link = links[0]
                        logger.info(f"통계정보 링크 발견 (선택자: {selector}), 클릭 시도")
                        
                        # 스크린샷 (클릭 전)
                        driver.save_screenshot("before_stats_click.png")
                        
                        # ActionChains로 자연스러운 클릭
                        try:
                            actions = ActionChains(driver)
                            actions.move_to_element(stats_link)
                            actions.pause(random.uniform(0.3, 0.7))
                            actions.click()
                            actions.perform()
                            logger.info("ActionChains로 통계정보 링크 클릭")
                        except Exception as action_err:
                            logger.warning(f"ActionChains 클릭 실패: {str(action_err)}")
                            # JavaScript로 클릭 (대체 방법)
                            driver.execute_script("arguments[0].click();", stats_link)
                            logger.info("JavaScript를 통한 클릭 실행")
                        
                        # URL 변경 대기
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
                
                # 페이지 로드 대기
                try:
                    WebDriverWait(driver, 15).until(
                        EC.presence_of_element_located((By.CLASS_NAME, "board_list"))
                    )
                    logger.info("통계정보 페이지 직접 접속 성공")
                except TimeoutException:
                    logger.warning("통계정보 페이지 로드 시간 초과, 계속 진행")
            
        except Exception as e:
            logger.error(f"랜딩 또는 통계정보 버튼 클릭 중 오류 발생, fallback으로 직접 접속: {str(e)}")
            
            # 브라우저 컨텍스트 초기화
            reset_browser_context(driver)
            
            # 직접 통계 페이지 접속
            driver.get(CONFIG['stats_url'])
            
            try:
                WebDriverWait(driver, 15).until(
                    EC.presence_of_element_located((By.CLASS_NAME, "board_list"))
                )
                logger.info("통계정보 페이지 직접 접속 성공 (오류 후 재시도)")
            except TimeoutException:
                logger.warning("통계정보 페이지 로드 시간 초과 (오류 후 재시도), 계속 진행")
        
        logger.info("MSIT 웹사이트 접근 완료")
        
        # 스크린샷 저장 (디버깅용)
        try:
            driver.save_screenshot("stats_page.png")
            logger.info("통계정보 페이지 스크린샷 저장 완료")
        except Exception as ss_err:
            logger.warning(f"스크린샷 저장 중 오류: {str(ss_err)}")
        
        # 모든 게시물 및 통신 통계 게시물 추적
        all_posts = []
        telecom_stats_posts = []
        continue_search = True
        page_num = 1
        
        # 페이지 파싱
        while continue_search:
            logger.info(f"페이지 {page_num} 파싱 중...")
            
            # 페이지 로드 상태 확인
            try:
                # 페이지가 제대로 로드되었는지 확인
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CLASS_NAME, "board_list"))
                )
            except TimeoutException:
                logger.warning(f"페이지 {page_num} 로드 시간 초과, 새로고침 시도")
                driver.refresh()
                time.sleep(3)
                
                try:
                    WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located((By.CLASS_NAME, "board_list"))
                    )
                    logger.info("새로고침 후 페이지 로드 성공")
                except TimeoutException:
                    logger.error("새로고침 후에도 페이지 로드 실패, 다음 단계로 진행")
            
            page_posts, stats_posts, should_continue = parse_page(driver, page_num, days_range=days_range)
            all_posts.extend(page_posts)
            telecom_stats_posts.extend(stats_posts)
            
            logger.info(f"페이지 {page_num} 파싱 결과: {len(page_posts)}개 게시물, {len(stats_posts)}개 통신 통계")
            
            if not should_continue:
                logger.info(f"날짜 범위 밖의 게시물 발견. 검색 중단")
                break
                
            if has_next_page(driver):
                if go_to_next_page(driver):
                    page_num += 1
                else:
                    logger.warning(f"페이지 {page_num}에서 다음 페이지로 이동 실패")
                    break
            else:
                logger.info(f"마지막 페이지 ({page_num}) 도달")
                break
        
        # 통신 통계 게시물 처리
        data_updates = []
        
        if gs_client and telecom_stats_posts and check_sheets:
            logger.info(f"{len(telecom_stats_posts)}개 통신 통계 게시물 처리 중")
            
            for i, post in enumerate(telecom_stats_posts):
                try:
                    logger.info(f"게시물 {i+1}/{len(telecom_stats_posts)} 처리 중: {post['title']}")
                    
                    # 바로보기 링크 파라미터 추출 (개선된 방식)
                    file_params = find_view_link_params(driver, post)
                    
                    if not file_params:
                        logger.warning(f"바로보기 링크 파라미터 추출 실패: {post['title']}")
                        continue
                    
                    # 바로보기 링크가 있는 경우
                    if 'atch_file_no' in file_params and 'file_ord' in file_params:
                        # iframe 직접 접근하여 데이터 추출 (OCR 폴백 포함)
                        sheets_data = access_iframe_direct(driver, file_params)
                        
                        if sheets_data:
                            # Google Sheets 업데이트
                            update_data = {
                                'sheets': sheets_data,
                                'post_info': post
                            }
                            
                            if 'date' in file_params:
                                update_data['date'] = file_params['date']
                            
                            success = update_google_sheets(gs_client, update_data)
                            if success:
                                logger.info(f"Google Sheets 업데이트 성공: {post['title']}")
                                data_updates.append(update_data)
                            else:
                                logger.warning(f"Google Sheets 업데이트 실패: {post['title']}")
                                
                                # 실패 시 다른 방식으로 재시도
                                logger.info(f"다른 방식으로 업데이트 재시도 중...")
                                
                                # 첫 번째 시트만 사용
                                first_sheet_name = next(iter(sheets_data))
                                first_df = sheets_data[first_sheet_name]
                                
                                report_type = determine_report_type(post['title'])
                                success = update_google_sheets_with_full_table(
                                    client=gs_client,
                                    sheet_name=report_type,
                                    dataframe=first_df,
                                    post_info=post
                                )
                                
                                if success:
                                    logger.info(f"대체 방식으로 업데이트 성공: {post['title']}")
                                    data_updates.append({
                                        'post_info': post,
                                        'dataframe': first_df
                                    })
                        else:
                            logger.warning(f"iframe에서 데이터 추출 실패: {post['title']}")
                            
                            # OCR 기반 최종 시도
                            if CONFIG['ocr_enabled']:
                                # 전체 페이지 캡처
                                screenshot_path = capture_full_table(driver, f"ocr_fallback_{post['post_id']}.png")
                                ocr_data = extract_data_from_screenshot(screenshot_path)
                                
                                if ocr_data and len(ocr_data) > 0:
                                    df = ocr_data[0]
                                    logger.info(f"OCR로 데이터 추출 성공: {df.shape}")
                                    
                                    # Google Sheets 업데이트
                                    report_type = determine_report_type(post['title'])
                                    success = update_google_sheets_with_full_table(
                                        client=gs_client,
                                        sheet_name=report_type,
                                        dataframe=df,
                                        post_info=post
                                    )
                                    
                                    if success:
                                        logger.info(f"OCR 데이터로 업데이트 성공: {post['title']}")
                                        data_updates.append({
                                            'post_info': post,
                                            'dataframe': df,
                                            'ocr_generated': True
                                        })
                                else:
                                    logger.warning(f"OCR 데이터 추출 실패: {post['title']}")
                            
                            # 대체 데이터 생성
                            placeholder_df = create_placeholder_dataframe(post)
                            if not placeholder_df.empty:
                                update_data = {
                                    'dataframe': placeholder_df,
                                    'post_info': post
                                }
                                
                                if 'date' in file_params:
                                    update_data['date'] = file_params['date']
                                
                                success = update_google_sheets_with_full_table(
                                    client=gs_client, 
                                    sheet_name=determine_report_type(post['title']), 
                                    dataframe=placeholder_df, 
                                    post_info=post
                                )
                                
                                if success:
                                    logger.info(f"대체 데이터로 업데이트 성공: {post['title']}")
                                    data_updates.append(update_data)
                    
                    # 게시물 내용만 있는 경우
                    elif 'content' in file_params:
                        logger.info(f"게시물 내용으로 처리 중: {post['title']}")
                        
                        # OCR 시도
                        if CONFIG['ocr_enabled'] and 'ocr_generated' not in file_params:
                            try:
                                # 현재 화면 캡처 
                                screenshot_path = capture_full_table(driver, f"content_ocr_{post['post_id']}.png")
                                ocr_data = extract_data_from_screenshot(screenshot_path)
                                
                                if ocr_data and len(ocr_data) > 0:
                                    df = ocr_data[0]
                                    logger.info(f"게시물 내용 OCR 추출 성공: {df.shape}")
                                    
                                    # Google Sheets 업데이트
                                    report_type = determine_report_type(post['title'])
                                    success = update_google_sheets_with_full_table(
                                        client=gs_client,
                                        sheet_name=report_type,
                                        dataframe=df,
                                        post_info=post
                                    )
                                    
                                    if success:
                                        logger.info(f"OCR 데이터로 업데이트 성공: {post['title']}")
                                        data_updates.append({
                                            'post_info': post,
                                            'dataframe': df,
                                            'ocr_generated': True
                                        })
                                        continue
                            except Exception as ocr_err:
                                logger.warning(f"OCR 처리 중 오류: {str(ocr_err)}")
                        
                        # 대체 데이터 생성
                        placeholder_df = create_placeholder_dataframe(post)
                        if not placeholder_df.empty:
                            update_data = {
                                'dataframe': placeholder_df,
                                'post_info': post
                            }
                            
                            if 'date' in file_params:
                                update_data['date'] = file_params['date']
                            
                            success = update_google_sheets_with_full_table(
                                client=gs_client, 
                                sheet_name=determine_report_type(post['title']), 
                                dataframe=placeholder_df, 
                                post_info=post
                            )
                            
                            if success:
                                logger.info(f"내용 기반 데이터로 업데이트 성공: {post['title']}")
                                data_updates.append(update_data)
                    
                    # iframe 결과가 있는 경우
                    elif 'iframe_results' in file_params:
                        logger.info(f"iframe 결과로 처리 중: {post['title']}")
                        
                        # iframe 스크린샷에서 OCR 추출 시도
                        iframe_screenshots = []
                        for iframe_id, iframe_info in file_params['iframe_results'].items():
                            if 'screenshot' in iframe_info:
                                iframe_screenshots.append(iframe_info['screenshot'])
                        
                        if iframe_screenshots and CONFIG['ocr_enabled']:
                            ocr_data = None
                            for screenshot in iframe_screenshots:
                                ocr_result = extract_data_from_screenshot(screenshot)
                                if ocr_result and len(ocr_result) > 0:
                                    ocr_data = ocr_result[0]
                                    logger.info(f"iframe 스크린샷에서 OCR 데이터 추출: {ocr_data.shape}")
                                    break
                            
                            if ocr_data is not None:
                                # Google Sheets 업데이트
                                report_type = determine_report_type(post['title'])
                                success = update_google_sheets_with_full_table(
                                    client=gs_client,
                                    sheet_name=report_type,
                                    dataframe=ocr_data,
                                    post_info=post
                                )
                                
                                if success:
                                    logger.info(f"iframe OCR 데이터로 업데이트 성공: {post['title']}")
                                    data_updates.append({
                                        'post_info': post,
                                        'dataframe': ocr_data,
                                        'ocr_generated': True
                                    })
                                    continue
                        
                        # 대체 데이터 생성
                        placeholder_df = create_placeholder_dataframe(post)
                        placeholder_df['iframe 발견'] = ['있음']
                        
                        if not placeholder_df.empty:
                            update_data = {
                                'dataframe': placeholder_df,
                                'post_info': post
                            }
                            
                            if 'date' in file_params:
                                update_data['date'] = file_params['date']
                            
                            success = update_google_sheets_with_full_table(
                                client=gs_client,
                                sheet_name=determine_report_type(post['title']),
                                dataframe=placeholder_df,
                                post_info=post
                            )
                            
                            if success:
                                logger.info(f"iframe 정보로 업데이트 성공: {post['title']}")
                                data_updates.append(update_data)
                    
                    # AJAX 데이터가 있는 경우
                    elif 'ajax_data' in file_params:
                        logger.info(f"AJAX 데이터로 처리 중: {post['title']}")
                        
                        # JSON에서 데이터프레임 생성 시도
                        ajax_df = None
                        try:
                            ajax_data = file_params['ajax_data']
                            # AJAX 응답에서 데이터 추출 시도
                            if isinstance(ajax_data, dict):
                                # 중첩 데이터 평탄화
                                flat_data = {}
                                
                                def flatten_dict(d, parent_key=''):
                                    for key, value in d.items():
                                        new_key = f"{parent_key}_{key}" if parent_key else key
                                        if isinstance(value, dict):
                                            flatten_dict(value, new_key)
                                        elif isinstance(value, list) and all(isinstance(x, dict) for x in value):
                                            # 딕셔너리 리스트인 경우
                                            for i, item in enumerate(value):
                                                flatten_dict(item, f"{new_key}_{i}")
                                        else:
                                            flat_data[new_key] = value
                                
                                flatten_dict(ajax_data)
                                ajax_df = pd.DataFrame([flat_data])
                                logger.info(f"AJAX 데이터에서 데이터프레임 생성: {ajax_df.shape}")
                        except Exception as ajax_err:
                            logger.warning(f"AJAX 데이터 처리 중 오류: {str(ajax_err)}")
                        
                        # 생성된 데이터프레임이 있으면 업데이트
                        if ajax_df is not None and not ajax_df.empty:
                            report_type = determine_report_type(post['title'])
                            success = update_google_sheets_with_full_table(
                                client=gs_client,
                                sheet_name=report_type,
                                dataframe=ajax_df,
                                post_info=post
                            )
                            
                            if success:
                                logger.info(f"AJAX 데이터로 업데이트 성공: {post['title']}")
                                data_updates.append({
                                    'post_info': post,
                                    'dataframe': ajax_df
                                })
                                continue
                        
                        # 대체 데이터 생성
                        placeholder_df = create_placeholder_dataframe(post)
                        placeholder_df['AJAX 데이터'] = ['있음']
                        
                        if not placeholder_df.empty:
                            update_data = {
                                'dataframe': placeholder_df,
                                'post_info': post
                            }
                            
                            if 'date' in file_params:
                                update_data['date'] = file_params['date']
                            
                            success = update_google_sheets_with_full_table(
                                client=gs_client,
                                sheet_name=determine_report_type(post['title']),
                                dataframe=placeholder_df,
                                post_info=post
                            )
                            
                            if success:
                                logger.info(f"AJAX 데이터로 업데이트 성공: {post['title']}")
                                data_updates.append(update_data)
                    
                    # 데이터프레임이 있는 경우
                    elif 'dataframe' in file_params:
                        logger.info(f"데이터프레임으로 처리 중: {post['title']}")
                        
                        df = file_params['dataframe']
                        report_type = determine_report_type(post['title'])
                        success = update_google_sheets_with_full_table(
                            client=gs_client,
                            sheet_name=report_type,
                            dataframe=df,
                            post_info=post
                        )
                        
                        if success:
                            logger.info(f"데이터프레임으로 업데이트 성공: {post['title']}")
                            data_updates.append({
                                'post_info': post,
                                'dataframe': df
                            })
                    
                    # API 속도 제한 방지를 위한 지연
                    time.sleep(2)
                
                except Exception as e:
                    logger.error(f"게시물 처리 중 오류: {str(e)}")
                    
                    # 브라우저 컨텍스트 초기화 (오류 후 복구)
                    try:
                        reset_browser_context(driver, delete_cookies=False)
                        logger.info("게시물 처리 오류 후 브라우저 컨텍스트 초기화")
                        
                        # 통계 페이지로 다시 이동
                        driver.get(CONFIG['stats_url'])
                        WebDriverWait(driver, 10).until(
                            EC.presence_of_element_located((By.CLASS_NAME, "board_list"))
                        )
                        logger.info("통계 페이지로 복귀 성공")
                    except Exception as recovery_err:
                        logger.error(f"오류 복구 실패: {str(recovery_err)}")
        
        # 종료 시간 및 실행 시간 계산
        end_time = time.time()
        execution_time = end_time - start_time
        logger.info(f"실행 시간: {execution_time:.2f}초")
        
        # 텔레그램 알림 전송
        if all_posts or data_updates:
            await send_telegram_message(all_posts, data_updates)
            logger.info(f"알림 전송 완료: {len(all_posts)}개 게시물, {len(data_updates)}개 업데이트")
        else:
            logger.info(f"최근 {days_range}일 내 새 게시물이 없습니다")
            
            # 결과 없음 알림 (선택적)
            if days_range > 7:  # 장기간 검색한 경우에만 알림
                try:
                    bot = telegram.Bot(token=CONFIG['telegram_token'])
                    await bot.send_message(
                        chat_id=int(CONFIG['chat_id']),
                        text=f"MSIT 통신 통계 모니터링 결과: 최근 {days_range}일 내 새 게시물이 없습니다."
                    )
                except Exception as telegram_err:
                    logger.error(f"텔레그램 알림 전송 중 오류: {str(telegram_err)}")
        
        return all_posts, telecom_stats_posts, data_updates
        
    except Exception as e:
        logger.error(f"모니터링 실행 중 오류 발생: {str(e)}")
        # 오류 알림 전송
        try:
            bot = telegram.Bot(token=CONFIG['telegram_token'])
            await bot.send_message(
                chat_id=int(CONFIG['chat_id']),
                text=f"MSIT 통신 통계 모니터링 중 오류 발생:\n{str(e)}"
            )
        except Exception as telegram_err:
            logger.error(f"텔레그램 오류 알림 전송 중 추가 오류: {str(telegram_err)}")
        
        return [], [], []
        
    finally:
        # 리소스 정리
        if driver:
            try:
                driver.quit()
                logger.info("WebDriver 세션 종료 완료")
            except Exception as driver_err:
                logger.error(f"WebDriver 세션 종료 중 오류: {str(driver_err)}")

def update_google_sheets(client, update_data):
    """Google Sheets 업데이트 (sheets 데이터 기반)"""
    if not client or not update_data:
        logger.error("Google Sheets 클라이언트 또는 업데이트 데이터가 없습니다.")
        return False
        
    try:
        logger.info(f"Google Sheets 업데이트 시작: {update_data['post_info']['title']}")
        
        if 'sheets' not in update_data:
            logger.warning("업데이트할 시트 데이터가 없습니다.")
            return False
            
        # 스프레드시트 열기
        try:
            spreadsheet = client.open_by_key(CONFIG['spreadsheet_id'])
            logger.info(f"스프레드시트 열기 성공: {spreadsheet.title}")
        except Exception as open_err:
            logger.error(f"스프레드시트 열기 실패: {str(open_err)}")
            return False
            
        # 각 시트 데이터 처리
        sheets_updated = 0
        for sheet_name, sheet_data in update_data['sheets'].items():
            try:
                # 시트 이름 정리 (유효한 워크시트 이름으로 변환)
                clean_sheet_name = clean_sheet_name_for_gspread(sheet_name)
                
                # 시트 확인 또는 생성
                try:
                    worksheet = spreadsheet.worksheet(clean_sheet_name)
                    logger.info(f"기존 워크시트 접근: {clean_sheet_name}")
                except:
                    # 워크시트가 없으면 생성
                    worksheet = spreadsheet.add_worksheet(title=clean_sheet_name, rows=1000, cols=26)
                    logger.info(f"새 워크시트 생성: {clean_sheet_name}")
                
                # 시트 초기화 (필요한 경우)
                try:
                    worksheet.clear()
                    logger.info(f"워크시트 내용 초기화: {clean_sheet_name}")
                except Exception as clear_err:
                    logger.warning(f"워크시트 초기화 중 오류 (계속 진행): {str(clear_err)}")
                
                # 데이터프레임을 리스트로 변환
                if isinstance(sheet_data, pd.DataFrame):
                    # 헤더 포함하여 데이터 리스트로 변환
                    all_values = [sheet_data.columns.tolist()]
                    all_values.extend(sheet_data.values.tolist())
                    
                    # 날짜와 게시물 정보 추가
                    all_values.insert(0, [""])  # 공백 행
                    all_values.insert(0, [update_data['post_info']['title']])
                    
                    if 'date' in update_data:
                        all_values.insert(1, [f"기준 날짜: {update_data['date']['year']}년 {update_data['date']['month']}월"])
                    
                    all_values.insert(len(all_values) if 'date' in update_data else 2, 
                                     [f"게시일: {update_data['post_info']['date']}, URL: {update_data['post_info']['url']}"])
                                    
                    # 빈 셀을 빈 문자열로 변환
                    for i, row in enumerate(all_values):
                        all_values[i] = ['' if pd.isna(cell) else cell for cell in row]
                    
                    # 업데이트
                    worksheet.update(all_values)
                    logger.info(f"워크시트 업데이트 성공: {clean_sheet_name}, {len(all_values)}행")
                    sheets_updated += 1
                else:
                    logger.warning(f"변환할 수 없는 시트 데이터 형식: {type(sheet_data)}")
            except Exception as sheet_err:
                logger.error(f"시트 '{sheet_name}' 업데이트 중 오류: {str(sheet_err)}")
                continue
        
        return sheets_updated > 0
        
    except Exception as e:
        logger.error(f"Google Sheets 업데이트 중 오류: {str(e)}")
        return False


def update_google_sheets_with_full_table(client, sheet_name, dataframe, post_info):
    """데이터프레임을 사용하여 Google Sheets 전체 테이블 업데이트"""
    if not client or dataframe is None or dataframe.empty:
        logger.warning("Google Sheets 클라이언트 또는 데이터프레임이 없습니다.")
        return False
        
    try:
        logger.info(f"Google Sheets 전체 테이블 업데이트 시작: {post_info['title']}")
        
        # 스프레드시트 열기
        try:
            spreadsheet = client.open_by_key(CONFIG['spreadsheet_id'])
            logger.info(f"스프레드시트 열기 성공: {spreadsheet.title}")
        except Exception as open_err:
            logger.error(f"스프레드시트 열기 실패: {str(open_err)}")
            return False
        
        # 시트 이름 정리 (유효한 워크시트 이름으로 변환)
        clean_sheet_name = clean_sheet_name_for_gspread(sheet_name)
        
        # 시트 확인 또는 생성
        try:
            worksheet = spreadsheet.worksheet(clean_sheet_name)
            logger.info(f"기존 워크시트 접근: {clean_sheet_name}")
        except:
            # 워크시트가 없으면 생성
            worksheet = spreadsheet.add_worksheet(title=clean_sheet_name, rows=1000, cols=26)
            logger.info(f"새 워크시트 생성: {clean_sheet_name}")
        
        # 날짜 정보 추출
        date_match = re.search(r'\((\d{4})년\s*(\d{1,2})월말\s*기준\)', post_info['title'])
        date_info = None
        if date_match:
            date_info = {
                'year': int(date_match.group(1)),
                'month': int(date_match.group(2))
            }
        
        # 데이터프레임 전처리
        # NaN 값을 빈 문자열로 변환
        dataframe = dataframe.fillna('')
        
        # 모든 값을 문자열로 변환 (gspread 호환성)
        for col in dataframe.columns:
            dataframe[col] = dataframe[col].astype(str)
            # 불필요한 '.0' 제거 (숫자 정리)
            dataframe[col] = dataframe[col].replace(r'\.0$', '', regex=True)
        
        # 데이터프레임을 리스트로 변환
        all_values = [dataframe.columns.tolist()]
        all_values.extend(dataframe.values.tolist())
        
        # 메타데이터 추가
        all_values.insert(0, [""])  # 공백 행
        all_values.insert(0, [post_info['title']])
        
        if date_info:
            all_values.insert(1, [f"기준 날짜: {date_info['year']}년 {date_info['month']}월"])
        
        # 최종 업데이트 정보 추가
        metadata_row = [f"게시일: {post_info['date']}, URL: {post_info['url'] if 'url' in post_info else '없음'}"]
        all_values.insert(len(all_values), [""])  # 공백 행
        all_values.insert(len(all_values), metadata_row)
        all_values.insert(len(all_values), [f"최종 업데이트: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"])
        
        # 지수 백오프를 사용한 재시도 메커니즘
        max_retries = 5
        retry_count = 0
        retry_delay = 1  # 초기 지연 시간 (초)
        
        while retry_count < max_retries:
            try:
                # 시트 크기 조정 (필요한 경우)
                rows_needed = len(all_values)
                cols_needed = max(len(row) for row in all_values)
                
                current_rows = worksheet.row_count
                current_cols = worksheet.col_count
                
                if rows_needed > current_rows or cols_needed > current_cols:
                    worksheet.resize(rows=max(rows_needed, current_rows), 
                                   cols=max(cols_needed, current_cols))
                    logger.info(f"워크시트 크기 조정: {current_rows}x{current_cols} -> {max(rows_needed, current_rows)}x{max(cols_needed, current_cols)}")
                
                # 시트 초기화
                worksheet.clear()
                
                # 데이터 업데이트
                worksheet.update(all_values)
                logger.info(f"워크시트 업데이트 성공: {clean_sheet_name}, {len(all_values)}행")
                
                # 서식 지정 (옵션)
                try:
                    # 헤더 행 굵게 설정
                    header_format = {"textFormat": {"bold": True}}
                    header_range = f"1:3" # 첫 3행 (제목, 날짜 정보 포함)
                    worksheet.format(header_range, header_format)
                    
                    # 메타데이터 행 기울임꼴로 설정
                    meta_format = {"textFormat": {"italic": True}}
                    meta_range = f"{len(all_values)-2}:{len(all_values)}"
                    worksheet.format(meta_range, meta_format)
                    
                    logger.info("워크시트 서식 업데이트 완료")
                except Exception as format_err:
                    logger.warning(f"서식 적용 중 오류 (무시됨): {str(format_err)}")
                
                # 성공적으로 업데이트 완료
                return True
                
            except Exception as update_err:
                retry_count += 1
                logger.warning(f"워크시트 업데이트 실패 (시도 {retry_count}/{max_retries}): {str(update_err)}")
                
                if retry_count < max_retries:
                    # 지수 백오프
                    sleep_time = retry_delay * (2 ** (retry_count - 1))
                    logger.info(f"{sleep_time}초 후 재시도...")
                    time.sleep(sleep_time)
                else:
                    logger.error(f"최대 재시도 횟수에 도달했습니다: {str(update_err)}")
                    return False
                    
        return False
        
    except Exception as e:
        logger.error(f"Google Sheets 전체 테이블 업데이트 중 오류: {str(e)}")
        return False


def clean_sheet_name_for_gspread(sheet_name):
    """시트 이름을 Google Sheets에 적합한 형식으로 변환"""
    # 특수 문자 제거 또는 변환
    cleaned = re.sub(r'[\\/*\[\]?:]', '_', str(sheet_name))
    # 길이 제한 (31자 이내)
    if len(cleaned) > 31:
        cleaned = cleaned[:28] + '...'
    return cleaned


def determine_report_type(title):
    """게시물 제목에서 보고서 유형 결정"""
    for report_type in CONFIG['report_types']:
        if report_type in title:
            return report_type
    
    # 기본값 (일반적인 보고서 유형)
    return "통신통계_기타"


def create_placeholder_dataframe(post):
    """게시물 정보를 이용한 대체 데이터프레임 생성"""
    try:
        # 날짜 정보 추출
        date_match = re.search(r'\((\d{4})년\s*(\d{1,2})월말\s*기준\)', post['title'])
        year = int(date_match.group(1)) if date_match else None
        month = int(date_match.group(2)) if date_match else None
        
        # 게시물 유형 결정
        report_type = determine_report_type(post['title'])
        
        # 기본 데이터 생성
        data = {
            '게시물 제목': [post['title']],
            '게시일': [post['date']],
            '부서': [post.get('department', '정보 없음')],
            'URL': [post.get('url', '정보 없음')],
            '데이터 상태': ['수동 확인 필요'],
        }
        
        if year and month:
            data['기준년도'] = [year]
            data['기준월'] = [month]
        
        # 보고서 유형별 추가 정보
        if "이동전화" in report_type:
            data['보고서 유형'] = ['이동전화 통계']
        elif "유선통신" in report_type:
            data['보고서 유형'] = ['유선통신 통계']
        elif "무선통신" in report_type:
            data['보고서 유형'] = ['무선통신 통계']
        elif "트래픽" in report_type:
            data['보고서 유형'] = ['데이터 트래픽 통계']
        else:
            data['보고서 유형'] = ['기타 통신 통계']
        
        return pd.DataFrame(data)
    except Exception as e:
        logger.error(f"대체 데이터프레임 생성 중 오류: {str(e)}")
        # 최소한의 데이터만 포함한 데이터프레임 반환
        return pd.DataFrame({
            '게시물 제목': [post['title']],
            '게시일': [post['date']],
            '데이터 상태': ['오류 발생, 수동 확인 필요']
        })


async def send_telegram_message(all_posts, data_updates):
    """텔레그램 알림 전송"""
    if not CONFIG['telegram_token'] or not CONFIG['chat_id']:
        logger.warning("텔레그램 토큰 또는 채팅 ID가 설정되지 않았습니다.")
        return False
        
    try:
        bot = telegram.Bot(token=CONFIG['telegram_token'])
        
        # 기본 메시지 구성
        message = "📊 *MSIT 통신 통계 모니터링 결과*\n\n"
        
        # 새 게시물 요약
        if all_posts:
            message += f"📋 *새 게시물: {len(all_posts)}개*\n"
            
            # 게시물 종류별 분류
            telecom_stats_count = len([p for p in all_posts if is_telecom_stats_post(p['title'])])
            other_count = len(all_posts) - telecom_stats_count
            
            message += f"├ 통신 통계: {telecom_stats_count}개\n"
            message += f"└ 기타 게시물: {other_count}개\n\n"
        else:
            message += "📋 *새 게시물이 없습니다.*\n\n"
        
        # 데이터 업데이트 요약
        if data_updates:
            message += f"🔄 *데이터 업데이트: {len(data_updates)}개*\n\n"
            
            # 최대 5개까지 상세 정보 표시
            max_display = min(5, len(data_updates))
            message += "*업데이트된 통계 목록:*\n"
            
            for i in range(max_display):
                update = data_updates[i]
                title = update['post_info']['title']
                
                # 제목 일부만 표시 (너무 길면 자름)
                if len(title) > 50:
                    title = title[:47] + "..."
                    
                message += f"{i+1}. {title}\n"
            
            # 더 있으면 생략 표시
            if len(data_updates) > max_display:
                message += f"... 외 {len(data_updates) - max_display}개\n"
        else:
            message += "🔄 *데이터 업데이트가 없습니다.*\n"
        
        # 메시지 전송
        await bot.send_message(
            chat_id=int(CONFIG['chat_id']),
            text=message,
            parse_mode='Markdown'
        )
        
        # 상세 정보 (별도 메시지로 전송)
        if data_updates and len(data_updates) > 0:
            detail_message = "*📈 통신 통계 데이터 상세 업데이트*\n\n"
            
            for i, update in enumerate(data_updates[:3]):  # 최대 3개만 상세 정보 표시
                title = update['post_info']['title']
                post_date = update['post_info']['date']
                
                detail_message += f"*{i+1}. {title}*\n"
                detail_message += f"  - 게시일: {post_date}\n"
                
                if 'dataframe' in update:
                    df = update['dataframe']
                    detail_message += f"  - 데이터: {df.shape[0]}행 {df.shape[1]}열\n"
                elif 'sheets' in update:
                    sheets = update['sheets']
                    detail_message += f"  - 시트: {len(sheets)}개\n"
                    for sheet_name in list(sheets.keys())[:2]:  # 최대 2개 시트만 표시
                        sheet_data = sheets[sheet_name]
                        if isinstance(sheet_data, pd.DataFrame):
                            detail_message += f"    · {sheet_name}: {sheet_data.shape[0]}행 {sheet_data.shape[1]}열\n"
                
                # 구분선 추가
                if i < min(2, len(data_updates) - 1):
                    detail_message += "\n---\n\n"
            
            await bot.send_message(
                chat_id=int(CONFIG['chat_id']),
                text=detail_message,
                parse_mode='Markdown'
            )
        
        return True
        
    except Exception as e:
        logger.error(f"텔레그램 메시지 전송 중 오류: {str(e)}")
        return False


def extract_table_from_html(html_content):
    """HTML 내용에서 표 추출 (개선된 버전)"""
    try:
        # pandas read_html 사용
        tables = pd.read_html(html_content, encoding='utf-8')
        
        if not tables:
            logger.warning("HTML에서 표를 찾을 수 없습니다")
            return None
        
        # 가장 큰 테이블 선택 (데이터가 가장 많은 테이블 선택)
        largest_table = max(tables, key=lambda t: t.size)
        
        # 테이블 정리 (필요한 경우)
        # 불필요한 행/열 제거 및 null 처리
        largest_table = largest_table.dropna(how='all')
        
        # 행과 열이 전부 비어있지 않은지 확인
        if largest_table.empty or largest_table.shape[0] == 0 or largest_table.shape[1] == 0:
            logger.warning("추출된 테이블이 비어 있습니다")
            return None
        
        # 헤더 정리 (필요한 경우)
        # 첫 번째 행이 모두 NaN이 아니면 헤더로 사용
        if not largest_table.iloc[0].isna().all():
            new_header = largest_table.iloc[0]
            largest_table = largest_table[1:]
            largest_table.columns = new_header
            largest_table = largest_table.reset_index(drop=True)
        
        logger.info(f"HTML에서 테이블 추출 성공: {largest_table.shape}")
        return largest_table
        
    except ValueError as ve:
        logger.warning(f"HTML 테이블 파싱 중 오류: {str(ve)}")
        return None
    except Exception as e:
        logger.error(f"HTML 테이블 추출 중 오류: {str(e)}")
        return None

async def main():
    """스크립트 메인 함수"""
    import argparse
    
    # 명령줄 인자 파싱
    parser = argparse.ArgumentParser(description='MSIT 통신 통계 모니터링 스크립트')
    parser.add_argument('--days', type=int, default=4,
                        help='검색할 일 수 (기본값: 4)')
    parser.add_argument('--no-sheets', action='store_true',
                        help='Google Sheets 업데이트 비활성화')
    parser.add_argument('--debug', action='store_true',
                        help='디버그 모드 활성화')
    
    args = parser.parse_args()
    
    # 디버그 모드 설정
    if args.debug:
        logging.getLogger('msit_monitor').setLevel(logging.DEBUG)
        logger.info("디버그 모드 활성화됨")
    
    # 모니터링 실행
    days_range = args.days
    check_sheets = not args.no_sheets
    
    logger.info(f"모니터링 시작: days_range={days_range}, check_sheets={check_sheets}")
    
    try:
        all_posts, telecom_posts, data_updates = await run_monitor(
            days_range=days_range,
            check_sheets=check_sheets
        )
        
        # 결과 요약 출력
        logger.info("=" * 40)
        logger.info(f"모니터링 완료 결과:")
        logger.info(f"  - 총 게시물: {len(all_posts)}개")
        logger.info(f"  - 통신 통계 게시물: {len(telecom_posts)}개")
        logger.info(f"  - 데이터 업데이트: {len(data_updates)}개")
        logger.info("=" * 40)
        
        return 0  # 성공 코드
    except Exception as e:
        logger.error(f"메인 함수 실행 중 오류: {str(e)}")
        return 1  # 오류 코드


if __name__ == "__main__":
    import asyncio
    
    # 비동기 메인 함수 실행
    try:
        exit_code = asyncio.run(main())
        sys.exit(exit_code)
    except KeyboardInterrupt:
        logger.info("사용자에 의해 중단됨")
        sys.exit(130)  # 사용자 중단 코드
    except Exception as e:
        logger.critical(f"치명적 오류: {str(e)}")
        sys.exit(1)  # 오류 코드
