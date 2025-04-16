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
    'spreadsheet_name': os.environ.get('SPREADSHEET_NAME', 'MSIT 통신 통계')
}

# 임시 디렉토리 설정
TEMP_DIR = Path("./downloads")
TEMP_DIR.mkdir(exist_ok=True)



def setup_driver():
    """Selenium WebDriver 설정 (자동화 감지 회피 강화)"""
    options = Options()
    
    # 기존 옵션들 (유지)
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-gpu')
    options.add_argument('--window-size=1920,1080')
    options.add_argument('--disable-extensions')
    options.add_argument('--disable-popup-blocking')
    options.add_argument('--disable-web-security')
    options.add_argument('--blink-settings=imagesEnabled=true')
    
    # 랜덤 User-Agent
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.71 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.1 Safari/605.1.15",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:96.0) Gecko/20100101 Firefox/96.0"
    ]
    selected_ua = random.choice(user_agents)
    options.add_argument(f"user-agent={selected_ua}")
    logger.info(f"선택된 User-Agent: {selected_ua}")
    
    # 자동화 감지 우회 설정
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)
    
    # 무작위 사용자 데이터 디렉토리
    temp_user_data_dir = f"/tmp/chrome-user-data-{int(time.time())}-{random.randint(1000, 9999)}"
    options.add_argument(f'--user-data-dir={temp_user_data_dir}')
    logger.info(f"임시 사용자 데이터 디렉토리 생성: {temp_user_data_dir}")
    
    try:
        # WebDriver 설정
        from webdriver_manager.chrome import ChromeDriverManager
        service = Service(ChromeDriverManager().install())
        logger.info("ChromeDriverManager를 통한 드라이버 설치 완료")
    except Exception as e:
        # 기본 경로 사용
        if os.path.exists('/usr/bin/chromedriver'):
            service = Service('/usr/bin/chromedriver')
            logger.info("기본 경로 chromedriver 사용")
        else:
            raise Exception("ChromeDriver를 찾을 수 없습니다")
    
    # 드라이버 생성
    driver = webdriver.Chrome(service=service, options=options)
    
    # 페이지 로드 타임아웃 증가
    driver.set_page_load_timeout(90)
    
    # CDP를 사용하여 자동화 감지 우회 스크립트 주입 (더 안정적인 방법)
    try:
        driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
            "source": """
                // 자동화 감지 회피 스크립트
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined
                });
                
                // 권한 쿼리 감지 회피
                const originalPermissionsQuery = window.navigator.permissions?.query;
                if (originalPermissionsQuery) {
                    window.navigator.permissions.query = (parameters) => {
                        if (parameters.name === 'notifications' || parameters.name === 'clipboard-read') {
                            return Promise.resolve({state: "prompt", onchange: null});
                        }
                        return originalPermissionsQuery(parameters);
                    };
                }
                
                // 스크립트 감지 피하기
                Object.defineProperty(navigator, 'plugins', {
                    get: () => [1, 2, 3, 4, 5]
                });
                
                // 쿠키 관련 함수 조작 방지
                Object.defineProperty(document, 'cookie', {
                    get: function() {
                        return "__cookieDetectionDefense=true; " + document.__originalCookie;
                    },
                    set: function(val) {
                        document.__originalCookie = val;
                        return val;
                    }
                });
                
                
                // 언어 설정
                Object.defineProperty(navigator, 'languages', {
                    get: () => ['ko-KR', 'ko', 'en-US', 'en']
                });
                
                // 하드웨어 정보 숨기기
                Object.defineProperty(navigator, 'deviceMemory', {
                    get: () => 8
                });
                
                // 스크린 정보 무작위화
                const screenDetails = {
                    width: Math.floor(Math.random() * 200) + 1366,
                    height: Math.floor(Math.random() * 100) + 768,
                    colorDepth: 24,
                    pixelDepth: 24
                };
                
                Object.defineProperty(screen, 'width', { get: () => screenDetails.width });
                Object.defineProperty(screen, 'height', { get: () => screenDetails.height });
                Object.defineProperty(screen, 'colorDepth', { get: () => screenDetails.colorDepth });
                Object.defineProperty(screen, 'pixelDepth', { get: () => screenDetails.pixelDepth });
                
                // Chrome 객체 숨기기
                window.chrome = {
                    runtime: {},
                    loadTimes: function() {},
                    csi: function() {},
                    app: {}
                };
            """
        })
        logger.info("CDP를 통한 자동화 감지 회피 스크립트 주입 완료")
    except Exception as cdp_err:
        logger.warning(f"CDP 스크립트 주입 중 오류: {str(cdp_err)}")
        
        # 대체 방법: 일반 JavaScript 실행
        try:
            stealth_script = """
                Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
            """
            driver.execute_script(stealth_script)
            logger.info("기본 JavaScript로 webdriver 속성 재정의 완료")
        except Exception as js_err:
            logger.warning(f"JavaScript 실행 중 오류: {str(js_err)}")
    
    # 실행 환경에 따라 selenium-stealth 적용 (있는 경우)
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

def take_screenshot(driver, name):
    """특정 페이지의 스크린샷 저장"""
    try:
        screenshots_dir = Path("./screenshots")
        screenshots_dir.mkdir(exist_ok=True)
        
        screenshot_path = f"screenshots/{name}_{int(time.time())}.png"
        driver.save_screenshot(screenshot_path)
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

##############################
def find_view_link_params(driver, post):
    """게시물에서 바로보기 링크 파라미터 찾기 (개선된 버전)"""
    if not post.get('post_id'):
        logger.error(f"게시물 접근 불가 {post['title']} - post_id 누락")
        return None
    
    logger.info(f"게시물 열기: {post['title']}")
    
    # 게시물 상세 페이지 직접 접근
    detail_url = f"https://www.msit.go.kr/bbs/view.do?sCode=user&mId=99&mPid=74&nttSeqNo={post['post_id']}"
    logger.info(f"게시물 상세 페이지 접근: {detail_url}")
    
    # 최대 재시도 횟수
    max_retries = 3
    for attempt in range(max_retries):
        try:
            # 페이지 로드
            driver.get(detail_url)
            
            # 페이지 로드 대기
            try:
                WebDriverWait(driver, 15).until(
                    EC.presence_of_element_located((By.CLASS_NAME, "view_head"))
                )
                logger.info("게시물 상세 페이지 로드 완료")
            except TimeoutException:
                logger.warning("게시물 상세 페이지 로드 시간 초과")
            
            # 스크린샷 저장 (디버깅용)
            take_screenshot(driver, f"post_view_{post['post_id']}")
            
            # 바로보기 링크 찾기 (우선순위대로 시도)
            view_link = None
            
            # 전략 1: getExtension_path 함수를 사용하는 링크 찾기
            links = driver.find_elements(By.XPATH, "//a[contains(@onclick, 'getExtension_path')]")
            if links:
                view_link = links[0]
                onclick_attr = view_link.get_attribute('onclick')
                logger.info(f"바로보기 링크 발견 (getExtension_path): {onclick_attr}")
                
                # 파라미터 추출
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
            
            # 전략 2: view 클래스나 바로보기 텍스트를 포함하는 링크 찾기
            if not view_link:
                xpath_patterns = [
                    "//a[contains(@class, 'view')]",
                    "//a[contains(text(), '바로보기')]",
                    "//a[@title='새창 열림']",
                    "//a[contains(@href, 'documentView.do')]"
                ]
                
                for xpath in xpath_patterns:
                    links = driver.find_elements(By.XPATH, xpath)
                    if links:
                        view_link = links[0]
                        href_attr = view_link.get_attribute('href')
                        onclick_attr = view_link.get_attribute('onclick')
                        logger.info(f"바로보기 링크 발견 (패턴 '{xpath}'): href={href_attr}, onclick={onclick_attr}")
                        
                        # onclick 속성에서 파라미터 추출 시도
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
                        
                        # href 속성에서 파라미터 추출 시도
                        if href_attr and 'documentView.do' in href_attr:
                            match = re.search(r"atchFileNo=(\d+)&fileOrdr=(\d+)", href_attr)
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
                        
                        # 직접 다운로드 URL인 경우
                        if href_attr and any(ext in href_attr.lower() for ext in ['.xls', '.xlsx', '.csv', '.pdf', '.hwp']):
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
                            
                            return {
                                'download_url': href_attr,
                                'post_info': post
                            }
                        
                        break
            
            # 전략 3: 첨부파일 영역에서 첫 번째 파일 링크 찾기
            if not view_link:
                attachment_sections = driver.find_elements(By.XPATH, "//div[contains(@class, 'view_file')] | //div[contains(@class, 'attach')]")
                if attachment_sections:
                    file_links = attachment_sections[0].find_elements(By.TAG_NAME, "a")
                    if file_links:
                        view_link = file_links[0]
                        href_attr = view_link.get_attribute('href')
                        onclick_attr = view_link.get_attribute('onclick')
                        logger.info(f"첨부파일 영역에서 링크 발견: href={href_attr}, onclick={onclick_attr}")
                        
                        # onclick 속성 처리
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
            
            # 바로보기 링크를 찾지 못한 경우
            if not view_link:
                logger.warning(f"바로보기 링크를 찾을 수 없음: {post['title']}")
                
                # AJAX를 통한 첨부파일 정보 가져오기 시도
                try:
                    file_info = driver.execute_script("""
                        return new Promise((resolve, reject) => {
                            const xhr = new XMLHttpRequest();
                            xhr.open('GET', '/bbs/api/getAttachmentList.do?nttSeqNo=""" + post['post_id'] + """', true);
                            xhr.onload = function() {
                                if (this.status >= 200 && this.status < 300) {
                                    resolve(xhr.responseText);
                                } else {
                                    reject(xhr.statusText);
                                }
                            };
                            xhr.onerror = function() {
                                reject(xhr.statusText);
                            };
                            xhr.send();
                        });
                    """)
                    
                    if file_info:
                        try:
                            file_data = json.loads(file_info)
                            if 'attachList' in file_data and file_data['attachList']:
                                attachment = file_data['attachList'][0]  # 첫 번째 첨부파일
                                atch_file_no = attachment.get('atchFileNo')
                                file_ord = attachment.get('fileOrdr', 1)
                                
                                if atch_file_no:
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
                        except json.JSONDecodeError:
                            logger.warning("AJAX 응답을 JSON으로 파싱할 수 없음")
                except Exception as ajax_err:
                    logger.warning(f"AJAX 요청 중 오류: {str(ajax_err)}")
                
                # 날짜 정보만 추출하여 반환
                date_match = re.search(r'\((\d{4})년\s*(\d{1,2})월말\s*기준\)', post['title'])
                if date_match:
                    year = int(date_match.group(1))
                    month = int(date_match.group(2))
                    
                    # 게시물 내용 추출
                    content_elements = driver.find_elements(By.CSS_SELECTOR, "div.view_cont, .bbs_content, .view_content")
                    content = ""
                    if content_elements:
                        content = content_elements[0].text
                    
                    return {
                        'content': content,
                        'date': {'year': year, 'month': month},
                        'post_info': post
                    }
                
                # 날짜 정보도 없는 경우
                return None
                
        except Exception as e:
            logger.error(f"게시물 {post['title']} 처리 중 오류: {str(e)}")
            
            if attempt < max_retries - 1:
                logger.info(f"재시도 {attempt+1}/{max_retries}...")
                time.sleep(3)  # 잠시 대기 후 재시도
            else:
                # 날짜 정보만 추출하여 반환
                date_match = re.search(r'\((\d{4})년\s*(\d{1,2})월말\s*기준\)', post['title'])
                if date_match:
                    year = int(date_match.group(1))
                    month = int(date_match.group(2))
                    
                    return {
                        'error': str(e),
                        'date': {'year': year, 'month': month},
                        'post_info': post
                    }
                return None
    
    # 모든 시도 실패
    return None
    
def direct_access_view_link_params(driver, post):
    """직접 URL 접근 방식으로 게시물에서 바로보기 링크 파라미터 찾기 (기존 방식)"""
    if not post.get('post_id'):
        logger.error(f"게시물 접근 불가 {post['title']} - post_id 누락")
        return None
    
    logger.info(f"직접 URL 접근 방식으로 게시물 열기: {post['title']}")
    
    # 최대 재시도 횟수
    max_retries = 5
    retry_delay = 3
    
    for attempt in range(max_retries):
        try:
            # 게시물 상세 페이지로 이동
            detail_url = f"https://www.msit.go.kr/bbs/view.do?sCode=user&mId=99&mPid=74&nttSeqNo={post['post_id']}"
            logger.info(f"게시물 URL 접근 시도 ({attempt+1}/{max_retries}): {detail_url}")
            
            # 요청 헤더 설정
            driver.execute_script("""
                Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
            """)
            
            # 직접 요청
            driver.get(detail_url)
            
            # 페이지 로드 대기 시간 증가
            time.sleep(retry_delay + attempt * 2)  # 점진적으로 대기 시간 증가

            # 스크린샷 저장
            take_screenshot(driver, f"post_view_{post['post_id']}_attempt_{attempt}")
            
            # 오류 페이지 감지 (명확한 감지)
            if "시스템 점검 안내" in driver.page_source or "error-type" in driver.page_source or "error-wrap" in driver.page_source:
                logger.warning(f"오류 페이지 감지됨 (시도 {attempt+1}/{max_retries})")
                
                # HTML 스니펫 로깅
                html_snippet = driver.page_source[:500]
                logger.debug(f"오류 페이지 HTML 스니펫: {html_snippet}")
                
                if attempt < max_retries - 1:
                    # 대기 시간 증가 후 재시도
                    time.sleep(retry_delay * 2)
                    continue
                else:
                    # 마지막 시도에서 실패했으면 AJAX 방식 시도
                    ajax_result = try_ajax_access(driver, post)
                    if ajax_result:
                        return ajax_result
                        
                    # 날짜 정보 추출
                    date_match = re.search(r'\((\d{4})년\s*(\d{1,2})월말\s*기준\)', post['title'])
                    if date_match:
                        year = int(date_match.group(1))
                        month = int(date_match.group(2))
                        
                        return {
                            'content': "시스템 점검 또는 오류 페이지",
                            'date': {'year': year, 'month': month},
                            'post_info': post
                        }
                    return None
            
            # JavaScript 오류로 인한 로딩 문제 해결 시도
            try:
                # JavaScript 오류 처리 (일부 스크립트 오류 무시)
                driver.execute_script("""
                    window.onerror = function(message, source, lineno, colno, error) { 
                        console.log('JavaScript 오류 무시: ' + message);
                        return true; 
                    }
                """)
                
                # 일부 사이트에서 오버레이 또는 팝업 제거
                driver.execute_script("""
                    document.querySelectorAll('.overlay, .popup, .modal').forEach(e => e.remove());
                    document.querySelectorAll('div[class*="overlay"], div[class*="popup"], div[class*="modal"]').forEach(e => e.remove());
                """)
            except Exception as js_err:
                logger.warning(f"JavaScript 실행 중 오류: {str(js_err)}")
            
            # 다양한 요소 중 하나라도 로드되면 성공으로 간주
            wait_elements = [
                (By.CLASS_NAME, "view_head"),
                (By.CLASS_NAME, "view_file"),
                (By.CLASS_NAME, "view_cont"),
                (By.CSS_SELECTOR, ".bbs_wrap .view"),
                (By.XPATH, "//div[contains(@class, 'view')]")
            ]
            
            element_found = False
            for by_type, selector in wait_elements:
                try:
                    # 대기 시간 증가
                    element = WebDriverWait(driver, 15).until(
                        EC.presence_of_element_located((by_type, selector))
                    )
                    logger.info(f"페이지 로드 성공: {selector} 요소 발견")
                    element_found = True
                    break
                except TimeoutException:
                    continue
            
            # 디버깅용 스크린샷 저장
            try:
                screenshot_path = f"post_view_{post['post_id']}_loaded.png"
                driver.save_screenshot(screenshot_path)
                logger.info(f"게시물 페이지 스크린샷 저장: {screenshot_path}")
            except Exception as ss_err:
                logger.warning(f"스크린샷 저장 중 오류: {str(ss_err)}")
            
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


def access_iframe_direct(driver, file_params):
    """iframe에 직접 접근하여 데이터 추출"""
    if not file_params or not file_params.get('atch_file_no') or not file_params.get('file_ord'):
        logger.error("파일 파라미터가 없습니다.")
        return None
    
    atch_file_no = file_params['atch_file_no']
    file_ord = file_params['file_ord']
    
    # 바로보기 URL 구성
    view_url = f"https://www.msit.go.kr/bbs/documentView.do?atchFileNo={atch_file_no}&fileOrdr={file_ord}"
    logger.info(f"바로보기 URL: {view_url}")
    
    # 여러 번 재시도
    max_retries = 3
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
                
                # 시트 탭 찾기
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
                                tab.click()
                                time.sleep(3)  # 시트 전환 대기
                            except Exception as click_err:
                                logger.error(f"시트 탭 클릭 실패 ({sheet_name}): {str(click_err)}")
                                continue
                        
                        try:
                            # iframe 찾기
                            iframe = WebDriverWait(driver, 40).until(
                                EC.presence_of_element_located((By.ID, "innerWrap"))
                            )
                            
                            # iframe으로 전환
                            driver.switch_to.frame(iframe)
                            
                            # 페이지 소스 가져오기
                            iframe_html = driver.page_source
                            
                            # 테이블 추출
                            df = extract_table_from_html(iframe_html)
                            
                            # 기본 프레임으로 복귀
                            driver.switch_to.default_content()
                            
                            if df is not None and not df.empty:
                                all_sheets[sheet_name] = df
                                logger.info(f"시트 '{sheet_name}'에서 데이터 추출 성공: {df.shape[0]}행, {df.shape[1]}열")
                            else:
                                logger.warning(f"시트 '{sheet_name}'에서 테이블 추출 실패")
                        except Exception as iframe_err:
                            logger.error(f"시트 '{sheet_name}' 처리 중 오류: {str(iframe_err)}")
                            try:
                                # 오류 발생 시 기본 프레임으로 복귀
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
                            return None
                else:
                    logger.info("시트 탭 없음, 단일 iframe 처리 시도")
                    try:
                        iframe = WebDriverWait(driver, 40).until(
                            EC.presence_of_element_located((By.ID, "innerWrap"))
                        )
                        driver.switch_to.frame(iframe)
                        html_content = driver.page_source
                        df = extract_table_from_html(html_content)
                        driver.switch_to.default_content()
                        
                        if df is not None and not df.empty:
                            logger.info(f"단일 iframe에서 데이터 추출 성공: {df.shape[0]}행, {df.shape[1]}열")
                            return {"기본 시트": df}
                        else:
                            logger.warning("단일 iframe에서 테이블 추출 실패")
                            if attempt < max_retries - 1:
                                logger.info(f"재시도 중... ({attempt+1}/{max_retries})")
                                continue
                            else:
                                return None
                    except Exception as iframe_err:
                        logger.error(f"단일 iframe 처리 중 오류: {str(iframe_err)}")
                        try:
                            driver.switch_to.default_content()
                        except:
                            pass
                        
                        if attempt < max_retries - 1:
                            logger.info(f"재시도 중... ({attempt+1}/{max_retries})")
                            continue
                        else:
                            return None
            else:
                logger.info("SynapDocViewServer 미감지, 일반 HTML 페이지 처리")
                try:
                    # 현재 창 핸들 저장 (팝업이 있을 수 있음)
                    original_handle = driver.current_window_handle
                    
                    # 새 창이 열렸는지 확인
                    window_handles = driver.window_handles
                    if len(window_handles) > 1:
                        logger.info(f"새 창이 열렸습니다. 전환 시도...")
                        for handle in window_handles:
                            if handle != original_handle:
                                driver.switch_to.window(handle)
                                break
                    
                    # pandas의 read_html 사용
                    tables = pd.read_html(driver.page_source)
                    
                    if tables:
                        largest_table = max(tables, key=lambda t: t.size)
                        logger.info(f"가장 큰 테이블 선택: {largest_table.shape}")
                        return {"기본 테이블": largest_table}
                    else:
                        logger.warning("페이지에서 테이블을 찾을 수 없습니다.")
                        if attempt < max_retries - 1:
                            logger.info(f"재시도 중... ({attempt+1}/{max_retries})")
                            continue
                        else:
                            return None
                except Exception as table_err:
                    logger.error(f"HTML 테이블 추출 중 오류: {str(table_err)}")
                    if attempt < max_retries - 1:
                        logger.info(f"재시도 중... ({attempt+1}/{max_retries})")
                        continue
                    else:
                        return None
        
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
            else:
                return None
    
    return None

def extract_from_synap_viewer(driver, file_params):
    """향상된 Synap Document Viewer 데이터 추출 - 중첩된 iframe 처리"""
    view_url = f"https://www.msit.go.kr/bbs/documentView.do?atchFileNo={file_params['atch_file_no']}&fileOrdr={file_params['file_ord']}"
    logger.info(f"바로보기 URL 접근: {view_url}")
    
    driver.get(view_url)
    time.sleep(5)  # 리디렉션 대기
    
    # 현재 URL 확인 (리디렉션 후)
    current_url = driver.current_url
    logger.info(f"리디렉션 후 URL: {current_url}")
    
    if 'SynapDocViewServer' in current_url or 'doc.msit.go.kr' in current_url:
        logger.info("Synap Document Viewer 감지됨")
        
        # 시트 탭 대기
        try:
            WebDriverWait(driver, 20).until(
                EC.presence_of_element_located((By.CLASS_NAME, "sheet-list__sheet-tab"))
            )
            
            # 스크린샷 (디버깅용)
            driver.save_screenshot(f"synap_viewer_{file_params['atch_file_no']}.png")
            
            # 시트 탭 확인
            sheet_tabs = driver.find_elements(By.CLASS_NAME, "sheet-list__sheet-tab")
            logger.info(f"시트 탭 {len(sheet_tabs)}개 발견")
            
            all_data = {}
            
            for i, tab in enumerate(sheet_tabs):
                sheet_name = tab.text.strip() if tab.text.strip() else f"시트{i+1}"
                logger.info(f"시트 {i+1}/{len(sheet_tabs)} 처리: {sheet_name}")
                
                # 첫 번째가 아닌 시트는 클릭하여 활성화
                if i > 0:
                    try:
                        # JavaScript로 클릭 (더 안정적)
                        driver.execute_script("arguments[0].click();", tab)
                        logger.info(f"시트 탭 '{sheet_name}' 클릭")
                        time.sleep(3)  # 시트 전환 대기
                        
                        # 활성 탭 변경 확인
                        try:
                            active_tab = driver.find_element(By.CSS_SELECTOR, ".sheet-list__sheet-tab--on")
                            if active_tab.text.strip() != sheet_name:
                                logger.warning(f"시트 전환 확인 실패: {active_tab.text.strip()} != {sheet_name}")
                                # 재시도
                                driver.execute_script("arguments[0].click();", tab)
                                time.sleep(2)
                        except Exception as tab_err:
                            logger.warning(f"활성 탭 확인 중 오류: {str(tab_err)}")
                    except Exception as e:
                        logger.error(f"시트 전환 중 오류: {str(e)}")
                        continue
                
                # 외부 iframe 처리
                try:
                    # 외부 iframe 식별
                    outer_iframe = driver.find_element(By.ID, "innerWrap")
                    outer_iframe_src = outer_iframe.get_attribute('src')
                    logger.info(f"외부 iframe 소스: {outer_iframe_src}")
                    
                    # 외부 iframe으로 전환
                    driver.switch_to.frame(outer_iframe)
                    logger.info("외부 iframe으로 전환 완료")
                    
                    # 내부에 중첩된 iframe이 있는지 확인
                    inner_iframes = driver.find_elements(By.TAG_NAME, "iframe")
                    
                    if inner_iframes:
                        logger.info(f"{len(inner_iframes)}개의 내부 iframe 발견")
                        inner_iframe = inner_iframes[0]  # 첫 번째 내부 iframe 사용
                        
                        # 내부 iframe 소스 확인
                        inner_iframe_src = inner_iframe.get_attribute('src')
                        logger.info(f"내부 iframe 소스: {inner_iframe_src}")
                        
                        # 내부 iframe으로 전환
                        driver.switch_to.frame(inner_iframe)
                        logger.info("내부 iframe으로 전환 완료")
                        
                        # 현재 페이지 소스 가져오기 (내부 iframe)
                        inner_html = driver.page_source
                        
                        # BeautifulSoup으로 파싱
                        soup = BeautifulSoup(inner_html, 'html.parser')
                        
                        # 테이블 또는 div 그리드 찾기
                        tables = soup.find_all('table')
                        
                        if tables:
                            logger.info(f"{len(tables)}개 테이블 발견")
                            df = extract_table_from_html_element(tables[0])  # 첫 번째 테이블 처리
                            if df is not None and not df.empty:
                                all_data[sheet_name] = df
                                logger.info(f"시트 '{sheet_name}'에서 테이블 추출 성공: {df.shape}")
                        else:
                            # div 기반 그리드 찾기
                            grid_divs = soup.find_all('div', class_=lambda c: c and ('grid' in c.lower() or 'table' in c.lower()))
                            if grid_divs:
                                logger.info(f"{len(grid_divs)}개 그리드 div 발견")
                                df = extract_data_from_div_grid(grid_divs[0])
                                if df is not None and not df.empty:
                                    all_data[sheet_name] = df
                                    logger.info(f"시트 '{sheet_name}'에서 div 그리드 추출 성공: {df.shape}")
                            else:
                                # 구조화된 div 찾기
                                content_divs = soup.find_all('div', class_=lambda c: c and ('content' in c.lower() or 'data' in c.lower()))
                                if content_divs:
                                    logger.info(f"{len(content_divs)}개 콘텐츠 div 발견")
                                    df = extract_structured_data_from_divs(content_divs[0])
                                    if df is not None and not df.empty:
                                        all_data[sheet_name] = df
                                        logger.info(f"시트 '{sheet_name}'에서 구조화된 데이터 추출 성공: {df.shape}")
                                else:
                                    logger.warning(f"시트 '{sheet_name}'에서 테이블/그리드 요소를 찾을 수 없음")
                        
                        # 내부 iframe에서 빠져나오기
                        driver.switch_to.default_content()
                        driver.switch_to.frame(outer_iframe)  # 다시 외부 iframe으로
                    else:
                        # 내부 iframe이 없는 경우 - 현재 iframe에서 직접 처리
                        logger.info("내부 iframe이 없음, 외부 iframe에서 직접 처리")
                        
                        # 현재 iframe 소스 가져오기
                        iframe_html = driver.page_source
                        
                        # 데이터 추출 시도
                        tables = pd.read_html(iframe_html)
                        if tables:
                            largest_table = max(tables, key=lambda t: t.shape[0] * t.shape[1])
                            all_data[sheet_name] = largest_table
                            logger.info(f"외부 iframe에서 테이블 추출 성공: {largest_table.shape}")
                        else:
                            # BeautifulSoup으로 시도
                            soup = BeautifulSoup(iframe_html, 'html.parser')
                            tables = soup.find_all('table')
                            
                            if tables:
                                df = extract_table_from_html_element(tables[0])
                                if df is not None and not df.empty:
                                    all_data[sheet_name] = df
                                    logger.info(f"외부 iframe BeautifulSoup 테이블 추출 성공: {df.shape}")
                            else:
                                # div 그리드 시도
                                grid_divs = soup.find_all('div', class_=lambda c: c and ('grid' in c.lower() or 'table' in c.lower()))
                                if grid_divs:
                                    df = extract_data_from_div_grid(grid_divs[0])
                                    if df is not None and not df.empty:
                                        all_data[sheet_name] = df
                                        logger.info(f"외부 iframe div 그리드 추출 성공: {df.shape}")
                                else:
                                    logger.warning("외부 iframe에서 테이블/그리드 요소를 찾을 수 없음")
                    
                    # iframe에서 빠져나오기
                    driver.switch_to.default_content()
                    
                except Exception as iframe_err:
                    logger.error(f"iframe 처리 중 오류: {str(iframe_err)}")
                    # 기본 프레임으로 복귀 시도
                    try:
                        driver.switch_to.default_content()
                    except:
                        pass
            
            # 결과 반환
            if all_data:
                logger.info(f"{len(all_data)}개 시트에서 데이터 추출 완료")
                return all_data
            else:
                logger.warning("모든 시트에서 데이터 추출 실패")
                
                # 마지막 시도: 전체 페이지 스크린샷을 이용한 OCR 제안
                logger.info("스크린샷을 이용한 OCR 데이터 추출을 고려해 보세요")
                return None
            
        except Exception as e:
            logger.error(f"Synap Document Viewer 처리 중 오류: {str(e)}")
            return None
    else:
        # 직접 파일 다운로드 시도
        return try_direct_file_download(file_params)

def extract_structured_data_from_divs(parent_div):
    """
    구조화된 div 컨테이너에서 테이블 형식의 데이터 추출
    """
    try:
        # 후보 행 요소들 찾기
        row_candidates = [
            parent_div.find_all('div', class_=lambda c: c and ('row' in c.lower())),
            parent_div.find_all('div', style=lambda s: s and ('display: flex' in s.lower() or 'display:flex' in s.lower())),
            parent_div.find_all('div', recursive=False)  # 최상위 자식 div들
        ]
        
        # 가장 많은 요소를 가진 후보 선택
        row_elements = max(row_candidates, key=len) if row_candidates else []
        
        if not row_elements:
            logger.warning("구조화된 행 요소를 찾을 수 없음")
            return None
        
        logger.info(f"{len(row_elements)}개 행 요소 감지")
        
        # 첫 번째 행이 헤더인지 확인
        first_row = row_elements[0]
        header_candidates = [
            first_row.find_all('div', class_=lambda c: c and ('header' in c.lower() or 'head' in c.lower() or 'title' in c.lower())),
            first_row.find_all('div', style=lambda s: s and ('font-weight: bold' in s.lower() or 'font-weight:bold' in s.lower())),
            first_row.find_all('div', recursive=False)  # 첫 번째 행의 직계 자식들
        ]
        
        # 가장 많은 요소를 가진 헤더 후보 선택
        header_elements = max(header_candidates, key=len) if header_candidates else []
        
        # 헤더 텍스트 추출
        headers = []
        if header_elements:
            headers = [he.get_text(strip=True) for he in header_elements]
            # 빈 헤더 처리
            headers = [f"Column_{i+1}" if not h else h for i, h in enumerate(headers)]
        
        # 데이터 행 처리 (첫 번째 행이 헤더인 경우 건너뛰기)
        data_rows = []
        for row_idx, row in enumerate(row_elements):
            if row_idx == 0 and header_elements:
                continue  # 헤더 행 건너뛰기
            
            # 셀 요소 찾기
            cell_candidates = [
                row.find_all('div', class_=lambda c: c and ('cell' in c.lower() or 'col' in c.lower())),
                row.find_all('span', recursive=False),
                row.find_all('div', recursive=False)  # 행의 직계 자식들
            ]
            
            # 가장 많은 요소를 가진 셀 후보 선택
            cell_elements = max(cell_candidates, key=len) if cell_candidates else []
            
            if cell_elements:
                row_data = [ce.get_text(strip=True) for ce in cell_elements]
                data_rows.append(row_data)
        
        if not data_rows:
            logger.warning("데이터 행을 추출할 수 없음")
            return None
        
        # 모든 행의 최대 길이 확인
        max_cols = max([len(row) for row in data_rows])
        
        # 헤더가 없거나 길이가 맞지 않는 경우 생성
        if not headers or len(headers) != max_cols:
            headers = [f"Column_{i+1}" for i in range(max_cols)]
        
        # 행 길이 정규화
        normalized_rows = []
        for row in data_rows:
            if len(row) < max_cols:
                normalized_rows.append(row + [''] * (max_cols - len(row)))
            else:
                normalized_rows.append(row[:max_cols])
        
        # DataFrame 생성
        df = pd.DataFrame(normalized_rows, columns=headers)
        
        # 빈 행/열 제거
        df = df.loc[:, ~df.isna().all() & ~(df == '').all()]
        df = df.loc[~df.isna().all(axis=1) & ~(df == '').all(axis=1)]
        
        return df
        
    except Exception as e:
        logger.error(f"구조화된 div에서 데이터 추출 중 오류: {str(e)}")
        return None


def extract_table_from_html_element(table_element):
    """
    BeautifulSoup 테이블 요소에서 DataFrame 추출
    colspan, rowspan 속성 처리 포함
    """
    rows = []
    headers = []
    
    # 헤더 행 처리
    thead = table_element.find('thead')
    if thead:
        th_elements = thead.find_all('th')
        headers = [th.get_text(strip=True) for th in th_elements]
    
    # 헤더가 없으면 첫 번째 행을 헤더로 시도
    if not headers:
        first_row = table_element.find('tr')
        if first_row:
            header_cells = first_row.find_all(['th', 'td'])
            headers = [cell.get_text(strip=True) for cell in header_cells]
            
            # 숫자로만 이루어진 헤더나 빈 헤더는 열 인덱스로 대체
            for i, header in enumerate(headers):
                if header.isdigit() or not header:
                    headers[i] = f"Column_{i+1}"
    
    # 데이터 행 처리
    # 첫 번째 행이 헤더인 경우 건너뛰기
    rows_to_process = table_element.find_all('tr')[1:] if headers else table_element.find_all('tr')
    
    # 행/열 병합 처리를 위한 그리드 추적
    grid = {}  # (row, col) -> value
    
    for row_idx, row in enumerate(rows_to_process):
        cells = row.find_all(['td', 'th'])
        row_data = []
        col_idx = 0
        
        # 이전 행에서 rowspan으로 확장된 셀 처리
        while (row_idx, col_idx) in grid:
            row_data.append(grid[(row_idx, col_idx)])
            col_idx += 1
            
        for cell in cells:
            # 현재 열에 이미 값이 있으면 다음 열로 이동
            while (row_idx, col_idx) in grid:
                col_idx += 1
            
            # 셀 값 추출
            cell_value = cell.get_text(strip=True)
            
            # colspan 및 rowspan 속성 처리
            colspan = int(cell.get('colspan', 1))
            rowspan = int(cell.get('rowspan', 1))
            
            # 현재 셀 추가
            row_data.append(cell_value)
            
            # rowspan이 있는 경우 다음 행에 값 추가
            if rowspan > 1:
                for r in range(1, rowspan):
                    grid[(row_idx + r, col_idx)] = cell_value
            
            # colspan이 있는 경우 현재 행에 값 추가
            if colspan > 1:
                for c in range(1, colspan):
                    row_data.append(cell_value)
                    
                    # rowspan과 colspan이 모두 있는 경우
                    if rowspan > 1:
                        for r in range(1, rowspan):
                            grid[(row_idx + r, col_idx + c)] = cell_value
            
            col_idx += colspan
        
        # 빈 행 무시
        if any(cell for cell in row_data):
            rows.append(row_data)
    
    # 열 수 정규화 (모든 행이 같은 열 수를 갖도록)
    max_cols = max([len(row) for row in rows]) if rows else 0
    if headers:
        # 헤더가 부족하면 추가
        if len(headers) < max_cols:
            for i in range(len(headers), max_cols):
                headers.append(f"Column_{i+1}")
        # 헤더가 너무 많으면 자르기
        elif len(headers) > max_cols:
            headers = headers[:max_cols]
    else:
        # 헤더가 없으면 생성
        headers = [f"Column_{i+1}" for i in range(max_cols)]
    
    # 행 길이 정규화
    normalized_rows = []
    for row in rows:
        if len(row) < max_cols:
            normalized_rows.append(row + [''] * (max_cols - len(row)))
        elif len(row) > max_cols:
            normalized_rows.append(row[:max_cols])
        else:
            normalized_rows.append(row)
    
    # DataFrame 생성
    df = pd.DataFrame(normalized_rows, columns=headers)
    
    # 데이터 전처리
    # 빈 열 제거
    df = df.loc[:, ~df.isna().all() & ~(df == '').all()]
    
    # 빈 행 제거
    df = df.loc[~df.isna().all(axis=1) & ~(df == '').all(axis=1)]
    
    # 인덱스 재설정
    df = df.reset_index(drop=True)
    
    return df


def extract_data_from_div_grid(div_element):
    """
    div 기반 그리드에서 테이블 데이터 추출
    (테이블 태그를 사용하지 않는 경우)
    """
    try:
        # 행 역할을 하는 div 요소 찾기
        row_divs = div_element.find_all('div', recursive=False)
        if not row_divs:
            # 직계 자식이 없는 경우 더 깊게 탐색
            row_divs = div_element.find_all('div', class_=lambda c: c and ('row' in c.lower() or 'tr' in c.lower()))
        
        if not row_divs:
            logger.warning("div 그리드에서 행을 찾을 수 없음")
            return None
        
        # 데이터 수집
        table_data = []
        
        for row_div in row_divs:
            # 열 역할을 하는 자식 요소 찾기
            cell_divs = row_div.find_all(['div', 'span'], recursive=False)
            if not cell_divs:
                # 직계 자식이 없는 경우 더 깊게 탐색
                cell_divs = row_div.find_all(['div', 'span'], class_=lambda c: c and ('cell' in c.lower() or 'td' in c.lower()))
            
            row_data = [cell.get_text(strip=True) for cell in cell_divs]
            if row_data:
                table_data.append(row_data)
        
        if not table_data:
            logger.warning("div 그리드에서 데이터를 추출할 수 없음")
            return None
        
        # 헤더와 데이터 분리
        headers = table_data[0]
        data = table_data[1:]
        
        # 헤더 정규화
        for i, header in enumerate(headers):
            if not header:
                headers[i] = f"Column_{i+1}"
        
        # DataFrame 생성
        df = pd.DataFrame(data, columns=headers)
        
        # 데이터 전처리
        # 빈 열 제거
        df = df.loc[:, ~df.isna().all() & ~(df == '').all()]
        
        # 빈 행 제거
        df = df.loc[~df.isna().all(axis=1) & ~(df == '').all(axis=1)]
        
        # 인덱스 재설정
        df = df.reset_index(drop=True)
        
        return df
        
    except Exception as e:
        logger.error(f"div 그리드 데이터 추출 중 오류: {str(e)}")
        return None

def try_direct_file_download(file_params):
    """
    직접 파일 다운로드 시도 (Synap Document Viewer 실패 시)
    """
    try:
        # 직접 다운로드 URL 구성
        download_url = f"https://www.msit.go.kr/bbs/fileDown.do?atchFileNo={file_params['atch_file_no']}&fileOrdr={file_params['file_ord']}"
        logger.info(f"직접 다운로드 시도: {download_url}")
        
        # Session 객체 생성 (쿠키 유지)
        session = requests.Session()
        
        # User-Agent 및 Referer 설정
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36',
            'Referer': 'https://www.msit.go.kr/'
        }
        
        # 다운로드 요청
        response = session.get(download_url, headers=headers, stream=True, timeout=30)
        
        if response.status_code != 200:
            logger.error(f"파일 다운로드 실패, 상태 코드: {response.status_code}")
            return None
        
        # Content-Type 및 Content-Disposition 확인
        content_type = response.headers.get('Content-Type', '')
        content_disposition = response.headers.get('Content-Disposition', '')
        
        logger.info(f"다운로드 Content-Type: {content_type}")
        logger.info(f"Content-Disposition: {content_disposition}")
        
        # 파일명 추출 (Content-Disposition에서)
        filename = None
        if content_disposition:
            filename_match = re.search(r'filename[^;=\n]*=(([\'"]).*?\2|[^;\n]*)', content_disposition)
            if filename_match:
                filename = filename_match.group(1).strip('"\'')
                # URL 인코딩 처리
                filename = urllib.parse.unquote(filename)
                logger.info(f"파일명 추출: {filename}")
        
        # 파일 확장자 결정
        file_ext = '.tmp'
        if filename:
            _, ext = os.path.splitext(filename)
            if ext:
                file_ext = ext
        elif 'excel' in content_type.lower() or 'spreadsheet' in content_type.lower():
            file_ext = '.xlsx'
        elif 'csv' in content_type.lower():
            file_ext = '.csv'
        elif 'text/plain' in content_type.lower():
            file_ext = '.txt'
        
        logger.info(f"파일 확장자: {file_ext}")
        
        # 임시 파일 생성
        with tempfile.NamedTemporaryFile(delete=False, suffix=file_ext) as temp_file:
            temp_file_path = temp_file.name
            
            # 파일 저장
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    temp_file.write(chunk)
        
        logger.info(f"파일 다운로드 완료: {temp_file_path}")
        
        # 파일 형식에 따라 처리
        all_data = {}
        
        if file_ext.lower() in ['.xlsx', '.xls']:
            # Excel 파일 처리
            try:
                xl = pd.ExcelFile(temp_file_path)
                
                # 모든 시트 처리
                for sheet_name in xl.sheet_names:
                    df = pd.read_excel(xl, sheet_name=sheet_name)
                    
                    # 빈 시트 제외
                    if not df.empty:
                        all_data[sheet_name] = df
                        logger.info(f"시트 '{sheet_name}' 추출 완료: {df.shape}")
                
                if not all_data:
                    logger.warning("Excel 파일에서 유효한 데이터를 찾을 수 없음")
            except Exception as excel_err:
                logger.error(f"Excel 파일 처리 중 오류: {str(excel_err)}")
        
        elif file_ext.lower() == '.csv':
            # CSV 파일 처리
            try:
                # 인코딩 자동 감지
                import chardet
                with open(temp_file_path, 'rb') as f:
                    result = chardet.detect(f.read())
                encoding = result['encoding']
                
                # CSV 파일 읽기
                df = pd.read_csv(temp_file_path, encoding=encoding)
                
                # 파일명에서 시트명 추출 (확장자 제외)
                sheet_name = os.path.basename(filename).rsplit('.', 1)[0] if filename else "Sheet1"
                all_data[sheet_name] = df
                logger.info(f"CSV 시트 '{sheet_name}' 추출 완료: {df.shape}")
                
            except Exception as csv_err:
                logger.error(f"CSV 파일 처리 중 오류: {str(csv_err)}")
        
        # 임시 파일 삭제
        try:
            os.unlink(temp_file_path)
            logger.info(f"임시 파일 삭제 완료: {temp_file_path}")
        except Exception as unlink_err:
            logger.warning(f"임시 파일 삭제 실패: {str(unlink_err)}")
        
        if all_data:
            return all_data
        else:
            logger.warning("파일에서 데이터를 추출할 수 없음")
            return None
            
    except requests.exceptions.RequestException as req_err:
        logger.error(f"파일 다운로드 요청 중 오류: {str(req_err)}")
        return None
    except Exception as e:
        logger.error(f"파일 다운로드 처리 중 오류: {str(e)}")
        return None

def download_and_process_file(download_url):
    """
    Excel/CSV 파일 직접 다운로드 및 처리
    임시 파일 사용 및 예외 처리 강화
    """
    try:
        # 세션 생성 (쿠키 유지)
        session = requests.Session()
        
        # User-Agent 설정
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36',
            'Referer': 'https://www.msit.go.kr/',
        }
        
        # 다운로드 시도
        response = session.get(download_url, headers=headers, stream=True, timeout=30)
        
        if response.status_code != 200:
            logger.error(f"파일 다운로드 실패, 상태 코드: {response.status_code}")
            return None
        
        # Content-Type 확인
        content_type = response.headers.get('Content-Type', '')
        content_disposition = response.headers.get('Content-Disposition', '')
        
        logger.info(f"다운로드 Content-Type: {content_type}")
        logger.info(f"Content-Disposition: {content_disposition}")
        
        # 파일 확장자 결정
        file_ext = '.xlsx'  # 기본값
        if 'excel' in content_type or '.xls' in content_disposition:
            file_ext = '.xlsx'
        elif 'sheet' in content_type or '.csv' in content_disposition:
            file_ext = '.csv'
        elif 'hwp' in content_type or '.hwp' in content_disposition:
            logger.warning("한글 파일은 지원하지 않습니다")
            return None
        
        # 임시 파일 생성
        with tempfile.NamedTemporaryFile(delete=False, suffix=file_ext) as temp_file:
            temp_file_path = temp_file.name
            
            # 파일 저장
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    temp_file.write(chunk)
        
        logger.info(f"파일 다운로드 완료: {temp_file_path}")
        
        # 파일 처리
        all_data = {}
        
        if file_ext == '.csv':
            # CSV 파일 처리
            try:
                df = pd.read_csv(temp_file_path, encoding='utf-8')
                all_data['Sheet1'] = df
                logger.info(f"CSV 파일 처리 완료: {df.shape}")
            except UnicodeDecodeError:
                # 인코딩 자동 감지 시도
                try:
                    import chardet
                    with open(temp_file_path, 'rb') as f:
                        result = chardet.detect(f.read())
                    encoding = result['encoding']
                    df = pd.read_csv(temp_file_path, encoding=encoding)
                    all_data['Sheet1'] = df
                    logger.info(f"CSV 파일 처리 완료 (인코딩: {encoding}): {df.shape}")
                except Exception as e:
                    logger.error(f"CSV 파일 인코딩 감지 실패: {str(e)}")
        else:
            # Excel 파일 처리
            try:
                # 엑셀 파일 열기
                xl = pd.ExcelFile(temp_file_path)
                
                # 모든 시트 처리
                for sheet_name in xl.sheet_names:
                    df = pd.read_excel(xl, sheet_name=sheet_name)
                    
                    # 빈 시트 제외
                    if not df.empty:
                        all_data[sheet_name] = df
                        logger.info(f"시트 '{sheet_name}' 처리 완료: {df.shape}")
                
            except Exception as excel_err:
                logger.error(f"Excel 파일 처리 중 오류: {str(excel_err)}")
        
        # 임시 파일 삭제
        try:
            os.unlink(temp_file_path)
            logger.info(f"임시 파일 삭제 완료: {temp_file_path}")
        except Exception as unlink_err:
            logger.warning(f"임시 파일 삭제 실패: {str(unlink_err)}")
        
        if all_data:
            return all_data
        else:
            logger.warning("파일에서 데이터를 추출할 수 없습니다")
            return None
            
    except requests.exceptions.RequestException as req_err:
        logger.error(f"파일 다운로드 요청 중 오류: {str(req_err)}")
        return None
    except Exception as e:
        logger.error(f"파일 다운로드 및 처리 중 오류: {str(e)}")
        return None

def extract_table_from_html(html_content):
    """HTML 내용에서 테이블 추출 (colspan 및 rowspan 처리 포함)"""
    try:
        soup = BeautifulSoup(html_content, 'html.parser')
        tables = soup.find_all('table')
        
        if not tables:
            logger.warning("HTML에서 테이블을 찾을 수 없음")
            return None
        
        def parse_table(table):
            """테이블 파싱 함수 (행/열 병합 처리)"""
            table_data = []
            pending = {}  # 병합된 셀을 추적하기 위한 딕셔너리
            
            rows = table.find_all('tr')
            for row_idx, row in enumerate(rows):
                current_row = []
                col_idx = 0
                
                # 이전 행에서 병합된 셀 처리
                while (row_idx, col_idx) in pending:
                    current_row.append(pending[(row_idx, col_idx)])
                    del pending[(row_idx, col_idx)]
                    col_idx += 1
                
                # 현재 행의 셀 처리
                cells = row.find_all(['td', 'th'])
                for cell in cells:
                    # 이전 열에서 병합된 셀 처리
                    while (row_idx, col_idx) in pending:
                        current_row.append(pending[(row_idx, col_idx)])
                        del pending[(row_idx, col_idx)]
                        col_idx += 1
                    
                    # 셀 텍스트 가져오기
                    text = cell.get_text(strip=True)
                    
                    # colspan 및 rowspan 처리
                    try:
                        colspan = int(cell.get("colspan", 1))
                    except (ValueError, TypeError):
                        colspan = 1
                        
                    try:
                        rowspan = int(cell.get("rowspan", 1))
                    except (ValueError, TypeError):
                        rowspan = 1
                    
                    # 현재 셀의 데이터 추가 (colspan 고려)
                    for i in range(colspan):
                        current_row.append(text)
                        
                        # rowspan 처리
                        if rowspan > 1:
                            for r in range(1, rowspan):
                                # 병합된 행에 대한 데이터 저장
                                pending[(row_idx + r, col_idx)] = text
                        
                        col_idx += 1
                
                # 행의 끝에 남은 병합된 셀 처리
                while (row_idx, col_idx) in pending:
                    current_row.append(pending[(row_idx, col_idx)])
                    del pending[(row_idx, col_idx)]
                    col_idx += 1
                
                if current_row:  # 빈 행 제외
                    table_data.append(current_row)
            
            return table_data
        
        # 모든 테이블 파싱 및 가장 큰 테이블 선택
        parsed_tables = []
        for table in tables:
            data = parse_table(table)
            if data and len(data) >= 2:  # 헤더와 최소 1개의 데이터 행 필요
                parsed_tables.append((len(data), data))
        
        if not parsed_tables:
            logger.warning("전처리된 테이블 데이터가 충분하지 않음")
            return None
        
        # 행 수가 가장 많은 테이블 선택
        _, largest_table = max(parsed_tables, key=lambda x: x[0])
        
        if len(largest_table) < 2:
            logger.warning("테이블 데이터가 충분하지 않음")
            return None
        
        # 헤더 행과 데이터 행 준비
        header = largest_table[0]
        data_rows = []
        
        # 데이터 행 정규화 (열 개수 맞추기)
        for row in largest_table[1:]:
            # 헤더보다 열이 적은 경우 빈 값 추가
            if len(row) < len(header):
                row.extend([""] * (len(header) - len(row)))
            # 헤더보다 열이 많은 경우 초과 열 제거
            elif len(row) > len(header):
                row = row[:len(header)]
            
            data_rows.append(row)
        
        # 중복 헤더 처리
        unique_headers = []
        header_count = {}
        
        for h in header:
            if h in header_count:
                header_count[h] += 1
                unique_headers.append(f"{h}_{header_count[h]}")
            else:
                header_count[h] = 0
                unique_headers.append(h)
        
        # 데이터프레임 생성
        df = pd.DataFrame(data_rows, columns=unique_headers)
        
        # 빈 값 및 중복 처리
        df = df.fillna("")  # NaN 값을 빈 문자열로 변환
        
        # 공백 열 제거 (모든 값이 빈 문자열인 열)
        df = df.loc[:, ~(df == "").all()]
        
        # 중복 행 제거
        df = df.drop_duplicates().reset_index(drop=True)
        
        logger.info(f"테이블 추출 성공: {df.shape[0]}행 {df.shape[1]}열")
        return df
        
    except Exception as e:
        logger.error(f"HTML에서 테이블 추출 중 오류: {str(e)}")
        return None

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
    첨부파일의 형태를 유지하면서 Google Sheets 업데이트
    """
    if not client or not data:
        logger.error("Google Sheets 업데이트 불가: 클라이언트 또는 데이터 없음")
        return False
    
    try:
        # 정보 추출
        post_info = data['post_info']
        
        # 날짜 정보 직접 제공 또는 제목에서 추출
        if 'date' in data:
            year = data['date']['year']
            month = data['date']['month']
        else:
            # 제목에서 날짜 정보 추출
            date_match = re.search(r'\((\d{4})년\s*(\d{1,2})월말\s*기준\)', post_info['title'])
            if not date_match:
                logger.error(f"제목에서 날짜를 추출할 수 없음: {post_info['title']}")
                return False
                
            year = int(date_match.group(1))
            month = int(date_match.group(2))
        
        # 날짜 문자열 포맷
        date_str = f"{year}년 {month}월"
        
        # 스프레드시트 열기
        spreadsheet = None
        retry_count = 0
        max_retries = 3
        
        while retry_count < max_retries and not spreadsheet:
            try:
                # ID로 먼저 시도
                if CONFIG['spreadsheet_id']:
                    try:
                        spreadsheet = client.open_by_key(CONFIG['spreadsheet_id'])
                        logger.info(f"ID로 기존 스프레드시트 찾음: {CONFIG['spreadsheet_id']}")
                    except Exception as e:
                        logger.warning(f"ID로 스프레드시트를 열 수 없음: {CONFIG['spreadsheet_id']}, 오류: {str(e)}")
                
                # ID로 찾지 못한 경우 이름으로 시도
                if not spreadsheet:
                    try:
                        spreadsheet = client.open(CONFIG['spreadsheet_name'])
                        logger.info(f"이름으로 기존 스프레드시트 찾음: {CONFIG['spreadsheet_name']}")
                    except gspread.exceptions.SpreadsheetNotFound:
                        # 새 스프레드시트 생성
                        spreadsheet = client.create(CONFIG['spreadsheet_name'])
                        logger.info(f"새 스프레드시트 생성: {CONFIG['spreadsheet_name']}")
                        logger.info(f"새 스프레드시트 ID: {spreadsheet.id}")
                
            except gspread.exceptions.APIError as api_err:
                retry_count += 1
                logger.warning(f"Google API 오류 (시도 {retry_count}/{max_retries}): {str(api_err)}")
                
                if "RESOURCE_EXHAUSTED" in str(api_err) or "RATE_LIMIT_EXCEEDED" in str(api_err):
                    # 속도 제한 처리 (지수 백오프)
                    wait_time = 2 ** retry_count
                    logger.info(f"API 속도 제한 감지. {wait_time}초 대기 중...")
                    time.sleep(wait_time)
                elif retry_count >= max_retries:
                    logger.error(f"Google Sheets API 오류, 최대 재시도 횟수 초과: {str(api_err)}")
                    return False
            
            except Exception as e:
                logger.error(f"Google Sheets 열기 중 오류: {str(e)}")
                return False
        
        # 시트 데이터 처리
        if 'sheets' in data or all(isinstance(v, pd.DataFrame) for v in data.values() if not isinstance(v, str) and not isinstance(v, dict)):
            # sheets 키가 있거나 데이터프레임 값들이 있는 경우
            sheets_data = data.get('sheets', data)
            
            # post_info와 같은 비 DataFrame 항목 제외
            sheets_data = {k: v for k, v in sheets_data.items() if isinstance(v, pd.DataFrame)}
            
            # 모든 시트 업데이트
            success_count = 0
            for sheet_name, df in sheets_data.items():
                if df is not None and not df.empty:
                    # 첨부파일의 시트명 그대로 사용
                    success = update_sheet_with_raw_data(spreadsheet, sheet_name, df, date_str)
                    if success:
                        success_count += 1
                    else:
                        logger.warning(f"시트 '{sheet_name}' 업데이트 실패")
            
            logger.info(f"전체 {len(sheets_data)}개 시트 중 {success_count}개 업데이트 성공")
            return success_count > 0
        
        elif 'dataframe' in data:
            # 단일 데이터프레임인 경우
            sheet_name = post_info.get('title', '통신 통계')
            return update_sheet_with_raw_data(spreadsheet, sheet_name, data['dataframe'], date_str)
        
        else:
            logger.error("업데이트할 데이터가 없습니다")
            return False
            
    except Exception as e:
        logger.error(f"Google Sheets 업데이트 중 오류: {str(e)}")
        return False

def update_single_sheet(spreadsheet, sheet_name, df, date_str):
    """단일 시트 업데이트"""
    try:
        # API 속도 제한 방지를 위한 지연 시간
        time.sleep(1)
        
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
            time.sleep(1)  # API 속도 제한 방지
        
        # 날짜 열 확인
        headers = worksheet.row_values(1)
        
        # 빈 헤더 채우기
        if not headers or len(headers) == 0:
            worksheet.update_cell(1, 1, "항목")
            headers = ["항목"]
            time.sleep(1)  # API 속도 제한 방지
        
        if date_str in headers:
            col_idx = headers.index(date_str) + 1
            logger.info(f"'{date_str}' 열이 이미 위치 {col_idx}에 존재합니다")
        else:
            # 새 날짜 열 추가
            col_idx = len(headers) + 1
            worksheet.update_cell(1, col_idx, date_str)
            logger.info(f"위치 {col_idx}에 새 열 '{date_str}' 추가")
            time.sleep(1)  # API 속도 제한 방지
        
        # 데이터프레임 형식 검증 및 정리
        if df.shape[1] < 2:
            logger.warning(f"데이터프레임 열이 부족합니다: {df.shape[1]} 열. 최소 2열 필요")
            
            # 최소 열 추가
            if df.shape[1] == 1:
                col_name = df.columns[0]
                df['값'] = df[col_name]
            else:
                # 데이터프레임이 비어있는 경우
                df = pd.DataFrame({
                    '항목': ['데이터 없음'],
                    '값': ['업데이트 실패']
                })
        
        # 데이터프레임으로 시트 업데이트 (배치 처리)
        update_sheet_from_dataframe(worksheet, df, col_idx)
        
        logger.info(f"워크시트 '{sheet_name}'에 '{date_str}' 데이터 업데이트 완료")
        return True
        
    except gspread.exceptions.APIError as api_err:
        if "RESOURCE_EXHAUSTED" in str(api_err) or "RATE_LIMIT_EXCEEDED" in str(api_err):
            logger.warning(f"Google Sheets API 속도 제한 발생: {str(api_err)}")
            logger.info("대기 후 재시도 중...")
            time.sleep(5)  # 더 긴 대기 시간
            
            # 간소화된 방식으로 재시도
            try:
                # 워크시트 찾기 또는 생성
                try:
                    worksheet = spreadsheet.worksheet(sheet_name)
                except gspread.exceptions.WorksheetNotFound:
                    worksheet = spreadsheet.add_worksheet(title=sheet_name, rows="1000", cols="20")
                    worksheet.update_cell(1, 1, "항목")
                
                # 날짜 열 위치 결정 (단순화)
                headers = worksheet.row_values(1)
                if date_str in headers:
                    col_idx = headers.index(date_str) + 1
                else:
                    col_idx = len(headers) + 1
                    worksheet.update_cell(1, col_idx, date_str)
                
                # 최소한의 데이터만 업데이트
                if df.shape[0] > 0:
                    first_col_name = df.columns[0]
                    items = df[first_col_name].astype(str).tolist()[:10]  # 처음 10개 항목
                    values = ["업데이트 성공"] * len(items)
                    
                    for i, (item, value) in enumerate(zip(items, values)):
                        row_idx = i + 2  # 헤더 행 이후
                        worksheet.update_cell(row_idx, 1, item)
                        worksheet.update_cell(row_idx, col_idx, value)
                        time.sleep(1)  # 더 긴 지연 시간
                
                logger.info(f"제한된 데이터로 워크시트 '{sheet_name}' 업데이트 완료")
                return True
                
            except Exception as retry_err:
                logger.error(f"재시도 중 오류: {str(retry_err)}")
                return False
        else:
            logger.error(f"Google Sheets API 오류: {str(api_err)}")
            return False
            
    except Exception as e:
        logger.error(f"시트 '{sheet_name}' 업데이트 중 오류: {str(e)}")
        return False


def update_sheet_with_raw_data(spreadsheet, sheet_name, df, date_str):
    """
    원본 데이터 구조를 유지하면서 시트 업데이트
    """
    try:
        # API 속도 제한 방지를 위한 지연 시간
        time.sleep(1)
        
        # 시트명 정리 (특수 문자 제거)
        clean_sheet_name = re.sub(r'[\\/*[\]?:]', '', sheet_name)
        if not clean_sheet_name:
            clean_sheet_name = "Sheet1"
        elif len(clean_sheet_name) > 100:
            clean_sheet_name = clean_sheet_name[:97] + "..."
        
        # 워크시트 찾기 또는 생성
        try:
            worksheet = spreadsheet.worksheet(clean_sheet_name)
            logger.info(f"기존 워크시트 찾음: {clean_sheet_name}")
        except gspread.exceptions.WorksheetNotFound:
            # 새 워크시트 생성
            worksheet = spreadsheet.add_worksheet(title=clean_sheet_name, rows="1000", cols="50")
            logger.info(f"새 워크시트 생성: {clean_sheet_name}")
            
            # 헤더 추가
            worksheet.update_cell(1, 1, "참고")
            worksheet.update_cell(2, 1, f"{date_str} 데이터를 추출할 수 있습니다")
            time.sleep(1)  # API 속도 제한 방지
        
        # 데이터프레임 전처리
        df = df.fillna('')  # NaN 값을 빈 문자열로 변환
        
        # 헤더와 데이터 준비
        headers = df.columns.tolist()
        values = df.values.tolist()
        
        # 전체 업데이트 데이터 준비
        update_data = [headers]  # 첫 번째 행: 헤더
        update_data.extend(values)  # 나머지 행: 데이터
        
        # 행과 열 수 계산
        num_rows = len(update_data)
        num_cols = max(len(row) for row in update_data)
        
        # 업데이트할 셀 범위
        range_str = f"A1:{chr(65 + num_cols - 1)}{num_rows}"
        
        # 시트 초기화 (기존 데이터 지우기)
        try:
            worksheet.clear()
            logger.info(f"워크시트 '{clean_sheet_name}' 초기화 완료")
        except Exception as clear_err:
            logger.warning(f"워크시트 초기화 중 오류: {str(clear_err)}")
        
        # 시트 크기 조정 (필요한 경우)
        try:
            current_rows = worksheet.row_count
            current_cols = worksheet.col_count
            
            if num_rows > current_rows or num_cols > current_cols:
                # 필요한 행/열 추가
                new_rows = max(num_rows, current_rows)
                new_cols = max(num_cols, current_cols)
                worksheet.resize(rows=new_rows, cols=new_cols)
                logger.info(f"워크시트 크기 조정: {new_rows}행 x {new_cols}열")
        except Exception as resize_err:
            logger.warning(f"워크시트 크기 조정 중 오류: {str(resize_err)}")
        
        # 배치 업데이트 시도
        try:
            worksheet.update(range_str, update_data)
            logger.info(f"워크시트 '{clean_sheet_name}' 일괄 업데이트 완료: {len(update_data)}행 x {num_cols}열")
            
            # 날짜 정보 추가 (A1 셀에)
            try:
                worksheet.update_cell(1, 1, f"{date_str} {sheet_name}")
                logger.info(f"날짜 정보 추가 완료: {date_str}")
            except Exception as date_err:
                logger.warning(f"날짜 정보 추가 중 오류: {str(date_err)}")
            
            return True
        except gspread.exceptions.APIError as api_err:
            logger.warning(f"일괄 업데이트 중 API 오류: {str(api_err)}")
            
            # 속도 제한 또는 용량 초과 시 분할 업데이트 시도
            if "RESOURCE_EXHAUSTED" in str(api_err) or "RATE_LIMIT_EXCEEDED" in str(api_err):
                logger.info("분할 업데이트 시도 중...")
                
                # API 제한 대비 분할 업데이트
                batch_size = 100  # 한 번에 업데이트할 행 수
                success = True
                
                # 헤더 먼저 업데이트
                try:
                    header_range = f"A1:{chr(65 + num_cols - 1)}1"
                    worksheet.update(header_range, [headers])
                    logger.info("헤더 업데이트 완료")
                    time.sleep(2)  # API 속도 제한 방지
                except Exception as header_err:
                    logger.warning(f"헤더 업데이트 중 오류: {str(header_err)}")
                    success = False
                
                # 데이터 분할 업데이트
                for i in range(0, len(values), batch_size):
                    batch = values[i:i+batch_size]
                    start_row = i + 2  # 헤더 행(1) 이후부터 시작
                    end_row = start_row + len(batch) - 1
                    
                    batch_range = f"A{start_row}:{chr(65 + num_cols - 1)}{end_row}"
                    
                    try:
                        worksheet.update(batch_range, batch)
                        logger.info(f"배치 {i//batch_size + 1} 업데이트 완료: 행 {start_row}-{end_row}")
                        time.sleep(2)  # API 속도 제한 방지
                    except Exception as batch_err:
                        logger.error(f"배치 {i//batch_size + 1} 업데이트 중 오류: {str(batch_err)}")
                        success = False
                
                return success
            else:
                # 기타 API 오류
                logger.error(f"API 오류: {str(api_err)}")
                return False
        
    except Exception as e:
        logger.error(f"시트 업데이트 중 오류: {str(e)}")
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

def navigate_through_iframes(driver, target_frame_pattern=None):
    """
    중첩된 iframe 구조를 탐색하며 모든 iframe을 확인
    특정 패턴의 iframe을 찾으면 해당 iframe으로 이동
    
    :param driver: Selenium WebDriver 객체
    :param target_frame_pattern: 찾고자 하는 iframe 소스 URL의 패턴 (정규식 문자열)
    :return: (성공 여부, iframe 경로 리스트)
    """
    iframe_path = []  # iframe 경로 추적 (인덱스 저장)
    visited_frames = set()  # 이미 방문한 프레임 소스 URL
    
    def explore_iframe(depth=0, max_depth=5):
        """재귀적으로 iframe 탐색"""
        if depth >= max_depth:
            return False  # 최대 깊이 제한
        
        # 현재 프레임의 URL
        current_url = driver.current_url
        current_src = f"{current_url}#{depth}"
        
        if current_src in visited_frames:
            return False  # 이미 방문한 프레임
        
        visited_frames.add(current_src)
        logger.info(f"프레임 깊이 {depth}: URL {current_url}")
        
        # 현재 프레임에서 iframe 요소 찾기
        try:
            iframes = driver.find_elements(By.TAG_NAME, "iframe")
            logger.info(f"프레임 깊이 {depth}에서 {len(iframes)}개 iframe 발견")
            
            # 각 iframe 확인
            for i, iframe in enumerate(iframes):
                try:
                    iframe_src = iframe.get_attribute('src')
                    iframe_id = iframe.get_attribute('id') or f"iframe_{i}"
                    logger.info(f"  iframe {i}: id={iframe_id}, src={iframe_src}")
                    
                    # 대상 패턴에 매칭되는지 확인
                    if target_frame_pattern and iframe_src and re.search(target_frame_pattern, iframe_src):
                        logger.info(f"대상 패턴과 일치하는 iframe 발견: {iframe_src}")
                        driver.switch_to.frame(iframe)
                        iframe_path.append(i)
                        return True
                    
                    # 해당 iframe으로 이동
                    driver.switch_to.frame(iframe)
                    iframe_path.append(i)
                    
                    # 재귀적으로 해당 iframe 내부 탐색
                    if explore_iframe(depth + 1, max_depth):
                        return True  # 대상 찾음
                    
                    # 찾지 못했으면 부모 프레임으로 복귀
                    driver.switch_to.parent_frame()
                    iframe_path.pop()
                    
                except Exception as frame_err:
                    logger.warning(f"iframe {i} 탐색 중 오류: {str(frame_err)}")
                    # 오류 발생 시 부모 프레임으로 복귀 시도
                    try:
                        driver.switch_to.parent_frame()
                        if iframe_path:
                            iframe_path.pop()
                    except:
                        pass
            
            return False  # 모든 iframe 탐색했지만 찾지 못함
            
        except Exception as e:
            logger.error(f"iframe 탐색 중 오류: {str(e)}")
            return False
    
    # 기본 프레임에서 시작
    driver.switch_to.default_content()
    iframe_path = []
    
    # iframe 탐색 시작
    result = explore_iframe()
    
    return result, iframe_path

def enhance_stealth_with_cdp(driver):
    """
    Chrome DevTools Protocol을 사용하여 향상된 스텔스 기능 적용
    """
    try:
        # 탐지 방지 스크립트 주입
        driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
            "source": """
                // navigator.webdriver 속성 숨기기
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined
                });
                
                // 권한 감지 우회
                if (navigator.permissions) {
                    navigator.permissions.__proto__.query = navigator.permissions.__proto__.query || (() => {});
                    const originalQuery = navigator.permissions.__proto__.query;
                    navigator.permissions.__proto__.query = function() {
                        const promiseResult = Promise.resolve({state: "prompt", onchange: null});
                        promiseResult.state = "prompt";
                        return promiseResult;
                    };
                }
                
                // 브라우저 지문 감지 방지
                const originalGetParameter = WebGLRenderingContext.prototype.getParameter;
                WebGLRenderingContext.prototype.getParameter = function(parameter) {
                    // UNMASKED_RENDERER_WEBGL or UNMASKED_VENDOR_WEBGL
                    if (parameter === 37446) {
                        return "Intel Open Source Technology Center";
                    }
                    if (parameter === 37447) {
                        return "Mesa DRI Intel(R) Haswell Mobile";
                    }
                    return originalGetParameter.apply(this, arguments);
                };
                
                // Chrome 객체 정의
                window.chrome = {
                    app: {
                        isInstalled: false,
                        InstallState: {
                            DISABLED: 'disabled',
                            INSTALLED: 'installed',
                            NOT_INSTALLED: 'not_installed'
                        },
                        RunningState: {
                            CANNOT_RUN: 'cannot_run',
                            READY_TO_RUN: 'ready_to_run',
                            RUNNING: 'running'
                        }
                    },
                    runtime: {
                        OnInstalledReason: {
                            CHROME_UPDATE: 'chrome_update',
                            INSTALL: 'install',
                            SHARED_MODULE_UPDATE: 'shared_module_update',
                            UPDATE: 'update'
                        },
                        OnRestartRequiredReason: {
                            APP_UPDATE: 'app_update',
                            OS_UPDATE: 'os_update',
                            PERIODIC: 'periodic'
                        },
                        PlatformArch: {
                            ARM: 'arm',
                            ARM64: 'arm64',
                            MIPS: 'mips',
                            MIPS64: 'mips64',
                            X86_32: 'x86-32',
                            X86_64: 'x86-64'
                        },
                        PlatformNaclArch: {
                            ARM: 'arm',
                            MIPS: 'mips',
                            MIPS64: 'mips64',
                            X86_32: 'x86-32',
                            X86_64: 'x86-64'
                        },
                        PlatformOs: {
                            ANDROID: 'android',
                            CROS: 'cros',
                            LINUX: 'linux',
                            MAC: 'mac',
                            OPENBSD: 'openbsd',
                            WIN: 'win'
                        },
                        RequestUpdateCheckStatus: {
                            NO_UPDATE: 'no_update',
                            THROTTLED: 'throttled',
                            UPDATE_AVAILABLE: 'update_available'
                        }
                    }
                };
                
                // 언어 설정
                Object.defineProperty(navigator, 'language', {
                    get: function() {
                        return "ko-KR";
                    }
                });
                Object.defineProperty(navigator, 'languages', {
                    get: function() {
                        return ["ko-KR", "ko", "en-US", "en"];
                    }
                });
                
                // 플랫폼 설정
                Object.defineProperty(navigator, 'platform', {
                    get: function() {
                        return "Win32";
                    }
                });
            """
        })
        
        logger.info("CDP를 통한 향상된 스텔스 설정 적용 완료")
        return True
    except Exception as e:
        logger.error(f"CDP 스텔스 설정 중 오류: {str(e)}")
        return False


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

async def run_monitor(days_range=4, check_sheets=True):
    """모니터링 실행 (함수형 구현)"""
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
            
            # 웹드라이버 감지 회피
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
        
        # 랜딩 페이지 접속
        try:
            # 랜딩 페이지 접속
            landing_url = CONFIG['landing_url']
            driver.get(landing_url)
            
            # 페이지 로드 대기
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.ID, "skip_nav"))
            )
            logger.info("랜딩 페이지 접속 완료 - 쿠키 및 세션 정보 획득")
            
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
                        
                        # JavaScript로 클릭 (더 신뢰성 있음)
                        driver.execute_script("arguments[0].click();", stats_link)
                        
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
                    
                    # 바로보기 링크 파라미터 추출 (수정된 방식)
                    file_params = find_view_link_params(driver, post)
                    
                    if not file_params:
                        logger.warning(f"바로보기 링크 파라미터 추출 실패: {post['title']}")
                        continue
                    
                    # 바로보기 링크가 있는 경우
                    if 'atch_file_no' in file_params and 'file_ord' in file_params:
                        # iframe 직접 접근하여 데이터 추출
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
                        else:
                            logger.warning(f"iframe에서 데이터 추출 실패: {post['title']}")
                            
                            # 대체 데이터 생성
                            placeholder_df = create_placeholder_dataframe(post)
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
                    
                    # 게시물 내용만 있는 경우
                    elif 'content' in file_params:
                        logger.info(f"게시물 내용으로 처리 중: {post['title']}")
                        
                        # 대체 데이터 생성
                        placeholder_df = create_placeholder_dataframe(post)
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
                    
                    # AJAX 데이터가 있는 경우
                    elif 'ajax_data' in file_params:
                        logger.info(f"AJAX 데이터로 처리 중: {post['title']}")
                        
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
                            
                            success = update_google_sheets(gs_client, update_data)
                            if success:
                                logger.info(f"AJAX 데이터로 업데이트 성공: {post['title']}")
                                data_updates.append(update_data)
                    
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
                bot = telegram.Bot(token=CONFIG['telegram_token'])
                await bot.send_message(
                    chat_id=int(CONFIG['chat_id']),
                    text=f"📊 MSIT 통신 통계 모니터링: 최근 {days_range}일 내 새 게시물이 없습니다. ({datetime.now().strftime('%Y-%m-%d %H:%M')})"
                )
    
    except Exception as e:
        logger.error(f"모니터링 중 오류 발생: {str(e)}")
        
        try:
            # 오류 스크린샷 저장
            if driver:
                try:
                    driver.save_screenshot("error_screenshot.png")
                    logger.info("오류 발생 시점 스크린샷 저장 완료")
                except Exception as ss_err:
                    logger.error(f"오류 스크린샷 저장 실패: {str(ss_err)}")
            
            # 오류 알림 전송
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
        # 리소스 정리
        if driver:
            driver.quit()
            logger.info("WebDriver 종료")
        
        logger.info("=== MSIT 통신 통계 모니터링 종료 ===")


async def main():
    """메인 함수: 환경 변수 처리 및 모니터링 실행"""
    # 환경 변수 가져오기 (향상된 버전)
    try:
        days_range = int(os.environ.get('DAYS_RANGE', '4'))
    except ValueError:
        logger.warning("잘못된 DAYS_RANGE 형식. 기본값 4일 사용")
        days_range = 4
        
    check_sheets_str = os.environ.get('CHECK_SHEETS', 'true').lower()
    check_sheets = check_sheets_str in ('true', 'yes', '1', 'y')
    
    spreadsheet_name = os.environ.get('SPREADSHEET_NAME', 'MSIT 통신 통계')
    
    # 환경 설정 로그
    logger.info(f"MSIT 모니터 시작 - days_range={days_range}, check_sheets={check_sheets}")
    logger.info(f"스프레드시트 이름: {spreadsheet_name}")
    
    # 전역 설정 업데이트
    CONFIG['spreadsheet_name'] = spreadsheet_name
    
    # 모니터링 실행
    try:
        await run_monitor(days_range=days_range, check_sheets=check_sheets)
    except Exception as e:
        logging.error(f"메인 함수 오류: {str(e)}", exc_info=True)
        
        # 치명적 오류 시 텔레그램 알림 시도
        try:
            bot = telegram.Bot(token=CONFIG['telegram_token'])
            await bot.send_message(
                chat_id=int(CONFIG['chat_id']),
                text=f"⚠️ *MSIT 모니터링 치명적 오류*\n\n{str(e)}\n\n시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
                parse_mode='Markdown'
            )
        except Exception as telegram_err:
            logger.error(f"텔레그램 메시지 전송 중 추가 오류: {str(telegram_err)}")


if __name__ == "__main__":
    asyncio.run(main())
