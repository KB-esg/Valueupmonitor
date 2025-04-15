import os
import re    
import json
import time
import logging
import asyncio
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd

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

class MSITMonitor:
    def __init__(self):
        # 필수 환경 변수 검증
        required_vars = ['TELCO_NEWS_TOKEN', 'TELCO_NEWS_TESTER']
        missing_vars = [var for var in required_vars if not os.environ.get(var)]
        
        if missing_vars:
            raise ValueError(f"필수 환경 변수가 설정되지 않았습니다: {', '.join(missing_vars)}")
        
        # Telegram 설정
        self.telegram_token = os.environ.get('TELCO_NEWS_TOKEN')
        self.chat_id = os.environ.get('TELCO_NEWS_TESTER')
        
        # Google Sheets 설정
        self.gspread_creds = os.environ.get('MSIT_GSPREAD_ref')
        self.spreadsheet_id = os.environ.get('MSIT_SPREADSHEET_ID')
        self.spreadsheet_name = os.environ.get('SPREADSHEET_NAME', 'MSIT 통신 통계')
        
        if not self.gspread_creds:
            logger.warning("환경 변수 MSIT_GSPREAD_ref가 설정되지 않았습니다. Google Sheets 업데이트는 비활성화됩니다.")
        
        # MSIT URL (기본 URL)
        self.landing_url = "https://www.msit.go.kr"
        self.stats_url = "https://www.msit.go.kr/bbs/list.do?sCode=user&mPid=74&mId=99"
        
        # Telegram 봇 초기화
        self.bot = telegram.Bot(token=self.telegram_token)
        
        # 추적할 보고서 유형
        self.report_types = [
            "이동전화 및 트래픽 통계",
            "이동전화 및 시내전화 번호이동 현황",
            "유선통신서비스 가입 현황",
            "무선통신서비스 가입 현황", 
            "특수부가통신사업자현황",
            "무선데이터 트래픽 통계",
            "유·무선통신서비스 가입 현황 및 무선데이터 트래픽 통계"
        ]
        
        # 임시 디렉토리
        self.temp_dir = Path("./downloads")
        self.temp_dir.mkdir(exist_ok=True)

    def setup_driver(self):
        """Selenium WebDriver 설정 (향상된 버전)"""
        options = Options()
        # 비-headless 모드 실행 (GitHub Actions에서 Xvfb 사용 시)
        # 일반 환경에서는 headless 모드를 사용할 수 있음
        # options.add_argument('--headless')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-gpu')
        options.add_argument('--window-size=1920,1080')

        # 사용자 에이전트 설정
        options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36")
        
        # 자동화 감지 우회를 위한 옵션
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option("useAutomationExtension", False)
        options.add_argument("--disable-blink-features=AutomationControlled")
        
        # 성능 최적화 설정
        prefs = {
            "profile.default_content_setting_values.images": 2,  # 이미지 로딩 비활성화
            "profile.default_content_setting_values.cookies": 1,  # 쿠키 활성화 (세션 유지를 위해)
            "profile.managed_default_content_settings.javascript": 1  # JavaScript 활성화 (필요함)
        }
        options.add_experimental_option("prefs", prefs)
        
        # 불필요한 로그 비활성화
        options.add_experimental_option('excludeSwitches', ['enable-logging'])
        
        # 서비스 설정 (향상된 버전)
        try:
            # 첫번째: chromium-browser 확인
            if os.path.exists('/usr/bin/chromium-browser'):
                options.binary_location = '/usr/bin/chromium-browser'
                service = Service('/usr/bin/chromedriver')
                logger.info("Chromium 브라우저 및 드라이버 사용")
            # 두번째: chrome-browser 확인 
            elif os.path.exists('/usr/bin/google-chrome-stable'):
                options.binary_location = '/usr/bin/google-chrome-stable'
                service = Service('/usr/bin/chromedriver')
                logger.info("Chrome 브라우저 및 드라이버 사용")
            # 세번째: webdriver-manager 사용
            else:
                try:
                    from webdriver_manager.chrome import ChromeDriverManager
                    service = Service(ChromeDriverManager().install())
                    logger.info("ChromeDriverManager를 통한 드라이버 설치 완료")
                except ImportError:
                    service = Service('/usr/bin/chromedriver')
                    logger.info("기본 경로 chromedriver 사용")
        except Exception as e:
            logger.error(f"WebDriver 설정 중 오류: {str(e)}")
            service = Service('/usr/bin/chromedriver')
            logger.info("오류 발생 후 기본 경로 chromedriver 사용")
        
        driver = webdriver.Chrome(service=service, options=options)
        driver.set_page_load_timeout(60)
        
        # Selenium Stealth 적용 (있는 경우)
        try:
            from selenium_stealth import stealth
            stealth(driver,
                languages=["ko-KR", "ko"],
                vendor="Google Inc.",
                platform="Win32",
                webgl_vendor="Intel Inc.",
                renderer="Intel Iris OpenGL Engine",
                fix_hairline=True)
            logger.info("Selenium Stealth 적용 완료")
        except ImportError:
            logger.warning("selenium-stealth 라이브러리를 찾을 수 없습니다. 기본 모드로 계속합니다.")
        
        return driver

    def setup_gspread_client(self):
        """Google Sheets 클라이언트 초기화"""
        if not self.gspread_creds:
            return None
        
        try:
            # 환경 변수에서 자격 증명 파싱
            creds_dict = json.loads(self.gspread_creds)
            
            # 임시 파일에 자격 증명 저장
            temp_creds_path = self.temp_dir / "temp_creds.json"
            with open(temp_creds_path, 'w') as f:
                json.dump(creds_dict, f)
            
            # gspread 클라이언트 설정
            scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
            credentials = ServiceAccountCredentials.from_json_keyfile_name(str(temp_creds_path), scope)
            client = gspread.authorize(credentials)
            
            # 임시 파일 삭제
            os.unlink(temp_creds_path)
            
            # 스프레드시트 접근 테스트
            if self.spreadsheet_id:
                try:
                    test_sheet = client.open_by_key(self.spreadsheet_id)
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

    def is_telecom_stats_post(self, title):
        """게시물이 통신 통계 보고서인지 확인"""
        if not title:
            return False
            
        # "(YYYY년 MM월말 기준)" 형식의 날짜 패턴 확인 (더 유연한 정규식)
        date_pattern = r'\((\d{4})년\s*(\d{1,2})월말\s*기준\)'
        has_date_pattern = re.search(date_pattern, title) is not None
        
        if not has_date_pattern:
            return False
        
        # 제목에 보고서 유형이 포함되어 있는지 확인
        contains_report_type = any(report_type in title for report_type in self.report_types)
        
        return contains_report_type

    def extract_post_id(self, item):
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

    def get_post_url(self, post_id):
        """게시물 ID로부터 URL 생성"""
        if not post_id:
            return None
        return f"https://www.msit.go.kr/bbs/view.do?sCode=user&mId=99&mPid=74&nttSeqNo={post_id}"

    def is_in_date_range(self, date_str, days=4):
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

    def has_next_page(self, driver):
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

    def go_to_next_page(self, driver):
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

    def parse_page(self, driver, days_range=4):
        """현재 페이지에서 관련 게시물 파싱"""
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
                    WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located((by_type, selector))
                    )
                    loaded = True
                    break
                except TimeoutException:
                    continue
            
            if not loaded:
                logger.error("페이지 로드 시간 초과")
                return [], [], False
            
            # 페이지가 로드되면 약간의 지연 시간 추가
            time.sleep(2)
            
            # 페이지 소스 저장 (디버깅용)
            try:
                with open('last_parsed_page.html', 'w', encoding='utf-8') as f:
                    f.write(driver.page_source)
                logger.info("현재 페이지 소스 저장 완료: last_parsed_page.html")
            except Exception as save_err:
                logger.warning(f"페이지 소스 저장 중 오류: {str(save_err)}")
            
            # 스크린샷 저장 (디버깅용)
            try:
                driver.save_screenshot('last_parsed_page.png')
                logger.info("현재 페이지 스크린샷 저장 완료: last_parsed_page.png")
            except Exception as ss_err:
                logger.warning(f"스크린샷 저장 중 오류: {str(ss_err)}")
            
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
                logger.warning("게시물을 찾을 수 없음")
                return [], [], False
            
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
                    if not self.is_in_date_range(date_str, days=days_range):
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
                    post_id = self.extract_post_id(item)
                    post_url = self.get_post_url(post_id)
                    
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
                    if self.is_telecom_stats_post(title):
                        logger.info(f"통신 통계 게시물 발견: {title}")
                        telecom_stats_posts.append(post_info)
                        
                except Exception as e:
                    logger.error(f"게시물 파싱 중 에러: {str(e)}")
                    continue
            
            return all_posts, telecom_stats_posts, continue_search
            
        except Exception as e:
            logger.error(f"페이지 파싱 중 에러: {str(e)}")
            return [], [], False

    def find_view_link_params(self, driver, post):
        """게시물에서 바로보기 링크 파라미터 찾기"""
        if not post.get('post_id'):
            logger.error(f"게시물 접근 불가 {post['title']} - post_id 누락")
            return None
        
        logger.info(f"게시물 열기: {post['title']}")
        
        # 최대 재시도 횟수
        max_retries = 5
        retry_delay = 3
        
        for attempt in range(max_retries):
            try:
                # 게시물 상세 페이지로 이동
                detail_url = f"https://www.msit.go.kr/bbs/view.do?sCode=user&mId=99&mPid=74&nttSeqNo={post['post_id']}"
                driver.get(detail_url)
                time.sleep(retry_delay)
                
                # 시스템 점검 오버레이 제거 시도
                try:
                    driver.execute_script("document.querySelectorAll('.overlay').forEach(e => e.remove());")
                    overlay_elements = driver.find_elements(By.XPATH, "//*[contains(text(), '시스템 점검 안내')]")
                    if overlay_elements:
                        logger.info("시스템 점검 안내 오버레이 감지됨. 오버레이 제거 시도 중...")
                        driver.execute_script("document.querySelectorAll('//*[contains(text(), \"시스템 점검 안내\")]').forEach(e => e.remove());")
                        time.sleep(1)
                except Exception:
                    logger.info("시스템 점검 안내 오버레이 없음 또는 이미 제거됨.")
                
                # 다양한 요소 중 하나라도 로드되면 성공으로 간주
                wait_elements = [
                    (By.CLASS_NAME, "view_head"),
                    (By.CLASS_NAME, "view_file"),
                    (By.CLASS_NAME, "view_cont")
                ]
                
                element_found = False
                for by_type, selector in wait_elements:
                    try:
                        WebDriverWait(driver, 10).until(
                            EC.presence_of_element_located((by_type, selector))
                        )
                        logger.info(f"페이지 로드 성공: {selector} 요소 발견")
                        element_found = True
                        break
                    except TimeoutException:
                        continue
                
                # 시스템 점검 페이지 감지
		    # 시스템 점검 페이지 감지
                if "시스템 점검 안내" in driver.page_source:
                    if attempt < max_retries - 1:
                        logger.warning("시스템 점검 중입니다. 나중에 다시 시도합니다.")
                        time.sleep(retry_delay * 2)  # 더 오래 대기
                        continue
                    else:
                        # 최종 시도 후에도 시스템 점검 페이지면 오류 반환
                        logger.warning("시스템 점검 중입니다.")
                        
                        # 페이지 소스 미리보기 저장
                        html_snippet = driver.page_source[:1000]
                        logger.error(f"최종 시도 후 페이지 HTML 스니펫:\n{html_snippet}")
                        
                        # 날짜 정보 추출 시도
                        date_match = re.search(r'\((\d{4})년\s*(\d{1,2})월말\s*기준\)', post['title'])
                        if date_match:
                            year = int(date_match.group(1))
                            month = int(date_match.group(2))
                            
                            return {
                                'content': "시스템 점검 중입니다.",
                                'date': {'year': year, 'month': month},
                                'post_info': post
                            }
                        
                        return None
                
                # 로드 실패 시
                if not element_found:
                    if attempt < max_retries - 1:
                        logger.warning(f"페이지 요소를 찾을 수 없음 (시도 {attempt+1}/{max_retries})")
                        time.sleep(retry_delay)
                        retry_delay *= 1.5  # 대기 시간 증가
                        continue
                    else:
                        logger.error(f"{max_retries}번 시도 후 페이지 요소를 찾을 수 없음")
                        
                        # 날짜 정보 추출 시도
                        date_match = re.search(r'\((\d{4})년\s*(\d{1,2})월말\s*기준\)', post['title'])
                        if date_match:
                            year = int(date_match.group(1))
                            month = int(date_match.group(2))
                            
                            return {
                                'content': "페이지 로드 실패",
                                'date': {'year': year, 'month': month},
                                'post_info': post
                            }
                        
                        return None
                
                # 디버깅용 스크린샷 저장
                try:
                    driver.save_screenshot(f"post_view_{post['post_id']}.png")
                    logger.info(f"게시물 페이지 스크린샷 저장: post_view_{post['post_id']}.png")
                except Exception as ss_err:
                    logger.warning(f"스크린샷 저장 중 오류: {str(ss_err)}")
                
                # 바로보기 링크 찾기
                try:
                    # 여러 선택자로 바로보기 링크 찾기
                    view_links = driver.find_elements(By.CSS_SELECTOR, "a.view[title='새창 열림']")
                    
                    if not view_links:
                        # onclick 속성으로 찾기
                        all_links = driver.find_elements(By.TAG_NAME, "a")
                        view_links = [link for link in all_links if 'getExtension_path' in (link.get_attribute('onclick') or '')]
                    
                    if not view_links:
                        # 텍스트로 찾기
                        all_links = driver.find_elements(By.TAG_NAME, "a")
                        view_links = [link for link in all_links if '바로보기' in (link.text or '')]
                    
                    if view_links:
                        view_link = view_links[0]
                        onclick_attr = view_link.get_attribute('onclick')
                        logger.info(f"바로보기 링크 발견, onclick: {onclick_attr}")
                        
                        # getExtension_path('49234', '1') 형식에서 매개변수 추출
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
                    
                    # 바로보기 링크를 찾을 수 없는 경우
                    logger.warning(f"바로보기 링크를 찾을 수 없음: {post['title']}")
                    
                    # 날짜 정보 추출
                    date_match = re.search(r'\((\d{4})년\s*(\d{1,2})월말\s*기준\)', post['title'])
                    if date_match:
                        year = int(date_match.group(1))
                        month = int(date_match.group(2))
                        
                        # 게시물 내용 추출
                        try:
                            content_div = driver.find_element(By.CLASS_NAME, "view_cont")
                            content = content_div.text if content_div else ""
                            
                            return {
                                'content': content,
                                'date': {'year': year, 'month': month},
                                'post_info': post
                            }
                        except NoSuchElementException:
                            logger.warning("게시물 내용을 찾을 수 없음")
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
                    time.sleep(retry_delay)
                    retry_delay *= 1.5  # 대기 시간 증가
                else:
                    logger.error(f"{max_retries}번 시도 후 페이지 로드 실패")
                    
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

    def access_iframe_direct(self, driver, file_params):
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
                                df = self.extract_table_from_html(iframe_html)
                                
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
                            df = self.extract_table_from_html(html_content)
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

    def extract_table_from_html(self, html_content):
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

    def create_placeholder_dataframe(self, post_info):
        """데이터 추출 실패 시 기본 데이터프레임 생성"""
        try:
            # 날짜 정보 추출
            date_match = re.search(r'\((\d{4})년\s*(\d{1,2})월말\s*기준\)', post_info['title'])
            if date_match:
                year = date_match.group(1)
                month = date_match.group(2)
                
                # 보고서 유형 결정
                report_type = self.determine_report_type(post_info['title'])
                
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


    def determine_report_type(self, title):
        """게시물 제목에서 보고서 유형 결정"""
        for report_type in self.report_types:
            if report_type in title:
                return report_type
                
        # 부분 매칭 시도
        for report_type in self.report_types:
            # 주요 키워드 추출
            keywords = report_type.split()
            if any(keyword in title for keyword in keywords if len(keyword) > 1):
                return report_type
                
        return "기타 통신 통계"

    def update_google_sheets(self, client, data):
        """Google Sheets 업데이트"""
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
                # 제목에서 날짜 정보 추출 (향상된 정규식)
                date_match = re.search(r'\((\d{4})년\s*(\d{1,2})월말\s*기준\)', post_info['title'])
                if not date_match:
                    logger.error(f"제목에서 날짜를 추출할 수 없음: {post_info['title']}")
                    return False
                    
                year = int(date_match.group(1))
                month = int(date_match.group(2))
            
            # 날짜 문자열 포맷
            date_str = f"{year}년 {month}월"
            report_type = self.determine_report_type(post_info['title'])
            
            # 스프레드시트 열기 (재시도 로직 포함)
            spreadsheet = None
            retry_count = 0
            max_retries = 3
            
            while retry_count < max_retries and not spreadsheet:
                try:
                    # ID로 먼저 시도
                    if self.spreadsheet_id:
                        try:
                            spreadsheet = client.open_by_key(self.spreadsheet_id)
                            logger.info(f"ID로 기존 스프레드시트 찾음: {self.spreadsheet_id}")
                        except Exception as e:
                            logger.warning(f"ID로 스프레드시트를 열 수 없음: {self.spreadsheet_id}, 오류: {str(e)}")
                    
                    # ID로 찾지 못한 경우 이름으로 시도
                    if not spreadsheet:
                        try:
                            spreadsheet = client.open(self.spreadsheet_name)
                            logger.info(f"이름으로 기존 스프레드시트 찾음: {self.spreadsheet_name}")
                        except gspread.exceptions.SpreadsheetNotFound:
                            # 새 스프레드시트 생성
                            spreadsheet = client.create(self.spreadsheet_name)
                            logger.info(f"새 스프레드시트 생성: {self.spreadsheet_name}")
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
            if 'sheets' in data:
                # 여러 시트 처리
                success_count = 0
                for sheet_name, df in data['sheets'].items():
                    if df is not None and not df.empty:
                        success = self.update_single_sheet(spreadsheet, sheet_name, df, date_str)
                        if success:
                            success_count += 1
                        else:
                            logger.warning(f"시트 '{sheet_name}' 업데이트 실패")
                
                logger.info(f"전체 {len(data['sheets'])}개 시트 중 {success_count}개 업데이트 성공")
                return success_count > 0
                
            elif 'dataframe' in data:
                # 단일 데이터프레임 처리
                return self.update_single_sheet(spreadsheet, report_type, data['dataframe'], date_str)
                
            else:
                logger.error("업데이트할 데이터가 없습니다")
                return False
                
        except Exception as e:
            logger.error(f"Google Sheets 업데이트 중 오류: {str(e)}")
            return False

    def update_single_sheet(self, spreadsheet, sheet_name, df, date_str):
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
            self.update_sheet_from_dataframe(worksheet, df, col_idx)
            
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

    def update_sheet_from_dataframe(self, worksheet, df, col_idx):
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

    async def send_telegram_message(self, posts, data_updates=None):
        """텔레그램으로 알림 메시지 전송"""
        if not posts and not data_updates:
            logger.info("알림을 보낼 내용이 없습니다")
            return
            
        try:
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
                    report_type = self.determine_report_type(post_info['title'])
                    
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
            if data_updates and self.spreadsheet_id:
                spreadsheet_url = f"https://docs.google.com/spreadsheets/d/{self.spreadsheet_id}"
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
                    
                    chat_id = int(self.chat_id)
                    await self.bot.send_message(
                        chat_id=chat_id,
                        text=chunk,
                        parse_mode='Markdown'
                    )
                    time.sleep(1)  # 메시지 사이 지연
                
                logger.info(f"텔레그램 메시지 {len(chunks)}개 청크로 분할 전송 완료")
            else:
                # 단일 메시지 전송
                chat_id = int(self.chat_id)
                await self.bot.send_message(
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
                await self.bot.send_message(
                    chat_id=int(self.chat_id),
                    text=simple_msg
                )
                logger.info("단순화된 텔레그램 메시지 전송 성공")
            except Exception as simple_err:
                logger.error(f"단순화된 텔레그램 메시지 전송 중 오류: {str(simple_err)}")

    async def run_monitor(self, days_range=4, check_sheets=True):
        """모니터링 실행 (향상된 버전)"""
        driver = None
        gs_client = None
        
        try:
            # 시작 시간 기록
            start_time = time.time()
            logger.info(f"=== MSIT 통신 통계 모니터링 시작 (days_range={days_range}, check_sheets={check_sheets}) ===")
            
            # WebDriver 초기화
            driver = self.setup_driver()
            logger.info("WebDriver 초기화 완료")
            
            # Google Sheets 클라이언트 초기화
            if check_sheets and self.gspread_creds:
                gs_client = self.setup_gspread_client()
                if gs_client:
                    logger.info("Google Sheets 클라이언트 초기화 완료")
                else:
                    logger.warning("Google Sheets 클라이언트 초기화 실패")
            
            # 랜딩 페이지 접속 후 '통계정보' 버튼 클릭하여 쿠키/세션 정보 설정
            try:
                landing_url = "https://www.msit.go.kr"
                driver.get(landing_url)
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.ID, "skip_nav"))
                )
                logger.info("랜딩 페이지 접속 완료 - 쿠키 및 세션 정보 획득")
                
                try:
                    driver.save_screenshot("landing_page.png")
                    logger.info("랜딩 페이지 스크린샷 저장 완료")
                except Exception as ss_err:
                    logger.warning(f"스크린샷 저장 중 오류: {str(ss_err)}")
                
                # 통계정보 링크 찾기 및 클릭
                stats_link_selectors = [
                    "a[href*='mId=99'][href*='mPid=74']",
                    "a:contains('통계정보')"
                ]
                
                stats_link_found = False
                for selector in stats_link_selectors:
                    try:
                        if ':contains' in selector:
                            # XPath로 텍스트 포함 요소 검색
                            links = driver.find_elements(By.XPATH, "//a[contains(text(), '통계정보')]")
                        else:
                            # CSS 선택자 검색
                            links = driver.find_elements(By.CSS_SELECTOR, selector)
                        
                        if links:
                            stats_link = links[0]
                            logger.info("통계정보 링크 발견, 클릭 시도")
                            stats_link.click()
                            WebDriverWait(driver, 10).until(EC.url_contains("/bbs/list.do"))
                            stats_link_found = True
                            logger.info("통계정보 페이지로 이동 완료")
                            break
                    except Exception as link_err:
                        logger.warning(f"통계정보 링크 클릭 실패: {str(link_err)}")
                
                if not stats_link_found:
                    logger.warning("통계정보 링크를 찾을 수 없음, 직접 URL로 접속")
                    driver.get(self.stats_url)
                    WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located((By.CLASS_NAME, "board_list"))
                    )
                
            except Exception as e:
                logger.error(f"랜딩 또는 통계정보 버튼 클릭 중 오류 발생, fallback으로 직접 접속: {str(e)}")
                driver.get(self.stats_url)
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CLASS_NAME, "board_list"))
                )
            
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
                
                page_posts, stats_posts, should_continue = self.parse_page(driver, days_range=days_range)
                all_posts.extend(page_posts)
                telecom_stats_posts.extend(stats_posts)
                
                logger.info(f"페이지 {page_num} 파싱 결과: {len(page_posts)}개 게시물, {len(stats_posts)}개 통신 통계")
                
                if not should_continue:
                    logger.info(f"날짜 범위 밖의 게시물 발견. 검색 중단")
                    break
                    
                if self.has_next_page(driver):
                    if self.go_to_next_page(driver):
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
                        
                        # 바로보기 링크 파라미터 추출
                        file_params = self.find_view_link_params(driver, post)
                        
                        if not file_params:
                            logger.warning(f"바로보기 링크 파라미터 추출 실패: {post['title']}")
                            continue
                        
                        # 바로보기 링크가 있는 경우
                        if 'atch_file_no' in file_params and 'file_ord' in file_params:
                            # iframe 직접 접근하여 데이터 추출
                            sheets_data = self.access_iframe_direct(driver, file_params)
                            
                            if sheets_data:
                                # Google Sheets 업데이트
                                update_data = {
                                    'sheets': sheets_data,
                                    'post_info': post
                                }
                                
                                if 'date' in file_params:
                                    update_data['date'] = file_params['date']
                                
				success = self.update_google_sheets(gs_client, update_data)
                                if success:
                                    logger.info(f"Google Sheets 업데이트 성공: {post['title']}")
                                    data_updates.append(update_data)
                                else:
                                    logger.warning(f"Google Sheets 업데이트 실패: {post['title']}")
                            else:
                                logger.warning(f"iframe에서 데이터 추출 실패: {post['title']}")
                                
                                # 대체 데이터 생성
                                placeholder_df = self.create_placeholder_dataframe(post)
                                if not placeholder_df.empty:
                                    update_data = {
                                        'dataframe': placeholder_df,
                                        'post_info': post
                                    }
                                    
                                    if 'date' in file_params:
                                        update_data['date'] = file_params['date']
                                    
                                    success = self.update_google_sheets(gs_client, update_data)
                                    if success:
                                        logger.info(f"대체 데이터로 업데이트 성공: {post['title']}")
                                        data_updates.append(update_data)
                        
                        # 게시물 내용만 있는 경우
                        elif 'content' in file_params:
                            logger.info(f"게시물 내용으로 처리 중: {post['title']}")
                            
                            # 대체 데이터 생성
                            placeholder_df = self.create_placeholder_dataframe(post)
                            if not placeholder_df.empty:
                                update_data = {
                                    'dataframe': placeholder_df,
                                    'post_info': post
                                }
                                
                                if 'date' in file_params:
                                    update_data['date'] = file_params['date']
                                
                                success = self.update_google_sheets(gs_client, update_data)
                                if success:
                                    logger.info(f"내용 기반 데이터로 업데이트 성공: {post['title']}")
                                    data_updates.append(update_data)
                        
                        # API 속도 제한 방지를 위한 지연
                        time.sleep(2)
                    
                    except Exception as e:
                        logger.error(f"게시물 처리 중 오류: {str(e)}")
            
            # 종료 시간 및 실행 시간 계산
            end_time = time.time()
            execution_time = end_time - start_time
            logger.info(f"실행 시간: {execution_time:.2f}초")
            
            # 텔레그램 알림 전송
            if all_posts or data_updates:
                await self.send_telegram_message(all_posts, data_updates)
                logger.info(f"알림 전송 완료: {len(all_posts)}개 게시물, {len(data_updates)}개 업데이트")
            else:
                logger.info(f"최근 {days_range}일 내 새 게시물이 없습니다")
                
                # 결과 없음 알림 (선택적)
                if days_range > 7:  # 장기간 검색한 경우에만 알림
                    await self.bot.send_message(
                        chat_id=int(self.chat_id),
                        text=f"📊 MSIT 통신 통계 모니터링: 최근 {days_range}일 내 새 게시물이 없습니다. ({datetime.now().strftime('%Y-%m-%d %H:%M')})"
                    )
        
        except Exception as e:
            logger.error(f"모니터링 중 오류 발생: {str(e)}")
            
            try:
                # 오류 알림 전송
                error_post = {
                    'title': f"모니터링 오류: {str(e)}",
                    'date': datetime.now().strftime('%Y. %m. %d'),
                    'department': 'System Error'
                }
                await self.send_telegram_message([error_post])
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
    
    # 모니터 생성 및 실행
    try:
        monitor = MSITMonitor()
        await monitor.run_monitor(days_range=days_range, check_sheets=check_sheets)
    except Exception as e:
        logging.error(f"메인 함수 오류: {str(e)}", exc_info=True)
        
        # 치명적 오류 시 텔레그램 알림 시도
        try:
            bot = telegram.Bot(token=os.environ.get('TELCO_NEWS_TOKEN'))
            await bot.send_message(
                chat_id=int(os.environ.get('TELCO_NEWS_TESTER')),
                text=f"⚠️ *MSIT 모니터링 치명적 오류*\n\n{str(e)}\n\n시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
                parse_mode='Markdown'
            )
        except:
            pass


if __name__ == "__main__":
    asyncio.run(main())
