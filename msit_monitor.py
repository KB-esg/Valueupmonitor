
import os
import re    
import json
import time
import logging
import asyncio
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd
import requests
from urllib.parse import urlparse, parse_qs

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

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('msit_monitor')

class MSITMonitor:
    def __init__(self):
        # Telegram configuration
        self.telegram_token = os.environ.get('TELCO_NEWS_TOKEN')
        self.chat_id = os.environ.get('TELCO_NEWS_TESTER')
        if not self.telegram_token or not self.chat_id:
            raise ValueError("환경 변수 TELCO_NEWS_TOKEN과 TELCO_NEWS_TESTER가 필요합니다.")
        
        # Google Sheets configuration
        self.gspread_creds = os.environ.get('MSIT_GSPREAD_ref')
        self.spreadsheet_id = os.environ.get('MSIT_SPREADSHEET_ID')
        self.spreadsheet_name = os.environ.get('SPREADSHEET_NAME', 'MSIT 통신 통계')
    
        if not self.gspread_creds:
            logger.warning("환경 변수 MSIT_GSPREAD_ref가 설정되지 않았습니다. Google Sheets 업데이트는 비활성화됩니다.")
        
        # MSIT URL
        self.url = "https://www.msit.go.kr/bbs/list.do?sCode=user&mPid=74&mId=99"
        
        # Initialize Telegram bot
        self.bot = telegram.Bot(token=self.telegram_token)
        
        # Report types for tracking
        self.report_types = [
            "이동전화 및 트래픽 통계",
            "이동전화 및 시내전화 번호이동 현황",
            "유선통신서비스 가입 현황",
            "무선통신서비스 가입 현황", 
            "특수부가통신사업자현황",
            "무선데이터 트래픽 통계",
            "유·무선통신서비스 가입 현황 및 무선데이터 트래픽 통계"
        ]
        
        # Temporary directory for downloads
        self.temp_dir = Path("./downloads")
        self.temp_dir.mkdir(exist_ok=True)


        # 현재 처리 중인 파일 정보를 저장할 변수
        self.current_file_info = None
        
        # 직접 파일 다운로드 시도 횟수
        self.direct_download_attempt = 0
        
        # 세션 유지
        self.session = requests.Session()

    
    def setup_driver(self):
        """Initialize Selenium WebDriver"""
        chrome_options = Options()
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument('--window-size=1920,1080')  # 큰 창 크기 설정

        # 브라우저 페이지 로드 전략 설정
        chrome_options.page_load_strategy = 'eager'  # DOM이 준비되면 로드 완료로 간주
        
        
        # Set download preferences
        prefs = {
            "download.default_directory": str(self.temp_dir.absolute()),
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": False,
            "profile.default_content_setting_values.images": 2,  # 이미지 로드 비활성화
            "profile.default_content_settings.popups": 0,
            "profile.default_content_setting_values.notifications": 2
        }
        chrome_options.add_experimental_option("prefs", prefs)

        # 불필요한 로그 비활성화
        chrome_options.add_experimental_option('excludeSwitches', ['enable-logging'])

        # Use either environment-specific or standard Chrome path
        if os.path.exists('/usr/bin/chromium-browser'):
            chrome_options.binary_location = '/usr/bin/chromium-browser'
            service = Service('/usr/bin/chromedriver')
        else:
            # Use webdriver-manager for local development
            try:
                from webdriver_manager.chrome import ChromeDriverManager
                service = Service(ChromeDriverManager().install())
            except ImportError:
                # Fallback for environments without webdriver-manager
                service = Service('/usr/bin/chromedriver')
        
        driver = webdriver.Chrome(service=service, options=chrome_options)
        
            # 페이지 로드 타임아웃 설정 (초 단위)
        driver.set_page_load_timeout(60)  # 60초로 설정

        # 암시적 대기 설정 - 요소를 찾을 때 최대 10초까지 대기
        driver.implicitly_wait(10)
        
        return driver

    def setup_gspread_client(self):
        """Initialize Google Sheets client"""
        if not self.gspread_creds:
            return None
        
        try:
            # Parse credentials from environment variable
            creds_dict = json.loads(self.gspread_creds)
            
            # Write credentials to a temporary file
            temp_creds_path = self.temp_dir / "temp_creds.json"
            with open(temp_creds_path, 'w') as f:
                json.dump(creds_dict, f)
            
            # Set up the gspread client
            scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
            credentials = ServiceAccountCredentials.from_json_keyfile_name(str(temp_creds_path), scope)
            client = gspread.authorize(credentials)
            
            # Clean up temporary file
            os.unlink(temp_creds_path)
            
            return client
        except Exception as e:
            logger.error(f"Google Sheets 클라이언트 초기화 중 오류: {str(e)}")
            return None

    def is_telecom_stats_post(self, title):
        """Check if the post is a telecommunication statistics report"""
        # Check if title contains a date pattern like "(YYYY년 MM월말 기준)"
        date_pattern = r'\((\d{4})년\s+(\d{1,2})월말\s+기준\)'
        has_date_pattern = re.search(date_pattern, title) is not None
    
        # Check if title contains any of the report types
        contains_report_type = any(report_type in title for report_type in self.report_types)
    
        return has_date_pattern and contains_report_type

    def extract_post_id(self, item):
        """Extract the post ID from a BeautifulSoup item"""
        try:
            link_elem = item.find('a')
            if not link_elem:
                return None
                
            onclick_attr = link_elem.get('onclick', '')
            match = re.search(r"fn_detail\((\d+)\)", onclick_attr)
            if match:
                return match.group(1)
            return None
        except Exception as e:
            logger.error(f"게시물 ID 추출 중 에러: {str(e)}")
            return None

    def get_post_url(self, post_id):
        """Generate the post URL from the post ID"""
        if not post_id:
            return None
        return f"https://www.msit.go.kr/bbs/view.do?sCode=user&mId=99&mPid=74&nttSeqNo={post_id}"

    def is_in_date_range(self, date_str, days=4):
        """Check if the post date is within the specified range"""
        try:
            # Normalize date string
            date_str = date_str.replace(',', ' ').strip()
        
            # Try various date formats
            try:
                # "YYYY. MM. DD" format
                post_date = datetime.strptime(date_str, '%Y. %m. %d').date()
            except ValueError:
                try:
                    # "MMM DD YYYY" format
                    post_date = datetime.strptime(date_str, '%b %d %Y').date()
                except ValueError:
                    try:
                        # "YYYY-MM-DD" format
                        post_date = datetime.strptime(date_str, '%Y-%m-%d').date()
                    except ValueError:
                        # 다른 형식 시도
                        logger.warning(f"Unknown date format: {date_str}, trying regex pattern")
                        date_match = re.search(r'(\d{4})[.\-\s]+(\d{1,2})[.\-\s]+(\d{1,2})', date_str)
                        if date_match:
                            post_date = datetime(int(date_match.group(1)), int(date_match.group(2)), int(date_match.group(3))).date()
                        else:
                            logger.error(f"Could not parse date: {date_str}")
                            return True  # 날짜를 파싱할 수 없는 경우 포함시켜 검사
        
            # Calculate date range (Korean timezone)
            korea_tz = datetime.now() + timedelta(hours=9)  # UTC to KST
            days_ago = (korea_tz - timedelta(days=days)).date()
        
            logger.info(f"게시물 날짜 확인: {post_date} vs {days_ago} ({days}일 전, 한국 시간 기준)")
            return post_date >= days_ago
        
        except Exception as e:
            logger.error(f"날짜 파싱 에러: {str(e)}")
            return True  # 에러 발생 시 기본적으로 포함시켜 검사

    def has_next_page(self, driver):
        """Check if there's a next page to parse"""
        try:
            current_page = int(driver.find_element(By.CSS_SELECTOR, "a.page-link[aria-current='page']").text)
            next_page_link = driver.find_elements(By.CSS_SELECTOR, f"a.page-link[href*='pageIndex={current_page + 1}']")
            return len(next_page_link) > 0
        except Exception as e:
            logger.error(f"다음 페이지 확인 중 에러: {str(e)}")
            return False

    def go_to_next_page(self, driver):
        """Navigate to the next page"""
        try:
            current_page = int(driver.find_element(By.CSS_SELECTOR, "a.page-link[aria-current='page']").text)
            next_page = driver.find_element(By.CSS_SELECTOR, f"a.page-link[href*='pageIndex={current_page + 1}']")
            next_page.click()
            WebDriverWait(driver, 10).until(
                EC.staleness_of(driver.find_element(By.CSS_SELECTOR, "div.board_list"))
            )
            return True
        except Exception as e:
            logger.error(f"다음 페이지 이동 중 에러: {str(e)}")
            return False

    def parse_page(self, driver, days_range=4):
        """Parse the current page for relevant posts"""
        all_posts = []
        telecom_stats_posts = []
        continue_search = True
        
        try:
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CLASS_NAME, "board_list"))
            )
            
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            posts = soup.find_all('div', {'class': 'toggle'})
            
            for item in posts:
                # Skip header row
                if 'thead' in item.get('class', []):
                    continue

                try:
                    # Extract date information
                    date_elem = item.find('div', {'class': 'date', 'aria-label': '등록일'})
                    if not date_elem:
                        date_elem = item.find('div', {'class': 'date'})
                    if not date_elem:
                        continue
                        
                    date_str = date_elem.text.strip()
                    if not date_str or date_str == '등록일':
                        continue
                    
                    logger.info(f"Found date string: {date_str}")
                    
                    # Check if post is within date range
                    if not self.is_in_date_range(date_str, days=days_range):
                        continue_search = False
                        break
                    
                    # Extract title and post ID
                    title_elem = item.find('p', {'class': 'title'})
                    if not title_elem:
                        continue
                        
                    title = title_elem.text.strip()
                    post_id = self.extract_post_id(item)
                    post_url = self.get_post_url(post_id)
                    
                    # Extract department information
                    dept_elem = item.find('dd', {'id': lambda x: x and 'td_CHRG_DEPT_NM' in x})
                    dept_text = dept_elem.text.strip() if dept_elem else "부서 정보 없음"
                    
                    # Create post info dictionary
                    post_info = {
                        'title': title,
                        'date': date_str,
                        'department': dept_text,
                        'url': post_url,
                        'post_id': post_id
                    }
                    
                    # Add to all posts list
                    all_posts.append(post_info)
                    
                    # Check if it's a telecom stats post
                    if self.is_telecom_stats_post(title):
                        logger.info(f"Found telecom stats post: {title}")
                        telecom_stats_posts.append(post_info)
                        
                except Exception as e:
                    logger.error(f"게시물 파싱 중 에러: {str(e)}")
                    continue
            
            return all_posts, telecom_stats_posts, continue_search
            
        except Exception as e:
            logger.error(f"페이지 파싱 중 에러: {str(e)}")
            return [], [], False

    def extract_file_info(self, driver, post):
        """Extract file information from a post with improved handling for SynapDocViewServer"""
        if not post.get('post_id'):
            logger.error(f"Cannot access post {post['title']} - missing post ID")
            return None
        
        logger.info(f"Opening post: {post['title']}")
        
        # 최대 3번까지 재시도
        max_retries = 3
        for attempt in range(max_retries):
            try:
                # 게시물 상세 페이지 URL
                detail_url = f"https://www.msit.go.kr/bbs/view.do?sCode=user&mId=99&mPid=74&nttSeqNo={post['post_id']}"
                logger.info(f"Navigating to post detail URL: {detail_url}")
                
                # 페이지 로드 전 쿠키와 캐시 초기화
                driver.delete_all_cookies()
                driver.execute_script("window.localStorage.clear();")
                
                # URL 직접 이동
                driver.get(detail_url)
                
                # 명시적인 대기 시간 추가
                time.sleep(5)
                
                # 페이지 로드 확인 - 다양한 요소 중 하나라도 존재하는지 확인
                try:
                    WebDriverWait(driver, 20).until(
                        lambda x: (
                            len(x.find_elements(By.CLASS_NAME, "view_head")) > 0 or 
                            len(x.find_elements(By.CLASS_NAME, "view_file")) > 0 or
                            len(x.find_elements(By.ID, "cont-wrap")) > 0
                        )
                    )
                    logger.info("Post detail page loaded successfully")
                    break  # 성공하면 루프 종료
                except TimeoutException:
                    logger.warning(f"Timeout waiting for page elements, retrying... ({attempt+1}/{max_retries})")
                    if attempt < max_retries - 1:
                        continue
                    else:
                        # 마지막 시도에서는 페이지 소스 확인
                        logger.info(f"Page source preview: {driver.page_source[:500]}")
                
            except Exception as e:
                logger.error(f"Error accessing post detail: {str(e)}")
                if attempt < max_retries - 1:
                    time.sleep(3)
                else:
                    return None
        
        # 파일 정보 추출
        try:
            # 방법 1: 바로보기 링크 찾기 - getExtension_path 함수 호출 링크
            view_links = driver.find_elements(By.CSS_SELECTOR, "a.view[title='새창 열림']")
            
            if not view_links:
                # 다른 선택자로 시도
                view_links = driver.find_elements(By.CSS_SELECTOR, "a[onclick*='getExtension_path']")
            
            if not view_links:
                # 텍스트로 찾기
                all_links = driver.find_elements(By.TAG_NAME, "a")
                view_links = [link for link in all_links if '바로보기' in (link.text or '')]
            
            if view_links:
                view_link = view_links[0]
                onclick_attr = view_link.get_attribute('onclick')
                logger.info(f"Found view link with onclick: {onclick_attr}")
                
                # getExtension_path('49234', '1')에서 매개변수 추출
                match = re.search(r"getExtension_path\s*\(\s*['\"]([\d]+)['\"]?\s*,\s*['\"]([\d]+)['\"]", onclick_attr)
                if match:
                    atch_file_no = match.group(1)
                    file_ord = match.group(2)
                    
                    # 파일 이름 추출 시도
                    try:
                        parent_li = view_link.find_element(By.XPATH, "./ancestor::li")
                        file_name_element = parent_li.find_element(By.TAG_NAME, "a")
                        file_name = file_name_element.text.strip()
                    except:
                        # 파일 이름을 찾을 수 없는 경우, 게시물 제목에서 유추
                        date_match = re.search(r'\((\d{4})년\s+(\d{1,2})월말\s+기준\)', post['title'])
                        if date_match:
                            year = date_match.group(1)
                            month = date_match.group(2).zfill(2)
                            file_name = f"{year}년 {month}월말 기준 통계.xlsx"
                        else:
                            file_name = f"통계자료_{atch_file_no}.xlsx"
                    
                    # 파일 정보 반환 
                    file_info = {
                        'file_name': file_name,
                        'atch_file_no': atch_file_no,
                        'file_ord': file_ord,
                        'use_view': True,
                        'post_info': post
                    }
                    
                    logger.info(f"Successfully extracted file info: {file_info}")
                    return file_info
            
            # 방법 2: 다운로드 링크 직접 찾기
            download_links = driver.find_elements(By.CSS_SELECTOR, "a[onclick*='fn_download']")
            
            if download_links:
                for link in download_links:
                    onclick_attr = link.get_attribute('onclick')
                    match = re.search(r"fn_download\s*\(\s*['\"]([\d]+)['\"]?\s*,\s*['\"]([\d]+)['\"]", onclick_attr)
                    if match:
                        atch_file_no = match.group(1)
                        file_ord = match.group(2)
                        file_name = link.text.strip()
                        
                        file_info = {
                            'file_name': file_name,
                            'atch_file_no': atch_file_no,
                            'file_ord': file_ord,
                            'use_download': True,  # 다운로드 사용 플래그
                            'post_info': post
                        }
                        
                        logger.info(f"Found direct download link: {file_info}")
                        return file_info
            
            # 방법 3: 게시물 내용에서 데이터 추출 시도
            logger.info("No file links found, attempting to extract from content")
            
            # 게시물 내용 영역 찾기
            content_div = driver.find_element(By.CLASS_NAME, "view_cont")
            if content_div:
                content_text = content_div.text
                
                # 날짜 정보 추출 시도
                date_match = re.search(r'\((\d{4})년\s+(\d{1,2})월말\s+기준\)', post['title'])
                if date_match:
                    year = int(date_match.group(1))
                    month = int(date_match.group(2))
                    
                    # 콘텐츠 기반 파일 정보 생성
                    return {
                        'extract_from_content': True,
                        'content_data': content_text,
                        'date': {'year': year, 'month': month},
                        'post_info': post,
                        'file_name': f"{year}년 {month}월말 기준 통계.xlsx"
                    }
            
            logger.warning(f"No file information could be extracted from post: {post['title']}")
            return None
            
        except Exception as e:
            logger.error(f"Error extracting file info: {str(e)}")
            return None
    
 
    def direct_download_file(self, file_info):
        """Directly download file using requests"""
        if not file_info:
            return None
            
        if not (file_info.get('atch_file_no') and file_info.get('file_ord')):
            return None
            
        try:
            atch_file_no = file_info['atch_file_no']
            file_ord = file_info['file_ord']
            
            # 직접 다운로드 URL 구성
            download_url = f"https://www.msit.go.kr/ssm/file/fileDown.do?atchFileNo={atch_file_no}&fileOrd={file_ord}&fileBtn=A"
            
            logger.info(f"Attempting direct download from: {download_url}")
            
            # 요청 헤더 설정
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7'
            }
            
            # 세션을 사용하여 파일 다운로드
            response = self.session.get(download_url, headers=headers, stream=True)
            
            if response.status_code == 200:
                # 파일명 추출 시도
                content_disposition = response.headers.get('Content-Disposition')
                if content_disposition:
                    filename_match = re.search(r'filename=(?:\"?)([^\";\n]+)', content_disposition)
                    if filename_match:
                        filename = filename_match.group(1)
                    else:
                        filename = f"download_{atch_file_no}_{file_ord}.xlsx"
                else:
                    # 파일명이 없는 경우 기본 이름 사용
                    filename = file_info.get('file_name', f"download_{atch_file_no}_{file_ord}.xlsx")
                
                # 안전한 파일명 생성
                safe_filename = "".join(c for c in filename if c.isalnum() or c in "._- ").strip()
                file_path = self.temp_dir / safe_filename
                
                # 파일 저장
                with open(file_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                
                logger.info(f"Successfully downloaded file to: {file_path}")
                return file_path
            else:
                logger.error(f"Failed to download file: HTTP {response.status_code}")
                return None
                
        except Exception as e:
            logger.error(f"Error during direct download: {str(e)}")
            return None

    
    def process_view_data(self, driver, file_info):
        """Process view data without relying on iframe access"""
        if not file_info:
            return None
            
        # 이미 콘텐츠에서 추출한 경우
        if file_info.get('extract_from_content'):
            return self.process_content_data(file_info)
            
        # 바로보기 URL 구성
        if file_info.get('atch_file_no') and file_info.get('file_ord'):
            atch_file_no = file_info['atch_file_no']
            file_ord = file_info['file_ord']
            
            # 1. 직접 파일 다운로드 시도
            if self.direct_download_attempt < 2:  # 최대 2번까지만 시도
                self.direct_download_attempt += 1
                file_path = self.direct_download_file(file_info)
                
                if file_path and file_path.exists():
                    # 다운로드 성공 시 엑셀 파일 처리
                    logger.info(f"Processing downloaded file: {file_path}")
                    return self.process_excel_file(file_path, file_info['post_info'])
            
            # 2. 바로보기 페이지에서 데이터 추출 시도
            view_url = f"https://www.msit.go.kr/bbs/documentView.do?atchFileNo={atch_file_no}&fileOrdr={file_ord}"
            logger.info(f"Accessing view URL: {view_url}")
            
            try:
                # 페이지 로드
                driver.get(view_url)
                time.sleep(5)
                
                # 새 창이 열렸는지 확인
                if len(driver.window_handles) > 1:
                    # 새 창으로 전환
                    driver.switch_to.window(driver.window_handles[-1])
                    logger.info(f"Switched to new window: {driver.current_url}")
                
                # SynapDocViewServer 감지
                current_url = driver.current_url
                if 'SynapDocViewServer' in current_url:
                    logger.info("Detected SynapDocViewServer viewer")
                    
                    # 문서 내용 추출 시도
                    # content_frame = driver.find_element(By.ID, "contents-area")
                    # if content_frame:
                    #     return self.extract_synap_content(driver)
                    
                    # 날짜 정보 추출
                    date_match = re.search(r'\((\d{4})년\s+(\d{1,2})월말\s+기준\)', file_info['post_info']['title'])
                    if date_match:
                        year = int(date_match.group(1))
                        month = int(date_match.group(2))
                        
                        # 문서 제목에서 데이터 유형 추출 시도
                        report_type = "unknown"
                        for rt in self.report_types:
                            if rt in file_info['post_info']['title']:
                                report_type = rt
                                break
                        
                        # 가상 데이터 생성 (실제 데이터를 추출할 수 없는 경우)
                        df = pd.DataFrame({
                            '구분': [f'{month}월 통계'],
                            '값': [f'자동 생성 - {report_type}'],
                            '비고': ['바로보기에서 데이터를 추출할 수 없습니다.']
                        })
                        
                        return {
                            'type': 'dataframe',
                            'data': df,
                            'date': {'year': year, 'month': month},
                            'post_info': file_info['post_info']
                        }
                
                # 일반 HTML 페이지에서 테이블 추출 시도
                tables = pd.read_html(driver.page_source)
                if tables:
                    logger.info(f"Found {len(tables)} tables in the view page")
                    
                    # 가장 큰 테이블 선택
                    largest_table = max(tables, key=lambda df: df.size)
                    
                    # 날짜 정보 추출
                    date_match = re.search(r'\((\d{4})년\s+(\d{1,2})월말\s+기준\)', file_info['post_info']['title'])
                    if date_match:
                        year = int(date_match.group(1))
                        month = int(date_match.group(2))
                        
                        return {
                            'type': 'dataframe',
                            'data': largest_table,
                            'date': {'year': year, 'month': month},
                            'post_info': file_info['post_info']
                        }
                
                # 모든 시도 실패 시 콘텐츠 기반 처리
                return self.process_content_data(file_info)
                
            except Exception as e:
                logger.error(f"Error processing view data: {str(e)}")
                return self.process_content_data(file_info)
            
        return None


    def process_content_data(self, file_info):
        """텍스트 콘텐츠에서 데이터 추출"""
        if not file_info or not file_info.get('post_info'):
            return None
            
        try:
            post_info = file_info['post_info']
            content_text = file_info.get('content_data', '')
            
            # 날짜 정보 추출
            date_match = re.search(r'\((\d{4})년\s+(\d{1,2})월말\s+기준\)', post_info['title'])
            if not date_match:
                logger.error(f"Could not extract date from title: {post_info['title']}")
                return None
                
            year = int(date_match.group(1))
            month = int(date_match.group(2))
            
            # 텍스트에서 테이블 구조 찾기 시도
            lines = content_text.split('\n')
            data_rows = []
            
            # 문자열 처리 - 간단한 규칙 기반 파싱
            for line in lines:
                line = line.strip()
                if not line:
                    continue
                    
                # 숫자가 포함된 행은 데이터 행일 가능성이 높음
                if re.search(r'\d', line) and len(line) > 5:  # 최소 길이 체크
                    cells = re.split(r'\s{2,}|\t', line)
                    if len(cells) >= 2:  # 최소 2개 이상의 셀이 있어야 데이터 행
                        data_rows.append(cells)
            
            # 데이터프레임 생성
            if data_rows:
                if len(data_rows) > 1:
                    # 첫 번째 행을 헤더로 사용
                    df = pd.DataFrame(data_rows[1:], columns=data_rows[0])
                else:
                    # 데이터가 한 행뿐이라면 기본 컬럼명 사용
                    df = pd.DataFrame([data_rows[0]], columns=[f'Column{i}' for i in range(len(data_rows[0]))])
                
                logger.info(f"Created dataframe from content with shape {df.shape}")
            else:
                # 데이터를 찾지 못한 경우 기본 데이터프레임 생성
                logger.warning("No structured data found in content, creating placeholder dataframe")
                
                # 게시물 제목에서 통계 유형 추출
                report_type = "통계"
                for rt in self.report_types:
                    if rt in post_info['title']:
                        report_type = rt
                        break
                
                df = pd.DataFrame({
                    '구분': [f'{month}월 {report_type}'],
                    '값': ['데이터를 추출할 수 없습니다'],
                    '비고': [post_info['title']]
                })
            
            return {
                'type': 'dataframe',
                'data': df,
                'date': {'year': year, 'month': month},
                'post_info': post_info
            }
            
        except Exception as e:
            logger.error(f"Error processing content data: {str(e)}")
            return None

    
    def process_excel_file(self, file_path, post_info):
        """Process the downloaded Excel/CSV file"""
        if not file_path or not file_path.exists():
            logger.error("Cannot process file: file not found")
            return None
        
        logger.info(f"Processing file: {file_path}")
        
        try:
            # Extract date from post title
            date_pattern = r'\((\d{4})년\s+(\d{1,2})월말\s+기준\)'
            match = re.search(date_pattern, post_info['title'])
            
            if not match:
                logger.error(f"Could not extract date from title: {post_info['title']}")
                return None
                
            year = int(match.group(1))
            month = int(match.group(2))
            
            # Read the file based on extension
            if file_path.suffix.lower() in ['.xlsx', '.xls']:
                # Read all sheets from Excel file
                xls = pd.ExcelFile(file_path)
                sheet_names = xls.sheet_names
                
                data = {}
                for sheet in sheet_names:
                    data[sheet] = pd.read_excel(file_path, sheet_name=sheet)
                    
                logger.info(f"Successfully processed Excel file with {len(sheet_names)} sheets")
                return {
                    'type': 'excel',
                    'sheets': data,
                    'date': {'year': year, 'month': month},
                    'post_info': post_info
                }
            elif file_path.suffix.lower() == '.csv':
                df = pd.read_csv(file_path)
                logger.info(f"Successfully processed CSV file with {len(df)} rows")
                return {
                    'type': 'csv',
                    'data': df,
                    'date': {'year': year, 'month': month},
                    'post_info': post_info
                }
            else:
                logger.error(f"Unsupported file format: {file_path.suffix}")
                return None
                
        except Exception as e:
            logger.error(f"Error processing file: {str(e)}")
            return None
        finally:
            # Clean up downloaded file
            try:
                os.unlink(file_path)
            except Exception as e:
                logger.warning(f"Could not delete temporary file {file_path}: {str(e)}")

    def determine_report_type(self, post_title):
        """Determine the report type from the post title"""
        for report_type in self.report_types:
            if report_type in post_title:
                return report_type
        return "기타 통신 통계"

    
    def update_google_sheets(self, client, data):
        """Update Google Sheets with improved data handling"""
        if not client or not data:
            logger.error("Cannot update Google Sheets: missing client or data")
            return False

        try:
            # Extract information
            date_info = data['date']
            post_info = data['post_info']
            report_type = self.determine_report_type(post_info['title'])
    
            # Format date string
            date_str = f"{date_info['year']}년 {date_info['month']}월"
    
            # Open or create spreadsheet
            try:
                # Try to open by ID first if provided
                if self.spreadsheet_id:
                    try:
                        spreadsheet = client.open_by_key(self.spreadsheet_id)
                        logger.info(f"Found existing spreadsheet by ID: {self.spreadsheet_id}")
                    except gspread.exceptions.APIError:
                        logger.warning(f"Could not open spreadsheet with ID: {self.spreadsheet_id}")
                        spreadsheet = None
                else:
                    spreadsheet = None
        
                # If not found by ID, try by name
                if not spreadsheet:
                    try:
                        spreadsheet = client.open(self.spreadsheet_name)
                        logger.info(f"Found existing spreadsheet by name: {self.spreadsheet_name}")
                    except gspread.exceptions.SpreadsheetNotFound:
                        # Create new spreadsheet
                        spreadsheet = client.create(self.spreadsheet_name)
                        logger.info(f"Created new spreadsheet: {self.spreadsheet_name}")
                
                        # Log the ID for future reference
                        logger.info(f"New spreadsheet ID: {spreadsheet.id}")
            except Exception as e:
                logger.error(f"Error opening Google Sheets: {str(e)}")
                return False
        
            # Find or create worksheet for this report type
            worksheet = None
            for sheet in spreadsheet.worksheets():
                if report_type in sheet.title:
                    worksheet = sheet
                    break
            
            if not worksheet:
                worksheet = spreadsheet.add_worksheet(title=report_type, rows="1000", cols="50")
                logger.info(f"Created new worksheet: {report_type}")
        
                # Add header row
                worksheet.update_cell(1, 1, "항목")
        
            # Check if date column already exists
            headers = worksheet.row_values(1)
            if date_str in headers:
                col_idx = headers.index(date_str) + 1
                logger.info(f"Column for {date_str} already exists at position {col_idx}")
            else:
                # Add new column for this date
                col_idx = len(headers) + 1
                worksheet.update_cell(1, col_idx, date_str)
                logger.info(f"Added new column for {date_str} at position {col_idx}")
    
        # Process data based on file type
            if data['type'] == 'dataframe':
                # 데이터프레임 직접 사용
                df = data['data']
                self.update_sheet_from_dataframe(worksheet, df, col_idx)
        
            elif data['type'] == 'excel':
                # 엑셀 파일에서 시트 선택
                if len(data['sheets']) == 1:
                    # If only one sheet, use it
                    sheet_name = list(data['sheets'].keys())[0]
                    df = data['sheets'][sheet_name]
                else:
                    # Try to find most relevant sheet
                    best_sheet = None
                    for name in data['sheets'].keys():
                        if report_type in name or any(term in name for term in report_type.split()):
                            best_sheet = name
                            break
            
                    if not best_sheet:
                        # Use first sheet as fallback
                        best_sheet = list(data['sheets'].keys())[0]
                    
                    df = data['sheets'][best_sheet]
            
                # Update data from dataframe
                self.update_sheet_from_dataframe(worksheet, df, col_idx)
            
            elif data['type'] == 'csv':
                # Update from CSV data
                df = data['data']
                self.update_sheet_from_dataframe(worksheet, df, col_idx)
        
            logger.info(f"Successfully updated Google Sheets with {report_type} data for {date_str}")
            return True
    
        except Exception as e:
            logger.error(f"Error updating Google Sheets: {str(e)}")
            return False


    def update_sheet_from_dataframe(self, worksheet, df, col_idx):
        """Update worksheet with data from a dataframe with improved error handling"""
        try:
            # Get current row labels (first column)
            existing_labels = worksheet.col_values(1)[1:]  # Skip header
        
            if df.shape[0] > 0:
                # Get labels and values from dataframe
                # Assuming first column contains labels and second contains values
                if df.shape[1] >= 2:
                    # 열 이름 정규화 (공백, 특수문자 제거 등)
                    normalized_columns = [str(col).strip() for col in df.columns]
                    
                    # 첫 번째 열을 라벨로, 두 번째 열을 값으로 사용
                    if df.shape[1] >= 2:
                        # 명확한 컬럼 선택
                        label_col = df.iloc[:, 0]
                        value_col = df.iloc[:, 1]
                    
                        new_labels = label_col.astype(str).tolist()
                        values = value_col.astype(str).tolist()
                    
                        # Batch update preparation
                        cell_updates = []
                    
                        for i, (label, value) in enumerate(zip(new_labels, values)):
                            if label and not pd.isna(label):  # Skip empty or NaN labels
                            # 라벨 정규화 (특수문자 및 공백 처리)
                                label = str(label).strip()
                            
                                # Check if label already exists
                                if label in existing_labels:
                                    row_idx = existing_labels.index(label) + 2  # +2 for header and 0-indexing
                                else:
                                    # Add new row
                                    row_idx = len(existing_labels) + 2
                                    # Update label
                                    cell_updates.append({
                                        'range': f'A{row_idx}',
                                        'values': [[label]]
                                    })
                                    existing_labels.append(label)
                                
                            # Update value - NaN 처리
                                value_to_update = "" if pd.isna(value) else str(value).strip()
                                cell_updates.append({
                                    'range': f'{chr(64 + col_idx)}{row_idx}',
                                    'values': [[value_to_update]]
                                })
                    
                    # Execute batch update if there are updates
                        if cell_updates:
                            worksheet.batch_update(cell_updates)
                            logger.info(f"Updated {len(cell_updates)} cells in Google Sheets")
                            
                return True
            
            else:
                logger.warning("DataFrame is empty, no data to update")
                return False
            
        except Exception as e:
            logger.error(f"Error updating worksheet from dataframe: {str(e)}")
            return False


    async def send_telegram_message(self, posts, data_updates=None):
        """Send notification via Telegram with improved formatting"""
        if not posts and not data_updates:
            logger.info("No posts or data updates to notify about")
            return
        
        try:
            message = "📊 *MSIT 통신 통계 모니터링 알림*\n\n"
                
                # Add information about new posts
            if posts:
                message += "📱 *새로운 통신 관련 게시물:*\n\n"
            
                for post in posts:
                    message += f"📅 {post['date']}\n"
                    message += f"📑 {post['title']}\n"
                    message += f"🏢 {post['department']}\n"
                    if post.get('url'):
                        message += f"🔗 [게시물 바로가기]({post['url']})\n"
                    message += "\n"
        
                # Add information about data updates
            if data_updates:
                message += "📊 *Google Sheets 데이터 업데이트 완료:*\n\n"
            
                for update in data_updates:
                    report_type = self.determine_report_type(update['post_info']['title'])
                    date_str = f"{update['date']['year']}년 {update['date']['month']}월"
                
                    message += f"📅 *{date_str}*\n"
                    message += f"📑 {report_type}\n"
                    message += f"📗 업데이트 완료\n\n"
        
                # Send the message
            chat_id = int(self.chat_id)
            await self.bot.send_message(
                chat_id=chat_id,
                text=message,
                parse_mode='Markdown'
            )
            logger.info("Telegram message sent successfully")
        
        except Exception as e:
            logger.error(f"Error sending Telegram message: {str(e)}")


    async def run_monitor(self, days_range=4, check_sheets=True):
            """Main monitoring function with improved error handling"""
            driver = None
            gs_client = None

            try:
                # Initialize WebDriver
                driver = self.setup_driver()
                logger.info("WebDriver initialized successfully")
    
                # Initialize Google Sheets client if needed
                if check_sheets and self.gspread_creds:
                    gs_client = self.setup_gspread_client()
                    if gs_client:
                        logger.info("Google Sheets client initialized successfully")
                    else:
                        logger.warning("Failed to initialize Google Sheets client")
    
                # Navigate to MSIT website
                driver.get(self.url)
                logger.info("Navigated to MSIT website")
    
                # Variables to track posts
                all_posts = []
                telecom_stats_posts = []
                continue_search = True
    
                # Parse pages
                while continue_search:
                    posts, stats_posts, should_continue = self.parse_page(driver, days_range=days_range)
                    all_posts.extend(posts)
                    telecom_stats_posts.extend(stats_posts)
        
                    if not should_continue:
                        break
                    
                    if self.has_next_page(driver):
                        if not self.go_to_next_page(driver):
                            break
                    else:
                        break
    
                # Process telecom stats posts if Google Sheets client is available
                data_updates = []
                if gs_client and telecom_stats_posts and check_sheets:
                    logger.info(f"Processing {len(telecom_stats_posts)} telecom stats posts")

                    for post in telecom_stats_posts:
                        # 1. 파일 정보 추출
                        file_info = self.extract_file_info(driver, post)
                        if not file_info:
                            logger.warning(f"No file information found for post: {post['title']}")
                            continue
                
                        # 2. 데이터 처리 - 바로보기 또는 직접 다운로드 시도
                        self.direct_download_attempt = 0  # 시도 횟수 초기화
                        data = self.process_view_data(driver, file_info)
                
                        if data:
                            # 3. Google Sheets 업데이트
                            success = self.update_google_sheets(gs_client, data)
                            if success:
                                logger.info(f"Successfully updated Google Sheets for: {post['title']}")
                                data_updates.append(data)
                            else:
                                logger.warning(f"Failed to update Google Sheets for: {post['title']}")
                        else:
                            logger.warning(f"Failed to extract data for post: {post['title']}")
        
                # Send Telegram notification if there are new posts or data updates
                if all_posts or data_updates:
                    await self.send_telegram_message(all_posts, data_updates)
                else:
                    logger.info(f"No new posts found within the last {days_range} days")
    
            except Exception as e:
                error_message = f"Error in run_monitor: {str(e)}"
                logger.error(error_message, exc_info=True)
    
                # Send error notification
                try:
                    await self.send_telegram_message([{
                        'title': f"모니터링 중 오류 발생: {str(e)}",
                        'date': datetime.now().strftime('%Y. %m. %d'),
                        'department': 'System Error'
                    }])
                except Exception as telegram_err:
                    logger.error(f"Error sending Telegram notification: {str(telegram_err)}")
    
            finally:
        # Clean up
                if driver:
                    driver.quit()
                    logger.info("WebDriver closed")

def extract_file_info(self, driver, post):
    """Extract file information using the 'View' button instead of download"""
    if not post.get('post_id'):
        logger.error(f"Cannot access post {post['title']} - missing post ID")
        return None
    
    logger.info(f"Opening post: {post['title']}")
    
    # 게시물 상세 페이지로 이동
    detail_url = f"https://www.msit.go.kr/bbs/view.do?sCode=user&mId=99&mPid=74&nttSeqNo={post['post_id']}"
    driver.get(detail_url)
    time.sleep(3)  # 페이지 로드 대기
    
    # 현재 페이지 URL 확인
    current_url = driver.current_url
    logger.info(f"Current page URL: {current_url}")
    
    # 바로보기 링크 찾기
    try:
        # 1. 클래스로 찾기
        view_links = driver.find_elements(By.CSS_SELECTOR, "a.view[title='새창 열림']")
        
        # 2. 다른 방법: onclick 속성에 getExtension_path가 포함된 링크 찾기
        if not view_links:
            all_links = driver.find_elements(By.TAG_NAME, "a")
            view_links = [link for link in all_links if 'getExtension_path' in (link.get_attribute('onclick') or '')]
        
        # 3. 텍스트로 찾기
        if not view_links:
            all_links = driver.find_elements(By.TAG_NAME, "a")
            view_links = [link for link in all_links if '바로보기' in (link.text or '')]
        
        # 적절한 링크 발견
        if view_links:
            view_link = view_links[0]
            onclick_attr = view_link.get_attribute('onclick')
            logger.info(f"Found view link with onclick: {onclick_attr}")
            
            # getExtension_path('49234', '1')에서 매개변수 추출
            match = re.search(r"getExtension_path\('(\d+)',\s*'(\d+)'\)", onclick_attr)
            if match:
                atch_file_no = match.group(1)
                file_ord = match.group(2)
                
                # 파일 이름은 부모 요소에서 추출할 수 있음
                try:
                    parent_li = view_link.find_element(By.XPATH, "./ancestor::li")
                    file_name_element = parent_li.find_element(By.TAG_NAME, "a")
                    file_name = file_name_element.text.strip()
                except:
                    # 파일 이름을 찾을 수 없는 경우, 게시물 제목에서 유추
                    date_match = re.search(r'\((\d{4})년\s+(\d{1,2})월말\s+기준\)', post['title'])
                    if date_match:
                        year = date_match.group(1)
                        month = date_match.group(2).zfill(2)
                        file_name = f"{year}년 {month}월말 기준 통계.xlsx"
                    else:
                        file_name = f"통계자료_{atch_file_no}.xlsx"
                
                file_info = {
                    'file_name': file_name,
                    'atch_file_no': atch_file_no,
                    'file_ord': file_ord,
                    'use_view': True  # 바로보기 사용 플래그
                }
                
                logger.info(f"Successfully extracted file info for view: {file_name}")
                return file_info
            else:
                logger.error(f"Could not extract file params from onclick: {onclick_attr}")
        else:
            logger.warning("No view links found")
            
            # 페이지 구조 로깅
            logger.info("Available link elements:")
            all_links = driver.find_elements(By.TAG_NAME, "a")
            for i, link in enumerate(all_links[:10]):  # 처음 10개만 로깅
                logger.info(f"Link {i+1}: text='{link.text}', onclick='{link.get_attribute('onclick')}'")
    
    except Exception as e:
        logger.error(f"Error finding view link: {str(e)}")
    
    return None

def access_view_page(self, driver, file_info):
    """Access the view page instead of downloading the file"""
    if not file_info or not file_info.get('use_view'):
        return None
    
    logger.info(f"Accessing view page for file: {file_info['file_name']}")
    
    # 바로보기 URL 구성
    view_url = f"https://www.msit.go.kr/bbs/documentView.do?atchFileNo={file_info['atch_file_no']}&fileOrdr={file_info['file_ord']}"
    
    logger.info(f"View URL: {view_url}")
    driver.get(view_url)
    
    # 새 창에서 열리는 경우 핸들 전환
    original_window = driver.current_window_handle
    if len(driver.window_handles) > 1:
        for window_handle in driver.window_handles:
            if window_handle != original_window:
                driver.switch_to.window(window_handle)
                logger.info(f"Switched to new window: {driver.current_url}")
                break
    
    # 뷰어 페이지 로드 대기
    try:
        # 몇 가지 가능한 요소 대기
        WebDriverWait(driver, 20).until(
            lambda x: (
                len(x.find_elements(By.ID, "mainTable")) > 0 or  # 테이블 형식
                len(x.find_elements(By.TAG_NAME, "table")) > 0 or  # 일반 테이블
                len(x.find_elements(By.TAG_NAME, "iframe")) > 0  # iframe 내 콘텐츠
            )
        )
        
        logger.info("View page loaded successfully")
        return True
    except TimeoutException:
        logger.error("Timeout waiting for view page to load")
        
        # 페이지 소스 로깅
        logger.info("Current URL: " + driver.current_url)
        logger.info("Page source snippet:")
        logger.info(driver.page_source[:500])
        
        return False
    except Exception as e:
        logger.error(f"Error accessing view page: {str(e)}")
        return False

def extract_data_from_view(self, driver):
    """Extract data from the view page"""
    try:
        # 페이지에 iframe이 있는지 확인
        iframes = driver.find_elements(By.TAG_NAME, "iframe")
        if iframes:
            logger.info(f"Found {len(iframes)} iframes on the page")
            # iframe으로 전환
            driver.switch_to.frame(iframes[0])
            logger.info("Switched to iframe")
        
        # 데이터 추출 시도
        tables = []
        
        # 1. mainTable 요소 확인 (MSIT 특화 구조)
        main_table = driver.find_elements(By.ID, "mainTable")
        if main_table:
            logger.info("Found mainTable element")
            
            # mainTable 내의 모든 div 요소를 찾아 테이블 형태로 재구성
            rows = []
            current_row = []
            row_count = 0
            
            divs = main_table[0].find_elements(By.TAG_NAME, "div")
            for div in divs:
                # 클래스 이름으로 셀 유형 구분
                class_name = div.get_attribute("class") or ""
                
                # 셀 데이터 추출
                cell_text = div.text.strip()
                
                # td 클래스가 있으면 데이터 셀
                if "td" in class_name:
                    current_row.append(cell_text)
                
                # tr 클래스가 있으면 행 구분
                if "tr" in class_name or "row" in class_name:
                    row_count += 1
                    if current_row:
                        rows.append(current_row)
                        current_row = []
            
            # 마지막 행 추가
            if current_row:
                rows.append(current_row)
            
            # 데이터프레임 생성
            if rows:
                df = pd.DataFrame(rows)
                tables.append(df)
                logger.info(f"Extracted table with shape {df.shape}")
        
        # 2. 일반 HTML 테이블 확인
        html_tables = driver.find_elements(By.TAG_NAME, "table")
        if html_tables:
            logger.info(f"Found {len(html_tables)} HTML tables")
            
            for i, table in enumerate(html_tables):
                rows = []
                
                # 헤더 추출
                headers = table.find_elements(By.TAG_NAME, "th")
                if headers:
                    header_row = [h.text.strip() for h in headers]
                    rows.append(header_row)
                
                # 데이터 행 추출
                tr_elements = table.find_elements(By.TAG_NAME, "tr")
                for tr in tr_elements:
                    # td 요소 추출
                    cells = tr.find_elements(By.TAG_NAME, "td")
                    if cells:
                        row = [cell.text.strip() for cell in cells]
                        rows.append(row)
                
                # 데이터프레임 생성
                if rows:
                    df = pd.DataFrame(rows[1:], columns=rows[0] if rows else None)
                    tables.append(df)
                    logger.info(f"Extracted HTML table {i+1} with shape {df.shape}")
        
        # 데이터프레임이 없는 경우 모든 텍스트를 추출하여 파싱 시도
        if not tables:
            logger.warning("No tables found, attempting to extract structured text")
            
            # 모든 텍스트 콘텐츠 가져오기
            body_text = driver.find_element(By.TAG_NAME, "body").text
            
            # 줄 단위로 분할
            lines = body_text.split('\n')
            
            # 데이터 행 인식 (숫자와 텍스트가 포함된 행)
            data_rows = []
            for line in lines:
                # 숫자와 문자가 모두 포함된 행을 데이터로 가정
                if re.search(r'\d', line) and re.search(r'[가-힣a-zA-Z]', line):
                    # 공백이나 탭으로 분할
                    cells = re.split(r'\s{2,}|\t', line)
                    if len(cells) >= 2:  # 최소 2개 이상의 셀이 있어야 데이터 행
                        data_rows.append(cells)
            
            if data_rows:
                # 첫 번째 행을 헤더로 가정
                df = pd.DataFrame(data_rows[1:], columns=data_rows[0] if data_rows else None)
                tables.append(df)
                logger.info(f"Extracted structured text as table with shape {df.shape}")
        
        # 추출된 테이블이 있는지 확인
        if tables:
            # 가장 큰 테이블 선택 (가장 많은 데이터를 포함)
            largest_table = max(tables, key=lambda df: df.size)
            
            logger.info(f"Selected largest table with shape {largest_table.shape}")
            logger.info(f"Table columns: {largest_table.columns.tolist()}")
            logger.info(f"First few rows: {largest_table.head(3).to_dict()}")
            
            return largest_table
        else:
            logger.error("No data could be extracted from the view page")
            return None
        
    except Exception as e:
        logger.error(f"Error extracting data from view: {str(e)}")
        return None
    finally:
        # iframe에서 빠져나옴
        try:
            driver.switch_to.default_content()
        except:
            pass


async def main():
    # Get environment variables
    days_range = int(os.environ.get('DAYS_RANGE', '4'))
    check_sheets = os.environ.get('CHECK_SHEETS', 'true').lower() == 'true'
    

    # Create and run monitor
    try:
        logger.info(f"Starting MSIT Monitor with days_range={days_range}, check_sheets={check_sheets}")
        logger.info(f"Spreadsheet name: {os.environ.get('SPREADSHEET_NAME', 'MSIT 통신 통계')}")
        
        monitor = MSITMonitor()
        await monitor.run_monitor(days_range=days_range, check_sheets=check_sheets)
    except Exception as e:
        logging.error(f"Main function error: {str(e)}", exc_info=True)

if __name__ == "__main__":
    asyncio.run(main())
