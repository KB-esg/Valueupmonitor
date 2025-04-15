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
from selenium.common.exceptions import TimeoutException, NoSuchElementException

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

    def setup_driver(self):
        """Initialize Selenium WebDriver"""
        chrome_options = Options()
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--disable-gpu')
        
        # Set download preferences
        prefs = {
            "download.default_directory": str(self.temp_dir.absolute()),
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": False
        }
        chrome_options.add_experimental_option("prefs", prefs)
        
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
                    # "YYYY-MM-DD" format
                    post_date = datetime.strptime(date_str, '%Y-%m-%d').date()
            
            # Calculate date range (Korean timezone)
            korea_tz = datetime.now() + timedelta(hours=9)  # UTC to KST
            days_ago = (korea_tz - timedelta(days=days)).date()
            
            logger.info(f"게시물 날짜 확인: {post_date} vs {days_ago} ({days}일 전, 한국 시간 기준)")
            return post_date >= days_ago
            
        except Exception as e:
            logger.error(f"날짜 파싱 에러: {str(e)}")
            return False

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
        """Extract Excel/CSV file information from a post"""
        if not post.get('post_id'):
            logger.error(f"Cannot access post {post['title']} - missing post ID")
            return None
    
        logger.info(f"Opening post: {post['title']}")
    
    # 최대 3번까지 재시도
        max_retries = 3
        for attempt in range(max_retries):
            try:
                # Navigate to the post detail page
                detail_url = f"https://www.msit.go.kr/bbs/view.do?sCode=user&mId=99&mPid=74&nttSeqNo={post['post_id']}"
                driver.get(detail_url)
            
                # 명시적인 대기 시간 추가
                time.sleep(3)
            
                # 여러 가능한 요소 중 하나라도 로드되면 성공으로 간주
                WebDriverWait(driver, 20).until(
                    lambda x: len(x.find_elements(By.CLASS_NAME, "view_head")) > 0 or 
                             len(x.find_elements(By.CLASS_NAME, "view_file")) > 0
                )
                break  # 성공하면 루프 종료
            except TimeoutException:
                if attempt < max_retries - 1:
                    logger.warning(f"Timeout loading page, retry attempt {attempt+1}/{max_retries}")
                    time.sleep(3)  # 재시도 전 3초 대기
                else:
                    logger.error(f"Failed to load page after {max_retries} attempts")
                    return None
            except Exception as e:
                logger.error(f"Error accessing post detail: {str(e)}")
                return None
    
        # Look for Excel/CSV file attachments
        try:
            download_links = driver.find_elements(By.CSS_SELECTOR, ".down_file li a.down")
            excel_file_link = None
        
            for link in download_links:
                if any(ext in link.text.lower() for ext in ['.xlsx', '.xls', '.csv']):
                    excel_file_link = link
                    break
            
            if excel_file_link:
                # Get file name
                parent_element = excel_file_link.find_element(By.XPATH, "./..")
                file_link = parent_element.find_element(By.CSS_SELECTOR, "a:first-child")
                file_name = file_link.text.strip()
            
                # Get onclick attribute
                onclick_attr = file_link.get_attribute("onclick")
            
                # Extract atchFileNo and fileOrd
                match = re.search(r"(?:fn_download|getExtension_path)\('(\d+)',\s*'(\d+)'", onclick_attr)
                if match:
                    atch_file_no = match.group(1)
                    file_ord = match.group(2)
                
                    file_info = {
                        'file_name': file_name,
                        'atch_file_no': atch_file_no,
                        'file_ord': file_ord
                    }
                
                    logger.info(f"Found file: {file_name}")
                    return file_info
                else:
                    logger.error("Could not extract file parameters from onclick attribute")
            else:
                logger.warning("No Excel/CSV file found in attachments")
            
        except NoSuchElementException as e:
            logger.error(f"Error finding file attachment: {str(e)}")
        except Exception as e:
            logger.error(f"Unexpected error processing file attachment: {str(e)}")
        
        return None


    

    def download_file(self, driver, file_info):
        """Download a file from MSIT website"""
        if not file_info:
            return None
        
        logger.info(f"Downloading file: {file_info['file_name']}")
        
        # Construct download URL
        download_url = f"https://www.msit.go.kr/ssm/file/fileDown.do?atchFileNo={file_info['atch_file_no']}&fileOrd={file_info['file_ord']}&fileBtn=A"
        
        # Navigate to download URL
        driver.get(download_url)
        
        # Wait for download to complete
        timeout = 30  # seconds
        start_time = time.time()
        
        # Generate safe filename
        safe_filename = "".join(c for c in file_info['file_name'] if c.isalnum() or c in "._- ").strip()
        
        # Wait for file to appear in downloads directory
        while time.time() - start_time < timeout:
            # Check if any file exists in the downloads directory
            files = list(self.temp_dir.glob("*"))
            if files:
                downloaded_file = files[0]  # Take the first file
                logger.info(f"File downloaded: {downloaded_file}")
                return downloaded_file
            
            time.sleep(1)
        
        logger.error("Download timeout")
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
        """Update Google Sheets with the processed data"""
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
            if data['type'] == 'excel':
                # Determine which sheet to use
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
        """Update worksheet with data from a dataframe"""
        try:
            # Get current row labels (first column)
            existing_labels = worksheet.col_values(1)[1:]  # Skip header
            
            if df.shape[0] > 0:
                # Get labels and values from dataframe
                # Assuming first column contains labels and second contains values
                if df.shape[1] >= 2:
                    new_labels = df.iloc[:, 0].tolist()
                    values = df.iloc[:, 1].tolist()
                    
                    # Batch update preparation
                    cell_updates = []
                    
                    for i, (label, value) in enumerate(zip(new_labels, values)):
                        if label:  # Skip empty labels
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
                            
                            # Update value
                            cell_updates.append({
                                'range': f'{chr(64 + col_idx)}{row_idx}',
                                'values': [[value]]
                            })
                    
                    # Execute batch update if there are updates
                    if cell_updates:
                        worksheet.batch_update(cell_updates)
                        
            return True
            
        except Exception as e:
            logger.error(f"Error updating worksheet from dataframe: {str(e)}")
            return False

    async def send_telegram_message(self, posts, data_updates=None):
        """Send notification via Telegram"""
        if not posts and not data_updates:
            logger.info("No posts or data updates to notify about")
            return
            
        try:
            message = "📊 MSIT 통신 통계 모니터링 알림\n\n"
            
            # Add information about new posts
            if posts:
                message += "📱 새로운 통신 관련 게시물:\n\n"
                
                for post in posts:
                    message += f"📅 {post['date']}\n"
                    message += f"📑 {post['title']}\n"
                    message += f"🏢 {post['department']}\n"
                    if post.get('url'):
                        message += f"🔗 <a href='{post['url']}'>게시물 바로가기</a>\n"
                    message += "\n"
            
            # Add information about data updates
            if data_updates:
                message += "📊 Google Sheets 데이터 업데이트 완료:\n\n"
                
                for update in data_updates:
                    report_type = self.determine_report_type(update['post_info']['title'])
                    date_str = f"{update['date']['year']}년 {update['date']['month']}월"
                    
                    message += f"📅 {date_str}\n"
                    message += f"📑 {report_type}\n"
                    message += f"📗 업데이트 완료\n\n"
            
            # Send the message
            chat_id = int(self.chat_id)
            await self.bot.send_message(
                chat_id=chat_id,
                text=message,
                parse_mode='HTML'
            )
            logger.info("Telegram message sent successfully")
            
        except Exception as e:
            logger.error(f"Error sending Telegram message: {str(e)}")
            raise

    async def run_monitor(self, days_range=4, check_sheets=True):
        """Main monitoring function"""
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
                    # Extract file info
                    file_info = self.extract_file_info(driver, post)
                    if not file_info:
                        logger.warning(f"No file information found for post: {post['title']}")
                        continue
                
                    # Download the file
                    file_path = self.download_file(driver, file_info)
                    if not file_path:
                        logger.warning(f"Failed to download file for post: {post['title']}")
                        continue
                
                    # Process the file
                    data = self.process_excel_file(file_path, post)
                    if not data:
                        logger.warning(f"Failed to process file for post: {post['title']}")
                        continue
                
                    # Update Google Sheets
                    success = self.update_google_sheets(gs_client, data)
                    if success:
                        logger.info(f"Successfully updated Google Sheets for: {post['title']}")
                        data_updates.append(data)
                    else:
                        logger.warning(f"Failed to update Google Sheets for: {post['title']}")
        
            # Send Telegram notification if there are new posts or data updates
            if all_posts or data_updates:
                await self.send_telegram_message(all_posts, data_updates)
            else:
                logger.info(f"No new posts found within the last {days_range} days")
        
        except Exception as e:
            error_message = f"Error in run_monitor: {str(e)}"
            logger.error(error_message, exc_info=True)
        
            # Send error notification
            await self.send_telegram_message([{
                'title': f"모니터링 중 오류 발생: {str(e)}",
                'date': datetime.now().strftime('%Y. %m. %d'),
                'department': 'System Error'
            }])
        
        finally:
            # Clean up
            if driver:
                driver.quit()
                logger.info("WebDriver closed")


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
