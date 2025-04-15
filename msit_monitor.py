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
from selenium.common.exceptions import TimeoutException, NoSuchElementException

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
        # 기존 초기화 코드 유지...
        self.telegram_token = os.environ.get('TELCO_NEWS_TOKEN')
        self.chat_id = os.environ.get('TELCO_NEWS_TESTER')
        
        self.gspread_creds = os.environ.get('MSIT_GSPREAD_ref')
        self.spreadsheet_id = os.environ.get('MSIT_SPREADSHEET_ID')
        self.spreadsheet_name = os.environ.get('SPREADSHEET_NAME', 'MSIT 통신 통계')
        
        self.url = "https://www.msit.go.kr/bbs/list.do?sCode=user&mPid=74&mId=99"
        self.bot = telegram.Bot(token=self.telegram_token)
        
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
        """Selenium WebDriver 설정"""
        options = Options()
        options.add_argument('--headless')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-gpu')
        
        # 이미지 로딩 비활성화로 성능 향상
        prefs = {
            "profile.default_content_setting_values.images": 2
        }
        options.add_experimental_option("prefs", prefs)
        
        # 서비스 설정
        if os.path.exists('/usr/bin/chromium-browser'):
            options.binary_location = '/usr/bin/chromium-browser'
            service = Service('/usr/bin/chromedriver')
        else:
            try:
                from webdriver_manager.chrome import ChromeDriverManager
                service = Service(ChromeDriverManager().install())
            except ImportError:
                service = Service('/usr/bin/chromedriver')
        
        driver = webdriver.Chrome(service=service, options=options)
        driver.set_page_load_timeout(60)
        return driver

    def is_telecom_stats_post(self, title):
        """통신 통계 게시물인지 확인"""
        date_pattern = r'\((\d{4})년\s+(\d{1,2})월말\s+기준\)'
        has_date_pattern = re.search(date_pattern, title) is not None
        contains_report_type = any(report_type in title for report_type in self.report_types)
        return has_date_pattern and contains_report_type

    def parse_board(self, driver, days_range=4):
        """게시판 파싱 및 관련 게시물 추출"""
        logger.info("과기정통부 통신 통계 게시판 접근 중...")
        driver.get(self.url)
        
        try:
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.CLASS_NAME, 'board_list'))
            )
        except TimeoutException:
            # 시스템 점검 페이지 확인
            if "시스템 점검 안내" in driver.page_source:
                logger.warning("시스템 점검 중입니다.")
                return []
            logger.error("게시판 로딩 타임아웃")
            return []
            
        all_posts = []
        telecom_posts = []
        
        # 페이지 파싱
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        items = soup.select('div.toggle')
        
        for item in items:
            title_tag = item.find('p', class_='title')
            date_tag = item.find('div', class_='date')
            dept_tag = item.find('dd', {'id': lambda x: x and 'td_CHRG_DEPT_NM' in x})
            
            if not title_tag or not date_tag:
                continue
                
            title = title_tag.text.strip()
            date_str = date_tag.text.strip()
            dept = dept_tag.text.strip() if dept_tag else "부서 정보 없음"
            
            # 날짜 확인
            try:
                post_date = self.parse_date(date_str)
                cutoff_date = (datetime.now() - timedelta(days=days_range)).date()
                if post_date < cutoff_date:
                    continue
            except ValueError:
                # 날짜 파싱 실패 시 포함 (정확한 필터링을 위해)
                logger.warning(f"날짜 파싱 실패: {date_str}")
            
            # 게시물 ID 추출
            onclick = title_tag.find('a').get('onclick', '')
            match = re.search(r"fn_detail\((\d+)\)", onclick)
            if not match:
                continue
                
            post_id = match.group(1)
            post_url = f"https://www.msit.go.kr/bbs/view.do?sCode=user&mId=99&mPid=74&nttSeqNo={post_id}"
            
            post_info = {
                'title': title,
                'date': date_str,
                'department': dept,
                'post_id': post_id,
                'url': post_url
            }
            
            all_posts.append(post_info)
            
            # 통신 통계 게시물 분류
            if self.is_telecom_stats_post(title):
                telecom_posts.append(post_info)
                logger.info(f"통신 통계 게시물 발견: {title}")
        
        return all_posts, telecom_posts

    def parse_date(self, date_str):
        """다양한 형식의 날짜 문자열 파싱"""
        date_str = date_str.replace(',', ' ').strip()
        
        # 다양한 형식 시도
        formats = [
            '%Y. %m. %d',  # 2025. 4. 10
            '%b %d %Y',    # Apr 10 2025
            '%Y-%m-%d'     # 2025-04-10
        ]
        
        for fmt in formats:
            try:
                return datetime.strptime(date_str, fmt).date()
            except ValueError:
                continue
                
        # 정규식으로 시도
        match = re.search(r'(\d{4})[.\-\s]+(\d{1,2})[.\-\s]+(\d{1,2})', date_str)
        if match:
            year, month, day = map(int, match.groups())
            return datetime(year, month, day).date()
            
        raise ValueError(f"날짜 형식을 파싱할 수 없음: {date_str}")

    def extract_file_info(self, driver, post):
        """게시물에서 파일 정보 추출"""
        if not post.get('post_id'):
            return None
            
        logger.info(f"게시물 열기: {post['title']}")
        
        detail_url = f"https://www.msit.go.kr/bbs/view.do?sCode=user&mId=99&mPid=74&nttSeqNo={post['post_id']}"
        driver.get(detail_url)
        
        try:
            # 페이지 로드 대기
            WebDriverWait(driver, 15).until(
                lambda x: (
                    len(x.find_elements(By.CLASS_NAME, "view_head")) > 0 or 
                    len(x.find_elements(By.CLASS_NAME, "view_file")) > 0
                )
            )
        except TimeoutException:
            # 시스템 점검 페이지 확인
            if "시스템 점검 안내" in driver.page_source:
                logger.warning("시스템 점검 중입니다.")
                return {'post_info': post, 'system_maintenance': True}
            logger.error("상세 페이지 로딩 타임아웃")
            return None
            
        # 바로보기 링크 찾기
        view_links = driver.find_elements(By.CSS_SELECTOR, "a.view[title='새창 열림']")
        if not view_links:
            view_links = driver.find_elements(By.CSS_SELECTOR, "a[onclick*='getExtension_path']")
        
        if view_links:
            view_link = view_links[0]
            onclick_attr = view_link.get_attribute('onclick')
            
            # getExtension_path('49234', '1') 형식에서 매개변수 추출
            match = re.search(r"getExtension_path\s*\(\s*['\"]([\d]+)['\"]?\s*,\s*['\"]([\d]+)['\"]", onclick_attr)
            if match:
                atch_file_no = match.group(1)
                file_ord = match.group(2)
                
                return {
                    'atch_file_no': atch_file_no,
                    'file_ord': file_ord,
                    'post_info': post
                }
        
        # 바로보기 링크가 없으면 게시물 내용 추출
        content = driver.find_element(By.CLASS_NAME, "view_cont").text
        return {
            'content': content,
            'post_info': post
        }

    def access_document_viewer(self, driver, atch_file_no, file_ord):
        """SynapDocViewServer 문서 뷰어 접근"""
        view_url = f"https://www.msit.go.kr/bbs/documentView.do?atchFileNo={atch_file_no}&fileOrdr={file_ord}"
        logger.info(f"문서 뷰어 접근: {view_url}")
        
        driver.get(view_url)
        time.sleep(5)  # 페이지 로드 대기
        
        # SynapDocViewServer 뷰어 감지
        current_url = driver.current_url
        if 'SynapDocViewServer' not in current_url:
            logger.warning("SynapDocViewServer 뷰어가 아닙니다.")
            return None
            
        # 시트 정보 추출 (탭 목록)
        sheet_list = driver.find_elements(By.CSS_SELECTOR, ".sheet-list__sheet-tab")
        sheets = [sheet.text for sheet in sheet_list]
        logger.info(f"발견된 시트: {sheets}")
        
        data_frames = {}
        
        # 각 시트 접근 및 데이터 추출
        for i, sheet_name in enumerate(sheets):
            try:
                # 각 시트로 전환
                if i > 0:  # 첫 번째 시트는 이미 활성화됨
                    sheet_tab = driver.find_element(By.ID, f"sheet{i}")
                    sheet_tab.click()
                    time.sleep(2)  # 시트 전환 대기
                
                # iframe 접근
                iframe = driver.find_element(By.ID, "innerWrap")
                driver.switch_to.frame(iframe)
                
                # 테이블 추출
                iframe_content = driver.page_source
                df = self.extract_table_from_html(iframe_content)
                
                if df is not None:
                    data_frames[sheet_name] = df
                    logger.info(f"시트 '{sheet_name}'에서 데이터 추출 성공")
                
                # 기본 프레임으로 복귀
                driver.switch_to.default_content()
                
            except Exception as e:
                logger.error(f"시트 '{sheet_name}' 처리 중 오류: {str(e)}")
                driver.switch_to.default_content()  # 오류 발생해도 기본 프레임으로 복귀
        
        return data_frames

    def extract_table_from_html(self, html_content):
        """HTML에서 테이블 데이터 추출"""
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # 테이블 요소 찾기
        tables = soup.find_all('table')
        if not tables:
            logger.warning("테이블을 찾을 수 없음")
            return None
            
        # 가장 큰 테이블 선택 (일반적으로 주요 데이터 테이블)
        largest_table = max(tables, key=lambda t: len(t.find_all('tr')))
        
        # 테이블에서 행 추출
        rows = []
        for tr in largest_table.find_all('tr'):
            row = [td.get_text(strip=True) for td in tr.find_all(['td', 'th'])]
            if row and any(row):  # 빈 행 제외
                rows.append(row)
        
        # 행이 충분하지 않으면 실패
        if len(rows) < 2:
            logger.warning("충분한 데이터 행이 없음")
            return None
        
        # 데이터프레임 생성 - 첫 행을 헤더로
        df = pd.DataFrame(rows[1:], columns=rows[0])
        return df

    def update_google_sheets(self, client, data):
        """Google Sheets 업데이트"""
        if not client or not data:
            return False
            
        try:
            post_info = data['post_info']
            
            # 날짜 정보 추출
            date_match = re.search(r'\((\d{4})년\s+(\d{1,2})월말\s+기준\)', post_info['title'])
            if not date_match:
                logger.error(f"제목에서 날짜를 추출할 수 없음: {post_info['title']}")
                return False
                
            year = int(date_match.group(1))
            month = int(date_match.group(2))
            date_str = f"{year}년 {month}월"
            
            # 스프레드시트 열기
            if self.spreadsheet_id:
                spreadsheet = client.open_by_key(self.spreadsheet_id)
            else:
                spreadsheet = client.open(self.spreadsheet_name)
            
            # 각 시트 처리
            updated = False
            
            if 'sheets' in data:  # 여러 시트 데이터
                for sheet_name, df in data['sheets'].items():
                    # 워크시트 찾기 또는 생성
                    try:
                        worksheet = spreadsheet.worksheet(sheet_name)
                    except gspread.exceptions.WorksheetNotFound:
                        worksheet = spreadsheet.add_worksheet(title=sheet_name, rows="1000", cols="50")
                        worksheet.update_cell(1, 1, "항목")
                    
                    # 날짜 열 확인
                    headers = worksheet.row_values(1)
                    if date_str in headers:
                        col_idx = headers.index(date_str) + 1
                    else:
                        col_idx = len(headers) + 1
                        worksheet.update_cell(1, col_idx, date_str)
                    
                    # 데이터 업데이트
                    self.update_worksheet_with_df(worksheet, df, col_idx)
                    updated = True
                    
            elif 'dataframe' in data:  # 단일 데이터프레임
                # 적절한 워크시트 찾기
                report_type = next((rt for rt in self.report_types if rt in post_info['title']), "기타 통계")
                
                try:
                    worksheet = spreadsheet.worksheet(report_type)
                except gspread.exceptions.WorksheetNotFound:
                    worksheet = spreadsheet.add_worksheet(title=report_type, rows="1000", cols="50")
                    worksheet.update_cell(1, 1, "항목")
                
                # 날짜 열 확인
                headers = worksheet.row_values(1)
                if date_str in headers:
                    col_idx = headers.index(date_str) + 1
                else:
                    col_idx = len(headers) + 1
                    worksheet.update_cell(1, col_idx, date_str)
                
                # 데이터 업데이트
                self.update_worksheet_with_df(worksheet, data['dataframe'], col_idx)
                updated = True
            
            return updated
            
        except Exception as e:
            logger.error(f"Google Sheets 업데이트 중 오류: {str(e)}")
            return False

    def update_worksheet_with_df(self, worksheet, df, col_idx):
        """데이터프레임으로 워크시트 업데이트"""
        try:
            # 기존 항목 (첫 번째 열) 가져오기
            existing_items = worksheet.col_values(1)[1:]  # 헤더 제외
            
            # 데이터프레임의 항목 및 값 추출
            if df.shape[1] >= 2:
                items = df.iloc[:, 0].astype(str).tolist()
                values = df.iloc[:, 1].astype(str).tolist()
                
                # 배치 업데이트 준비
                updates = []
                
                for i, (item, value) in enumerate(zip(items, values)):
                    if not item or pd.isna(item):
                        continue
                        
                    # 항목이 이미 존재하는지 확인
                    if item in existing_items:
                        row_idx = existing_items.index(item) + 2  # 헤더(1) + 0-인덱스 보정(1)
                    else:
                        # 새 항목 추가
                        row_idx = len(existing_items) + 2
                        updates.append({
                            'range': f'A{row_idx}',
                            'values': [[item]]
                        })
                        existing_items.append(item)
                    
                    # 값 업데이트
                    value_to_update = "" if pd.isna(value) else value
                    updates.append({
                        'range': f'{chr(64 + col_idx)}{row_idx}',
                        'values': [[value_to_update]]
                    })
                
                # 일괄 업데이트 실행
                if updates:
                    worksheet.batch_update(updates)
                    
            return True
        
        except Exception as e:
            logger.error(f"워크시트 업데이트 중 오류: {str(e)}")
            return False

    async def send_telegram_message(self, posts, data_updates=None):
        """텔레그램 알림 전송"""
        if not posts and not data_updates:
            return
            
        message = "📊 *MSIT 통신 통계 모니터링*\n\n"
        
        # 새 게시물 정보 추가
        if posts:
            message += "📱 *새로운 통신 관련 게시물*\n\n"
            
            for post in posts:
                message += f"📅 {post['date']}\n"
                message += f"📑 {post['title']}\n"
                message += f"🏢 {post['department']}\n"
                if post.get('url'):
                    message += f"🔗 [게시물 링크]({post['url']})\n"
                message += "\n"
        
        # 데이터 업데이트 정보 추가
        if data_updates:
            message += "📊 *Google Sheets 데이터 업데이트*\n\n"
            
            for update in data_updates:
                post_info = update['post_info']
                date_match = re.search(r'\((\d{4})년\s+(\d{1,2})월말\s+기준\)', post_info['title'])
                if date_match:
                    year = date_match.group(1)
                    month = date_match.group(2)
                    date_str = f"{year}년 {month}월"
                else:
                    date_str = "날짜 정보 없음"
                
                message += f"📅 *{date_str}*\n"
                message += f"📑 {post_info['title']}\n"
                message += f"📗 업데이트 완료\n\n"
        
        await self.bot.send_message(
            chat_id=int(self.chat_id),
            text=message,
            parse_mode='Markdown'
        )

    async def run_monitor(self, days_range=4):
    
        """모니터링 실행"""
        driver = None
        gs_client = None
    
        try:
            # WebDriver 초기화
            driver = self.setup_driver()
            logger.info("WebDriver 초기화 완료")
        
        # Google Sheets 클라이언트 초기화
            if self.gspread_creds:
                gs_client = self.setup_gspread_client()
                if gs_client:
                    logger.info("Google Sheets 클라이언트 초기화 완료")
                else:
                    logger.warning("Google Sheets 클라이언트 초기화 실패")
        
        # MSIT 웹사이트로 이동
            driver.get(self.url)
            logger.info("MSIT 웹사이트 접근 완료")
        
        # 모든 게시물 및 통신 통계 게시물 추적
            all_posts = []
            telecom_posts = []
            continue_search = True
        
        # 페이지 파싱
            while continue_search:
            # 현재 페이지 게시물 파싱
                try:
                    WebDriverWait(driver, 15).until(
                        EC.presence_of_element_located((By.CLASS_NAME, 'board_list'))
                    )
                
                    soup = BeautifulSoup(driver.page_source, 'html.parser')
                    posts = soup.find_all('div', {'class': 'toggle'})
                
                    for item in posts:
                        # 제목 추출
                        title_tag = item.find('p', {'class': 'title'})
                        if not title_tag:
                            continue
                    
                    # 날짜 추출
                        date_tag = item.find('div', {'class': 'date'})
                        if not date_tag:
                            continue
                    
                        date_str = date_tag.text.strip()
                    
                    # 날짜가 범위를 벗어나면 검색 중단
                        if not self.is_in_date_range(date_str, days=days_range):
                            continue_search = False
                            break
                    
                        # 제목 추출
                        title = title_tag.text.strip()
                        
                    # 게시물 ID 추출
                        onclick = title_tag.find('a').get('onclick', '')
                        match = re.search(r"fn_detail\((\d+)\)", onclick)
                        if not match:
                            continue
                    
                        post_id = match.group(1)
                        post_url = f"https://www.msit.go.kr/bbs/view.do?sCode=user&mId=99&mPid=74&nttSeqNo={post_id}"
                    
                    # 부서 정보 추출
                        dept_tag = item.find('dd', {'id': lambda x: x and 'td_CHRG_DEPT_NM' in x})
                        dept = dept_tag.text.strip() if dept_tag else "부서 정보 없음"
                    
                    # 게시물 정보 구성
                        post_info = {
                            'title': title,
                            'date': date_str,
                            'department': dept,
                            'post_id': post_id,
                            'url': post_url
                        }
                    
                    # 게시물 리스트에 추가
                        all_posts.append(post_info)
                        
                    # 통신 통계 게시물 분류
                        if self.is_telecom_stats_post(title):
                            telecom_posts.append(post_info)
                            logger.info(f"통신 통계 게시물 발견: {title}")
                
                # 날짜 범위를 벗어난 경우 검색 중단
                    if not continue_search:
                        break
                
                # 다음 페이지 확인 및 이동
                    if self.has_next_page(driver):
                        if not self.go_to_next_page(driver):
                            break
                    else:
                        break
                    
                except Exception as e:
                    logger.error(f"페이지 파싱 중 오류: {str(e)}")
                    break
        
        # 통신 통계 게시물이 없으면 종료
            if not telecom_posts:
                logger.info(f"최근 {days_range}일 내 통신 통계 게시물이 없습니다.")
                return
        
        # 통신 통계 게시물 처리
            data_updates = []
            
            for post in telecom_posts:
                try:
                # 파일 정보 추출
                    file_info = self.extract_file_info(driver, post)
                
                    if not file_info:
                        logger.warning(f"파일 정보를 추출할 수 없음: {post['title']}")
                        continue
                
                # 시스템 점검 중이면 건너뛰기
                    if file_info.get('system_maintenance'):
                        logger.warning("시스템 점검으로 인해 처리를 건너뜁니다.")
                        continue
                
                # 문서 뷰어 접근 및 데이터 추출
                    if 'atch_file_no' in file_info and 'file_ord' in file_info:
                        sheets_data = self.access_document_viewer(
                            driver, 
                            file_info['atch_file_no'], 
                            file_info['file_ord']
                        )
                    
                        if sheets_data:
                            # Google Sheets 업데이트
                            if gs_client:
                                update_data = {
                                    'sheets': sheets_data,
                                    'post_info': post
                                }
                            
                                success = self.update_google_sheets(gs_client, update_data)
                                if success:
                                    logger.info(f"스프레드시트 업데이트 성공: {post['title']}")
                                    data_updates.append(update_data)
                                else:
                                    logger.warning(f"스프레드시트 업데이트 실패: {post['title']}")
                        else:
                        # 대체 데이터 생성
                            placeholder_df = self.create_placeholder_dataframe(post)
                            if not placeholder_df.empty and gs_client:
                                update_data = {
                                    'dataframe': placeholder_df,
                                    'post_info': post
                                }
                            
                                success = self.update_google_sheets(gs_client, update_data)
                                if success:
                                    logger.info(f"대체 데이터로 스프레드시트 업데이트: {post['title']}")
                                    data_updates.append(update_data)
                
                # 게시물 내용으로 처리
                    elif 'content' in file_info:
                        # 게시물 내용에서 데이터 추출 시도할 수 있음
                        # 여기에서는 단순히 플레이스홀더 데이터 생성
                        placeholder_df = self.create_placeholder_dataframe(post)
                        if not placeholder_df.empty and gs_client:
                            update_data = {
                                'dataframe': placeholder_df,
                                'post_info': post
                            }
                        
                            success = self.update_google_sheets(gs_client, update_data)
                            if success:
                                logger.info(f"게시물 내용 기반 데이터로 스프레드시트 업데이트: {post['title']}")
                                data_updates.append(update_data)
                
                except Exception as e:
                    logger.error(f"게시물 처리 중 오류: {str(e)}")
        
            # 텔레그램 알림 전송
            if all_posts or data_updates:
                await self.send_telegram_message(all_posts, data_updates)
        
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
            except:
                pass
            
        finally:
        # 리소스 정리
            if driver:
                driver.quit()
                logger.info("WebDriver 종료")
                
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
        
        return client
    except Exception as e:
        logger.error(f"Google Sheets 클라이언트 초기화 중 오류: {str(e)}")
        return None

def determine_report_type(self, title):
    """게시물 제목에서 보고서 유형 결정"""
    for report_type in self.report_types:
        if report_type in title:
            return report_type
    return "기타 통신 통계"

def is_in_date_range(self, date_str, days=4):
    """게시물 날짜가 지정된 범위 내에 있는지 확인"""
    try:
        post_date = self.parse_date(date_str)
        cutoff_date = (datetime.now() - timedelta(days=days)).date()
        
        logger.info(f"게시물 날짜 확인: {post_date} vs {cutoff_date} ({days}일 전)")
        return post_date >= cutoff_date
    except Exception as e:
        logger.error(f"날짜 확인 중 오류: {str(e)}")
        return True  # 오류 시 기본적으로 포함

def has_next_page(self, driver):
    """다음 페이지가 있는지 확인"""
    try:
        current_page = int(driver.find_element(By.CSS_SELECTOR, "a.page-link[aria-current='page']").text)
        next_page_link = driver.find_elements(By.CSS_SELECTOR, f"a.page-link[href*='pageIndex={current_page + 1}']")
        return len(next_page_link) > 0
    except Exception as e:
        logger.error(f"다음 페이지 확인 중 오류: {str(e)}")
        return False

def go_to_next_page(self, driver):
    """다음 페이지로 이동"""
    try:
        current_page = int(driver.find_element(By.CSS_SELECTOR, "a.page-link[aria-current='page']").text)
        next_page = driver.find_element(By.CSS_SELECTOR, f"a.page-link[href*='pageIndex={current_page + 1}']")
        next_page.click()
        WebDriverWait(driver, 10).until(
            EC.staleness_of(driver.find_element(By.CSS_SELECTOR, "div.board_list"))
        )
        return True
    except Exception as e:
        logger.error(f"다음 페이지 이동 중 오류: {str(e)}")
        return False

def create_placeholder_dataframe(self, post_info):
    """데이터 추출 실패 시 기본 데이터프레임 생성"""
    # 날짜 정보 추출
    date_match = re.search(r'\((\d{4})년\s+(\d{1,2})월말\s+기준\)', post_info['title'])
    if date_match:
        year = date_match.group(1)
        month = date_match.group(2)
        
        report_type = self.determine_report_type(post_info['title'])
        
        # 기본 데이터프레임 생성
        df = pd.DataFrame({
            '구분': [f'{month}월 {report_type}'],
            '값': ['데이터를 추출할 수 없습니다'],
            '비고': [post_info['title']]
        })
        
        return df
    
    return pd.DataFrame()

async def main():
    days_range = int(os.environ.get('DAYS_RANGE', '4'))
    
    try:
        monitor = MSITMonitor()
        await monitor.run_monitor(days_range=days_range)
    except Exception as e:
        logging.error(f"기본 함수 오류: {str(e)}", exc_info=True)

if __name__ == "__main__":
    asyncio.run(main())
