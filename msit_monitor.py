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

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('msit_monitor')

class MSITMonitor:
    def __init__(self):
        # Telegram 설정
        self.telegram_token = os.environ.get('TELCO_NEWS_TOKEN')
        self.chat_id = os.environ.get('TELCO_NEWS_TESTER')
        if not self.telegram_token or not self.chat_id:
            raise ValueError("환경 변수 TELCO_NEWS_TOKEN과 TELCO_NEWS_TESTER가 필요합니다.")
        
        # Google Sheets 설정
        self.gspread_creds = os.environ.get('MSIT_GSPREAD_ref')
        self.spreadsheet_id = os.environ.get('MSIT_SPREADSHEET_ID')
        self.spreadsheet_name = os.environ.get('SPREADSHEET_NAME', 'MSIT 통신 통계')
    
        if not self.gspread_creds:
            logger.warning("환경 변수 MSIT_GSPREAD_ref가 설정되지 않았습니다. Google Sheets 업데이트는 비활성화됩니다.")
        
        # MSIT URL
        self.url = "https://www.msit.go.kr/bbs/list.do?sCode=user&mPid=74&mId=99"
        
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
        """Selenium WebDriver 설정"""
        options = Options()
        options.add_argument('--headless')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-gpu')
        options.add_argument('--window-size=1920,1080')
        
        # 성능 최적화 설정
        prefs = {
            "profile.default_content_setting_values.images": 2  # 이미지 로딩 비활성화
        }
        options.add_experimental_option("prefs", prefs)
        
        # 불필요한 로그 비활성화
        options.add_experimental_option('excludeSwitches', ['enable-logging'])
        
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

    def is_telecom_stats_post(self, title):
        """게시물이 통신 통계 보고서인지 확인"""
        # "(YYYY년 MM월말 기준)" 형식의 날짜 패턴 확인
        date_pattern = r'\((\d{4})년\s+(\d{1,2})월말\s+기준\)'
        has_date_pattern = re.search(date_pattern, title) is not None
        
        # 제목에 보고서 유형이 포함되어 있는지 확인
        contains_report_type = any(report_type in title for report_type in self.report_types)
        
        return has_date_pattern and contains_report_type

    def extract_post_id(self, item):
        """BeautifulSoup 항목에서 게시물 ID 추출"""
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
            try:
                # "YYYY. MM. DD" 형식
                post_date = datetime.strptime(date_str, '%Y. %m. %d').date()
            except ValueError:
                try:
                    # "MMM DD YYYY" 형식
                    post_date = datetime.strptime(date_str, '%b %d %Y').date()
                except ValueError:
                    try:
                        # "YYYY-MM-DD" 형식
                        post_date = datetime.strptime(date_str, '%Y-%m-%d').date()
                    except ValueError:
                        # 정규식으로 시도
                        match = re.search(r'(\d{4})[.\-\s]+(\d{1,2})[.\-\s]+(\d{1,2})', date_str)
                        if match:
                            year, month, day = map(int, match.groups())
                            post_date = datetime(year, month, day).date()
                        else:
                            logger.warning(f"알 수 없는 날짜 형식: {date_str}")
                            return True  # 알 수 없는 경우 포함
            
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
            current_page = int(driver.find_element(By.CSS_SELECTOR, "a.page-link[aria-current='page']").text)
            next_page_link = driver.find_elements(By.CSS_SELECTOR, f"a.page-link[href*='pageIndex={current_page + 1}']")
            return len(next_page_link) > 0
        except Exception as e:
            logger.error(f"다음 페이지 확인 중 에러: {str(e)}")
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
            logger.error(f"다음 페이지 이동 중 에러: {str(e)}")
            return False

    def parse_page(self, driver, days_range=4):
        """현재 페이지에서 관련 게시물 파싱"""
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
                # 헤더 행 건너뛰기
                if 'thead' in item.get('class', []):
                    continue

                try:
                    # 날짜 정보 추출
                    date_elem = item.find('div', {'class': 'date', 'aria-label': '등록일'})
                    if not date_elem:
                        date_elem = item.find('div', {'class': 'date'})
                    if not date_elem:
                        continue
                        
                    date_str = date_elem.text.strip()
                    if not date_str or date_str == '등록일':
                        continue
                    
                    logger.info(f"날짜 문자열 발견: {date_str}")
                    
                    # 게시물이 날짜 범위 내에 있는지 확인
                    if not self.is_in_date_range(date_str, days=days_range):
                        continue_search = False
                        break
                    
                    # 제목 및 게시물 ID 추출
                    title_elem = item.find('p', {'class': 'title'})
                    if not title_elem:
                        continue
                        
                    title = title_elem.text.strip()
                    post_id = self.extract_post_id(item)
                    post_url = self.get_post_url(post_id)
                    
                    # 부서 정보 추출
                    dept_elem = item.find('dd', {'id': lambda x: x and 'td_CHRG_DEPT_NM' in x})
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
        
        # 최대 3번까지 재시도
        max_retries = 3
        for attempt in range(max_retries):
            try:
                # 게시물 상세 페이지로 이동
                detail_url = f"https://www.msit.go.kr/bbs/view.do?sCode=user&mId=99&mPid=74&nttSeqNo={post['post_id']}"
                driver.get(detail_url)
                
                # 명시적인 대기 시간 추가
                time.sleep(3)
                
                # 다양한 요소 중 하나라도 로드되면 성공으로 간주
                WebDriverWait(driver, 20).until(
                    lambda x: len(x.find_elements(By.CLASS_NAME, "view_head")) > 0 or 
                             len(x.find_elements(By.CLASS_NAME, "view_file")) > 0
                )
                break  # 성공하면 루프 종료
            except TimeoutException:
                if attempt < max_retries - 1:
                    logger.warning(f"페이지 로드 타임아웃, 재시도 {attempt+1}/{max_retries}")
                    time.sleep(3)  # 재시도 전 3초 대기
                else:
                    logger.error(f"{max_retries}번 시도 후 페이지 로드 실패")
                    
                    # 시스템 점검 페이지 확인
                    if "시스템 점검 안내" in driver.page_source:
                        logger.warning("시스템 점검 중입니다.")
                        return None
                    
                    return None
            except Exception as e:
                logger.error(f"게시물 상세 정보 접근 중 오류: {str(e)}")
                return None
        
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
                    date_match = re.search(r'\((\d{4})년\s+(\d{1,2})월말\s+기준\)', post['title'])
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
            date_match = re.search(r'\((\d{4})년\s+(\d{1,2})월말\s+기준\)', post['title'])
            if date_match:
                year = int(date_match.group(1))
                month = int(date_match.group(2))
                
                # 게시물 내용 추출
                content_div = driver.find_element(By.CLASS_NAME, "view_cont")
                content = content_div.text if content_div else ""
                
                return {
                    'content': content,
                    'date': {'year': year, 'month': month},
                    'post_info': post
                }
            
            return None
            
        except Exception as e:
            logger.error(f"바로보기 링크 파라미터 추출 중 오류: {str(e)}")
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
        
        try:
            # 페이지 로드
            driver.get(view_url)
            time.sleep(5)  # 페이지 로드 대기
            
            # 현재 창 핸들 저장
            original_handle = driver.current_window_handle
            
            # 새 창이 열렸는지 확인
            if len(driver.window_handles) > 1:
                logger.info("새 창이 열렸습니다. 전환 시도...")
                for handle in driver.window_handles:
                    if handle != original_handle:
                        driver.switch_to.window(handle)
                        break
            
            # 현재 URL 확인
            current_url = driver.current_url
            logger.info(f"현재 URL: {current_url}")
            
            # SynapDocViewServer 확인
            if 'SynapDocViewServer' in current_url:
                logger.info("SynapDocViewServer 감지됨")
                
                # 시트 목록 확인
                sheet_tabs = driver.find_elements(By.CSS_SELECTOR, ".sheet-list__sheet-tab")
                
                if sheet_tabs:
                    logger.info(f"시트 탭 {len(sheet_tabs)}개 발견")
                    
                    all_sheets = {}
                    
                    # 각 시트 처리
                    for i, tab in enumerate(sheet_tabs):
                        sheet_name = tab.text.strip()
                        logger.info(f"시트 {i+1}/{len(sheet_tabs)} 처리 중: {sheet_name}")
                        
                        # 첫 번째가 아닌 시트는 클릭하여 전환
                        if i > 0:
                            try:
                                tab.click()
                                time.sleep(3)  # 시트 전환 대기
                            except Exception as click_err:
                                logger.error(f"시트 탭 클릭 실패: {str(click_err)}")
                                continue
                        
                        try:
                            # iframe 찾기
                            iframe = driver.find_element(By.ID, "innerWrap")
                            
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
                                logger.info(f"시트 '{sheet_name}'에서 데이터프레임 추출 성공: {df.shape}")
                            else:
                                logger.warning(f"시트 '{sheet_name}'에서 테이블 추출 실패")
                                
                        except Exception as iframe_err:
                            logger.error(f"iframe 처리 중 오류: {str(iframe_err)}")
                            # 오류 발생 시 기본 프레임으로 복귀
                            driver.switch_to.default_content()
                    
                    # 모든 시트 처리 후 결과 반환
                    if all_sheets:
                        logger.info(f"총 {len(all_sheets)}개 시트에서 데이터 추출 완료")
                        return all_sheets
                    else:
                        logger.warning("어떤 시트에서도 데이터를 추출하지 못했습니다.")
                        return None
                        
                else:
                    # 시트 탭이 없는 경우 단일 iframe 처리
                    logger.info("시트 탭이 없습니다. 단일 iframe 처리 시도...")
                    
                    try:
                        # iframe 찾기
                        iframe = driver.find_element(By.ID, "innerWrap")
                        
                        # iframe으로 전환
                        driver.switch_to.frame(iframe)
                        
                        # 페이지 소스 가져오기
                        iframe_html = driver.page_source
                        
                        # 테이블 추출
                        df = self.extract_table_from_html(iframe_html)
                        
                        # 기본 프레임으로 복귀
                        driver.switch_to.default_content()
                        
                        if df is not None and not df.empty:
                            logger.info(f"단일 iframe에서 데이터프레임 추출 성공: {df.shape}")
                            return {"기본 시트": df}
                        else:
                            logger.warning("iframe에서 테이블 추출 실패")
                            return None
                            
                    except Exception as single_iframe_err:
                        logger.error(f"단일 iframe 처리 중 오류: {str(single_iframe_err)}")
                        # 오류 발생 시 기본 프레임으로 복귀
                        driver.switch_to.default_content()
                        return None
            
            else:
                # 일반 HTML 페이지에서 테이블 찾기
                logger.info("일반 HTML 페이지에서 테이블 검색")
                
                try:
                    # 테이블 찾기
                    tables = pd.read_html(driver.page_source)
                    
                    if tables:
                        logger.info(f"{len(tables)}개 테이블 발견")
                        
                        # 가장 큰 테이블 선택
                        largest_table = max(tables, key=lambda t: t.size)
                        
                        logger.info(f"가장 큰 테이블 선택: {largest_table.shape}")
                        return {"기본 테이블": largest_table}
                    else:
                        logger.warning("페이지에서 테이블을 찾을 수 없습니다.")
                        return None
                        
                except Exception as table_err:
                    logger.error(f"HTML 테이블 추출 중 오류: {str(table_err)}")
                    return None
            
        except Exception as e:
            logger.error(f"iframe 직접 접근 중 오류: {str(e)}")
            return None

    def extract_table_from_html(self, html_content):
        """HTML 내용에서 테이블 추출"""
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # 테이블 요소 찾기
            tables = soup.find_all('table')
            if not tables:
                logger.warning("HTML에서 테이블을 찾을 수 없음")
                return None
            
            # 가장 큰 테이블 선택 (데이터 행이 가장 많은 테이블)
            largest_table = max(tables, key=lambda t: len(t.find_all('tr')))
            
            # 테이블 데이터 추출
            rows = []
            for tr in largest_table.find_all('tr'):
                row = [td.get_text(strip=True) for td in tr.find_all(['td', 'th'])]
                if row and any(row):  # 빈 행 제외
                    rows.append(row)
            
            # 데이터가 충분한지 확인
            if len(rows) < 2:
                logger.warning("테이블 데이터가 충분하지 않음")
                return None
            
            # 첫 행을 헤더로 사용하여 데이터프레임 생성
            df = pd.DataFrame(rows[1:], columns=rows[0])
            logger.info(f"테이블 추출 성공: {df.shape[0]}행 {df.shape[1]}열")
            return df

        
        except Exception as e:
            logger.error(f"HTML에서 테이블 추출 중 오류: {str(e)}")
            return None

    def create_placeholder_dataframe(self, post_info):
        """데이터 추출 실패 시 기본 데이터프레임 생성"""
        try:
            # 날짜 정보 추출
            date_match = re.search(r'\((\d{4})년\s+(\d{1,2})월말\s+기준\)', post_info['title'])
            if date_match:
                year = date_match.group(1)
                month = date_match.group(2)
                
                # 보고서 유형 결정
                report_type = self.determine_report_type(post_info['title'])
                
                # 기본 데이터프레임 생성
                df = pd.DataFrame({
                    '구분': [f'{year}년 {month}월 통계'],
                    '값': ['데이터를 추출할 수 없습니다'],
                    '비고': [f'{post_info["title"]} - 접근 오류']
                })
                
                logger.info(f"플레이스홀더 데이터프레임 생성: {year}년 {month}월 {report_type}")
                return df
                
            return pd.DataFrame()  # 날짜 정보가 없으면 빈 데이터프레임 반환
            
        except Exception as e:
            logger.error(f"플레이스홀더 데이터프레임 생성 중 오류: {str(e)}")
            return pd.DataFrame()  # 오류 발생 시 빈 데이터프레임 반환

    def determine_report_type(self, title):
        """게시물 제목에서 보고서 유형 결정"""
        for report_type in self.report_types:
            if report_type in title:
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
            
            # 날짜 정보가 직접 제공되었는지 확인
            if 'date' in data:
                year = data['date']['year']
                month = data['date']['month']
            else:
                # 제목에서 날짜 정보 추출
                date_match = re.search(r'\((\d{4})년\s+(\d{1,2})월말\s+기준\)', post_info['title'])
                if not date_match:
                    logger.error(f"제목에서 날짜를 추출할 수 없음: {post_info['title']}")
                    return False
                    
                year = int(date_match.group(1))
                month = int(date_match.group(2))
            
            # 날짜 문자열 포맷
            date_str = f"{year}년 {month}월"
            report_type = self.determine_report_type(post_info['title'])
            
            # 스프레드시트 열기
            try:
                # ID로 먼저 시도
                if self.spreadsheet_id:
                    try:
                        spreadsheet = client.open_by_key(self.spreadsheet_id)
                        logger.info(f"ID로 기존 스프레드시트 찾음: {self.spreadsheet_id}")
                    except gspread.exceptions.APIError:
                        logger.warning(f"ID로 스프레드시트를 열 수 없음: {self.spreadsheet_id}")
                        spreadsheet = None
                else:
                    spreadsheet = None
                
                # ID로 찾지 못한 경우 이름으로 시도
                if not spreadsheet:
                    try:
                        spreadsheet = client.open(self.spreadsheet_name)
                        logger.info(f"이름으로 기존 스프레드시트 찾음: {self.spreadsheet_name}")
                    except gspread.exceptions.SpreadsheetNotFound:
                        # 새 스프레드시트 생성
                        spreadsheet = client.create(self.spreadsheet_name)
                        logger.info(f"새 스프레드시트 생성: {self.spreadsheet_name}")
                        
                        # 참조용 ID 기록
                        logger.info(f"새 스프레드시트 ID: {spreadsheet.id}")
            except Exception as e:
                logger.error(f"Google Sheets 열기 중 오류: {str(e)}")
                return False
            
            # 시트 데이터 여부 확인
            if 'sheets' in data:
                # 여러 시트 처리
                for sheet_name, df in data['sheets'].items():
                    success = self.update_single_sheet(spreadsheet, sheet_name, df, date_str)
                    if not success:
                        logger.warning(f"시트 '{sheet_name}' 업데이트 실패")
                
                return True  # 최소한 하나의 시트는 업데이트 시도됨
                
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
            # 워크시트 찾기 또는 생성
            try:
                worksheet = spreadsheet.worksheet(sheet_name)
                logger.info(f"기존 워크시트 찾음: {sheet_name}")
            except gspread.exceptions.WorksheetNotFound:
                worksheet = spreadsheet.add_worksheet(title=sheet_name, rows="1000", cols="50")
                worksheet.update_cell(1, 1, "항목")
                logger.info(f"새 워크시트 생성: {sheet_name}")
            
            # 날짜 열 확인
            headers = worksheet.row_values(1)
            if date_str in headers:
                col_idx = headers.index(date_str) + 1
                logger.info(f"'{date_str}' 열이 이미 위치 {col_idx}에 존재합니다")
            else:
                # 새 날짜 열 추가
                col_idx = len(headers) + 1
                worksheet.update_cell(1, col_idx, date_str)
                logger.info(f"위치 {col_idx}에 새 열 '{date_str}' 추가")
            
            # 데이터프레임으로 시트 업데이트
            self.update_sheet_from_dataframe(worksheet, df, col_idx)
            
            logger.info(f"워크시트 '{sheet_name}'에 '{date_str}' 데이터 업데이트 완료")
            return True
            
        except Exception as e:
            logger.error(f"시트 '{sheet_name}' 업데이트 중 오류: {str(e)}")
            return False

    def update_sheet_from_dataframe(self, worksheet, df, col_idx):
        """데이터프레임으로 워크시트 업데이트"""
        try:
            # 기존 항목 (첫 번째 열) 가져오기
            existing_items = worksheet.col_values(1)[1:]  # 헤더 제외
            
            if df.shape[0] > 0:
                # 데이터프레임에서 항목과 값 추출
                # 첫 번째 열은 항목, 두 번째 열은 값으로 가정
                if df.shape[1] >= 2:
                    new_items = df.iloc[:, 0].astype(str).tolist()
                    values = df.iloc[:, 1].astype(str).tolist()
                    
                    # 배치 업데이트 준비
                    cell_updates = []
                    
                    for i, (item, value) in enumerate(zip(new_items, values)):
                        if item and not pd.isna(item):  # 빈 항목 제외
                            # 항목이 이미 존재하는지 확인
                            if item in existing_items:
                                row_idx = existing_items.index(item) + 2  # 헤더와 0-인덱스 보정
                            else:
                                # 새 항목 추가
                                row_idx = len(existing_items) + 2
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
                    
                    # 일괄 업데이트 실행
                    if cell_updates:
                        worksheet.batch_update(cell_updates)
                        logger.info(f"{len(cell_updates)}개 셀 업데이트 완료")
                        
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
                    
                    # 날짜 정보 추출
                    if 'date' in update:
                        year = update['date']['year']
                        month = update['date']['month']
                    else:
                        date_match = re.search(r'\((\d{4})년\s+(\d{1,2})월말\s+기준\)', post_info['title'])
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
                    message += f"📗 업데이트 완료\n\n"
            
            # 메시지 전송
            chat_id = int(self.chat_id)
            await self.bot.send_message(
                chat_id=chat_id,
                text=message,
                parse_mode='Markdown'
            )
            logger.info("텔레그램 메시지 전송 성공")
            
        except Exception as e:
            logger.error(f"텔레그램 메시지 전송 중 오류: {str(e)}")

    async def run_monitor(self, days_range=4, check_sheets=True):
        """모니터링 실행"""
        driver = None
        gs_client = None
        
        try:
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
            
            # MSIT 웹사이트로 이동
            driver.get(self.url)
            logger.info("MSIT 웹사이트 접근 완료")
            
            # 모든 게시물 및 통신 통계 게시물 추적
            all_posts = []
            telecom_stats_posts = []
            continue_search = True
            
            # 페이지 파싱
            while continue_search:
                page_posts, stats_posts, should_continue = self.parse_page(driver, days_range=days_range)
                all_posts.extend(page_posts)
                telecom_stats_posts.extend(stats_posts)
                
                if not should_continue:
                    break
                    
                if self.has_next_page(driver):
                    if not self.go_to_next_page(driver):
                        break
                else:
                    break
            
            # 통신 통계 게시물 처리
            data_updates = []
            
            if gs_client and telecom_stats_posts and check_sheets:
                logger.info(f"{len(telecom_stats_posts)}개 통신 통계 게시물 처리 중")
                
                for post in telecom_stats_posts:
                    try:
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
                    
                    except Exception as e:
                        logger.error(f"게시물 처리 중 오류: {str(e)}")
            
            # 텔레그램 알림 전송
            if all_posts or data_updates:
                await self.send_telegram_message(all_posts, data_updates)
            else:
                logger.info(f"최근 {days_range}일 내 새 게시물이 없습니다")
        
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

async def main():
    # 환경 변수 가져오기
    days_range = int(os.environ.get('DAYS_RANGE', '4'))
    check_sheets = os.environ.get('CHECK_SHEETS', 'true').lower() == 'true'
    
    # 모니터 생성 및 실행
    try:
        logger.info(f"MSIT 모니터 시작 - days_range={days_range}, check_sheets={check_sheets}")
        logger.info(f"스프레드시트 이름: {os.environ.get('SPREADSHEET_NAME', 'MSIT 통신 통계')}")
        
        monitor = MSITMonitor()
        await monitor.run_monitor(days_range=days_range, check_sheets=check_sheets)
    except Exception as e:
        logging.error(f"메인 함수 오류: {str(e)}", exc_info=True)

if __name__ == "__main__":
    asyncio.run(main())
