#!/usr/bin/env python3
import os
import re
import time
import logging
import requests
import pandas as pd
from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from bs4 import BeautifulSoup
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# 환경변수에서 설정 가져오기
TELEGRAM_TOKEN = os.environ.get('TELCO_NEWS_TOKEN')
TELEGRAM_CHAT_ID = os.environ.get('TELCO_NEWS_TESTER')
GSPREAD_JSON_BASE64 = os.environ.get('MSIT_GSPREAD_ref')
SPREADSHEET_ID = os.environ.get('MSIT_SPREADSHEET_ID', '')

# 입력 파라미터 설정
DAYS_RANGE = int(os.environ.get('DAYS_RANGE', 4))
CHECK_SHEETS = os.environ.get('CHECK_SHEETS', 'true').lower() == 'true'
SPREADSHEET_NAME = os.environ.get('SPREADSHEET_NAME', 'MSIT 통신 통계')

MAX_RETRIES = int(os.environ.get('MAX_RETRIES', 5))
RETRY_DELAY = int(os.environ.get('RETRY_DELAY', 5))

# 과기정통부 통신통계 URL
MSIT_URL = "https://www.msit.go.kr/bbs/list.do?sCode=user&mPid=74&mId=99"

# 통계 제목 패턴 - 정규식으로 변환
STATS_TITLE_PATTERNS = [
    r"이동전화 및 트래픽 통계",
    r"이동전화 및 시내전화 번호이동 현황",
    r"유선통신서비스 가입 현황",
    r"무선통신서비스 가입 현황",
    r"특수부가통신사업자현황\(웹하드, p2p\)",
    r"무선데이터 트래픽 통계",
    r"유·무선통신서비스 가입 현황 및 무선데이터 트래픽 통계"
]

def is_system_maintenance(driver):
    """시스템 점검 중인지 확인"""
    maintenance_texts = ["시스템 점검", "점검 중", "서비스 일시 중단"]
    page_source = driver.page_source
    return any(text in page_source for text in maintenance_texts)

def send_telegram_message(message):
    """텔레그램으로 메시지 전송"""
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        logger.warning("텔레그램 토큰 또는 채팅 ID가 설정되지 않았습니다.")
        return False
    
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    data = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "Markdown"
    }
    
    try:
        response = requests.post(url, data=data)
        response.raise_for_status()
        logger.info("텔레그램 메시지 전송 성공")
        return True
    except Exception as e:
        logger.error(f"텔레그램 메시지 전송 실패: {str(e)}")
        return False
        
def send_maintenance_alert():
    """시스템 점검 알림 전송"""
    message = "⚠️ *MSIT 시스템 점검 알림*\n"
    message += "현재 과학기술정보통신부 시스템이 점검 중입니다.\n"
    message += "자동 데이터 수집이 일시 중단되었습니다.\n"
    message += f"실행 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    
    send_telegram_message(message)

def init_webdriver():
    """웹드라이버 초기화"""
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    
    driver = webdriver.Chrome(options=chrome_options)
    driver.set_page_load_timeout(30)  # 페이지 로드 타임아웃 30초로 설정
    logger.info("WebDriver 초기화 완료")
    return driver

def init_gspread_client():
    """Google Sheets API 클라이언트 초기화"""
    import base64
    import json
    
    if not GSPREAD_JSON_BASE64 or not CHECK_SHEETS:
        return None
    
    try:
        json_str = base64.b64decode(GSPREAD_JSON_BASE64).decode('utf-8')
        json_data = json.loads(json_str)
        
        # API 범위 설정
        scope = ['https://spreadsheets.google.com/feeds',
                'https://www.googleapis.com/auth/drive']
        
        # 인증 및 클라이언트 생성
        creds = ServiceAccountCredentials.from_json_keyfile_dict(json_data, scope)
        client = gspread.authorize(creds)
        
        logger.info("Google Sheets 클라이언트 초기화 완료")
        return client
    except Exception as e:
        logger.error(f"Google Sheets 클라이언트 초기화 실패: {str(e)}")
        return None

def update_sheet(client, title, dataframe):
    """Google Sheets 업데이트"""
    if not client:
        logger.warning("Google Sheets 클라이언트가 초기화되지 않았습니다.")
        return False
    
    try:
        # 스프레드시트 열기
        spreadsheet = client.open(SPREADSHEET_NAME)
        
        # 시트 찾기 또는 생성
        try:
            worksheet = spreadsheet.worksheet(title)
        except:
            worksheet = spreadsheet.add_worksheet(title=title, rows=100, cols=20)
            logger.info(f"새 시트 생성: {title}")
        
        # 데이터프레임 전처리
        df_clean = dataframe.fillna('')  # NaN 값을 빈 문자열로 변환
        
        # 시트 초기화 및 데이터 업데이트
        worksheet.clear()
        worksheet.update([df_clean.columns.tolist()] + df_clean.values.tolist())
        
        # 업데이트 시간 기록
        update_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        worksheet.update_cell(1, len(df_clean.columns) + 1, f"업데이트: {update_time}")
        
        logger.info(f"시트 업데이트 완료: {title}")
        return True
    except Exception as e:
        logger.error(f"시트 업데이트 실패 ({title}): {str(e)}")
        return False

def extract_stats_tables(driver, detail_url, title):
    """통계 테이블 추출"""
    tables_data = {}
    
    # 페이지 로드 시도
    for attempt in range(MAX_RETRIES):
        try:
            driver.get(detail_url)
            
            # 시스템 점검 확인
            if is_system_maintenance(driver):
                logger.warning("시스템 점검 중입니다.")
                send_maintenance_alert()
                return None
            
            # 페이지 로드 대기
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.CLASS_NAME, "view_head"))
            )
            
            # 바로보기 링크 찾기
            view_links = driver.find_elements(By.XPATH, "//a[contains(text(), '바로보기')]")
            if not view_links:
                logger.warning(f"바로보기 링크를 찾을 수 없습니다: {title}")
                continue
                
            # 첫 번째 바로보기 링크 클릭
            view_links[0].click()
            
            # iframe 전환 대기
            WebDriverWait(driver, 15).until(
                EC.frame_to_be_available_and_switch_to_it((By.TAG_NAME, "iframe"))
            )
            
            # 테이블 대기
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.TAG_NAME, "table"))
            )
            
            # HTML 파싱
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            tables = soup.find_all('table')
            
            if not tables:
                logger.warning(f"테이블을 찾을 수 없습니다: {title}")
                driver.switch_to.default_content()  # iframe에서 나오기
                continue
                
            # 각 테이블 처리
            for i, table in enumerate(tables):
                if validate_table_structure(table):
                    try:
                        df = pd.read_html(str(table))[0]
                        df.fillna('N/A', inplace=True)  # NaN 값 처리
                        table_name = f"{title}_{i+1}" if len(tables) > 1 else title
                        tables_data[table_name] = df
                        logger.info(f"테이블 추출 성공: {table_name} ({df.shape[0]}행 x {df.shape[1]}열)")
                    except Exception as e:
                        logger.error(f"테이블 파싱 실패: {str(e)}")
            
            # iframe에서 나오기
            driver.switch_to.default_content()
            break
            
        except TimeoutException:
            logger.warning(f"페이지 로드 타임아웃, 재시도 {attempt+1}/{MAX_RETRIES}")
            time.sleep(RETRY_DELAY * (attempt+1))  # 점진적 대기 시간
        except Exception as e:
            logger.error(f"테이블 추출 중 오류 발생: {str(e)}")
            time.sleep(RETRY_DELAY)
    
    if not tables_data:
        logger.warning(f"추출된 테이블이 없습니다: {title}")
        
    return tables_data

def validate_table_structure(table):
    """테이블 구조 검증"""
    # 최소 행 및 셀 수 확인
    rows = table.find_all('tr')
    if len(rows) < 2:  # 헤더 + 최소 1개 데이터 행
        return False
        
    # 모든 행의 셀 수 확인
    cells_per_row = [len(row.find_all(['td', 'th'])) for row in rows]
    if min(cells_per_row) < 2:  # 최소 2개 이상의 열
        return False
        
    return True

def extract_view_link_param(driver, title):
    """바로보기 링크 파라미터 추출"""
    try:
        # 바로보기 링크 찾기
        view_links = driver.find_elements(By.XPATH, "//a[contains(text(), '바로보기')]")
        if not view_links:
            logger.warning(f"바로보기 링크를 찾을 수 없습니다: {title}")
            return None
            
        # 링크 URL 추출
        link_href = view_links[0].get_attribute('href')
        if not link_href:
            logger.warning(f"바로보기 링크 URL을 찾을 수 없습니다: {title}")
            return None
            
        # 파라미터 추출 (JavaScript 함수 호출 형태로부터)
        match = re.search(r"javascript:goFileViewer\('([^']+)'", link_href)
        if match:
            return match.group(1)
        else:
            logger.warning(f"바로보기 링크 파라미터 추출 실패: {title}")
            return None
    except Exception as e:
        logger.error(f"바로보기 링크 파라미터 추출 중 오류 발생: {str(e)}")
        return None

def main():
    """메인 함수"""
    logger.info(f"MSIT 모니터 시작 - days_range={DAYS_RANGE}, check_sheets={CHECK_SHEETS}")
    logger.info(f"스프레드시트 이름: {SPREADSHEET_NAME}")
    
    # WebDriver 초기화
    driver = init_webdriver()
    
    # Google Sheets 클라이언트 초기화
    gspread_client = init_gspread_client() if CHECK_SHEETS else None
    
    try:
        # 기준 날짜 설정: 현재 날짜 - DAYS_RANGE
        cutoff_date = datetime.now() - timedelta(days=DAYS_RANGE)
        cutoff_date_str = cutoff_date.strftime('%Y-%m-%d')
        
        # MSIT 웹사이트 접근
        driver.get(MSIT_URL)
        logger.info("MSIT 웹사이트 접근 완료")
        
        # 시스템 점검 확인
        if is_system_maintenance(driver):
            logger.warning("시스템 점검 중입니다.")
            send_maintenance_alert()
            return
            
        # 게시물 목록 대기
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, ".list_item"))
        )
        
        # 게시물 목록 파싱
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        list_items = soup.select(".list_item")
        
        # 최근 통계 게시물 필터링
        stats_posts = []
        
        for item in list_items:
            # 제목 추출
            title_elem = item.select_one(".list_title")
            if not title_elem:
                continue
                
            title = title_elem.text.strip()
            
            # 날짜 추출
            date_elem = item.select_one(".txtdate")
            if not date_elem:
                continue
                
            date_text = date_elem.text.strip()
            logger.info(f"날짜 문자열 발견: {date_text}")
            
            # 날짜 파싱 (Apr 11, 2025 형식)
            try:
                post_date = datetime.strptime(date_text, '%b %d, %Y')
                post_date_str = post_date.strftime('%Y-%m-%d')
            except:
                logger.warning(f"날짜 파싱 실패: {date_text}")
                continue
                
            logger.info(f"게시물 날짜 확인: {post_date_str} vs {cutoff_date_str} ({DAYS_RANGE}일 전, 한국 시간 기준)")
            
            # 날짜 필터링
            if post_date < cutoff_date:
                continue
                
            # 통계 게시물 확인 (YYYY년 MM월말 기준 패턴)
            if not re.match(r"\(\d{4}년 \d{1,2}월말 기준\)", title):
                continue
                
            # 통계 종류별 필터링
            for pattern in STATS_TITLE_PATTERNS:
                if re.search(pattern, title):
                    stats_posts.append((title, title_elem.get('href')))
                    logger.info(f"통신 통계 게시물 발견: {title}")
                    break
        
        # 발견된 통계 게시물 처리
        logger.info(f"{len(stats_posts)}개 통신 통계 게시물 처리 중")
        
        for title, link in stats_posts:
            logger.info(f"게시물 열기: {title}")
            
            # 게시물 상세 페이지 URL 구성
            detail_url = f"https://www.msit.go.kr{link}"
            
            # 페이지 로드 및 테이블 추출
            tables_data = extract_stats_tables(driver, detail_url, title)
            
            if not tables_data:
                logger.warning(f"바로보기 링크 파라미터 추출 실패: {title}")
                
                # 시스템 점검 확인
                if is_system_maintenance(driver):
                    send_maintenance_alert()
                    break
                    
                continue
            
            # Google Sheets 업데이트
            if CHECK_SHEETS and gspread_client:
                for table_name, df in tables_data.items():
                    # 시트 이름 결정 (기본 통계 카테고리명 사용)
                    sheet_name = None
                    for pattern in STATS_TITLE_PATTERNS:
                        if re.search(pattern, title):
                            sheet_name = re.sub(r"\([^)]+\)", "", pattern).strip()
                            break
                            
                    if not sheet_name:
                        sheet_name = table_name
                        
                    # 스프레드시트 업데이트
                    update_result = update_sheet(gspread_client, sheet_name, df)
                    if update_result:
                        logger.info(f"시트 업데이트 성공: {sheet_name}")
                    else:
                        logger.warning(f"시트 업데이트 실패: {sheet_name}")
                        
            # 성공 메시지 전송
            success_message = f"✅ *MSIT 통계 자료 업데이트 성공*\n"
            success_message += f"• 제목: {title}\n"
            success_message += f"• 시트: {SPREADSHEET_NAME}\n"
            success_message += f"• 테이블 수: {len(tables_data)}\n"
            success_message += f"• 처리 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            
            send_telegram_message(success_message)
            
    except Exception as e:
        logger.error(f"처리 중 오류 발생: {str(e)}")
        
        # 오류 알림 전송
        error_message = f"⚠️ *MSIT 통계 자료 처리 오류*\n"
        error_message += f"• 오류: {str(e)}\n"
        error_message += f"• 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        
        send_telegram_message(error_message)
        
    finally:
        # WebDriver 종료
        if driver:
            driver.quit()
            logger.info("WebDriver 종료")

if __name__ == "__main__":
    main()
