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
from selenium.webdriver.chrome.service import Service
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
DAYS_RANGE = int(os.environ.get('DAYS_RANGE', 4))
CHECK_SHEETS = os.environ.get('CHECK_SHEETS', 'true').lower() == 'true'
SPREADSHEET_NAME = os.environ.get('SPREADSHEET_NAME', 'MSIT 통신 통계')

# 과기정통부 통신통계 URL
MSIT_URL = "https://www.msit.go.kr/bbs/list.do?sCode=user&mPid=74&mId=99"

def is_system_maintenance(driver):
    """시스템 점검 중인지 확인"""
    maintenance_phrases = ["시스템 점검", "점검 중", "서비스 일시 중단"]
    page_source = driver.page_source.lower()
    return any(phrase.lower() in page_source for phrase in maintenance_phrases)

def send_telegram_message(message):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        logger.warning("텔레그램 토큰 또는 채팅 ID가 설정되지 않았습니다.")
        return False
    
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    data = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message
    }
    
    try:
        response = requests.post(url, data=data)
        response.raise_for_status()
        logger.info("텔레그램 메시지 전송 성공")
        return True
    except Exception as e:
        logger.error(f"텔레그램 메시지 전송 실패: {str(e)}")
        return False

def init_webdriver():
    """웹드라이버 초기화"""
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage") 
    chrome_options.add_argument("--window-size=1920,1080")
    
    # chromium-browser 경로 명시적 설정
    chrome_options.binary_location = "/usr/bin/chromium-browser"
    
    # ChromeDriver 경로 찾기
    chromedriver_path = "/usr/bin/chromedriver"
    service = Service(executable_path=chromedriver_path)
    
    driver = webdriver.Chrome(service=service, options=chrome_options)
    driver.set_page_load_timeout(30)
    logger.info("WebDriver 초기화 완료")
    return driver

def init_gspread_client():
    if not GSPREAD_JSON_BASE64 or not CHECK_SHEETS:
        return None
    
    try:
        import base64
        import json
        json_str = base64.b64decode(GSPREAD_JSON_BASE64).decode('utf-8')
        json_data = json.loads(json_str)
        
        scope = ['https://spreadsheets.google.com/feeds',
                'https://www.googleapis.com/auth/drive']
        
        creds = ServiceAccountCredentials.from_json_keyfile_dict(json_data, scope)
        client = gspread.authorize(creds)
        
        logger.info("Google Sheets 클라이언트 초기화 완료")
        return client
    except Exception as e:
        logger.error(f"Google Sheets 클라이언트 초기화 실패: {str(e)}")
        return None

def update_gspread(client, sheet_name, dataframe):
    if not client or dataframe is None:
        return False
    
    try:
        spreadsheet = client.open(SPREADSHEET_NAME)
        
        # 시트 찾기 또는 생성
        try:
            worksheet = spreadsheet.worksheet(sheet_name)
        except:
            worksheet = spreadsheet.add_worksheet(title=sheet_name, rows=100, cols=20)
            logger.info(f"새 시트 생성: {sheet_name}")
        
        # 데이터프레임을 리스트로 변환
        data_list = [dataframe.columns.tolist()]
        data_list.extend(dataframe.fillna('').values.tolist())
        
        # 시트 초기화 및 데이터 쓰기
        worksheet.clear()
        worksheet.update(data_list)
        
        logger.info(f"시트 업데이트 완료: {sheet_name}")
        return True
    except Exception as e:
        logger.error(f"시트 업데이트 실패: {str(e)}")
        return False

def main():
    logger.info(f"MSIT 모니터 시작 - days_range={DAYS_RANGE}, check_sheets={CHECK_SHEETS}")
    logger.info(f"스프레드시트 이름: {SPREADSHEET_NAME}")
    
    # 웹드라이버 초기화
    driver = None
    try:
        driver = init_webdriver()
        
        # Google Sheets 클라이언트 초기화
        gspread_client = init_gspread_client()
        
        # 기준 날짜 설정 (현재 날짜로부터 DAYS_RANGE일 전)
        cutoff_date = datetime.now() - timedelta(days=DAYS_RANGE)
        cutoff_date_str = cutoff_date.strftime('%Y-%m-%d')
        
        # MSIT 웹사이트 접근
        driver.get(MSIT_URL)
        logger.info("MSIT 웹사이트 접근 완료")
        
        # 시스템 점검 확인
        if is_system_maintenance(driver):
            logger.warning("시스템 점검 중입니다.")
            send_telegram_message("⚠️ MSIT 시스템 점검 중입니다. 데이터 수집이 일시 중단되었습니다.")
            return
        
        # 게시물 목록 로드 대기
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, ".list_item"))
        )
        
        # 게시물 목록 파싱
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        list_items = soup.select(".list_item")
        
        # 통계 게시물 리스트
        stats_posts = []
        
        for item in list_items:
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
            
            try:
                post_date = datetime.strptime(date_text, '%b %d, %Y')
                post_date_str = post_date.strftime('%Y-%m-%d')
            except ValueError:
                logger.warning(f"날짜 파싱 실패: {date_text}")
                continue
                
            logger.info(f"게시물 날짜 확인: {post_date_str} vs {cutoff_date_str} ({DAYS_RANGE}일 전, 한국 시간 기준)")
            
            # 날짜 필터링
            if post_date < cutoff_date:
                continue
                
            # 통계 게시물 확인 (XXXX년 X월말 기준 패턴)
            if "월말 기준" in title and any(stat_type in title for stat_type in [
                "이동전화", "유선통신서비스", "무선통신서비스", "특수부가통신", 
                "무선데이터", "트래픽 통계", "번호이동"
            ]):
                link = title_elem.get('href')
                if link:
                    stats_posts.append((title, link))
                    logger.info(f"통신 통계 게시물 발견: {title}")
        
        logger.info(f"{len(stats_posts)}개 통신 통계 게시물 처리 중")
        
        # 게시물 처리
        for title, link in stats_posts:
            logger.info(f"게시물 열기: {title}")
            
            # 게시물 상세 페이지 URL 구성
            detail_url = f"https://www.msit.go.kr{link}"
            
            # 최대 3번 재시도
            for attempt in range(3):
                try:
                    # 게시물 상세 페이지 로드
                    driver.get(detail_url)
                    
                    # 시스템 점검 확인
                    if is_system_maintenance(driver):
                        logger.warning("시스템 점검 중입니다.")
                        send_telegram_message("⚠️ MSIT 시스템 점검 중입니다. 데이터 수집이 일시 중단되었습니다.")
                        return
                    
                    # 페이지 로드 대기
                    WebDriverWait(driver, 15).until(
                        EC.presence_of_element_located((By.CLASS_NAME, "view_head"))
                    )
                    
                    # 바로보기 링크 찾기
                    view_links = driver.find_elements(By.XPATH, "//a[contains(text(), '바로보기')]")
                    if not view_links:
                        logger.warning(f"바로보기 링크를 찾을 수 없습니다: {title}")
                        break
                    
                    # 바로보기 링크 클릭
                    view_links[0].click()
                    
                    # iframe 전환 대기 및 전환
                    WebDriverWait(driver, 10).until(
                        EC.frame_to_be_available_and_switch_to_it((By.TAG_NAME, "iframe"))
                    )
                    
                    # 테이블 대기
                    WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located((By.TAG_NAME, "table"))
                    )
                    
                    # HTML 파싱
                    soup = BeautifulSoup(driver.page_source, 'html.parser')
                    tables = soup.find_all('table')
                    
                    if not tables:
                        logger.warning(f"테이블을 찾을 수 없습니다: {title}")
                        driver.switch_to.default_content()  # iframe에서 나오기
                        break
                    
                    # 시트 이름 결정
                    sheet_name = None
                    if "이동전화 및 트래픽" in title:
                        sheet_name = "이동전화 및 트래픽 통계"
                    elif "번호이동" in title:
                        sheet_name = "이동전화 및 시내전화 번호이동 현황"
                    elif "유선통신서비스" in title:
                        sheet_name = "유선통신서비스 가입 현황"
                    elif "무선통신서비스" in title and "유·무선통신서비스" not in title:
                        sheet_name = "무선통신서비스 가입 현황"
                    elif "특수부가통신" in title:
                        sheet_name = "특수부가통신사업자현황"
                    elif "무선데이터 트래픽" in title and "유·무선통신서비스" not in title:
                        sheet_name = "무선데이터 트래픽 통계"
                    elif "유·무선통신서비스" in title:
                        sheet_name = "유·무선통신서비스 가입 현황 및 무선데이터 트래픽 통계"
                    else:
                        sheet_name = title.split(") ")[1] if ") " in title else title
                    
                    # 각 테이블 처리
                    for i, table in enumerate(tables):
                        try:
                            # 테이블에 최소 2개 이상의 행이 있는지 확인
                            rows = table.find_all('tr')
                            if len(rows) < 2:
                                continue
                            
                            # 각 행에 최소 2개 이상의 셀이 있는지 확인
                            if any(len(row.find_all(['td', 'th'])) < 2 for row in rows):
                                continue
                            
                            # 테이블 데이터 추출
                            df = pd.read_html(str(table))[0]
                            df.fillna('N/A', inplace=True)  # NaN 값 처리
                            
                            # 테이블이 여러 개인 경우 시트 이름에 번호 추가
                            current_sheet_name = f"{sheet_name}_{i+1}" if len(tables) > 1 else sheet_name
                            
                            # 구글 스프레드시트 업데이트
                            if CHECK_SHEETS and gspread_client:
                                update_gspread(gspread_client, current_sheet_name, df)
                            
                            logger.info(f"테이블 추출 성공: {current_sheet_name} ({df.shape[0]}행 x {df.shape[1]}열)")
                        except Exception as e:
                            logger.error(f"테이블 파싱 실패: {str(e)}")
                    
                    # iframe에서 나오기
                    driver.switch_to.default_content()
                    
                    # 성공 메시지 전송
                    send_telegram_message(f"✅ MSIT 통계 업데이트 성공: {title}")
                    break
                    
                except TimeoutException:
                    logger.warning(f"페이지 로드 타임아웃, 재시도 {attempt+1}/3")
                    time.sleep(5)  # 5초 대기 후 재시도
                    
                    if attempt == 2:  # 마지막 시도 실패
                        logger.error("3번 시도 후 페이지 로드 실패")
                        if is_system_maintenance(driver):
                            logger.warning("시스템 점검 중입니다.")
                            send_telegram_message("⚠️ MSIT 시스템 점검 중입니다. 데이터 수집이 일시 중단되었습니다.")
                        else:
                            send_telegram_message(f"❌ 통계 업데이트 실패: {title} (타임아웃)")
                except Exception as e:
                    logger.error(f"처리 중 오류 발생: {str(e)}")
                    send_telegram_message(f"❌ 통계 업데이트 실패: {title} ({str(e)})")
                    break
                
    except Exception as e:
        logger.error(f"처리 중 오류 발생: {str(e)}")
        send_telegram_message(f"❌ MSIT 모니터링 오류: {str(e)}")
        
    finally:
        # WebDriver 종료
        if driver:
            driver.quit()
            logger.info("WebDriver 종료")

if __name__ == "__main__":
    main()
