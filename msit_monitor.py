#!/usr/bin/env python
# -*- coding: utf-8 -*-

# MSIT 통신 통계 데이터 모니터링 및 추출 스크립트
# 완전한 버전

import os
import sys
import time
import json
import random
import logging
import re
import pandas as pd
from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException, StaleElementReferenceException
from webdriver_manager.chrome import ChromeDriverManager
from selenium_stealth import stealth
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from bs4 import BeautifulSoup
import requests

# 텔레그램 알림 설정
def setup_telegram_bot():
    """텔레그램 봇 설정"""
    import telegram
    
    token = os.environ.get('TELCO_NEWS_TOKEN')
    chat_id = os.environ.get('TELCO_NEWS_TESTER')
    
    if not token or not chat_id:
        logging.warning("텔레그램 토큰 또는 채팅 ID가 없음")
        return None
    
    try:
        bot = telegram.Bot(token=token)
        return {
            'bot': bot,
            'chat_id': chat_id
        }
    except Exception as e:
        logging.error(f"텔레그램 봇 초기화 오류: {str(e)}")
        return None

def send_telegram_message(telegram_config, message, image_path=None):
    """텔레그램으로 메시지 전송"""
    if not telegram_config:
        return False
    
    try:
        bot = telegram_config['bot']
        chat_id = telegram_config['chat_id']
        
        if image_path and os.path.exists(image_path):
            with open(image_path, 'rb') as photo:
                bot.send_photo(chat_id=chat_id, photo=photo, caption=message)
        else:
            bot.send_message(chat_id=chat_id, text=message, parse_mode='HTML')
        
        logging.info("텔레그램 메시지 전송 성공")
        return True
    except Exception as e:
        logging.error(f"텔레그램 메시지 전송 오류: {str(e)}")
        return False

# WebDriver 설정
def setup_webdriver():
    """Chrome WebDriver 설정"""
    options = Options()
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--headless')
    
    # 랜덤 User-Agent 선택
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.71 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.1 Safari/605.1.15",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36"
    ]
    user_agent = random.choice(user_agents)
    logging.info(f"선택된 User-Agent: {user_agent}")
    options.add_argument(f'user-agent={user_agent}')
    
    # 고유 사용자 데이터 디렉토리 사용
    unique_dir = f'/tmp/chrome-user-data-{int(time.time())}-{random.randint(1000, 9999)}'
    options.add_argument(f'--user-data-dir={unique_dir}')
    logging.info(f"임시 사용자 데이터 디렉토리 생성: {unique_dir}")
    
    # 자동화 감지 방지 옵션
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)
    
    logging.info("ChromeDriverManager를 통한 드라이버 설치 시작...")
    driver_path = ChromeDriverManager().install()
    logging.info("ChromeDriverManager를 통한 드라이버 설치 완료")
    
    driver = webdriver.Chrome(service=Service(driver_path), options=options)
    
    # Selenium Stealth 설정
    stealth(
        driver,
        languages=["ko-KR", "ko", "en-US", "en"],
        vendor="Google Inc.",
        platform="Win32",
        webgl_vendor="Intel Inc.",
        renderer="Intel Iris OpenGL Engine",
        fix_hairline=True,
    )
    logging.info("Selenium Stealth 적용 완료")
    
    # 추가 스텔스 설정
    try:
        # WebDriver 속성 재정의
        driver.execute_script("""
        Object.defineProperty(navigator, 'webdriver', {
            get: () => false
        });
        """)
        
        # User-Agent 재설정
        driver.execute_script(f"Object.defineProperty(navigator, 'userAgent', {{get: () => '{user_agent}'}});")
        logging.info(f"JavaScript로 User-Agent 재설정: {user_agent}")
    except Exception as e:
        logging.warning(f"웹드라이버 감지 회피 JavaScript 실행 실패 (시도 1/3): {str(e)}")
    
    driver.set_page_load_timeout(30)
    logging.info("WebDriver 초기화 완료")
    
    return driver

# Google Sheets 설정
def setup_google_sheets(spreadsheet_name):
    """Google Sheets API 연결"""
    try:
        # 환경 변수에서 인증 정보 가져오기
        creds_json = os.environ.get('MSIT_GSPREAD_ref')
        if not creds_json:
            logging.warning("Google Sheets 인증 정보 없음")
            return None
        
        # 인증 정보를 임시 파일로 저장
        with open('gspread_credentials.json', 'w') as f:
            f.write(creds_json)
        
        # Google Sheets API 인증
        scope = [
            'https://spreadsheets.google.com/feeds',
            'https://www.googleapis.com/auth/drive'
        ]
        creds = ServiceAccountCredentials.from_json_keyfile_name('gspread_credentials.json', scope)
        client = gspread.authorize(creds)
        
        # 임시 파일 삭제
        os.remove('gspread_credentials.json')
        
        logging.info(f"Google Sheets API 연결 확인: {spreadsheet_name}")
        return client
    except Exception as e:
        logging.error(f"Google Sheets 연결 오류: {str(e)}")
        return None

# 문서 처리 함수들
def find_and_process_posts(driver, days_range=10):
    """MSIT 웹사이트에서 통신 통계 게시물 찾기 및 처리"""
    base_url = "https://www.msit.go.kr/bbs/list.do?sCode=user&mPid=74&mId=99"
    
    try:
        logging.info(f"MSIT 통계정보 페이지 접속: {base_url}")
        driver.get(base_url)
        
        # 페이지 로딩 대기
        wait = WebDriverWait(driver, 10)
        wait.until(EC.presence_of_element_located((By.CLASS_NAME, "list_wrap")))
        
        # 실패 시 페이지 소스 저장
        with open("msit_list_page.html", "w", encoding="utf-8") as f:
            f.write(driver.page_source)
        
        # 게시물 목록 파싱
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        posts = soup.select('ul.list li')
        
        telco_posts = []
        current_date = datetime.now()
        oldest_allowed_date = current_date - timedelta(days=days_range)
        
        for post in posts:
            try:
                # 게시물 정보 추출
                title_elem = post.select_one('p.title')
                date_elem = post.select_one('span.date')
                
                if not title_elem or not date_elem:
                    continue
                
                title = title_elem.get_text(strip=True)
                date_str = date_elem.get_text(strip=True)
                
                # 날짜 파싱
                try:
                    # 형식: "YYYY. MM. DD"
                    date_parts = date_str.split('.')
                    post_date = datetime(
                        year=int(date_parts[0].strip()),
                        month=int(date_parts[1].strip()),
                        day=int(date_parts[2].strip())
                    )
                    
                    # 날짜 범위 확인
                    if post_date < oldest_allowed_date:
                        logging.info(f"날짜 범위 밖의 게시물: {date_str}, 이후 게시물 검색 중단")
                        break
                    
                except Exception as date_error:
                    logging.warning(f"날짜 파싱 오류: {date_str} - {str(date_error)}")
                    continue
                
                # 통신 통계 게시물 필터링
                if "통신서비스" in title and ("가입" in title or "현황" in title or "통계" in title):
                    telco_posts.append({
                        'title': title,
                        'date': date_str,
                        'post_date': post_date,
                        'element': title_elem
                    })
            except Exception as post_error:
                logging.warning(f"게시물 처리 오류: {str(post_error)}")
        
        logging.info(f"페이지 1 파싱 결과: {len(posts)}개 게시물, {len(telco_posts)}개 통신 통계")
        
        # 통신 통계 게시물 처리
        processed_posts = []
        
        for i, post in enumerate(telco_posts):
            logging.info(f"게시물 {i+1}/{len(telco_posts)} 처리 중: {post['title']}")
            
            # 게시물 상세 페이지 열기
            post_data = open_post_detail(driver, post['title'])
            if post_data:
                processed_posts.append(post_data)
        
        return processed_posts
    
    except Exception as e:
        logging.error(f"게시물 검색 및 처리 오류: {str(e)}")
        driver.save_screenshot("error_page.png")
        return []

def open_post_detail(driver, post_title):
    """게시물 상세 페이지 열기"""
    try:
        logging.info(f"게시물 열기: {post_title}")
        
        # XPath로 게시물 링크 찾기
        xpath = f"//p[contains(@class, 'title') and contains(text(), '{post_title[:20]}')]"
        wait = WebDriverWait(driver, 10)
        link = wait.until(EC.presence_of_element_located((By.XPATH, xpath)))
        
        logging.info(f"게시물 링크 발견 (선택자: {xpath})")
        
        # 링크 클릭 전 스크린샷
        post_id = post_title.replace(" ", "_")[:20]
        screenshot_path = f"screenshots/before_click_{post_id}_{int(time.time())}.png"
        driver.save_screenshot(screenshot_path)
        logging.info(f"스크린샷 저장: {screenshot_path}")
        
        # 게시물 링크 클릭
        logging.info(f"게시물 링크 클릭 시도: {post_title}")
        
        try:
            link.click()
        except:
            # JavaScript로 클릭 시도
            logging.info("JavaScript를 통한 클릭 실행")
            driver.execute_script("arguments[0].click();", link)
        
        # 페이지 전환 대기
        time.sleep(1)
        current_url = driver.current_url
        logging.info(f"페이지 URL 변경 감지됨: {current_url}")
        
        # 상세 페이지 로드 대기
        wait = WebDriverWait(driver, 10)
        view_head = wait.until(EC.presence_of_element_located((By.CLASS_NAME, "view_head")))
        logging.info("상세 페이지 로드 완료: view_head 요소 발견")
        
        # 상세 페이지 스크린샷
        screenshot_path = f"screenshots/post_view_clicked_{post_id}_{int(time.time())}.png"
        driver.save_screenshot(screenshot_path)
        logging.info(f"스크린샷 저장: {screenshot_path}")
        
        # 바로보기 링크 찾기
        view_links = driver.find_elements(By.CSS_SELECTOR, "a[onclick*='getExtension_path']")
        file_view_url = None
        
        for link in view_links:
            if link.text and "바로보기" in link.text:
                onclick = link.get_attribute("onclick")
                href = link.get_attribute("href")
                
                logging.info(f"바로보기 링크 발견, onclick: {onclick}, href: {href}")
                
                # 파일 ID와 순서 ID 추출
                match = re.search(r"getExtension_path\('(\d+)', '(\d+)'\)", onclick)
                if match:
                    file_id = match.group(1)
                    order_id = match.group(2)
                    file_view_url = f"https://www.msit.go.kr/bbs/documentView.do?atchFileNo={file_id}&fileOrdr={order_id}"
                    logging.info(f"바로보기 URL: {file_view_url}")
                    break
        
        if not file_view_url:
            logging.warning(f"바로보기 링크를 찾을 수 없음: {post_title}")
            return None
        
        # 문서 데이터 추출
        document_data = extract_document_data(driver, file_view_url, post_title)
        
        return {
            'title': post_title,
            'detail_url': current_url,
            'file_view_url': file_view_url,
            'document_data': document_data
        }
    
    except Exception as e:
        logging.error(f"게시물 상세 페이지 열기 오류: {str(e)}")
        driver.save_screenshot(f"error_post_detail_{int(time.time())}.png")
        return None

def wait_for_sheet_stability(driver, timeout=10, check_interval=0.5):
    """시트 전환 후 페이지가 안정화될 때까지 대기"""
    start_time = time.time()
    last_html = ""
    stable_count = 0
    required_stable_checks = 3  # 연속 3회 HTML이 같으면 안정적으로 간주
    
    while time.time() - start_time < timeout:
        current_html = driver.page_source
        
        if current_html == last_html:
            stable_count += 1
            if stable_count >= required_stable_checks:
                logging.info(f"페이지 안정화 확인: {stable_count}회 연속 동일 HTML")
                return True
        else:
            stable_count = 0
        
        last_html = current_html
        time.sleep(check_interval)
    
    logging.warning(f"페이지 안정화 타임아웃: {timeout}초 경과")
    return False

def extract_tables_javascript(driver):
    """JavaScript를 통한 테이블 추출"""
    try:
        tables_data = driver.execute_script('''
            const tables = document.querySelectorAll('table');
            let result = [];
            
            tables.forEach((table, tableIndex) => {
                let tableData = {
                    index: tableIndex,
                    rows: []
                };
                
                const rows = table.querySelectorAll('tr');
                rows.forEach(row => {
                    let rowData = [];
                    const cells = row.querySelectorAll('td, th');
                    cells.forEach(cell => {
                        // 셀 병합 정보 추출
                        const rowspan = cell.getAttribute('rowspan') || 1;
                        const colspan = cell.getAttribute('colspan') || 1;
                        
                        rowData.push({
                            text: cell.textContent.trim(),
                            rowspan: parseInt(rowspan),
                            colspan: parseInt(colspan)
                        });
                    });
                    
                    if (rowData.length > 0) {
                        tableData.rows.push(rowData);
                    }
                });
                
                if (tableData.rows.length > 0) {
                    result.push(tableData);
                }
            });
            
            return result;
        ''')
        
        if tables_data and len(tables_data) > 0:
            return tables_data
        else:
            return None
    except Exception as e:
        logging.warning(f"JavaScript 테이블 추출 오류: {str(e)}")
        return None

def extract_tables_html(html_content):
    """BeautifulSoup을 사용한 테이블 추출"""
    try:
        soup = BeautifulSoup(html_content, 'html.parser')
        tables = soup.find_all('table')
        
        if tables and len(tables) > 0:
            return tables
        else:
            return None
    except Exception as e:
        logging.warning(f"HTML 테이블 추출 오류: {str(e)}")
        return None

def create_dataframe_from_table(table_data):
    """테이블 데이터를 DataFrame으로 변환"""
    try:
        if not table_data or 'rows' not in table_data or not table_data['rows']:
            return None
        
        # JavaScript로 추출한 데이터는 각 셀이 객체 형태임
        if isinstance(table_data['rows'][0][0], dict) and 'text' in table_data['rows'][0][0]:
            # 병합 셀을 고려한 그리드 생성
            grid = create_grid_from_table_data(table_data['rows'])
            df = convert_grid_to_dataframe(grid)
        else:
            # 단순 2D 배열 형태인 경우
            rows = table_data['rows']
            df = pd.DataFrame(rows)
            
            # 첫 번째 행이 헤더인지 확인
            if len(df) > 1:
                headers = df.iloc[0].tolist()
                df = df.iloc[1:]
                df.columns = headers
        
        return df
    except Exception as e:
        logging.warning(f"DataFrame 변환 오류: {str(e)}")
        return None

def create_grid_from_table_data(table_data):
    """병합 셀이 포함된 테이블에서 정규화된 그리드 생성"""
    if not table_data:
        return []
        
    # 그리드 크기 계산
    rows_count = len(table_data)
    cols_count = max([sum([cell['colspan'] for cell in row]) for row in table_data])
    
    # 빈 그리드 초기화
    grid = [[''] * cols_count for _ in range(rows_count)]
    
    # 셀 위치 추적용 점유 그리드
    occupied = [[False] * cols_count for _ in range(rows_count)]
    
    # 테이블 데이터를 그리드에 채우기
    for row_idx, row in enumerate(table_data):
        col_idx = 0
        for cell in row:
            # 사용 가능한 열 위치 찾기
            while col_idx < cols_count and occupied[row_idx][col_idx]:
                col_idx += 1
                
            if col_idx >= cols_count:
                break
                
            # 셀 데이터 추출
            text = cell['text']
            rowspan = min(cell['rowspan'], rows_count - row_idx)
            colspan = min(cell['colspan'], cols_count - col_idx)
            
            # 그리드에 셀 데이터 채우기
            for r in range(rowspan):
                for c in range(colspan):
                    if row_idx + r < rows_count and col_idx + c < cols_count:
                        if r == 0 and c == 0:
                            grid[row_idx + r][col_idx + c] = text
                        occupied[row_idx + r][col_idx + c] = True
            
            col_idx += colspan
    
    return grid

def convert_grid_to_dataframe(grid):
    """그리드 데이터를 pandas DataFrame으로 변환"""
    if not grid or len(grid) < 2:  # 헤더 + 데이터 최소 2행 필요
        return None
        
    try:
        # 헤더 추출 (첫 번째 행)
        headers = grid[0]
        
        # 빈 헤더 채우기
        for i in range(len(headers)):
            if not headers[i]:
                headers[i] = f"Column_{i+1}"
                
        # 데이터 행 (두 번째 행부터)
        data = grid[1:]
        
        # DataFrame 생성
        df = pd.DataFrame(data, columns=headers)
        
        # 데이터 정제
        # 1. 모든 열의, 모든 행이 비어있으면 해당 열 제거
        df = df.loc[:, ~df.isna().all()]
        df = df.loc[:, ~(df == '').all()]
        
        # 2. 모든 행의, 모든 열이 비어있으면 해당 행 제거
        df = df.dropna(how='all')
        df = df[~(df == '').all(axis=1)]
        
        return df
    
    except Exception as e:
        logging.warning(f"DataFrame 변환 오류: {str(e)}")
        return None

def create_dataframe_from_html_table(table_html):
    """HTML 테이블을 DataFrame으로 변환"""
    try:
        rows = []
        for tr in table_html.find_all('tr'):
            row = []
            for cell in tr.find_all(['td', 'th']):
                # 셀 병합 정보 추출
                rowspan = int(cell.get('rowspan', 1))
                colspan = int(cell.get('colspan', 1))
                text = cell.get_text(strip=True)
                
                # 기본 텍스트 추출
                row.append({
                    'text': text,
                    'rowspan': rowspan,
                    'colspan': colspan
                })
            
            if row:  # 빈 행 건너뛰기
                rows.append(row)
        
        if not rows:
            return None
        
        # 병합 셀 처리를 위한 그리드 생성
        grid = create_grid_from_table_data(rows)
        df = convert_grid_to_dataframe(grid)
        
        return df
    except Exception as e:
        logging.warning(f"HTML 테이블 DataFrame 변환 오류: {str(e)}")
        return None

def process_sheet_tabs(driver):
    """시트 탭 처리 및 데이터 추출"""
    sheet_tabs = driver.find_elements(By.CSS_SELECTOR, ".sheet-tab")
    if not sheet_tabs:
        logging.warning("시트 탭을 찾을 수 없음")
        return None
        
    logging.info(f"{len(sheet_tabs)}개 시트 탭 발견")
    all_sheets_data = {}
    
    for idx, tab in enumerate(sheet_tabs):
        try:
            sheet_name = tab.get_attribute("textContent") or f"Sheet{idx+1}"
            sheet_name = sheet_name.strip()
            logging.info(f"시트 {idx+1}/{len(sheet_tabs)} 처리 중: {sheet_name}")
            
            # 탭 클릭
            driver.execute_script("arguments[0].click();", tab)
            
            # 충분한 로딩 대기 (중요!)
            time.sleep(5)
            
            # 시트 전환 후 페이지 안정화 확인
            wait_for_sheet_stability(driver)
            
            # 데이터 추출 시도
            tables_data = extract_tables_javascript(driver)
            
            if tables_data:
                all_sheets_data[sheet_name] = tables_data
                logging.info(f"시트 '{sheet_name}'에서 {len(tables_data)}개 테이블 추출 성공")
            else:
                # 대체 방법으로 시도
                html_content = driver.page_source
                tables_from_html = extract_tables_html(html_content)
                
                if tables_from_html:
                    all_sheets_data[sheet_name] = tables_from_html
                    logging.info(f"대체 방법으로 시트 '{sheet_name}'에서 {len(tables_from_html)}개 테이블 추출")
                else:
                    logging.warning(f"시트 '{sheet_name}'에서 테이블 추출 실패")
        
        except Exception as e:
            logging.warning(f"시트 '{sheet_name}' 처리 오류: {str(e)}")
    
    return all_sheets_data

def extract_document_data(driver, file_view_url, post_title):
    """문서 뷰어에서 데이터 추출"""
    try:
        # 바로보기 URL 열기
        driver.get(file_view_url)
        time.sleep(5)  # 문서 뷰어 로딩 대기
        
        # 현재 URL 기록 (리디렉션 확인)
        current_url = driver.current_url
        logging.info(f"현재 URL: {current_url}")
        
        # 문서 뷰어 스크린샷
        screenshot_path = f"screenshots/iframe_view_{int(time.time())}.png"
        driver.save_screenshot(screenshot_path)
        logging.info(f"스크린샷 저장: {screenshot_path}")
        
        # 문서 뷰어 감지
        document_screenshot = f"document_view_{int(time.time())}.png"
        driver.save_screenshot(document_screenshot)
        logging.info(f"문서 뷰어 스크린샷 저장: {document_screenshot}")
        
        # 페이지 HTML 저장
        with open(f"document_view_{int(time.time())}.html", "w", encoding="utf-8") as f:
            f.write(driver.page_source)
        
        # 문서 뷰어 감지 확인
        if "SynapDocViewServer" in current_url:
            logging.info("문서 뷰어 감지됨")
            
            # 시트 탭 확인
            sheet_tabs = driver.find_elements(By.CSS_SELECTOR, ".sheet-tab")
            if sheet_tabs:
                logging.info(f"시트 탭 {len(sheet_tabs)}개 발견")
                
                # 시트 탭 처리
                return process_sheet_tabs(driver)
            else:
                logging.warning("시트 탭을 찾을 수 없음")
                
                # 직접 테이블 추출 시도
                tables_data = extract_tables_javascript(driver)
                if tables_data:
                    logging.info(f"{len(tables_data)}개 테이블 추출 성공")
                    
                    for table_idx, table in enumerate(tables_data):
                        df = create_dataframe_from_table(table)
                        if df is not None and not df.empty:
                            csv_filename = f"downloads/table_main_{table_idx+1}.csv"
                            df.to_csv(csv_filename, index=False, encoding="utf-8-sig")
                            logging.info(f"CSV 파일 저장: {csv_filename}")
                    
                    return {"main": tables_data}
                else:
                    # BeautifulSoup으로 추출 시도
                    tables_html = extract_tables_html(driver.page_source)
                    if tables_html:
                        logging.info(f"BeautifulSoup으로 {len(tables_html)}개 테이블 추출")
                        
                        for table_idx, table_html in enumerate(tables_html):
                            df = create_dataframe_from_html_table(table_html)
                            if df is not None and not df.empty:
                                csv_filename = f"downloads/table_bs_main_{table_idx+1}.csv"
                                df.to_csv(csv_filename, index=False, encoding="utf-8-sig")
                                logging.info(f"BeautifulSoup CSV 파일 저장: {csv_filename}")
                        
                        return {"main": tables_html}
        
        else:
            logging.warning(f"문서 뷰어를 감지할 수 없음: {current_url}")
        
        logging.warning(f"iframe에서 데이터 추출 실패: {post_title}")
        
        # 추출 실패 시 현재 날짜 정보 추출
        match = re.search(r'\((\d{4})년\s+(\d+)월말', post_title)
        if match:
            year = int(match.group(1))
            month = int(match.group(2))
            # 플레이스홀더 데이터프레임 생성
            placeholder_df = create_placeholder_dataframe(year, month)
            return {"placeholder": [{"dataframe": placeholder_df}]}
        
        return None
    
    except Exception as e:
        logging.error(f"문서 데이터 추출 오류: {str(e)}")
        driver.save_screenshot(f"error_document_extract_{int(time.time())}.png")
        return None

def update_google_sheets(client, spreadsheet_name, post_data):
    """Google Sheets 업데이트"""
    if not client:
        logging.warning("Google Sheets 클라이언트가 초기화되지 않음")
        return False
    
    try:
        # 스프레드시트 열기
        spreadsheet = client.open(spreadsheet_name)
        logging.info(f"스프레드시트 열기 성공: {spreadsheet_name}")
        
        if not post_data or not post_data.get('document_data'):
            logging.warning("업데이트할 시트 데이터가 없음")
            return False
        
        document_data = post_data['document_data']
        updated_sheets = []
        
        for sheet_name, tables in document_data.items():
            for table_idx, table in enumerate(tables):
                try:
                    df = None
                    
                    if isinstance(table, dict) and 'rows' in table:
                        # JavaScript로 추출한 테이블
                        df = create_dataframe_from_table(table)
                    elif isinstance(table, dict) and 'dataframe' in table:
                        # 이미 DataFrame으로 변환된 경우 (예: 플레이스홀더)
                        df = table['dataframe']
                    else:
                        # BeautifulSoup으로 추출한 테이블
                        df = create_dataframe_from_html_table(table)
                    
                    if df is not None and not df.empty:
                        # 워크시트 존재 여부 확인 또는 생성
                        worksheet_name = f"{sheet_name}_{table_idx+1}"
                        try:
                            worksheet = spreadsheet.worksheet(worksheet_name)
                        except:
                            worksheet = spreadsheet.add_worksheet(
                                title=worksheet_name,
                                rows=df.shape[0] + 1,
                                cols=df.shape[1]
                            )
                        
                        # 데이터 업데이트
                        headers = df.columns.tolist()
                        all_values = [headers] + df.values.tolist()
                        
                        worksheet.clear()
                        worksheet.update('A1', all_values)
                        
                        logging.info(f"시트 '{worksheet_name}' 업데이트 완료")
                        updated_sheets.append(worksheet_name)
                except Exception as sheet_error:
                    logging.warning(f"시트 '{sheet_name}_{table_idx+1}' 업데이트 오류: {str(sheet_error)}")
        
        return len(updated_sheets) > 0
    
    except Exception as e:
        logging.error(f"Google Sheets 업데이트 오류: {str(e)}")
        return False

def create_placeholder_dataframe(year, month):
    """플레이스홀더 데이터프레임 생성"""
    logging.info(f"플레이스홀더 데이터프레임 생성: {year}년 {month}월 무선통신서비스 가입 현황")
    
    # 샘플 데이터프레임 생성
    df = pd.DataFrame({
        '구분': ['이동전화', 'MVNO', 'MVNO비중'],
        '가입자수(만명)': ['', '', ''],
        '전월대비증감(만명)': ['', '', ''],
        '전월대비증감률(%)': ['', '', '']
    })
    
    return df

def save_tables_to_excel(document_data, output_file="extracted_tables.xlsx"):
    """추출된 테이블 데이터를 Excel 파일로 저장"""
    if not document_data:
        logging.warning("저장할 테이블 데이터가 없음")
        return False
    
    try:
        with pd.ExcelWriter(output_file) as writer:
            sheets_count = 0
            
            for sheet_name, tables in document_data.items():
                for table_idx, table in enumerate(tables):
                    df = None
                    
                    if isinstance(table, dict) and 'rows' in table:
                        # JavaScript로 추출한 테이블
                        df = create_dataframe_from_table(table)
                    elif isinstance(table, dict) and 'dataframe' in table:
                        # 이미 DataFrame으로 변환된 경우 (예: 플레이스홀더)
                        df = table['dataframe']
                    else:
                        # BeautifulSoup으로 추출한 테이블
                        df = create_dataframe_from_html_table(table)
                    
                    if df is not None and not df.empty:
                        # Excel 시트 이름 생성 (31자 제한)
                        sheet_tab_name = f"{sheet_name}_{table_idx+1}"
                        if len(sheet_tab_name) > 31:
                            sheet_tab_name = sheet_tab_name[:28] + "..."
                        
                        # DataFrame을 Excel 시트로 저장
                        df.to_excel(writer, sheet_name=sheet_tab_name, index=False)
                        sheets_count += 1
        
        if sheets_count > 0:
            logging.info(f"{sheets_count}개 테이블을 Excel 파일로 저장: {output_file}")
            return True
        else:
            logging.warning("저장할 테이블이 없음")
            return False
    
    except Exception as e:
        logging.error(f"Excel 파일 저장 오류: {str(e)}")
        return False

def main():
    """메인 함수"""
    # 로깅 설정
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler("msit_monitor.log", encoding="utf-8")
        ]
    )
    
    # 실행 인자 가져오기
    days_range = int(os.environ.get('DAYS_RANGE', '10'))
    check_sheets = os.environ.get('CHECK_SHEETS', 'true').lower() == 'true'
    spreadsheet_name = os.environ.get('SPREADSHEET_NAME', 'MSIT 통신 통계')
    
    logging.info(f"MSIT 모니터 시작 - days_range={days_range}, check_sheets={check_sheets}")
    logging.info(f"스프레드시트 이름: {spreadsheet_name}")
    logging.info(f"=== MSIT 통신 통계 모니터링 시작 (days_range={days_range}, check_sheets={check_sheets}) ===")
    
    # 디렉토리 생성
    os.makedirs("screenshots", exist_ok=True)
    os.makedirs("downloads", exist_ok=True)
    
    # 텔레그램 봇 설정
    telegram_config = setup_telegram_bot()
    
    # Google Sheets 설정
    sheets_client = None
    if check_sheets:
        sheets_client = setup_google_sheets(spreadsheet_name)
    
    # WebDriver 초기화
    driver = setup_webdriver()
    
    start_time = time.time()
    posts_count = 0
    updated_count = 0
    
    try:
        # 게시물 검색 및 처리
        posts = find_and_process_posts(driver, days_range)
        posts_count = len(posts)
        
        if posts:
            logging.info(f"{posts_count}개 통신 통계 게시물 처리 중")
            
            # 모든 게시물 데이터를 Excel로 저장
            for i, post in enumerate(posts):
                if post.get('document_data'):
                    excel_file = f"downloads/tables_{i+1}_{int(time.time())}.xlsx"
                    save_tables_to_excel(post['document_data'], excel_file)
            
            # Google Sheets 업데이트
            if check_sheets and sheets_client:
                for post in posts:
                    success = update_google_sheets(sheets_client, spreadsheet_name, post)
                    if success:
                        updated_count += 1
        else:
            logging.info("처리할 통신 통계 게시물이 없음")
        
        # 실행 시간 계산
        elapsed_time = time.time() - start_time
        logging.info(f"실행 시간: {elapsed_time:.2f}초")
        
        # 실행 결과 알림
        if telegram_config:
            message = f"MSIT 통신 통계 모니터링 완료\n"
            message += f"- 검색된 게시물: {posts_count}개\n"
            message += f"- 업데이트된 항목: {updated_count}개\n"
            message += f"- 실행 시간: {elapsed_time:.2f}초"
            
            send_telegram_message(telegram_config, message)
            logging.info(f"알림 전송 완료: {posts_count}개 게시물, {updated_count}개 업데이트")
    
    except Exception as e:
        logging.error(f"모니터링 실행 오류: {str(e)}")
        
        if telegram_config:
            error_message = f"MSIT 통신 통계 모니터링 오류 발생\n"
            error_message += f"- 오류 내용: {str(e)}\n"
            error_message += f"- 실행 시간: {time.time() - start_time:.2f}초"
            
            send_telegram_message(telegram_config, error_message)
    
    finally:
        # WebDriver 종료
        driver.quit()
        logging.info("WebDriver 종료")
        logging.info("=== MSIT 통신 통계 모니터링 종료 ===")

if __name__ == "__main__":
    main()
