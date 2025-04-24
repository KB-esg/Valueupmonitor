"""
통합 모듈 사용 예시와 메인 워크플로우 - 개선된 버전
"""

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
from selenium.common.exceptions import TimeoutException

import telegram
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('msit_monitor')

# 설정 로드
def load_config():
    """
    환경 변수 및 기본 설정 로드
    
    Returns:
        dict: 설정 딕셔너리
    """
    config = {
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
        'ocr_enabled': os.environ.get('OCR_ENABLED', 'true').lower() in ('true', 'yes', '1', 'y'),
        'cleanup_old_sheets': os.environ.get('CLEANUP_OLD_SHEETS', 'false').lower() in ('true', 'yes', '1', 'y'),
        'update_consolidation': os.environ.get('UPDATE_CONSOLIDATION', 'true').lower() in ('true', 'yes', '1', 'y'),
        'max_retries': int(os.environ.get('MAX_RETRIES', '3')),
        'api_request_wait': int(os.environ.get('API_REQUEST_WAIT', '2')),
        'page_load_timeout': int(os.environ.get('PAGE_LOAD_TIMEOUT', '30'))
    }
    
    return config

# 임시 디렉토리 설정
def setup_directories():
    """
    필요한 디렉토리 생성
    """
    dirs = [
        Path("./downloads"),
        Path("./screenshots"),
        Path("./logs")
    ]
    
    for directory in dirs:
        directory.mkdir(exist_ok=True)
        logger.info(f"디렉토리 확인: {directory}")

# 메인 처리 함수
async def process_telecom_stats_posts(driver, gs_client, telecom_stats_posts, check_sheets=True):
    """
    통신 통계 게시물 처리 및 데이터 추출
    
    Args:
        driver: Selenium WebDriver 인스턴스
        gs_client: Google Sheets 클라이언트
        telecom_stats_posts: 통신 통계 게시물 목록
        check_sheets: Google Sheets 업데이트 여부
        
    Returns:
        list: 처리된 데이터 업데이트 정보 목록
    """
    # 데이터 추출 관리자 초기화
    from extraction_functions import DataExtractionManager
    extraction_manager = DataExtractionManager(driver, CONFIG)
    
    data_updates = []
    
    if gs_client and telecom_stats_posts and check_sheets:
        logger.info(f"{len(telecom_stats_posts)}개 통신 통계 게시물 처리 중")
        
        for i, post in enumerate(telecom_stats_posts):
            try:
                logger.info(f"게시물 {i+1}/{len(telecom_stats_posts)} 처리 중: {post['title']}")
                
                # 게시물 링크 파라미터 찾기
                file_params = find_view_link_params(driver, post)
                
                if not file_params:
                    logger.warning(f"바로보기 링크 파라미터 추출 실패: {post['title']}")
                    continue
                
                # 파라미터 로깅
                param_log = {k: v for k, v in file_params.items() if k != 'post_info'}
                logger.info(f"Document extraction parameters: {json.dumps(param_log, default=str)}")
                
                # 통합 데이터 추출 (여러 방법 순차 시도)
                if 'atch_file_no' in file_params and 'file_ord' in file_params:
                    logger.info(f"문서 추출 시작 - atch_file_no: {file_params['atch_file_no']}, file_ord: {file_params['file_ord']}")
                    
                    # 데이터 추출 관리자를 사용하여 추출
                    sheets_data = extraction_manager.extract_data(file_params)
                    
                    if sheets_data and any(not df.empty for df in sheets_data.values()):
                        # 추출 데이터 정보 로깅
                        sheet_names = list(sheets_data.keys())
                        sheet_sizes = {name: sheets_data[name].shape for name in sheet_names}
                        logger.info(f"데이터 추출 성공: {len(sheet_names)}개 시트")
                        logger.info(f"시트 크기: {sheet_sizes}")
                        
                        # 업데이트 데이터 준비
                        update_data = {
                            'sheets': sheets_data,
                            'post_info': post
                        }
                        
                        if 'date' in file_params:
                            update_data['date'] = file_params['date']
                        
                        # Google Sheets 업데이트
                        # 스프레드시트 열기
                        spreadsheet = open_spreadsheet_with_retry(gs_client)
                        
                        if spreadsheet:
                            # 각 시트 업데이트
                            success_count = 0
                            
                            for sheet_name, df in sheets_data.items():
                                # 시트 이름에 Raw 접미사 추가
                                raw_sheet_name = f"{clean_sheet_name_for_gsheets(sheet_name)}_Raw"
                                
                                # 시트 업데이트
                                success = update_sheet(
                                    spreadsheet,
                                    raw_sheet_name,
                                    df,
                                    get_date_str(file_params),
                                    post,
                                    {'mode': 'replace'}
                                )
                                
                                if success:
                                    success_count += 1
                                    logger.info(f"시트 '{raw_sheet_name}' 업데이트 성공")
                                else:
                                    logger.warning(f"시트 '{raw_sheet_name}' 업데이트 실패")
                            
                            if success_count > 0:
                                logger.info(f"Google Sheets 업데이트 성공: {success_count}/{len(sheets_data)}개 시트")
                                data_updates.append(update_data)
                            else:
                                logger.warning(f"Google Sheets 업데이트 실패: {post['title']}")
                    else:
                        logger.warning(f"모든 방법으로 데이터 추출 실패: {post['title']}")
                        
                        # 플레이스홀더 데이터프레임 생성
                        placeholder_df = create_improved_placeholder_dataframe(post, file_params)
                        
                        if not placeholder_df.empty:
                            update_data = {
                                'dataframe': placeholder_df,
                                'post_info': post
                            }
                            
                            if 'date' in file_params:
                                update_data['date'] = file_params['date']
                            
                            # 스프레드시트 열기
                            spreadsheet = open_spreadsheet_with_retry(gs_client)
                            
                            if spreadsheet:
                                # 시트 이름 생성
                                report_type = determine_report_type(post['title'])
                                sheet_name = f"{clean_sheet_name_for_gsheets(report_type)}_Raw"
                                
                                # 시트 업데이트
                                success = update_sheet(
                                    spreadsheet,
                                    sheet_name,
                                    placeholder_df,
                                    get_date_str(file_params),
                                    post,
                                    {'mode': 'replace'}
                                )
                                
                                if success:
                                    logger.info(f"플레이스홀더 데이터로 업데이트 성공: {post['title']}")
                                    data_updates.append(update_data)
                elif 'content' in file_params or 'ajax_data' in file_params or 'download_url' in file_params:
                    # 그 외 추출 정보 처리 (내용, AJAX 데이터, 다운로드 URL)
                    logger.info(f"게시물 메타데이터로 처리 중: {post['title']}")
                    
                    # 플레이스홀더 데이터프레임 생성
                    placeholder_df = create_improved_placeholder_dataframe(post, file_params)
                    
                    if not placeholder_df.empty:
                        update_data = {
                            'dataframe': placeholder_df,
                            'post_info': post
                        }
                        
                        if 'date' in file_params:
                            update_data['date'] = file_params['date']
                        
                        # 스프레드시트 열기
                        spreadsheet = open_spreadsheet_with_retry(gs_client)
                        
                        if spreadsheet:
                            # 시트 이름 생성
                            report_type = determine_report_type(post['title'])
                            sheet_name = f"{clean_sheet_name_for_gsheets(report_type)}_Raw"
                            
                            # 시트 업데이트
                            success = update_sheet(
                                spreadsheet,
                                sheet_name,
                                placeholder_df,
                                get_date_str(file_params),
                                post,
                                {'mode': 'replace'}
                            )
                            
                            if success:
                                logger.info(f"메타데이터로 업데이트 성공: {post['title']}")
                                data_updates.append(update_data)
                
                # API 속도 제한 방지
                time.sleep(2)
                
            except Exception as e:
                logger.error(f"게시물 처리 중 오류: {str(e)}")
                
                # 오류 스크린샷 저장
                try:
                    error_screenshot = f"error_{int(time.time())}.png"
                    driver.save_screenshot(error_screenshot)
                    logger.info(f"오류 스크린샷 저장: {error_screenshot}")
                    
                    # 페이지 소스 저장
                    with open(f"error_source_{int(time.time())}.html", 'w', encoding='utf-8') as f:
                        f.write(driver.page_source)
                    
                    # 브라우저 컨텍스트 초기화 (쿠키 유지)
                    reset_browser_context(driver, delete_cookies=False)
                    
                    # 통계 페이지로 복귀
                    driver.get(CONFIG['stats_url'])
                    WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located((By.CLASS_NAME, "board_list"))
                    )
                    logger.info("통계 페이지로 복귀 성공")
                except Exception as recovery_err:
                    logger.error(f"오류 복구 실패: {str(recovery_err)}")
                    
                    try:
                        driver.quit()
                        driver = setup_driver()
                        driver.get(CONFIG['stats_url'])
                        logger.info("브라우저 완전 재설정 성공")
                    except Exception as reset_err:
                        logger.error(f"브라우저 재설정 실패: {str(reset_err)}")
        
        # 통합 시트 업데이트
        if CONFIG.get('update_consolidation', False) and data_updates:
            logger.info("통합 시트 업데이트 시작...")
            try:
                consolidated_updates = update_consolidated_sheets(gs_client, data_updates)
                if consolidated_updates:
                    logger.info(f"통합 시트 업데이트 성공: {consolidated_updates}개 시트")
                else:
                    logger.warning("통합 시트 업데이트 실패")
            except Exception as consol_err:
                logger.error(f"통합 시트 업데이트 중 오류: {str(consol_err)}")
    
    return data_updates

#--- 통합 유틸리티 함수 (중복 제거 및 개선) ---#

def get_date_str(file_params):
    """
    파일 파라미터에서 날짜 문자열 추출
    
    Args:
        file_params: 파일 파라미터
    
    Returns:
        str: 날짜 문자열 (YYYY년 MM월 형식)
    """
    if 'date' in file_params:
        year = file_params['date']['year']
        month = file_params['date']['month']
        return f"{year}년 {month}월"
    
    # post_info에서 날짜 추출 시도
    if 'post_info' in file_params:
        post_info = file_params['post_info']
        title = post_info.get('title', '')
        date_match = re.search(r'\((\d{4})년\s*(\d{1,2})월말\s*기준\)', title)
        if date_match:
            year = date_match.group(1)
            month = date_match.group(2)
            return f"{year}년 {month}월"
    
    # 현재 날짜 사용
    now = datetime.now()
    return f"{now.year}년 {now.month}월"

def determine_report_type(title):
    """
    게시물 제목에서 보고서 유형 결정
    
    Args:
        title (str): 게시물 제목
    
    Returns:
        str: 보고서 유형
    """
    report_types = {
        "유선통신서비스 가입 현황": ["유선통신서비스", "유선통신 서비스", "초고속인터넷", "전화", "인터넷전화"],
        "무선통신서비스 가입 현황": ["무선통신서비스", "무선통신 서비스", "이동전화", "LTE", "5G"],
        "무선데이터 트래픽 통계": ["무선데이터", "트래픽", "데이터 통계"],
        "이동전화 및 시내전화 번호이동 현황": ["번호이동", "이동전화 및 시내전화"],
        "특수부가통신사업자현황": ["특수부가", "부가통신", "사업자현황"]
    }
    
    combined_type = "유·무선통신서비스 가입 현황 및 무선데이터 트래픽 통계"
    if "유·무선" in title or ("유선" in title and "무선" in title and "트래픽" in title):
        return combined_type
    
    for report_type, keywords in report_types.items():
        if any(keyword in title for keyword in keywords):
            return report_type
    
    return "기타 통신 통계"

def clean_sheet_name_for_gsheets(name):
    """
    Google Sheets 시트 이름 규칙에 맞게 정리
    
    Args:
        name (str): 원본 시트 이름
    
    Returns:
        str: 정리된 시트 이름
    """
    # 최대 길이 제한 (100자)
    if len(name) > 95:  # '_Raw' 접미사 고려하여 95자로 제한
        name = name[:95]
    
    # 허용되지 않는 문자 제거 또는 대체
    invalid_chars = ['/', '\\', '?', '*', '[', ']', ':']
    for char in invalid_chars:
        name = name.replace(char, '_')
    
    # 시트 이름 앞뒤 공백 제거
    name = name.strip()
    
    # 빈 문자열인 경우 기본값 사용
    if not name:
        name = "Sheet"
    
    return name

def create_improved_placeholder_dataframe(post, file_params):
    """
    개선된 플레이스홀더 데이터프레임 생성
    
    Args:
        post (dict): 게시물 정보
        file_params (dict): 파일 파라미터
    
    Returns:
        DataFrame: 플레이스홀더 데이터프레임
    """
    # 기본 메타데이터 열
    metadata = {
        '기준일자': get_date_str(file_params),
        '제목': post.get('title', ''),
        '게시일': post.get('date', ''),
        '담당부서': post.get('department', ''),
        '원문링크': post.get('url', '')
    }
    
    # 파일 파라미터 정보 추가
    if 'atch_file_no' in file_params:
        metadata['첨부파일번호'] = file_params['atch_file_no']
    
    if 'file_ord' in file_params:
        metadata['파일순서'] = file_params['file_ord']
    
    # 날짜 정보 추가
    if 'date' in file_params:
        date_info = file_params['date']
        if 'year' in date_info and 'month' in date_info:
            metadata['보고서연도'] = date_info['year']
            metadata['보고서월'] = date_info['month']
    
    # 데이터프레임 생성
    df = pd.DataFrame([metadata])
    
    # 타임스탬프 추가
    df['Last Updated'] = datetime.now().strftime('%Y-%m-%d %H:%M')
    
    return df

def open_spreadsheet_with_retry(gs_client, max_retries=3, retry_delay=2):
    """
    재시도 로직이 포함된 스프레드시트 열기 함수
    
    Args:
        gs_client: gspread 클라이언트
        max_retries (int): 최대 재시도 횟수
        retry_delay (int): 재시도 간 대기 시간(초)
    
    Returns:
        Spreadsheet: 열린 스프레드시트 객체 또는 None
    """
    for attempt in range(max_retries):
        try:
            # 스프레드시트 ID로 열기
            if CONFIG['spreadsheet_id']:
                spreadsheet = gs_client.open_by_key(CONFIG['spreadsheet_id'])
                logger.info(f"스프레드시트 열기 성공: {spreadsheet.title}")
                return spreadsheet
            
            # 이름으로 열기 (ID가 없는 경우)
            elif CONFIG['spreadsheet_name']:
                spreadsheet = gs_client.open(CONFIG['spreadsheet_name'])
                logger.info(f"스프레드시트 열기 성공: {spreadsheet.title}")
                return spreadsheet
            
            # ID와 이름 모두 없는 경우
            else:
                logger.error("스프레드시트 ID 또는 이름이 설정되지 않음")
                return None
                
        except gspread.exceptions.APIError as e:
            if "RESOURCE_EXHAUSTED" in str(e) or "RATE_LIMIT_EXCEEDED" in str(e):
                wait_time = retry_delay * (2 ** attempt)
                logger.warning(f"API 속도 제한 - {wait_time}초 대기 중... (재시도 {attempt+1}/{max_retries})")
                time.sleep(wait_time)
            else:
                logger.error(f"스프레드시트 열기 오류: {str(e)}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                else:
                    return None
        except Exception as e:
            logger.error(f"스프레드시트 열기 중 예상치 못한 오류: {str(e)}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
            else:
                return None
    
    return None

def update_sheet(spreadsheet, sheet_name, df, date_str, post_info, options=None):
    """
    Google Sheet 업데이트 통합 함수
    
    Args:
        spreadsheet: gspread 스프레드시트 객체
        sheet_name (str): 시트 이름
        df (DataFrame): 업데이트할 데이터프레임
        date_str (str): 날짜 문자열
        post_info (dict): 게시물 정보
        options (dict): 추가 옵션
    
    Returns:
        bool: 성공 여부
    """
    if options is None:
        options = {}
    
    mode = options.get('mode', 'replace')  # 'replace', 'append', 'update'
    add_metadata = options.get('add_metadata', True)
    
    try:
        # 데이터프레임이 비어있는지 확인
        if df.empty:
            logger.warning(f"빈 데이터프레임으로 '{sheet_name}' 업데이트 불가")
            return False
        
        # 시트 찾기 또는 생성
        try:
            worksheet = spreadsheet.worksheet(sheet_name)
            logger.info(f"기존 시트 '{sheet_name}' 발견")
        except gspread.exceptions.WorksheetNotFound:
            worksheet = spreadsheet.add_worksheet(title=sheet_name, rows=1000, cols=50)
            logger.info(f"새 시트 '{sheet_name}' 생성")
        
        # 메타데이터 추가
        if add_metadata:
            title_row = {'A1': '기준일자', 'B1': date_str}
            title_row_2 = {'A2': '제목', 'B2': post_info.get('title', '')}
            title_row_3 = {'A3': '게시일', 'B3': post_info.get('date', '')}
            title_row_4 = {'A4': '담당부서', 'B4': post_info.get('department', '')}
            
            # 업데이트 시간 추가
            last_updated_row = {'A22': 'Last Updated', 'B22': datetime.now().strftime('%Y-%m-%d %H:%M')}
            
            # 메타데이터 업데이트
            worksheet.update(title_row)
            worksheet.update(title_row_2)
            worksheet.update(title_row_3)
            worksheet.update(title_row_4)
            worksheet.update(last_updated_row)
        
        # 데이터프레임 처리 및 업데이트
        if mode == 'replace':
            # 모든 데이터 지우기 (메타데이터 보존)
            all_values = worksheet.get_all_values()
            if len(all_values) > 25:  # 메타데이터 영역 아래 데이터만 지우기
                clear_range = f'A26:Z{len(all_values)}'
                worksheet.batch_clear([clear_range])
            
            # 데이터프레임을 값 목록으로 변환
            values = [df.columns.tolist()] + df.values.tolist()
            
            # 데이터 시작 위치
            start_row = 26
            
            # 데이터 업데이트 (기존 데이터 유지)
            if values:
                cell_range = f'A{start_row}:{chr(65 + len(values[0]) - 1)}{start_row + len(values) - 1}'
                worksheet.update(cell_range, values)
        
        elif mode == 'append':
            # 기존 데이터 가져오기
            existing_data = worksheet.get_all_values()
            
            # 시작 행 계산 (기존 데이터 이후)
            start_row = max(26, len(existing_data) + 1)
            
            # 데이터프레임을 값 목록으로 변환
            values = [df.columns.tolist()] + df.values.tolist()
            
            # 데이터 추가
            if values:
                cell_range = f'A{start_row}:{chr(65 + len(values[0]) - 1)}{start_row + len(values) - 1}'
                worksheet.update(cell_range, values)
        
        else:  # mode == 'update'
            # 기존 데이터 가져오기
            existing_data = worksheet.get_all_values()
            
            # 헤더를 기준으로 열 인덱스 매핑
            if len(existing_data) >= 26:
                headers = existing_data[25]  # 26번째 행(인덱스 25)이 헤더라고 가정
                
                # 매핑 테이블 생성
                header_map = {header: idx for idx, header in enumerate(headers) if header}
                
                # 업데이트할 값 준비
                update_cells = []
                
                # 데이터프레임의 각 행과 열에 대해
                for row_idx, row in df.iterrows():
                    for col_name, value in row.items():
                        if col_name in header_map:
                            # 기존 헤더에 있는 열만 업데이트
                            col_idx = header_map[col_name]
                            cell = gspread.Cell(row_idx + 27, col_idx + 1, value)  # 27은 헤더 다음 행 (26) + 1
                            update_cells.append(cell)
                
                # 일괄 업데이트
                if update_cells:
                    worksheet.update_cells(update_cells)
        
        logger.info(f"시트 '{sheet_name}' 업데이트 성공 (모드: {mode})")
        return True
    
    except Exception as e:
        logger.error(f"시트 '{sheet_name}' 업데이트 실패: {str(e)}")
        return False

def update_consolidated_sheets(gs_client, data_updates):
    """
    개선된 통합 시트 업데이트 함수
    
    Args:
        gs_client: gspread 클라이언트
        data_updates (list): 데이터 업데이트 목록
    
    Returns:
        int: 업데이트된 시트 수 또는 0(실패)
    """
    if not data_updates:
        logger.warning("업데이트할 데이터가 없음")
        return 0
    
    try:
        # 스프레드시트 열기
        spreadsheet = open_spreadsheet_with_retry(gs_client)
        if not spreadsheet:
            return 0
        
        # 통합 시트 이름 매핑
        consolidated_sheet_mapping = {
            "유선통신서비스_Raw": "유선통신서비스_통합",
            "무선통신서비스_Raw": "무선통신서비스_통합",
            "무선데이터트래픽_Raw": "무선데이터트래픽_통합",
            "이동전화및시내전화번호이동_Raw": "번호이동현황_통합",
            "특수부가통신사업자현황_Raw": "특수부가통신사업자_통합"
        }
        
        # 통합 시트별로 업데이트할 데이터 매핑
        consolidated_data = {}
        
        # 데이터 업데이트 목록 처리
        for update in data_updates:
            if 'sheets' in update:
                # 여러 시트 데이터가 있는 경우
                for sheet_name, df in update['sheets'].items():
                    raw_sheet_name = f"{clean_sheet_name_for_gsheets(sheet_name)}_Raw"
                    
                    # 해당 Raw 시트에 대응하는 통합 시트가 있는지 확인
                    for raw_pattern, consolidated_name in consolidated_sheet_mapping.items():
                        if raw_pattern in raw_sheet_name:
                            if consolidated_name not in consolidated_data:
                                consolidated_data[consolidated_name] = []
                            
                            # 데이터 임시 저장
                            date_info = {
                                'date_str': get_date_str(update),
                                'raw_sheet': raw_sheet_name,
                                'dataframe': df,
                                'post_info': update.get('post_info', {})
                            }
                            consolidated_data[consolidated_name].append(date_info)
            
            elif 'dataframe' in update:
                # 단일 데이터프레임만 있는 경우
                df = update['dataframe']
                post_info = update.get('post_info', {})
                
                # 보고서 유형 확인
                report_type = determine_report_type(post_info.get('title', ''))
                raw_sheet_name = f"{clean_sheet_name_for_gsheets(report_type)}_Raw"
                
                # 해당 Raw 시트에 대응하는 통합 시트가 있는지 확인
                for raw_pattern, consolidated_name in consolidated_sheet_mapping.items():
                    if raw_pattern in raw_sheet_name:
                        if consolidated_name not in consolidated_data:
                            consolidated_data[consolidated_name] = []
                        
                        # 데이터 임시 저장
                        date_info = {
                            'date_str': get_date_str(update),
                            'raw_sheet': raw_sheet_name,
                            'dataframe': df,
                            'post_info': post_info
                        }
                        consolidated_data[consolidated_name].append(date_info)
        
        # 업데이트된 통합 시트 수
        updated_count = 0
        
        # 각 통합 시트 업데이트
        for consolidated_name, data_list in consolidated_data.items():
            try:
                logger.info(f"통합 시트 '{consolidated_name}' 업데이트 시작")
                
                # 통합 시트 찾기 또는 생성
                try:
                    consolidated_sheet = spreadsheet.worksheet(consolidated_name)
                    logger.info(f"기존 통합 시트 '{consolidated_name}' 발견")
                except gspread.exceptions.WorksheetNotFound:
                    consolidated_sheet = spreadsheet.add_worksheet(title=consolidated_name, rows=1000, cols=100)
                    logger.info(f"새 통합 시트 '{consolidated_name}' 생성")
                
                # 각 데이터 처리
                for idx, data_item in enumerate(data_list):
                    try:
                        # Raw 시트에서 데이터 가져오기
                        raw_sheet_name = data_item['raw_sheet']
                        date_str = data_item['date_str']
                        df = data_item['dataframe']
                        post_info = data_item['post_info']
                        
                        logger.info(f"Raw 시트 '{raw_sheet_name}'에서 데이터 처리 중")
                        
                        # 통합 시트 기존 데이터 가져오기
                        consolidated_values = consolidated_sheet.get_all_values()
                        
                        # 통합 시트가 비어있는 경우 헤더 설정
                        if not consolidated_values or len(consolidated_values) < 1:
                            # 기본 헤더 설정
                            headers = ["기준일자", "항목", "값", "비고"]
                            cell_range = f'A1:D1'
                            consolidated_sheet.update(cell_range, [headers])
                            
                            # 타임스탬프 추가
                            consolidated_sheet.update('A2', [["Last Updated", datetime.now().strftime('%Y-%m-%d %H:%M')]])
                            
                            logger.info(f"통합 시트 '{consolidated_name}' 헤더 초기화 완료")
                            consolidated_values = consolidated_sheet.get_all_values()
                        
                        # 데이터프레임에서 유의미한 데이터 추출
                        if not df.empty:
                            # 헤더와 데이터 확인
                            if len(df.columns) > 0:
                                # 마지막 행이 '계' 또는 '합계'인지 확인
                                last_row_data = None
                                
                                # 첫 번째 열이 '기준일자'인 경우 메타데이터로 간주하고 건너뛰기
                                if "기준일자" in df.columns[0]:
                                    logger.info(f"메타데이터 행 건너뛰기: {df.columns[0]}")
                                else:
                                    if df.shape[0] > 0:
                                        # 데이터프레임을 값 배열로 변환 (특히 숫자 데이터)
                                        df_values = df.values.tolist()
                                        
                                        # 데이터 행 수 로깅
                                        logger.info(f"데이터프레임 행 수: {len(df_values)}")
                                        
                                        # 행이 존재하는 경우만 처리
                                        if df_values:
                                            # 마지막 행 또는 '계' 행 검색
                                            for row_idx, row in enumerate(df_values):
                                                # 빈 행이 아닌지 확인
                                                if row and any(str(cell).strip() for cell in row):
                                                    first_cell = str(row[0]).strip() if row[0] is not None else ""
                                                    if first_cell in ["계", "합계", "소계", "합", "총계"]:
                                                        last_row_data = row
                                                        logger.info(f"'계' 행 발견 (인덱스: {row_idx}): {last_row_data}")
                                                        break
                                            
                                            # '계' 행을 찾지 못한 경우 마지막 행 사용
                                            if not last_row_data and df_values:
                                                last_row_data = df_values[-1]
                                                logger.info(f"마지막 행 사용: {last_row_data}")
                                
                                # 기존 통합 시트 데이터 길이 확인
                                last_row_idx = len(consolidated_values)
                                for i in range(len(consolidated_values) - 1, 0, -1):
                                    if any(consolidated_values[i]):  # 빈 행이 아닌 경우
                                        last_row_idx = i + 1
                                        break
                                
                                # 추가할 데이터 준비
                                new_rows = []
                                
                                # 컬럼과 값 쌍 생성
                                if last_row_data:
                                    columns = df.columns.tolist()
                                    
                                    # 각 컬럼-값 쌍을 통합 시트에 추가
                                    for col_idx, col_name in enumerate(columns):
                                        if col_idx < len(last_row_data):
                                            value = last_row_data[col_idx]
                                            new_rows.append([date_str, col_name, value, f"{post_info.get('title', '')} 자료"])
                                
                                # 데이터 추가 (행이 있는 경우에만)
                                if new_rows:
                                    # 시작 행 계산 (마지막 행 다음)
                                    start_row = last_row_idx + 1
                                    
                                    # 데이터 추가
                                    range_end = start_row + len(new_rows) - 1
                                    cell_range = f'A{start_row}:D{range_end}'
                                    
                                    logger.info(f"통합 시트 '{consolidated_name}'에 {len(new_rows)}개 행 추가 (범위: {cell_range})")
                                    consolidated_sheet.update(cell_range, new_rows)
                                    
                                    # 타임스탬프 업데이트
                                    consolidated_sheet.update('B2', datetime.now().strftime('%Y-%m-%d %H:%M'))
                                    logger.info(f"{consolidated_name}에 타임스탬프 추가")
                        
                    except Exception as item_err:
                        logger.error(f"통합 시트 '{consolidated_name}' 항목 처리 중 오류: {str(item_err)}")
                
                updated_count += 1
                
            except Exception as sheet_err:
                logger.error(f"통합 시트 '{consolidated_name}' 업데이트 중 오류: {str(sheet_err)}")
        
        logger.info(f"통합 시트 업데이트 완료: {updated_count}개 시트 업데이트됨")
        return updated_count
    
    except Exception as e:
        logger.error(f"통합 시트 업데이트 중 오류: {str(e)}")
        return 0

def cleanup_date_specific_sheets(spreadsheet, max_age_days=90):
    """
    오래된 날짜별 시트 정리 함수
    
    Args:
        spreadsheet: gspread 스프레드시트 객체
        max_age_days (int): 보존할 최대 날짜 (일)
    
    Returns:
        int: 제거된 시트 수
    """
    removed_count = 0
    
    try:
        # 모든 워크시트 가져오기
        all_worksheets = spreadsheet.worksheets()
        
        # 오늘 날짜
        today = datetime.now()
        cutoff_date = today - timedelta(days=max_age_days)
        
        # 날짜 패턴 (YYYY년MM월DD일)
        date_pattern = re.compile(r'(\d{4})년(\d{1,2})월(\d{1,2})일')
        
        # 각 시트 확인
        for worksheet in all_worksheets:
            sheet_title = worksheet.title
            
            # 날짜 포맷 시트만 확인
            date_match = date_pattern.search(sheet_title)
            if date_match:
                try:
                    year = int(date_match.group(1))
                    month = int(date_match.group(2))
                    day = int(date_match.group(3))
                    
                    # 시트 날짜
                    sheet_date = datetime(year, month, day)
                    
                    # 오래된 시트인지 확인
                    if sheet_date < cutoff_date:
                        # 시트 제거
                        spreadsheet.del_worksheet(worksheet)
                        removed_count += 1
                        logger.info(f"오래된 시트 제거: {sheet_title}")
                        
                        # API 속도 제한 방지
                        time.sleep(1)
                except ValueError:
                    logger.warning(f"날짜 형식 파싱 오류: {sheet_title}")
                except Exception as e:
                    logger.error(f"시트 제거 중 오류 ({sheet_title}): {str(e)}")
        
        return removed_count
    
    except Exception as e:
        logger.error(f"날짜별 시트 정리 중 오류: {str(e)}")
        return removed_count

def setup_driver():
    """
    Selenium WebDriver 설정 (향상된 봇 탐지 회피)
    
    Returns:
        WebDriver 인스턴스
    """
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
    options.add_argument('--disable-features=WebglDraftExtensions,WebglDecoderExtensions')
    
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
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:96.0) Gecko/20100101 Firefox/96.0"
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
    
    # 페이지 로드 타임아웃 설정
    driver.set_page_load_timeout(CONFIG.get('page_load_timeout', 30))
    
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
        logger.warning("selenium-stealth 라이브러리를 찾을 수 없습니다.")
    
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
    """
    Google Sheets 클라이언트 초기화
    
    Returns:
        gspread 클라이언트 인스턴스 또는 None
    """
    if not CONFIG['gspread_creds']:
        return None
    
    try:
        # 환경 변수에서 자격 증명 파싱
        creds_dict = json.loads(CONFIG['gspread_creds'])
        
        # 임시 파일에 자격 증명 저장
        temp_creds_path = Path("./downloads/temp_creds.json")
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
        
        return client
    except json.JSONDecodeError:
        logger.error("Google Sheets 자격 증명 JSON 파싱 오류")
        return None
    except Exception as e:
        logger.error(f"Google Sheets 클라이언트 초기화 중 오류: {str(e)}")
        return None

def reset_browser_context(driver, delete_cookies=True, navigate_to_blank=True):
    """
    브라우저 컨텍스트 초기화
    
    Args:
        driver: WebDriver 인스턴스
        delete_cookies: 쿠키 삭제 여부
        navigate_to_blank: 빈 페이지로 이동 여부
        
    Returns:
        bool: 성공 여부
    """
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

def find_view_link_params(driver, post):
    """
    게시물에서 바로보기 링크 파라미터 찾기
    
    Args:
        driver: WebDriver 인스턴스
        post: 게시물 정보 딕셔너리
        
    Returns:
        dict: 파일 파라미터 정보 또는 None
    """
    try:
        # 게시물 URL 열기 (이미 열려있지 않은 경우)
        current_url = driver.current_url
        target_url = post.get('url')
        
        if target_url and current_url != target_url:
            logger.info(f"게시물 열기: {post['title']}")
            
            try:
                # 게시물 URL로 직접 이동
                driver.get(target_url)
                WebDriverWait(driver, 15).until(
                    EC.presence_of_element_located((By.CLASS_NAME, "view_head"))
                )
            except Exception as url_err:
                logger.warning(f"직접 URL 접근 실패, 제목으로 검색 시도: {str(url_err)}")
                
                # 게시물 목록으로 이동
                driver.get(CONFIG['stats_url'])
                WebDriverWait(driver, 15).until(
                    EC.presence_of_element_located((By.CLASS_NAME, "board_list"))
                )
                
                # XPath를 사용하여 제목으로 게시물 찾기
                title_snippet = post['title'][:20]  # 제목의 첫 20자
                xpath_selector = f"//p[contains(@class, 'title') and contains(text(), '{title_snippet}')]"
                
                logger.info(f"게시물 링크 발견 (선택자: {xpath_selector})")
                
                # 스크린샷 저장 (디버깅용)
                screenshot_path = f"screenshots/before_click_{post.get('id', random.randint(1000, 9999))}_{int(time.time())}.png"
                driver.save_screenshot(screenshot_path)
                logger.info(f"스크린샷 저장: {screenshot_path}")
                
                # 게시물 링크 클릭
                logger.info(f"게시물 링크 클릭 시도: {post['title']}")
                try:
                    # 요소 찾기
                    post_link = WebDriverWait(driver, 10).until(
                        EC.element_to_be_clickable((By.XPATH, xpath_selector))
                    )
                    
                    # JavaScript로 클릭 (더 안정적)
                    logger.info("JavaScript를 통한 클릭 실행")
                    driver.execute_script("arguments[0].click();", post_link)
                    
                    # 페이지 변경 확인
                    WebDriverWait(driver, 10).until(
                        lambda d: d.current_url != CONFIG['stats_url']
                    )
                    logger.info(f"페이지 URL 변경 감지됨: {driver.current_url}")
                    
                    # 게시물 상세 페이지로 이동 확인
                    WebDriverWait(driver, 15).until(
                        EC.presence_of_element_located((By.CLASS_NAME, "view_head"))
                    )
                    logger.info("상세 페이지 로드 완료: view_head 요소 발견")
                    
                except Exception as click_err:
                    logger.error(f"게시물 링크 클릭 실패: {str(click_err)}")
                    return None
        
        # 클릭 후 스크린샷 저장 (디버깅용)
        screenshot_path = f"screenshots/post_view_clicked_{post.get('id', random.randint(1000, 9999))}_{int(time.time())}.png"
        driver.save_screenshot(screenshot_path)
        logger.info(f"스크린샷 저장: {screenshot_path}")
        
        # 바로보기 링크 찾기
        view_links = driver.find_elements(By.CSS_SELECTOR, "a.fileView")
        
        if view_links:
            for link in view_links:
                onclick = link.get_attribute("onclick")
                href = link.get_attribute("href")
                
                logger.info(f"바로보기 링크 발견, onclick: {onclick}, href: {href}")
                
                # getExtension_path 함수 파라미터 추출
                if onclick and "getExtension_path" in onclick:
                    match = re.search(r"getExtension_path\('([^']+)',\s*'([^']+)'", onclick)
                    
                    if match:
                        atch_file_no = match.group(1)
                        file_ord = match.group(2)
                        
                        # 날짜 정보 추출 (제목에서)
                        title = post.get('title', '')
                        date_match = re.search(r'\((\d{4})년\s*(\d{1,2})월말\s*기준\)', title)
                        
                        date_info = {}
                        if date_match:
                            date_info = {
                                'year': int(date_match.group(1)),
                                'month': int(date_match.group(2))
                            }
                        
                        # 파라미터 반환
                        return {
                            'atch_file_no': atch_file_no,
                            'file_ord': file_ord,
                            'date': date_info,
                            'post_info': post
                        }
        
        # 첨부파일 링크 찾기
        file_links = driver.find_elements(By.CSS_SELECTOR, ".file_link a")
        
        if file_links:
            for link in file_links:
                onclick = link.get_attribute("onclick")
                href = link.get_attribute("href")
                
                # fnDownload 함수 파라미터 추출
                if onclick and "fnDownload" in onclick:
                    match = re.search(r"fnDownload\('([^']+)',\s*'([^']+)'", onclick)
                    
                    if match:
                        atch_file_no = match.group(1)
                        file_ord = match.group(2)
                        
                        # 날짜 정보 추출 (제목에서)
                        title = post.get('title', '')
                        date_match = re.search(r'\((\d{4})년\s*(\d{1,2})월말\s*기준\)', title)
                        
                        date_info = {}
                        if date_match:
                            date_info = {
                                'year': int(date_match.group(1)),
                                'month': int(date_match.group(2))
                            }
                        
                        # 파라미터 반환
                        return {
                            'atch_file_no': atch_file_no,
                            'file_ord': file_ord,
                            'date': date_info,
                            'post_info': post
                        }
                
                # 다운로드 URL 추출
                elif href and ("/cmm/fms/FileDown.do" in href or "/cmm/fms/FileView.do" in href):
                    # 날짜 정보 추출 (제목에서)
                    title = post.get('title', '')
                    date_match = re.search(r'\((\d{4})년\s*(\d{1,2})월말\s*기준\)', title)
                    
                    date_info = {}
                    if date_match:
                        date_info = {
                            'year': int(date_match.group(1)),
                            'month': int(date_match.group(2))
                        }
                    
                    # 파라미터 반환
                    return {
                        'download_url': href,
                        'date': date_info,
                        'post_info': post
                    }
        
        # 본문 내용 추출 (파일이 없는 경우)
        content_element = driver.find_element(By.CSS_SELECTOR, ".view_cont")
        if content_element:
            # 날짜 정보 추출 (제목에서)
            title = post.get('title', '')
            date_match = re.search(r'\((\d{4})년\s*(\d{1,2})월말\s*기준\)', title)
            
            date_info = {}
            if date_match:
                date_info = {
                    'year': int(date_match.group(1)),
                    'month': int(date_match.group(2))
                }
            
            # 파라미터 반환
            return {
                'content': content_element.text,
                'date': date_info,
                'post_info': post
            }
        
        # 어떤 정보도 찾지 못한 경우
        logger.warning(f"게시물에서 파일 파라미터를 찾을 수 없음: {post['title']}")
        return None
    
    except Exception as e:
        logger.error(f"바로보기 링크 파라미터 찾기 중 오류: {str(e)}")
        return None

async def send_telegram_message(posts, data_updates=None):
    """
    텔레그램으로 알림 메시지 전송
    
    Args:
        posts (list): 게시물 목록
        data_updates (list): 데이터 업데이트 목록
    
    Returns:
        bool: 성공 여부
    """
    if not CONFIG['telegram_token'] or not CONFIG['chat_id']:
        logger.warning("텔레그램 토큰 또는 채팅 ID가 설정되지 않음")
        return False
    
    try:
        bot = telegram.Bot(token=CONFIG['telegram_token'])
        
        # 기본 메시지 생성
        message = "📊 MSIT 통신 통계 모니터링 결과 📊\n\n"
        
        # 게시물 정보 추가
        if posts:
            message += f"📋 {len(posts)}개의 새 게시물 발견\n\n"
            
            for post in posts:
                post_date = post.get('date', '날짜 없음')
                title = post.get('title', '제목 없음')
                department = post.get('department', '부서 없음')
                url = post.get('url', '')
                
                message += f"📝 {title}\n"
                message += f"📅 {post_date}\n"
                message += f"🏢 {department}\n"
                
                if url:
                    message += f"🔗 {url}\n"
                
                message += "\n"
        
        # 데이터 업데이트 정보 추가
        if data_updates:
            message += f"🔄 {len(data_updates)}개의 데이터셋 업데이트 완료\n\n"
            
            for update in data_updates:
                if 'post_info' in update:
                    post_info = update['post_info']
                    title = post_info.get('title', '제목 없음')
                    message += f"✅ {title} 업데이트 완료\n"
        
        # 메시지 전송
        await bot.send_message(chat_id=CONFIG['chat_id'], text=message, parse_mode=telegram.constants.ParseMode.HTML)
        logger.info("텔레그램 메시지 전송 성공")
        return True
    
    except Exception as e:
        logger.error(f"텔레그램 메시지 전송 실패: {str(e)}")
        return False

async def main():
    """메인 실행 함수"""
    # 설정 및 디렉토리 준비
    global CONFIG
    CONFIG = load_config()
    setup_directories()
    
    # 명령행 인자 처리
    import argparse
    parser = argparse.ArgumentParser(description='MSIT 통신 통계 모니터링')
    parser.add_argument('--days', type=int, default=4, help='검색 기간 (일)')
    parser.add_argument('--start-page', type=int, default=1, help='시작 페이지')
    parser.add_argument('--end-page', type=int, default=5, help='종료 페이지')
    parser.add_argument('--no-sheets', action='store_true', help='Google Sheets 업데이트 비활성화')
    parser.add_argument('--reverse', action='store_true', help='역순 페이지 탐색 (기본 활성화)')
    args = parser.parse_args()
    
    days_range = args.days
    start_page = args.start_page
    end_page = args.end_page
    check_sheets = not args.no_sheets
    reverse_order = args.reverse
    
    # WebDriver 및 클라이언트 초기화
    driver = None
    gs_client = None
    all_posts = []
    telecom_stats_posts = []
    
    try:
        # 시작 시간 기록
        start_time = time.time()
        
        # 로깅
        if reverse_order:
            logger.info(f"=== MSIT 통신 통계 모니터링 시작 (days_range={days_range}, 페이지={end_page}~{start_page} 역순, check_sheets={check_sheets}) ===")
        else:
            logger.info(f"=== MSIT 통신 통계 모니터링 시작 (days_range={days_range}, 페이지={start_page}~{end_page}, check_sheets={check_sheets}) ===")
        
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
        
        # 웹사이트 접근 및 통신 통계 게시물 스크래핑
        # 이 부분은 실제 구현 시 원본 코드의 스크래핑 함수를 호출합니다
        # 예시 목적으로 여기서는 더미 데이터를 사용합니다
        # 실제 구현 시 아래 코드를 스크래핑 함수로 대체해주세요
        
        # 테스트용 더미 데이터
        telecom_stats_posts = [
            {
                'id': '3173675',
                'title': '(2025년 2월말 기준) 유·무선통신서비스 가입 현황 및 무선데이터 트래픽 통계',
                'date': '2025-03-29',
                'department': '네트워크정책과',
                'url': 'https://www.msit.go.kr/bbs/view.do?sCode=user&mId=99&mPid=74&pageIndex=&bbsSeqNo=79&nttSeqNo=3173675&searchOpt=ALL&searchTxt='
            }
        ]
        all_posts = telecom_stats_posts.copy()
        
        # 통신 통계 게시물 처리
        data_updates = await process_telecom_stats_posts(driver, gs_client, telecom_stats_posts, check_sheets)
        
        # 처리 완료 후 정리
        if gs_client and data_updates and CONFIG.get('cleanup_old_sheets', False):
            logger.info("날짜별 시트 정리 시작...")
            try:
                # 스프레드시트 열기
                spreadsheet = open_spreadsheet_with_retry(gs_client)
                
                if spreadsheet:
                    removed_count = cleanup_date_specific_sheets(spreadsheet)
                    logger.info(f"{removed_count}개 날짜별 시트 제거 완료")
            except Exception as cleanup_err:
                logger.error(f"시트 정리 중 오류: {str(cleanup_err)}")
        
        # 텔레그램 알림 전송
        if all_posts or data_updates:
            await send_telegram_message(all_posts, data_updates)
            logger.info(f"알림 전송 완료: {len(all_posts)}개 게시물, {len(data_updates)}개 업데이트")
        else:
            logger.info(f"최근 {days_range}일 내 새 게시물이 없습니다")
        
        # 실행 시간 계산
        end_time = time.time()
        execution_time = end_time - start_time
        logger.info(f"실행 시간: {execution_time:.2f}초")
        
    except Exception as e:
        logger.error(f"모니터링 중 오류 발생: {str(e)}")
        
        # 오류 처리
        try:
            # 오류 스크린샷 저장
            if driver:
                driver.save_screenshot("error_screenshot.png")
                logger.info("오류 발생 시점 스크린샷 저장 완료")
            
            # 스택 트레이스 저장
            import traceback
            error_trace = traceback.format_exc()
            logger.error(f"상세 오류 정보: {error_trace}")
            
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
        # 자원 정리
        if driver:
            driver.quit()
            logger.info("WebDriver 종료")
        
        logger.info("=== MSIT 통신 통계 모니터링 종료 ===")

if __name__ == "__main__":
    asyncio.run(main())
