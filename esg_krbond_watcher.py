import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime, timedelta
import gspread
from google.oauth2.service_account import Credentials
import json
import os
import sys
import time
import re

def scrape_krx_esg_trading_for_date(target_date):
    """특정 날짜의 KRX ESG 채권 거래 현황 데이터를 스크래핑합니다."""
    
    # API URL
    url = "https://esgbond.krx.co.kr/contents/99/SRI99000001.jspx"
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'Origin': 'https://esgbond.krx.co.kr',
        'Referer': 'https://esgbond.krx.co.kr/contents/04/04020000/SRI04020000.jsp',
        'X-Requested-With': 'XMLHttpRequest'
    }
    
    # 날짜를 YYYYMMDD 형식으로 변환
    if isinstance(target_date, str):
        date_str = target_date.replace('-', '')
    else:
        date_str = target_date.strftime('%Y%m%d')
    
    # POST 데이터 설정
    data = {
        'fr_work_dt': date_str,
        'to_work_dt': date_str,
        'pagePath': '/contents/04/04020000/SRI04020000.jsp',
        'code': '04/04020000/sri04020000',
        'pageFirstCall': 'Y'
    }
    
    try:
        # POST 요청
        response = requests.post(url, headers=headers, data=data)
        response.raise_for_status()
        
        # JSON 응답 파싱
        json_data = response.json()
        
        # 거래 현황 데이터 추출
        trading_data = []
        
        # 가능한 키들 확인
        raw_data = None
        for key in json_data.keys():
            if isinstance(json_data[key], list) and len(json_data[key]) > 0:
                raw_data = json_data[key]
                break
        
        if not raw_data:
            print(f"{date_str} - API 응답에서 데이터를 찾을 수 없습니다.")
            return pd.DataFrame()
        
        # 날짜를 YYYY-MM-DD 형식으로 변환
        formatted_date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
        
        # 데이터 파싱
        for item in raw_data:
            # 채권 종류별 데이터
            bond_type = item.get('bnd_clss_nm', '')
            
            if bond_type:  # 채권종류가 있는 경우만 처리
                record = {
                    '거래일자': formatted_date,
                    '채권종류': bond_type,
                    '거래량': float(str(item.get('acc_trdvol', 0)).replace(',', '')),
                    '거래대금': float(str(item.get('acc_trdval', 0)).replace(',', '')),
                    '발행기관수': int(str(item.get('isur_cnt', 0)).replace(',', '')),
                    '종목수': int(str(item.get('isu_cnt', 0)).replace(',', ''))
                }
                
                trading_data.append(record)
        
        if not trading_data:
            print(f"{formatted_date} - 유효한 데이터를 찾을 수 없습니다.")
            return pd.DataFrame()
        
        # 데이터프레임 생성
        df = pd.DataFrame(trading_data)
        
        print(f"{formatted_date} - 스크래핑 완료: {len(df)}개 행")
        return df
        
    except Exception as e:
        print(f"{date_str} - 스크래핑 중 오류 발생: {e}")
        return pd.DataFrame()

def scrape_krx_esg_trading_range(start_date, end_date):
    """날짜 범위에 대한 KRX ESG 채권 거래 현황을 스크래핑합니다."""
    
    # 날짜 문자열을 datetime 객체로 변환
    if isinstance(start_date, str):
        start = datetime.strptime(start_date.replace('-', ''), '%Y%m%d')
    else:
        start = start_date
    
    if isinstance(end_date, str):
        end = datetime.strptime(end_date.replace('-', ''), '%Y%m%d')
    else:
        end = end_date
    
    # 시작일이 종료일보다 늦은 경우 교환
    if start > end:
        start, end = end, start
    
    print(f"날짜 범위: {start.strftime('%Y-%m-%d')} ~ {end.strftime('%Y-%m-%d')}")
    
    all_data = []
    current_date = start
    
    # 일별로 데이터 수집
    while current_date <= end:
        print(f"\n{current_date.strftime('%Y-%m-%d')} 데이터 수집 중...")
        
        # 해당 날짜 데이터 스크래핑
        df = scrape_krx_esg_trading_for_date(current_date)
        
        if not df.empty:
            all_data.append(df)
        
        # 다음 날짜로 이동
        current_date += timedelta(days=1)
        
        # API 호출 간격 조절 (1초 대기)
        time.sleep(1)
    
    # 모든 데이터 합치기
    if all_data:
        result_df = pd.concat(all_data, ignore_index=True)
        print(f"\n전체 수집 완료: 총 {len(result_df)}개 행")
        return result_df
    else:
        print("\n수집된 데이터가 없습니다.")
        return pd.DataFrame()

def update_google_sheets_batch(df, spreadsheet_id, credentials_json):
    """Google Sheets에 여러 날짜의 거래 현황을 한 번에 업데이트합니다."""
    
    try:
        # 서비스 계정 인증
        credentials = Credentials.from_service_account_info(
            json.loads(credentials_json),
            scopes=['https://www.googleapis.com/auth/spreadsheets']
        )
        
        # gspread 클라이언트 초기화
        gc = gspread.authorize(credentials)
        
        # 스프레드시트 열기
        spreadsheet = gc.open_by_key(spreadsheet_id)
        
        # 워크시트 가져오기 또는 생성
        worksheet_name = "ESG채권거래현황"
        try:
            worksheet = spreadsheet.worksheet(worksheet_name)
        except:
            worksheet = spreadsheet.add_worksheet(title=worksheet_name, rows=10000, cols=10)
            # 헤더 추가
            headers = ['거래일자', '채권종류', '거래량', '거래대금', '발행기관수', '종목수']
            worksheet.update(values=[headers], range_name='A1')
            
            # 헤더 서식 설정
            worksheet.format('A1:F1', {
                'backgroundColor': {'red': 0.2, 'green': 0.2, 'blue': 0.2},
                'textFormat': {'foregroundColor': {'red': 1, 'green': 1, 'blue': 1}, 'bold': True},
                'horizontalAlignment': 'CENTER'
            })
        
        # 기존 데이터 가져오기
        existing_data = worksheet.get_all_records()
        existing_df = pd.DataFrame(existing_data)
        
        # 중복 제거: 새 데이터의 날짜 중 이미 존재하는 날짜 확인
        if not existing_df.empty:
            existing_dates = set(existing_df['거래일자'].unique())
            new_dates = set(df['거래일자'].unique())
            
            # 중복되는 날짜 찾기
            duplicate_dates = existing_dates.intersection(new_dates)
            
            if duplicate_dates:
                print(f"중복되는 날짜 발견: {sorted(duplicate_dates)}")
                
                # 중복 처리 옵션 확인
                update_option = os.environ.get('UPDATE_EXISTING', 'false').lower()
                
                if update_option == 'true' or ('GITHUB_ACTIONS' not in os.environ):
                    # 로컬 환경에서는 사용자에게 확인
                    if 'GITHUB_ACTIONS' not in os.environ:
                        response = input("중복된 날짜의 데이터를 덮어쓰시겠습니까? (y/n): ")
                        if response.lower() != 'y':
                            # 중복된 날짜 제외
                            df = df[~df['거래일자'].isin(duplicate_dates)]
                            print(f"중복된 날짜를 제외하고 진행합니다.")
                        else:
                            # 기존 데이터에서 중복된 날짜 삭제
                            all_values = worksheet.get_all_values()
                            rows_to_delete = []
                            
                            for i, row in enumerate(all_values[1:], start=2):
                                if row[0] in duplicate_dates:
                                    rows_to_delete.append(i)
                            
                            # 역순으로 삭제
                            for row_idx in reversed(rows_to_delete):
                                worksheet.delete_rows(row_idx)
                            
                            print(f"중복된 날짜의 기존 데이터를 삭제했습니다.")
                else:
                    # 중복된 날짜 제외
                    df = df[~df['거래일자'].isin(duplicate_dates)]
                    print(f"중복된 날짜를 제외하고 진행합니다.")
        
        # 새 데이터 추가
        if not df.empty:
            # 다음 빈 행 찾기
            next_row = len(worksheet.get_all_values()) + 1
            
            # 데이터 추가
            values = df.values.tolist()
            cell_range = f'A{next_row}:F{next_row + len(values) - 1}'
            worksheet.update(values=values, range_name=cell_range)
            
            # 숫자 컬럼 서식 설정
            worksheet.format(f'C{next_row}:F{next_row + len(values) - 1}', {
                'numberFormat': {'type': 'NUMBER', 'pattern': '#,##0'}
            })
            
            print(f"Google Sheets에 {len(values)}개 행 추가 완료")
        else:
            print("추가할 새로운 데이터가 없습니다.")
        
        # 전체 데이터를 날짜순으로 정렬
        all_data = worksheet.get_all_records()
        if all_data:
            sorted_df = pd.DataFrame(all_data).sort_values(['거래일자', '채권종류'])
            worksheet.clear()
            worksheet.update(values=[sorted_df.columns.tolist()] + sorted_df.values.tolist(), range_name='A1')
            
            # 헤더 서식 재설정
            worksheet.format('A1:F1', {
                'backgroundColor': {'red': 0.2, 'green': 0.2, 'blue': 0.2},
                'textFormat': {'foregroundColor': {'red': 1, 'green': 1, 'blue': 1}, 'bold': True},
                'horizontalAlignment': 'CENTER'
            })
        
        # 요약 시트 업데이트
        update_summary_sheet(spreadsheet)
        
        # 로그 기록
        update_log_sheet(spreadsheet, df)
        
    except Exception as e:
        print(f"Google Sheets 업데이트 중 오류 발생: {e}")
        raise

def update_summary_sheet(spreadsheet):
    """요약 시트를 업데이트합니다."""
    try:
        summary_sheet = spreadsheet.worksheet("거래현황요약")
    except:
        summary_sheet = spreadsheet.add_worksheet(title="거래현황요약", rows=1000, cols=10)
    
    # 메인 시트에서 데이터 가져오기
    worksheet = spreadsheet.worksheet("ESG채권거래현황")
    all_data = worksheet.get_all_records()
    
    if all_data:
        df = pd.DataFrame(all_data)
        
        # 거래일자별 전체 거래대금 합계
        daily_summary = df.groupby('거래일자').agg({
            '거래량': 'sum',
            '거래대금': 'sum',
            '발행기관수': 'max',  # 중복 제거를 위해 max 사용
            '종목수': 'max'
        }).reset_index()
        
        daily_summary = daily_summary.sort_values('거래일자')
        
        # 요약 시트 업데이트
        summary_sheet.clear()
        summary_sheet.update(values=[daily_summary.columns.tolist()] + daily_summary.values.tolist(), range_name='A1')
        
        # 서식 설정
        summary_sheet.format('A1:E1', {
            'backgroundColor': {'red': 0.2, 'green': 0.2, 'blue': 0.2},
            'textFormat': {'foregroundColor': {'red': 1, 'green': 1, 'blue': 1}, 'bold': True},
            'horizontalAlignment': 'CENTER'
        })
        
        # 숫자 서식
        summary_sheet.format(f'B2:E{len(daily_summary)+1}', {
            'numberFormat': {'type': 'NUMBER', 'pattern': '#,##0'}
        })

def update_log_sheet(spreadsheet, df):
    """로그 시트를 업데이트합니다."""
    try:
        log_sheet = spreadsheet.worksheet("업데이트로그")
    except:
        log_sheet = spreadsheet.add_worksheet(title="업데이트로그", rows=1000, cols=4)
        log_sheet.update(values=[['업데이트시간', '상태', '레코드수', '날짜범위']], range_name='A1')
    
    log_row = len(log_sheet.get_all_values()) + 1
    update_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    if not df.empty:
        date_range = f"{df['거래일자'].min()} ~ {df['거래일자'].max()}"
    else:
        date_range = "N/A"
    
    log_sheet.update(values=[[update_time, '성공', len(df), date_range]], range_name=f'A{log_row}')

def main():
    # 환경 변수 확인
    if 'GITHUB_ACTIONS' in os.environ:
        # GitHub Actions 환경 - 오늘 날짜만 처리
        spreadsheet_id = os.environ.get('KRDEBT_SPREADSHEET_ID')
        credentials_json = os.environ.get('GOOGLE_SERVICE')
        
        if not spreadsheet_id or not credentials_json:
            print("필수 환경 변수가 설정되지 않았습니다.")
            print(f"KRDEBT_SPREADSHEET_ID: {'설정됨' if spreadsheet_id else '미설정'}")
            print(f"GOOGLE_SERVICE: {'설정됨' if credentials_json else '미설정'}")
            sys.exit(1)
        
        print("KRX ESG 채권 거래 현황 스크래핑 시작 (자동 실행)...")
        
        # 오늘 날짜 데이터만 수집
        today = datetime.now()
        df = scrape_krx_esg_trading_for_date(today)
        
    else:
        # 로컬 테스트 환경 - 날짜 범위 입력 받기
        print("로컬 환경에서 실행 중...")
        
        # 날짜 범위 입력
        print("\n날짜 범위를 입력하세요 (YYYYMMDD 형식)")
        print("단일 날짜만 조회하려면 시작일과 종료일을 동일하게 입력하세요.")
        
        while True:
            start_date = input("시작일 (예: 20250612): ").strip()
            if len(start_date) == 8 and start_date.isdigit():
                break
            print("올바른 형식으로 입력해주세요 (YYYYMMDD)")
        
        while True:
            end_date = input("종료일 (예: 20250615): ").strip()
            if len(end_date) == 8 and end_date.isdigit():
                break
            print("올바른 형식으로 입력해주세요 (YYYYMMDD)")
        
        # 스프레드시트 정보 입력
        spreadsheet_id = input("\n스프레드시트 ID를 입력하세요: ").strip()
        credentials_path = input("인증 JSON 파일 경로를 입력하세요: ").strip()
        
        with open(credentials_path, 'r') as f:
            credentials_json = f.read()
        
        print(f"\nKRX ESG 채권 거래 현황 스크래핑 시작...")
        
        # 날짜 범위 데이터 수집
        df = scrape_krx_esg_trading_range(start_date, end_date)
    
    if df.empty:
        print("수집된 데이터가 없습니다.")
        sys.exit(1)
    
    # 수집된 데이터 요약 표시
    print("\n=== 수집된 데이터 요약 ===")
    print(f"총 레코드 수: {len(df)}")
    print(f"날짜 범위: {df['거래일자'].min()} ~ {df['거래일자'].max()}")
    print("\n날짜별 거래 현황:")
    summary = df.groupby('거래일자')['거래대금'].sum().sort_values(ascending=False)
    for date, amount in summary.items():
        print(f"  {date}: {amount:,.0f} 백만원")
    
    # Google Sheets 업데이트
    print("\nGoogle Sheets 업데이트 중...")
    update_google_sheets_batch(df, spreadsheet_id, credentials_json)
    
    print("\n작업이 완료되었습니다.")

if __name__ == "__main__":
    main()
