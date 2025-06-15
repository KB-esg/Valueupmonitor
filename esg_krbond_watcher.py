import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import gspread
from google.oauth2.service_account import Credentials
import json
import os
import sys
import time
import re

def scrape_krx_esg_trading_html():
    """KRX ESG 채권 거래 현황 페이지를 HTML로 스크래핑합니다."""
    
    # 메인 페이지 URL
    url = "https://esgbond.krx.co.kr/contents/04/04020000/SRI04020000.jsp"
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
        'Referer': 'https://esgbond.krx.co.kr/'
    }
    
    try:
        # 세션 생성
        session = requests.Session()
        
        # 메인 페이지 접속
        response = session.get(url, headers=headers)
        response.raise_for_status()
        
        # HTML 파싱
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # CI-GRID 형태의 테이블 찾기
        grid_body = soup.find('tbody', class_='CI-GRID-BODY-TABLE-TBODY')
        
        if not grid_body:
            print("CI-GRID 테이블을 찾을 수 없습니다.")
            return pd.DataFrame()
        
        # 테이블 데이터 추출
        data = []
        rows = grid_body.find_all('tr')
        
        for row in rows:
            cells = row.find_all('td')
            if cells and len(cells) >= 5:
                # 각 셀의 data-name 속성과 텍스트 추출
                row_data = {}
                for cell in cells:
                    data_name = cell.get('data-name', '')
                    cell_text = cell.get_text(strip=True)
                    
                    if data_name == 'bnd_clss_nm':
                        row_data['채권종류'] = cell_text
                    elif data_name == 'acc_trdvol':
                        # 쉼표 제거 및 float 변환
                        row_data['거래량'] = float(cell_text.replace(',', '') or '0')
                    elif data_name == 'acc_trdval':
                        # 쉼표 제거 및 float 변환
                        row_data['거래대금'] = float(cell_text.replace(',', '') or '0')
                    elif data_name == 'isur_cnt':
                        row_data['발행기관수'] = int(cell_text.replace(',', '') or '0')
                    elif data_name == 'isu_cnt':
                        row_data['종목수'] = int(cell_text.replace(',', '') or '0')
                
                if row_data:
                    data.append(row_data)
        
        if not data:
            print("테이블에서 데이터를 추출할 수 없습니다.")
            return pd.DataFrame()
        
        # 데이터프레임 생성
        df = pd.DataFrame(data)
        
        # 거래일자 추가 (HTML에서 날짜 추출 시도)
        date_input = soup.find('input', {'name': re.compile('schdate|fr_work_dt')})
        if date_input and date_input.get('value'):
            date_str = date_input['value']
            # YYYYMMDD 형식을 YYYY-MM-DD로 변환
            if len(date_str) == 8:
                today = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
            else:
                today = datetime.now().strftime('%Y-%m-%d')
        else:
            today = datetime.now().strftime('%Y-%m-%d')
        
        # 거래일자 컬럼 추가
        df.insert(0, '거래일자', today)
        
        print(f"HTML 스크래핑 완료: {len(df)}개 행")
        return df
        
    except Exception as e:
        print(f"HTML 스크래핑 중 오류 발생: {e}")
        return pd.DataFrame()

def scrape_krx_esg_trading_api():
    """KRX ESG 채권 거래 현황 데이터를 API로 스크래핑합니다."""
    
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
    
    # 현재 날짜 설정 (YYYYMMDD 형식)
    today = datetime.now()
    today_str = today.strftime('%Y%m%d')
    
    # POST 데이터 설정
    data = {
        'fr_work_dt': today_str,
        'to_work_dt': today_str,
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
            print("API 응답에서 데이터를 찾을 수 없습니다.")
            return pd.DataFrame()
        
        # 데이터 파싱
        for item in raw_data:
            # 채권 종류별 데이터
            bond_type = item.get('bnd_clss_nm', '')
            
            if bond_type:  # 채권종류가 있는 경우만 처리
                record = {
                    '거래일자': today.strftime('%Y-%m-%d'),
                    '채권종류': bond_type,
                    '거래량': float(str(item.get('acc_trdvol', 0)).replace(',', '')),
                    '거래대금': float(str(item.get('acc_trdval', 0)).replace(',', '')),
                    '발행기관수': int(str(item.get('isur_cnt', 0)).replace(',', '')),
                    '종목수': int(str(item.get('isu_cnt', 0)).replace(',', ''))
                }
                
                trading_data.append(record)
        
        if not trading_data:
            print("API 응답에서 유효한 데이터를 찾을 수 없습니다.")
            return pd.DataFrame()
        
        # 데이터프레임 생성
        df = pd.DataFrame(trading_data)
        
        print(f"API 스크래핑 완료: {len(df)}개 행")
        return df
        
    except Exception as e:
        print(f"API 스크래핑 중 오류 발생: {e}")
        return pd.DataFrame()

def scrape_krx_esg_trading():
    """KRX ESG 채권 거래 현황을 스크래핑합니다. (HTML 우선, 실패시 API)"""
    
    print("HTML 방식으로 스크래핑 시도...")
    df = scrape_krx_esg_trading_html()
    
    if df.empty:
        print("HTML 스크래핑 실패. API 방식으로 재시도...")
        df = scrape_krx_esg_trading_api()
    
    if not df.empty:
        print("\n수집된 ESG 채권 거래 현황:")
        print(df)
    
    return df

def update_google_sheets(df, spreadsheet_id, credentials_json):
    """Google Sheets에 거래 현황을 누적하여 업데이트합니다."""
    
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
            # 헤더 추가 (새로운 API 형식)
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
        
        # 오늘 날짜의 데이터가 이미 있는지 확인
        today_str = df['거래일자'].iloc[0] if not df.empty else datetime.now().strftime('%Y-%m-%d')
        today_data_exists = any(row['거래일자'] == today_str for row in existing_data)
        
        if today_data_exists:
            print(f"{today_str} 데이터가 이미 존재합니다.")
            
            # 기존 데이터 업데이트가 필요한 경우
            update_option = os.environ.get('UPDATE_EXISTING', 'false').lower()
            if update_option == 'true':
                # 오늘 날짜의 데이터 행 찾기
                all_values = worksheet.get_all_values()
                rows_to_delete = []
                
                for i, row in enumerate(all_values[1:], start=2):  # 헤더 제외
                    if row[0] == today_str:  # 첫 번째 컬럼이 거래일자
                        rows_to_delete.append(i)
                
                # 역순으로 삭제 (인덱스 변경 방지)
                for row_idx in reversed(rows_to_delete):
                    worksheet.delete_rows(row_idx)
                
                print(f"{today_str} 기존 데이터를 삭제하고 새 데이터로 교체합니다.")
            else:
                return
        
        # 새 데이터 추가
        if not df.empty:
            # 다음 빈 행 찾기
            next_row = len(worksheet.get_all_values()) + 1
            
            # 데이터 추가 (새로운 API 형식)
            values = df.values.tolist()
            cell_range = f'A{next_row}:F{next_row + len(values) - 1}'
            worksheet.update(values=values, range_name=cell_range)
            
            # 숫자 컬럼 서식 설정
            worksheet.format(f'C{next_row}:F{next_row + len(values) - 1}', {
                'numberFormat': {'type': 'NUMBER', 'pattern': '#,##0'}
            })
            
            print(f"Google Sheets에 {len(values)}개 행 추가 완료")
        
        # 차트 생성을 위한 요약 시트 업데이트
        try:
            summary_sheet = spreadsheet.worksheet("거래현황요약")
        except:
            summary_sheet = spreadsheet.add_worksheet(title="거래현황요약", rows=1000, cols=10)
        
        # 피벗 테이블 형태로 요약 데이터 생성
        all_data = worksheet.get_all_records()
        summary_df = pd.DataFrame(all_data)
        
        if not summary_df.empty:
            # 거래일자별 전체 거래대금 합계
            daily_summary = summary_df.groupby('거래일자')['거래대금'].sum().reset_index()
            daily_summary.columns = ['거래일자', '전체거래대금']
            
            # 요약 시트 업데이트 (새로운 API 형식)
            summary_sheet.clear()
            summary_sheet.update(values=[daily_summary.columns.tolist()] + daily_summary.values.tolist(), range_name='A1')
            
            # 서식 설정
            summary_sheet.format('A1:B1', {
                'backgroundColor': {'red': 0.2, 'green': 0.2, 'blue': 0.2},
                'textFormat': {'foregroundColor': {'red': 1, 'green': 1, 'blue': 1}, 'bold': True},
                'horizontalAlignment': 'CENTER'
            })
        
        # 업데이트 시간 기록 (별도 셀이 아닌 로그 시트에 기록)
        try:
            log_sheet = spreadsheet.worksheet("업데이트로그")
        except:
            log_sheet = spreadsheet.add_worksheet(title="업데이트로그", rows=1000, cols=3)
            log_sheet.update(values=[['업데이트시간', '상태', '레코드수']], range_name='A1')
        
        log_row = len(log_sheet.get_all_values()) + 1
        update_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        log_sheet.update(values=[[update_time, '성공', len(df)]], range_name=f'A{log_row}')
        
    except Exception as e:
        print(f"Google Sheets 업데이트 중 오류 발생: {e}")
        raise

def main():
    # 환경 변수 확인
    if 'GITHUB_ACTIONS' in os.environ:
        # GitHub Actions 환경
        spreadsheet_id = os.environ.get('KRDEBT_SPREADSHEET_ID')
        credentials_json = os.environ.get('GOOGLE_SERVICE')
        
        if not spreadsheet_id or not credentials_json:
            print("필수 환경 변수가 설정되지 않았습니다.")
            print(f"KRDEBT_SPREADSHEET_ID: {'설정됨' if spreadsheet_id else '미설정'}")
            print(f"GOOGLE_SERVICE: {'설정됨' if credentials_json else '미설정'}")
            sys.exit(1)
    else:
        # 로컬 테스트 환경
        print("로컬 환경에서 실행 중...")
        spreadsheet_id = input("스프레드시트 ID를 입력하세요: ")
        credentials_path = input("인증 JSON 파일 경로를 입력하세요: ")
        
        with open(credentials_path, 'r') as f:
            credentials_json = f.read()
    
    print("KRX ESG 채권 거래 현황 스크래핑 시작...")
    
    # 데이터 스크래핑
    df = scrape_krx_esg_trading()
    
    if df.empty:
        print("수집된 데이터가 없습니다.")
        sys.exit(1)
    
    # Google Sheets 업데이트
    print("\nGoogle Sheets 업데이트 중...")
    update_google_sheets(df, spreadsheet_id, credentials_json)
    
    print("\n작업이 완료되었습니다.")

if __name__ == "__main__":
    main()
