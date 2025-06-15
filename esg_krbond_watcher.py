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
        
        # 테이블 찾기 - 클래스나 ID로 찾기
        tables = soup.find_all('table')
        
        # 거래 현황 테이블 찾기 (보통 첫 번째 또는 두 번째 테이블)
        trading_table = None
        for table in tables:
            # 헤더에 '거래량' 또는 '거래대금'이 포함된 테이블 찾기
            headers_text = ' '.join([th.get_text(strip=True) for th in table.find_all('th')])
            if '거래량' in headers_text or '거래대금' in headers_text:
                trading_table = table
                break
        
        if not trading_table:
            print("거래 현황 테이블을 찾을 수 없습니다.")
            return pd.DataFrame()
        
        # 테이블 데이터 추출
        data = []
        rows = trading_table.find_all('tr')
        
        # 헤더 추출
        header_row = rows[0]
        headers = [th.get_text(strip=True) for th in header_row.find_all(['th', 'td'])]
        
        # 데이터 행 추출
        for row in rows[1:]:
            cells = row.find_all(['td', 'th'])
            if cells:
                row_data = [cell.get_text(strip=True) for cell in cells]
                if len(row_data) == len(headers):
                    data.append(row_data)
        
        # 데이터프레임 생성
        df = pd.DataFrame(data, columns=headers)
        
        # 데이터 정리
        today = datetime.now().strftime('%Y-%m-%d')
        
        # 컬럼명 표준화
        column_mapping = {
            '구분': '채권종류',
            '종류': '채권종류',
            '채권종류': '채권종류',
            '거래량': '거래량',
            '거래대금': '거래대금',
            '거래대금(백만원)': '거래대금',
            '발행기관수': '발행기관수',
            '발행기관': '발행기관수',
            '종목수': '종목수',
            '종목': '종목수'
        }
        
        # 컬럼명 변경
        df.rename(columns=column_mapping, inplace=True)
        
        # 거래일자 컬럼 추가
        df.insert(0, '거래일자', today)
        
        # 숫자 데이터 정리 (쉼표 제거 및 숫자 변환)
        numeric_columns = ['거래량', '거래대금', '발행기관수', '종목수']
        for col in numeric_columns:
            if col in df.columns:
                # 쉼표, 원 기호 등 제거
                df[col] = df[col].astype(str).str.replace(',', '').str.replace('원', '').str.replace('백만', '')
                # 빈 값이나 '-'를 0으로 변환
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
        
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
    
    # 현재 날짜 설정
    today = datetime.now()
    today_str = today.strftime('%Y%m%d')
    
    # POST 데이터 설정
    data = {
        'schdate': today_str,
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
        for key in ['result', 'block1', 'OutBlock1', 'output']:
            if key in json_data:
                if isinstance(json_data[key], list):
                    raw_data = json_data[key]
                    break
                elif isinstance(json_data[key], dict) and 'data' in json_data[key]:
                    raw_data = json_data[key]['data']
                    break
        else:
            print(f"응답 데이터 구조: {list(json_data.keys())}")
            return pd.DataFrame()
        
        # 데이터 파싱
        for item in raw_data:
            # 채권 종류 확인
            bond_type = item.get('sri_bnd_tp_nm', item.get('bnd_tp_nm', ''))
            if not bond_type:
                bond_type = item.get('tp_nm', '전체')
            
            record = {
                '거래일자': today.strftime('%Y-%m-%d'),
                '채권종류': bond_type,
                '거래량': int(str(item.get('trd_qty', item.get('qty', 0))).replace(',', '')),
                '거래대금': int(str(item.get('trd_amt', item.get('amt', 0))).replace(',', '')),
                '발행기관수': int(str(item.get('isur_cnt', item.get('inst_cnt', 0))).replace(',', '')),
                '종목수': int(str(item.get('isu_cnt', item.get('item_cnt', 0))).replace(',', ''))
            }
            
            trading_data.append(record)
        
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
            # 헤더 추가
            headers = ['거래일자', '채권종류', '거래량', '거래대금', '발행기관수', '종목수']
            worksheet.update('A1', [headers])
            
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
            
            # 데이터 추가
            values = df.values.tolist()
            cell_range = f'A{next_row}:F{next_row + len(values) - 1}'
            worksheet.update(cell_range, values)
            
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
            
            # 요약 시트 업데이트
            summary_sheet.clear()
            summary_sheet.update('A1', [daily_summary.columns.tolist()] + daily_summary.values.tolist())
            
            # 서식 설정
            summary_sheet.format('A1:B1', {
                'backgroundColor': {'red': 0.2, 'green': 0.2, 'blue': 0.2},
                'textFormat': {'foregroundColor': {'red': 1, 'green': 1, 'blue': 1}, 'bold': True},
                'horizontalAlignment': 'CENTER'
            })
        
        # 업데이트 시간 기록
        update_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        worksheet.update(f'H1', f'최종 업데이트: {update_time}')
        
    except Exception as e:
        print(f"Google Sheets 업데이트 중 오류 발생: {e}")
        raise

def main():
    # 환경 변수 확인
    if 'GITHUB_ACTIONS' in os.environ:
        # GitHub Actions 환경
        spreadsheet_id = os.environ.get('KRDEBT_SPREADSHEET_ID')
        credentials_json = os.environ.get('GOOGLE_CREDENTIALS_JSON')
        
        if not spreadsheet_id or not credentials_json:
            print("필수 환경 변수가 설정되지 않았습니다.")
            print(f"KRDEBT_SPREADSHEET_ID: {'설정됨' if spreadsheet_id else '미설정'}")
            print(f"GOOGLE_CREDENTIALS_JSON: {'설정됨' if credentials_json else '미설정'}")
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
