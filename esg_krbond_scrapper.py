import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import gspread
from google.oauth2.service_account import Credentials
import json
import os
import sys

def scrape_krx_esg_bonds():
    """KRX ESG 채권 현황 데이터를 스크래핑합니다."""
    
    # 요청 URL과 헤더 설정
    url = "https://esgbond.krx.co.kr/contents/99/SRI99000001.jspx"
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'Origin': 'https://esgbond.krx.co.kr',
        'Referer': 'https://esgbond.krx.co.kr/contents/02/02010000/SRI02010000.jsp',
        'X-Requested-With': 'XMLHttpRequest'
    }
    
    # 현재 날짜 설정 (YYYYMMDD 형식)
    today = datetime.now().strftime('%Y%m%d')
    
    # POST 데이터 설정
    data = {
        'sri_bnd_tp_cd': 'ALL',  # 전체 채권종류
        'isu_cdnm': '전체',      # 전체 발행기관
        'isur_cd': '',
        'isu_cd': '',
        'iss_inst_nm': '',
        'isu_srt_cd': '',
        'isu_nm': '',
        'bnd_tp_cd': 'ALL',      # 전체 채권유형
        'schdate': today,
        'pagePath': '/contents/02/02010000/SRI02010000.jsp',
        'code': '02/02010000/sri02010000_02',
        'pageFirstCall': 'Y'
    }
    
    try:
        # POST 요청
        response = requests.post(url, headers=headers, data=data)
        response.raise_for_status()
        
        # JSON 응답 파싱
        json_data = response.json()
        
        # 데이터 추출 (응답 구조에 따라 조정 필요)
        if 'block1' in json_data:
            bonds_data = json_data['block1']
        else:
            # 다른 가능한 키 확인
            keys = list(json_data.keys())
            if keys:
                bonds_data = json_data[keys[0]]
            else:
                print("데이터를 찾을 수 없습니다.")
                return pd.DataFrame()
        
        # 데이터프레임 생성
        df = pd.DataFrame(bonds_data)
        
        # 채권종류 추출 함수
        def extract_bond_type(name):
            if pd.isna(name):
                return ''
            
            name_str = str(name)
            if '(녹)' in name_str:
                return '녹색채권'
            elif '(사)' in name_str:
                return '사회적채권'
            elif '(지)' in name_str:
                return '지속가능채권'
            elif '(연)' in name_str:
                return '지속가능연계채권'
            else:
                return ''
        
        # 채권종류 컬럼 추가
        df['채권종류'] = df['isu_nm'].apply(extract_bond_type)
        
        # 컬럼명 한글로 변경
        column_mapping = {
            'com_abbrv': '발행기관',
            'isu_cd': '표준코드',
            'isu_nm': '종목명',
            'lst_dt': '상장일',
            'iss_dt': '발행일',
            'dis_dt': '상환일',
            'curr_iso_cd': '표면이자율',
            'iss_amt': '발행금액(백만)',
            'lst_amt': '상장금액(백만)',
            'bnd_tp_nm': '채권유형'
        }
        
        # 필요한 컬럼만 선택하고 이름 변경
        columns_to_keep = list(column_mapping.keys()) + ['채권종류']
        df = df[df.columns.intersection(columns_to_keep)]
        df.rename(columns=column_mapping, inplace=True)
        
        # 컬럼 순서 재정렬
        desired_order = ['발행기관', '표준코드', '종목명', '채권종류', '상장일', 
                        '발행일', '상환일', '표면이자율', '발행금액(백만)', 
                        '상장금액(백만)', '채권유형']
        
        # 존재하는 컬럼만 선택
        final_columns = [col for col in desired_order if col in df.columns]
        df = df[final_columns]
        
        # 날짜 형식 통일 (YYYY/MM/DD -> YYYY-MM-DD)
        date_columns = ['상장일', '발행일', '상환일']
        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce').dt.strftime('%Y-%m-%d')
        
        # 숫자 컬럼 정리
        numeric_columns = ['표면이자율', '발행금액(백만)', '상장금액(백만)']
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col].astype(str).str.replace(',', ''), errors='coerce')
        
        print(f"총 {len(df)}개의 ESG 채권 데이터를 수집했습니다.")
        return df
        
    except requests.exceptions.RequestException as e:
        print(f"요청 중 오류 발생: {e}")
        return pd.DataFrame()
    except Exception as e:
        print(f"데이터 처리 중 오류 발생: {e}")
        return pd.DataFrame()

def update_google_sheets(df, spreadsheet_id, credentials_json):
    """Google Sheets를 업데이트합니다."""
    
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
        
        # 첫 번째 워크시트 가져오기 또는 생성
        try:
            worksheet = spreadsheet.get_worksheet(0)
        except:
            worksheet = spreadsheet.add_worksheet(title="ESG채권현황", rows=1000, cols=20)
        
        # 워크시트 이름 변경
        worksheet.update_title("ESG채권현황")
        
        # 기존 데이터 모두 삭제
        worksheet.clear()
        
        # 헤더 추가
        headers = df.columns.tolist()
        worksheet.update('A1', [headers])
        
        # 데이터 추가
        if not df.empty:
            # NaN 값을 빈 문자열로 변경
            df = df.fillna('')
            
            # 모든 값을 문자열로 변환
            values = df.astype(str).values.tolist()
            
            # 데이터 업데이트
            worksheet.update(f'A2:K{len(values)+1}', values)
        
        # 서식 설정
        worksheet.format('A1:K1', {
            'backgroundColor': {'red': 0.2, 'green': 0.2, 'blue': 0.2},
            'textFormat': {'foregroundColor': {'red': 1, 'green': 1, 'blue': 1}, 'bold': True},
            'horizontalAlignment': 'CENTER'
        })
        
        # 업데이트 시간 기록
        update_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        last_row = len(df) + 3
        worksheet.update(f'A{last_row}', f'마지막 업데이트: {update_time}')
        
        print(f"Google Sheets 업데이트 완료: {len(df)}개 행")
        
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
            sys.exit(1)
    else:
        # 로컬 테스트 환경
        print("로컬 환경에서 실행 중...")
        spreadsheet_id = input("스프레드시트 ID를 입력하세요: ")
        credentials_path = input("인증 JSON 파일 경로를 입력하세요: ")
        
        with open(credentials_path, 'r') as f:
            credentials_json = f.read()
    
    print("KRX ESG 채권 데이터 스크래핑 시작...")
    
    # 데이터 스크래핑
    df = scrape_krx_esg_bonds()
    
    if df.empty:
        print("수집된 데이터가 없습니다.")
        sys.exit(1)
    
    # 데이터 미리보기
    print("\n수집된 데이터 미리보기:")
    print(df.head())
    print(f"\n채권종류별 개수:")
    print(df['채권종류'].value_counts())
    
    # Google Sheets 업데이트
    print("\nGoogle Sheets 업데이트 중...")
    update_google_sheets(df, spreadsheet_id, credentials_json)
    
    print("\n작업이 완료되었습니다.")

if __name__ == "__main__":
    main()
