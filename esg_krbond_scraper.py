import asyncio
from playwright.async_api import async_playwright
import pandas as pd
from datetime import datetime
import gspread
from google.oauth2.service_account import Credentials
import json
import os
import sys
import time
from dateutil.relativedelta import relativedelta
from tqdm import tqdm
import nest_asyncio

# Jupyter나 이미 실행 중인 이벤트 루프에서도 작동하도록
nest_asyncio.apply()

def get_monthly_dates(start_date, end_date):
    """시작일과 종료일 사이의 매월 1일 날짜 리스트를 반환합니다."""
    dates = []
    current = datetime.strptime(start_date, '%Y%m%d')
    end = datetime.strptime(end_date, '%Y%m%d')
    
    # 시작월의 1일부터 시작
    current = current.replace(day=1)
    
    while current <= end:
        dates.append(current.strftime('%Y%m%d'))
        # 다음 달로 이동
        current = current + relativedelta(months=1)
    
    return dates

async def scrape_krx_esg_bonds_by_date(page, query_date):
    """특정 날짜의 KRX ESG 채권 현황 데이터를 스크래핑합니다."""
    
    try:
        # 날짜 입력 필드를 클리어하고 새 날짜 입력
        date_input = await page.wait_for_selector('#schdatec16a5320fa475530d9583c34fd356ef5')
        await date_input.click()
        await date_input.fill('')  # 기존 값 클리어
        await date_input.fill(query_date)
        
        # 조회 버튼 클릭
        await page.click('#btnid8e296a067a37563370ded05f5a3bf3ec')
        
        # 데이터 로딩 대기 (그리드가 업데이트될 때까지)
        await page.wait_for_timeout(3000)  # 3초 대기
        
        # 더 안정적인 대기: 특정 요소가 나타날 때까지
        try:
            await page.wait_for_selector('.CI-GRID-BODY-TABLE tbody tr', timeout=10000)
        except:
            print(f"    → {query_date}: 데이터 없음")
            return pd.DataFrame()
        
        # JavaScript를 통해 데이터 추출
        data = await page.evaluate('''
            () => {
                const rows = document.querySelectorAll('.CI-GRID-BODY-TABLE tbody tr');
                const data = [];
                
                rows.forEach(row => {
                    const cells = row.querySelectorAll('td');
                    if (cells.length > 0) {
                        data.push({
                            com_abbrv: cells[0]?.textContent?.trim() || '',
                            isu_cd: cells[1]?.textContent?.trim() || '',
                            isu_nm: cells[2]?.textContent?.trim() || '',
                            lst_dt: cells[3]?.textContent?.trim() || '',
                            iss_dt: cells[4]?.textContent?.trim() || '',
                            dis_dt: cells[5]?.textContent?.trim() || '',
                            curr_iso_cd: cells[6]?.textContent?.trim() || '',
                            iss_amt: cells[7]?.textContent?.trim() || '',
                            lst_amt: cells[8]?.textContent?.trim() || '',
                            bnd_tp_nm: cells[9]?.textContent?.trim() || ''
                        });
                    }
                });
                
                return data;
            }
        ''')
        
        if not data:
            print(f"    → {query_date}: 데이터 없음")
            return pd.DataFrame()
        
        # 데이터프레임 생성
        df = pd.DataFrame(data)
        
        # 채권종류 추출 함수
        def extract_bond_type(name):
            if pd.isna(name) or name == '':
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
        
        # 조회일자 추가
        df['조회일자'] = query_date
        
        # 수집일시 추가
        df['수집일시'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
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
        columns_to_keep = list(column_mapping.keys()) + ['채권종류', '조회일자', '수집일시']
        df = df[df.columns.intersection(columns_to_keep)]
        df.rename(columns=column_mapping, inplace=True)
        
        # 컬럼 순서 재정렬
        desired_order = ['조회일자', '수집일시', '발행기관', '표준코드', '종목명', 
                        '채권종류', '상장일', '발행일', '상환일', '표면이자율', 
                        '발행금액(백만)', '상장금액(백만)', '채권유형']
        
        final_columns = [col for col in desired_order if col in df.columns]
        df = df[final_columns]
        
        # 날짜 형식 통일 (YYYY/MM/DD -> YYYY-MM-DD)
        date_columns = ['상장일', '발행일', '상환일']
        for col in date_columns:
            if col in df.columns:
                df[col] = df[col].str.replace('/', '-')
        
        # 숫자 컬럼 정리
        numeric_columns = ['표면이자율', '발행금액(백만)', '상장금액(백만)']
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col].astype(str).str.replace(',', ''), errors='coerce')
        
        return df
        
    except Exception as e:
        print(f"    → {query_date}: 오류 발생 - {e}")
        return pd.DataFrame()

async def scrape_all_dates(dates_list):
    """모든 날짜에 대해 데이터를 스크래핑합니다."""
    
    async with async_playwright() as p:
        # 브라우저 시작 (헤드리스 모드)
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        
        # 웹사이트 접속
        print("KRX ESG 채권 웹사이트 접속 중...")
        await page.goto('https://esgbond.krx.co.kr/contents/02/02010000/SRI02010000.jsp')
        
        # 초기 로딩 대기
        await page.wait_for_selector('#btnid8e296a067a37563370ded05f5a3bf3ec', timeout=30000)
        
        all_data = []
        
        # tqdm으로 진행 상황 표시
        for date in tqdm(dates_list, desc="데이터 수집 중"):
            df = await scrape_krx_esg_bonds_by_date(page, date)
            if not df.empty:
                all_data.append(df)
                tqdm.write(f"    → {date}: {len(df)}개 채권 수집 완료")
            
            # API 부하 방지를 위한 대기
            await page.wait_for_timeout(1000)
        
        await browser.close()
        
        if all_data:
            # 모든 데이터 병합
            return pd.concat(all_data, ignore_index=True)
        else:
            return pd.DataFrame()

def update_google_sheets(all_data_df, spreadsheet_id, credentials_json):
    """Google Sheets를 업데이트합니다."""
    
    try:
        # 서비스 계정 정보 파싱
        creds_info = json.loads(credentials_json)
        
        # 서비스 계정 이메일 출력
        service_account_email = creds_info.get('client_email', 'Unknown')
        print(f"\n서비스 계정 이메일: {service_account_email}")
        print(f"스프레드시트 ID: {spreadsheet_id}")
        
        # 서비스 계정 인증
        credentials = Credentials.from_service_account_info(
            creds_info,
            scopes=['https://www.googleapis.com/auth/spreadsheets']
        )
        
        # gspread 클라이언트 초기화
        gc = gspread.authorize(credentials)
        
        # 스프레드시트 열기
        try:
            spreadsheet = gc.open_by_key(spreadsheet_id)
            print(f"스프레드시트에 성공적으로 접근했습니다.")
        except gspread.exceptions.APIError as e:
            print(f"\n권한 오류: Google Sheets에 접근할 수 없습니다.")
            raise
        
        # 누적 데이터 워크시트
        try:
            cumulative_ws = spreadsheet.worksheet("누적데이터")
        except:
            cumulative_ws = spreadsheet.add_worksheet(title="누적데이터", rows=100000, cols=20)
        
        # 기존 누적 데이터 가져오기
        print("\n기존 누적 데이터 확인 중...")
        existing_data = cumulative_ws.get_all_values()
        
        if existing_data and len(existing_data) > 1:
            # 기존 데이터를 DataFrame으로 변환
            existing_df = pd.DataFrame(existing_data[1:], columns=existing_data[0])
            print(f"기존 누적 데이터: {len(existing_df)}개")
            
            # 새 데이터와 병합 (표준코드와 조회일자 기준 중복 제거)
            combined_df = pd.concat([all_data_df, existing_df], ignore_index=True)
            
            # 중복 제거
            combined_df = combined_df.drop_duplicates(
                subset=['표준코드', '조회일자'], 
                keep='first'
            )
            
            # 정렬
            combined_df = combined_df.sort_values(['조회일자', '표준코드'])
            
            print(f"중복 제거 후 총 데이터: {len(combined_df)}개")
        else:
            combined_df = all_data_df
            print("기존 데이터가 없습니다. 새 데이터로 시작합니다.")
        
        # 누적 데이터 업데이트 (tqdm으로 진행 상황 표시)
        print("\n누적 데이터 업데이트 중...")
        cumulative_ws.clear()
        
        # 헤더 추가
        headers = combined_df.columns.tolist()
        cumulative_ws.update('A1', [headers])
        
        # 데이터 추가 (배치 처리)
        if not combined_df.empty:
            combined_df = combined_df.fillna('')
            values = combined_df.astype(str).values.tolist()
            
            batch_size = 500
            total_rows = len(values)
            
            # tqdm으로 업로드 진행 상황 표시
            with tqdm(total=total_rows, desc="Google Sheets 업로드") as pbar:
                for i in range(0, total_rows, batch_size):
                    batch_end = min(i + batch_size, total_rows)
                    batch_data = values[i:batch_end]
                    
                    start_row = i + 2
                    end_row = batch_end + 1
                    
                    range_str = f'A{start_row}:M{end_row}'
                    
                    try:
                        cumulative_ws.update(range_str, batch_data)
                        pbar.update(batch_end - i)
                        time.sleep(1)
                    except Exception as e:
                        print(f"\n배치 업로드 오류: {e}")
                        time.sleep(2)
        
        # 최신 현황 워크시트 업데이트
        try:
            current_ws = spreadsheet.worksheet("최신현황")
        except:
            current_ws = spreadsheet.add_worksheet(title="최신현황", rows=5000, cols=20)
        
        # 가장 최근 조회일자의 데이터만 추출
        latest_date = combined_df['조회일자'].max()
        latest_df = combined_df[combined_df['조회일자'] == latest_date].copy()
        
        print(f"\n최신 현황 업데이트 중 (조회일자: {latest_date})")
        current_ws.clear()
        current_ws.update('A1', [headers])
        
        if not latest_df.empty:
            latest_values = latest_df.astype(str).values.tolist()
            
            # 한 번에 업데이트 (최신 현황은 보통 적음)
            range_str = f'A2:M{len(latest_values)+1}'
            current_ws.update(range_str, latest_values)
        
        # 요약 정보 업데이트
        try:
            summary_ws = spreadsheet.worksheet("요약")
        except:
            summary_ws = spreadsheet.add_worksheet(title="요약", rows=100, cols=10)
        
        # 조회일자별 채권 수 계산
        date_summary = combined_df.groupby('조회일자').size().reset_index(name='채권수')
        
        summary_data = [
            ["마지막 업데이트", datetime.now().strftime('%Y-%m-%d %H:%M:%S')],
            ["총 누적 데이터", str(len(combined_df))],
            ["고유 채권 수", str(combined_df['표준코드'].nunique())],
            [""],
            ["조회일자별 현황", ""]
        ]
        
        # 조회일자별 현황 추가
        for _, row in date_summary.iterrows():
            summary_data.append([row['조회일자'], str(row['채권수'])])
        
        summary_data.extend([
            [""],
            ["최신 채권종류별 현황", "개수"],
            ["녹색채권", str(len(latest_df[latest_df['채권종류'] == '녹색채권']))],
            ["사회적채권", str(len(latest_df[latest_df['채권종류'] == '사회적채권']))],
            ["지속가능채권", str(len(latest_df[latest_df['채권종류'] == '지속가능채권']))],
            ["지속가능연계채권", str(len(latest_df[latest_df['채권종류'] == '지속가능연계채권']))]
        ])
        
        summary_ws.clear()
        summary_ws.update('A1', summary_data)
        
        # 서식 설정
        try:
            cumulative_ws.format('A1:M1', {
                'backgroundColor': {'red': 0.2, 'green': 0.2, 'blue': 0.2},
                'textFormat': {'foregroundColor': {'red': 1, 'green': 1, 'blue': 1}, 'bold': True},
                'horizontalAlignment': 'CENTER'
            })
            
            current_ws.format('A1:M1', {
                'backgroundColor': {'red': 0.2, 'green': 0.4, 'blue': 0.2},
                'textFormat': {'foregroundColor': {'red': 1, 'green': 1, 'blue': 1}, 'bold': True},
                'horizontalAlignment': 'CENTER'
            })
        except:
            print("서식 설정 실패 (무시하고 계속)")
        
        print(f"\nGoogle Sheets 업데이트 완료:")
        print(f"- 누적 데이터: {len(combined_df)}개 행")
        print(f"- 최신 현황: {len(latest_df)}개 행")
        print(f"- 고유 채권: {combined_df['표준코드'].nunique()}개")
        
    except Exception as e:
        print(f"Google Sheets 업데이트 중 오류 발생: {e}")
        raise

def main():
    # 환경 변수 확인
    if 'GITHUB_ACTIONS' in os.environ:
        # GitHub Actions 환경
        spreadsheet_id = os.environ.get('KRDEBT_SPREADSHEET_ID')
        credentials_json = os.environ.get('GOOGLE_CREDENTIALS_JSON')
        
        # 날짜 범위 확인 (환경 변수로 전달)
        start_date = os.environ.get('START_DATE')
        end_date = os.environ.get('END_DATE')
        
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
        
        start_date = input("시작일자를 입력하세요 (YYYYMMDD, Enter=오늘): ")
        end_date = input("종료일자를 입력하세요 (YYYYMMDD, Enter=오늘): ")
    
    print("\nKRX ESG 채권 데이터 스크래핑 시작...")
    
    # 날짜 리스트 생성
    if start_date and end_date:
        print(f"날짜 범위: {start_date} ~ {end_date}")
        dates_list = get_monthly_dates(start_date, end_date)
        print(f"조회할 날짜 ({len(dates_list)}개): {', '.join(dates_list)}")
    else:
        # 날짜 범위가 없으면 오늘 날짜만
        today = datetime.now().strftime('%Y%m%d')
        dates_list = [today]
        print(f"단일 날짜 조회: {today}")
    
    # 비동기 스크래핑 실행
    all_data_df = asyncio.run(scrape_all_dates(dates_list))
    
    if all_data_df.empty:
        print("\n수집된 데이터가 없습니다.")
        sys.exit(1)
    
    # 데이터 요약
    print(f"\n총 수집된 데이터: {len(all_data_df)}개")
    print("\n조회일자별 수집 현황:")
    date_counts = all_data_df['조회일자'].value_counts().sort_index()
    for date, count in date_counts.items():
        print(f"  - {date}: {count}개")
    
    # Google Sheets 업데이트
    print("\nGoogle Sheets 업데이트 중...")
    update_google_sheets(all_data_df, spreadsheet_id, credentials_json)
    
    print("\n✅ 작업이 완료되었습니다.")

if __name__ == "__main__":
    main()
