import requests
import pandas as pd
from datetime import datetime, timedelta
import gspread
from google.oauth2.service_account import Credentials
import json
import os
import sys
import time
from dateutil.relativedelta import relativedelta
from tqdm import tqdm

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

def scrape_krx_esg_bonds_by_date(query_date):
    """특정 날짜의 KRX ESG 채권 현황 데이터를 스크래핑합니다."""
    
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
        'schdate': query_date,   # 조회일자
        'pagePath': '/contents/02/02010000/SRI02010000.jsp',
        'code': '02/02010000/sri02010000_02',
        'pageFirstCall': 'Y'
    }
    
    try:
        # POST 요청
        response = requests.post(url, headers=headers, data=data, timeout=30)
        response.raise_for_status()
        
        # JSON 응답 파싱
        json_data = response.json()
        
        # 데이터 추출
        if 'block1' in json_data:
            bonds_data = json_data['block1']
        else:
            keys = list(json_data.keys())
            if keys:
                bonds_data = json_data[keys[0]]
            else:
                return pd.DataFrame()
        
        if not bonds_data:
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
                df[col] = pd.to_datetime(df[col], errors='coerce').dt.strftime('%Y-%m-%d')
        
        # 숫자 컬럼 정리
        numeric_columns = ['표면이자율', '발행금액(백만)', '상장금액(백만)']
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col].astype(str).str.replace(',', ''), errors='coerce')
        
        return df
        
    except Exception as e:
        print(f"    → {query_date}: 오류 발생 - {e}")
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
            print(f"해결 방법:")
            print(f"1. Google Sheets 열기: https://docs.google.com/spreadsheets/d/{spreadsheet_id}")
            print(f"2. 공유 버튼 클릭")
            print(f"3. '{service_account_email}' 이메일 추가")
            print(f"4. '편집자' 권한 부여")
            print(f"5. 다시 실행하세요\n")
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
                        try:
                            cumulative_ws.update(range_str, batch_data)
                            pbar.update(batch_end - i)
                        except:
                            print(f"재시도 실패. 계속 진행합니다.")
                            continue
        
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
            batch_size = 500
            for i in range(0, len(latest_values), batch_size):
                batch_end = min(i + batch_size, len(latest_values))
                batch_data = latest_values[i:batch_end]
                
                start_row = i + 2
                end_row = batch_end + 1
                
                range_str = f'A{start_row}:M{end_row}'
                current_ws.update(range_str, batch_data)
                time.sleep(1)
        
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

def send_telegram_notification(all_data_df):
    """최근 일주일 내 상장한 ESG 채권 정보를 텔레그램으로 전송합니다."""
    
    bot_token = os.environ.get('TELCO_NEWS_TOKEN')
    chat_id = os.environ.get('TELCO_NEWS_TESTER')
    
    if not bot_token or not chat_id:
        print("텔레그램 환경 변수가 설정되지 않았습니다.")
        return
    
    try:
        # 최신 데이터에서 상장일 기준으로 최근 일주일 데이터 필터링
        today = datetime.now()
        week_ago = today - timedelta(days=7)
        
        # 상장일을 datetime으로 변환
        all_data_df['상장일_dt'] = pd.to_datetime(all_data_df['상장일'], errors='coerce')
        
        # 최근 일주일 내 상장된 채권 필터링
        recent_bonds = all_data_df[
            (all_data_df['상장일_dt'] >= week_ago) & 
            (all_data_df['상장일_dt'] <= today)
        ].copy()
        
        # 상장일 기준으로 정렬 (최신순)
        recent_bonds = recent_bonds.sort_values('상장일_dt', ascending=False)
        
        # 메시지 작성
        if len(recent_bonds) > 0:
            message = f"📊 KRX ESG 채권 업데이트 완료!\n\n"
            message += f"🗓️ 최근 일주일 신규 상장 ESG 채권 ({len(recent_bonds)}개)\n"
            message += f"({week_ago.strftime('%Y-%m-%d')} ~ {today.strftime('%Y-%m-%d')})\n\n"
            
            # 채권종류별 집계
            bond_type_counts = recent_bonds['채권종류'].value_counts()
            
            for bond_type, count in bond_type_counts.items():
                if bond_type == '녹색채권':
                    emoji = '🌱'
                elif bond_type == '사회적채권':
                    emoji = '🤝'
                elif bond_type == '지속가능채권':
                    emoji = '♻️'
                elif bond_type == '지속가능연계채권':
                    emoji = '🔗'
                else:
                    emoji = '📌'
                message += f"{emoji} {bond_type}: {count}개\n"
            
            message += "\n📋 상세 내역:\n"
            
            # 최대 10개까지만 표시
            for idx, row in recent_bonds.head(10).iterrows():
                bond_type_emoji = {
                    '녹색채권': '🌱',
                    '사회적채권': '🤝',
                    '지속가능채권': '♻️',
                    '지속가능연계채권': '🔗'
                }.get(row['채권종류'], '📌')
                
                message += f"\n{bond_type_emoji} [{row['상장일']}]\n"
                message += f"• 발행기관: {row['발행기관']}\n"
                message += f"• 종목명: {row['종목명']}\n"
                message += f"• 발행금액: {row['발행금액(백만)']:,.0f}백만원\n"
                
            if len(recent_bonds) > 10:
                message += f"\n... 외 {len(recent_bonds) - 10}개"
                
        else:
            message = f"📊 KRX ESG 채권 업데이트 완료!\n\n"
            message += f"🗓️ 최근 일주일({week_ago.strftime('%Y-%m-%d')} ~ {today.strftime('%Y-%m-%d')}) 동안\n"
            message += f"신규 상장된 ESG 채권이 없습니다."
        
        # 전체 통계 추가
        total_bonds = len(all_data_df)
        unique_bonds = all_data_df['표준코드'].nunique()
        
        message += f"\n\n📈 전체 ESG 채권 현황:\n"
        message += f"• 총 데이터: {total_bonds:,}개\n"
        message += f"• 고유 채권: {unique_bonds:,}개\n"
        
        # 채권종류별 전체 현황
        total_type_counts = all_data_df[all_data_df['조회일자'] == all_data_df['조회일자'].max()]['채권종류'].value_counts()
        message += "\n채권종류별 현황:\n"
        for bond_type, count in total_type_counts.items():
            if bond_type:  # 빈 값이 아닌 경우만
                message += f"• {bond_type}: {count}개\n"
        
        # 텔레그램 메시지 전송
        url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        data = {
            'chat_id': chat_id,
            'text': message,
            'parse_mode': 'HTML'
        }
        
        response = requests.post(url, data=data)
        
        if response.status_code == 200:
            print("\n✅ 텔레그램 알림 전송 성공!")
        else:
            print(f"\n❌ 텔레그램 알림 전송 실패: {response.status_code}")
            
    except Exception as e:
        print(f"\n텔레그램 알림 전송 중 오류 발생: {e}")

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
    
    all_data = []
    
    # tqdm으로 진행 상황 표시
    for date in tqdm(dates_list, desc="데이터 수집 중"):
        df = scrape_krx_esg_bonds_by_date(date)
        if not df.empty:
            all_data.append(df)
            tqdm.write(f"    → {date}: {len(df)}개 채권 수집 완료")
        else:
            tqdm.write(f"    → {date}: 데이터 없음")
        
        # API 부하 방지를 위한 대기
        time.sleep(2)
    
    if all_data:
        # 모든 데이터 병합
        all_data_df = pd.concat(all_data, ignore_index=True)
        print(f"\n총 수집된 데이터: {len(all_data_df)}개")
        
        # 조회일자별 수집 현황
        print("\n조회일자별 수집 현황:")
        date_counts = all_data_df['조회일자'].value_counts().sort_index()
        for date, count in date_counts.items():
            print(f"  - {date}: {count}개")
    else:
        print("\n수집된 데이터가 없습니다.")
        sys.exit(1)
    
    # Google Sheets 업데이트
    print("\nGoogle Sheets 업데이트 중...")
    update_google_sheets(all_data_df, spreadsheet_id, credentials_json)
    
    # 텔레그램 알림 전송 (GitHub Actions 환경에서만)
    if 'GITHUB_ACTIONS' in os.environ:
        send_telegram_notification(all_data_df)
    
    print("\n✅ 작업이 완료되었습니다.")

if __name__ == "__main__":
    main()
