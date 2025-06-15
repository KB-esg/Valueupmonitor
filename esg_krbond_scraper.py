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
        
        # 데이터구분 추가
        df['데이터구분'] = '국내'
        
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
        columns_to_keep = list(column_mapping.keys()) + ['채권종류', '조회일자', '수집일시', '데이터구분']
        df = df[df.columns.intersection(columns_to_keep)]
        df.rename(columns=column_mapping, inplace=True)
        
        # 컬럼 순서 재정렬
        desired_order = ['조회일자', '수집일시', '데이터구분', '발행기관', '표준코드', '종목명', 
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

def scrape_krx_overseas_esg_bonds():
    """한국기업 해외물 ESG채권 데이터를 스크래핑합니다."""
    
    # 요청 URL과 헤더 설정
    url = "https://esgbond.krx.co.kr/contents/99/SRI99000001.jspx"
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'Origin': 'https://esgbond.krx.co.kr',
        'Referer': 'https://esgbond.krx.co.kr/contents/02/02030000/SRI02030000.jsp',
        'X-Requested-With': 'XMLHttpRequest'
    }
    
    # POST 데이터 설정 (해외물 채권용)
    data = {
        'code': '02/02030000/sri02030000_01',
        'pagePath': '/contents/02/02030000/SRI02030000.jsp'
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
        
        # 조회일자 추가 (오늘 날짜)
        df['조회일자'] = datetime.now().strftime('%Y%m%d')
        
        # 수집일시 추가
        df['수집일시'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # 데이터구분 추가
        df['데이터구분'] = '해외물'
        
        # 채권종류 추출
        df['채권종류'] = df['isu_nm'].apply(lambda x: x if pd.notna(x) else '')
        
        # 컬럼명 한글로 변경
        column_mapping = {
            'isur_nm': '발행기관',
            'isu_nm': '채권유형',
            'usr_defin_nm1': '발행금액',
            'isu_dd': '발행연월',
            'usr_defin_nm2': '만기연월',
            'usr_defin_nm3': '기간',
            'usr_defin_nm4': '발행금리',
            'usr_defin_nm5': '표면금리',
            'misc_info': '주관사'
        }
        
        # 필요한 컬럼만 선택하고 이름 변경
        columns_to_keep = list(column_mapping.keys()) + ['채권종류', '조회일자', '수집일시', '데이터구분']
        df = df[df.columns.intersection(columns_to_keep)]
        df.rename(columns=column_mapping, inplace=True)
        
        # 컬럼 순서 재정렬
        desired_order = ['조회일자', '수집일시', '데이터구분', '발행기관', '채권유형', 
                        '채권종류', '발행연월', '만기연월', '기간', '발행금액', 
                        '발행금리', '표면금리', '주관사']
        
        final_columns = [col for col in desired_order if col in df.columns]
        df = df[final_columns]
        
        return df
        
    except Exception as e:
        print(f"해외물 채권 수집 중 오류 발생: {e}")
        return pd.DataFrame()

def update_google_sheets(domestic_df, overseas_df, spreadsheet_id, credentials_json):
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
        
        # 1. 국내 누적 데이터 워크시트
        try:
            domestic_ws = spreadsheet.worksheet("국내_누적데이터")
        except:
            domestic_ws = spreadsheet.add_worksheet(title="국내_누적데이터", rows=100000, cols=20)
        
        # 기존 국내 데이터 처리
        combined_domestic_df = update_worksheet_data(domestic_ws, domestic_df, "국내")
        
        # 2. 국내 최신 현황 워크시트
        try:
            domestic_current_ws = spreadsheet.worksheet("국내_최신현황")
        except:
            domestic_current_ws = spreadsheet.add_worksheet(title="국내_최신현황", rows=5000, cols=20)
        
        # 가장 최근 조회일자의 국내 데이터만 추출
        if not combined_domestic_df.empty:
            latest_date = combined_domestic_df['조회일자'].max()
            latest_domestic_df = combined_domestic_df[combined_domestic_df['조회일자'] == latest_date].copy()
            
            print(f"\n국내 최신 현황 업데이트 중 (조회일자: {latest_date})")
            update_worksheet_simple(domestic_current_ws, latest_domestic_df, force_update=True)
        
        # 3. 해외물 누적 데이터 워크시트
        try:
            overseas_cumulative_ws = spreadsheet.worksheet("해외물_누적데이터")
        except:
            overseas_cumulative_ws = spreadsheet.add_worksheet(title="해외물_누적데이터", rows=50000, cols=20)
        
        # 해외물 누적 데이터 처리
        combined_overseas_df = update_overseas_cumulative_data(overseas_cumulative_ws, overseas_df)
        
        # 4. 해외물 최신 현황 워크시트
        try:
            overseas_current_ws = spreadsheet.worksheet("해외물_최신현황")
        except:
            overseas_current_ws = spreadsheet.add_worksheet(title="해외물_최신현황", rows=5000, cols=20)
        
        # 해외물 최신 데이터 업데이트
        if not overseas_df.empty:
            print(f"\n해외물 최신 현황 업데이트 중...")
            update_worksheet_simple(overseas_current_ws, overseas_df, force_update=True)
        
        # 5. 요약 정보 업데이트
        try:
            summary_ws = spreadsheet.worksheet("요약")
        except:
            summary_ws = spreadsheet.add_worksheet(title="요약", rows=100, cols=10)
        
        update_summary_sheet(summary_ws, combined_domestic_df, combined_overseas_df)
        
        print(f"\nGoogle Sheets 업데이트 완료:")
        print(f"- 국내 누적 데이터: {len(combined_domestic_df)}개 행")
        print(f"- 해외물 누적 데이터: {len(combined_overseas_df)}개 행")
        print(f"- 해외물 최신 현황: {len(overseas_df)}개 행")
        
    except Exception as e:
        print(f"Google Sheets 업데이트 중 오류 발생: {e}")
        raise

def update_worksheet_data(worksheet, new_df, data_type):
    """워크시트에 누적 데이터를 업데이트합니다."""
    
    # 기존 데이터 가져오기
    print(f"\n{data_type} 기존 누적 데이터 확인 중...")
    existing_data = worksheet.get_all_values()
    
    if existing_data and len(existing_data) > 1:
        # 기존 데이터를 DataFrame으로 변환
        existing_df = pd.DataFrame(existing_data[1:], columns=existing_data[0])
        print(f"기존 {data_type} 누적 데이터: {len(existing_df)}개")
        
        # 데이터 타입 맞추기 (문자열로 통일)
        for col in existing_df.columns:
            existing_df[col] = existing_df[col].astype(str)
        for col in new_df.columns:
            new_df[col] = new_df[col].astype(str)
        
        # 국내 채권의 경우 표준코드와 종목명으로 고유 키 생성
        if '표준코드' in new_df.columns and '종목명' in new_df.columns:
            # 기존 데이터의 고유 키 집합 생성
            existing_keys = set()
            for _, row in existing_df.iterrows():
                key = f"{row.get('표준코드', '')}_{row.get('종목명', '')}"
                existing_keys.add(key)
            
            # 새 데이터에서 중복되지 않는 것만 필터링
            new_rows = []
            duplicates = 0
            for _, row in new_df.iterrows():
                key = f"{row.get('표준코드', '')}_{row.get('종목명', '')}"
                if key not in existing_keys:
                    new_rows.append(row)
                else:
                    duplicates += 1
            
            if duplicates > 0:
                print(f"  → 중복 제거: {duplicates}개 (이미 존재하는 채권)")
            
            if new_rows:
                new_unique_df = pd.DataFrame(new_rows)
                print(f"  → 신규 추가: {len(new_unique_df)}개")
                combined_df = pd.concat([existing_df, new_unique_df], ignore_index=True)
                
                # 정렬
                if '조회일자' in combined_df.columns:
                    # 조회일자를 datetime으로 변환하여 정렬
                    combined_df['조회일자_temp'] = pd.to_datetime(combined_df['조회일자'], format='%Y%m%d', errors='coerce')
                    combined_df = combined_df.sort_values(['조회일자_temp', '발행기관'], ascending=[False, True])
                    combined_df = combined_df.drop(columns=['조회일자_temp'])
                else:
                    combined_df = combined_df.sort_values(['발행기관'])
                
                print(f"최종 {data_type} 누적 데이터: {len(combined_df)}개")
                
                # 워크시트 업데이트 (변경사항이 있을 때만)
                update_worksheet_simple(worksheet, combined_df)
            else:
                print(f"  → 신규 채권 없음 (업데이트 생략)")
                combined_df = existing_df
        else:
            # 해외물의 경우 발행기관과 채권유형으로 중복 제거
            combined_df = pd.concat([existing_df, new_df], ignore_index=True)
            combined_df = combined_df.drop_duplicates(
                subset=['발행기관', '채권유형', '조회일자'], 
                keep='first'  # 기존 데이터 우선
            )
            
            # 정렬
            combined_df = combined_df.sort_values(['발행기관'])
            
            print(f"최종 {data_type} 누적 데이터: {len(combined_df)}개")
            
            # 워크시트 업데이트
            update_worksheet_simple(worksheet, combined_df)
    else:
        combined_df = new_df
        print(f"기존 {data_type} 데이터가 없습니다. 새 데이터로 시작합니다.")
        
        # 워크시트 업데이트
        update_worksheet_simple(worksheet, combined_df)
    
    return combined_df

def update_overseas_cumulative_data(worksheet, new_df):
    """해외물 채권의 누적 데이터를 업데이트합니다."""
    
    # 기존 누적 데이터 가져오기
    print(f"\n해외물 기존 누적 데이터 확인 중...")
    existing_data = worksheet.get_all_values()
    
    if existing_data and len(existing_data) > 1:
        # 기존 데이터를 DataFrame으로 변환
        existing_df = pd.DataFrame(existing_data[1:], columns=existing_data[0])
        print(f"기존 해외물 누적 데이터: {len(existing_df)}개")
        
        # 데이터 타입 맞추기 (문자열로 통일)
        for col in existing_df.columns:
            existing_df[col] = existing_df[col].astype(str)
        for col in new_df.columns:
            new_df[col] = new_df[col].astype(str)
        
        # 고유 키 생성 (발행기관 + 채권유형 + 발행금액 + 발행연월)
        existing_df['unique_key'] = (existing_df['발행기관'].astype(str) + '_' + 
                                    existing_df['채권유형'].astype(str) + '_' + 
                                    existing_df['발행금액'].astype(str) + '_' + 
                                    existing_df['발행연월'].astype(str))
        
        new_df['unique_key'] = (new_df['발행기관'].astype(str) + '_' + 
                              new_df['채권유형'].astype(str) + '_' + 
                              new_df['발행금액'].astype(str) + '_' + 
                              new_df['발행연월'].astype(str))
        
        # 기존 채권 중 활성 상태인 것들의 키 집합
        existing_active_keys = set(existing_df[existing_df.get('상태', '활성') == '활성']['unique_key'])
        
        # 현재 데이터의 키 집합
        current_keys = set(new_df['unique_key'])
        
        # 새로운 채권 (기존에 없던 것)
        new_keys = current_keys - set(existing_df['unique_key'])
        
        # 사라진 채권 (기존 활성 중 현재 없는 것)
        disappeared_keys = existing_active_keys - current_keys
        
        print(f"  → 신규 채권: {len(new_keys)}개")
        print(f"  → 만기/상환 추정: {len(disappeared_keys)}개")
        
        # 변경사항이 있는 경우만 처리
        if len(new_keys) > 0 or len(disappeared_keys) > 0:
            # 결과 DataFrame 구성
            result_rows = []
            
            # 1. 기존 데이터 처리
            for _, row in existing_df.iterrows():
                if row['unique_key'] in disappeared_keys:
                    # 사라진 채권은 상태를 '만기/상환'으로 변경
                    row['상태'] = '만기/상환'
                    row['최종확인일'] = new_df['조회일자'].iloc[0] if not new_df.empty else datetime.now().strftime('%Y%m%d')
                elif row['unique_key'] in current_keys:
                    # 여전히 존재하는 채권은 최신 정보로 업데이트
                    new_row = new_df[new_df['unique_key'] == row['unique_key']].iloc[0]
                    row = new_row.copy()
                    row['상태'] = '활성'
                # 이미 만기/상환 상태인 채권은 그대로 유지
                result_rows.append(row)
            
            # 2. 신규 채권 추가
            for key in new_keys:
                new_row = new_df[new_df['unique_key'] == key].iloc[0].copy()
                new_row['상태'] = '활성'
                result_rows.append(new_row)
            
            # DataFrame 생성 및 정리
            combined_df = pd.DataFrame(result_rows)
            
            # unique_key 컬럼 제거
            combined_df = combined_df.drop(columns=['unique_key'])
            
            # 정렬 (상태별, 발행연월 역순)
            combined_df['발행연월_sort'] = pd.to_datetime(
                combined_df['발행연월'].astype(str).str[:4] + '-' + 
                combined_df['발행연월'].astype(str).str[5:7] + '-01',
                errors='coerce'
            )
            
            combined_df = combined_df.sort_values(
                ['상태', '발행연월_sort', '발행기관'], 
                ascending=[True, False, True]
            )
            
            combined_df = combined_df.drop(columns=['발행연월_sort'])
            
            print(f"최종 해외물 누적 데이터: {len(combined_df)}개 (활성: {len(combined_df[combined_df['상태'] == '활성'])}개)")
            
            # 워크시트 업데이트 (변경사항이 있을 때만)
            update_worksheet_simple(worksheet, combined_df)
        else:
            print(f"  → 변경사항 없음 (업데이트 생략)")
            combined_df = existing_df
            combined_df = combined_df.drop(columns=['unique_key'])
            
    else:
        combined_df = new_df.copy()
        combined_df['상태'] = '활성'
        print(f"기존 해외물 데이터가 없습니다. 새 데이터로 시작합니다.")
        
        # 워크시트 업데이트
        update_worksheet_simple(worksheet, combined_df)
    
    return combined_df

def update_worksheet_simple(worksheet, df, force_update=False):
    """워크시트에 데이터를 간단히 업데이트합니다."""
    
    # force_update가 False이고 데이터가 비어있으면 업데이트하지 않음
    if not force_update and df.empty:
        print("  → 업데이트할 데이터가 없습니다.")
        return
    
    worksheet.clear()
    
    if df.empty:
        worksheet.update([['데이터가 없습니다']], 'A1')
        return
    
    # 헤더 추가
    headers = df.columns.tolist()
    worksheet.update([headers], 'A1')
    
    # 데이터 추가 (배치 처리)
    df = df.fillna('')
    values = df.astype(str).values.tolist()
    
    batch_size = 500
    total_rows = len(values)
    
    # tqdm으로 업로드 진행 상황 표시
    with tqdm(total=total_rows, desc="Google Sheets 업로드") as pbar:
        for i in range(0, total_rows, batch_size):
            batch_end = min(i + batch_size, total_rows)
            batch_data = values[i:batch_end]
            
            start_row = i + 2
            end_row = batch_end + 1
            
            num_cols = len(headers)
            end_col = chr(ord('A') + num_cols - 1) if num_cols <= 26 else 'Z'
            range_str = f'A{start_row}:{end_col}{end_row}'
            
            try:
                worksheet.update(batch_data, range_str)
                pbar.update(batch_end - i)
                time.sleep(1)
            except Exception as e:
                print(f"\n배치 업로드 오류: {e}")
                time.sleep(2)
                try:
                    worksheet.update(batch_data, range_str)
                    pbar.update(batch_end - i)
                except:
                    print(f"재시도 실패. 계속 진행합니다.")
                    continue

def update_summary_sheet(summary_ws, domestic_df, overseas_df):
    """요약 정보를 업데이트합니다."""
    
    # 국내 최신 데이터
    if not domestic_df.empty:
        latest_date = domestic_df['조회일자'].max()
        latest_domestic_df = domestic_df[domestic_df['조회일자'] == latest_date]
    else:
        latest_domestic_df = pd.DataFrame()
    
    # 해외물 활성 채권 수
    active_overseas_count = len(overseas_df[overseas_df['상태'] == '활성']) if '상태' in overseas_df.columns else len(overseas_df)
    expired_overseas_count = len(overseas_df[overseas_df['상태'] == '만기/상환']) if '상태' in overseas_df.columns else 0
    
    summary_data = [
        ["마지막 업데이트", datetime.now().strftime('%Y-%m-%d %H:%M:%S')],
        [""],
        ["국내 ESG 채권", ""],
        ["총 누적 데이터", str(len(domestic_df))],
        ["고유 채권 수", str(domestic_df['표준코드'].nunique()) if not domestic_df.empty else "0"],
        [""],
        ["채권종류별 현황 (최신)", "개수"],
        ["녹색채권", str(len(latest_domestic_df[latest_domestic_df['채권종류'] == '녹색채권']))],
        ["사회적채권", str(len(latest_domestic_df[latest_domestic_df['채권종류'] == '사회적채권']))],
        ["지속가능채권", str(len(latest_domestic_df[latest_domestic_df['채권종류'] == '지속가능채권']))],
        ["지속가능연계채권", str(len(latest_domestic_df[latest_domestic_df['채권종류'] == '지속가능연계채권']))],
        [""],
        ["해외물 ESG 채권", ""],
        ["총 누적 채권", str(len(overseas_df))],
        ["활성 채권", str(active_overseas_count)],
        ["만기/상환 채권", str(expired_overseas_count)],
        [""],
        ["해외물 채권유형별 현황 (활성)", "개수"]
    ]
    
    # 해외물 활성 채권유형별 현황
    if not overseas_df.empty and '상태' in overseas_df.columns:
        active_overseas_df = overseas_df[overseas_df['상태'] == '활성']
        if not active_overseas_df.empty:
            overseas_type_counts = active_overseas_df['채권유형'].value_counts()
            for bond_type, count in overseas_type_counts.items():
                summary_data.append([bond_type, str(count)])
    elif not overseas_df.empty:
        overseas_type_counts = overseas_df['채권유형'].value_counts()
        for bond_type, count in overseas_type_counts.items():
            summary_data.append([bond_type, str(count)])
    
    summary_ws.clear()
    summary_ws.update(summary_data, 'A1')

def send_telegram_notification(domestic_df, overseas_df):
    """ESG 채권 정보를 텔레그램으로 전송합니다."""
    
    bot_token = os.environ.get('TELCO_NEWS_TOKEN')
    chat_id = os.environ.get('TELCO_NEWS_TESTER')
    
    if not bot_token or not chat_id:
        print("텔레그램 환경 변수가 설정되지 않았습니다.")
        return
    
    try:
        # 최근 일주일 내 상장한 국내 채권
        today = datetime.now()
        week_ago = today - timedelta(days=7)
        
        message = f"📊 KRX ESG 채권 업데이트 완료!\n\n"
        
        # 국내 채권 정보
        if not domestic_df.empty:
            domestic_df['상장일_dt'] = pd.to_datetime(domestic_df['상장일'], errors='coerce')
            
            recent_domestic = domestic_df[
                (domestic_df['상장일_dt'] >= week_ago) & 
                (domestic_df['상장일_dt'] <= today)
            ].copy()
            
            recent_domestic = recent_domestic.sort_values('상장일_dt', ascending=False)
            
            if len(recent_domestic) > 0:
                message += f"🇰🇷 국내 ESG 채권 - 최근 일주일 신규 상장 ({len(recent_domestic)}개)\n"
                
                # 채권종류별 집계
                bond_type_counts = recent_domestic['채권종류'].value_counts()
                
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
                
                # 발행기관별로 그룹화하여 표시 (최대 10개 기관)
                message += "\n발행기관별 내역:\n"
                
                # 발행기관별로 그룹화
                issuer_groups = recent_domestic.groupby('발행기관').agg({
                    '채권종류': 'first',
                    '발행금액(백만)': 'sum',
                    '상장일': 'count'
                }).reset_index()
                
                issuer_groups.columns = ['발행기관', '채권종류', '총발행금액', '채권수']
                issuer_groups = issuer_groups.sort_values('총발행금액', ascending=False)
                
                for idx, row in issuer_groups.head(10).iterrows():
                    bond_type_emoji = {
                        '녹색채권': '🌱',
                        '사회적채권': '🤝',
                        '지속가능채권': '♻️',
                        '지속가능연계채권': '🔗'
                    }.get(row['채권종류'], '📌')
                    
                    if row['채권수'] > 1:
                        message += f"• {row['발행기관']} - {row['채권수']}개 채권, 총 {row['총발행금액']:,.0f}백만원\n"
                    else:
                        message += f"• {row['발행기관']} - {row['총발행금액']:,.0f}백만원\n"
                
                if len(issuer_groups) > 10:
                    message += f"... 외 {len(issuer_groups) - 10}개 기관\n"
            else:
                message += f"🇰🇷 국내: 최근 일주일 신규 상장 없음\n"
        
        # 해외물 채권 정보
        if not overseas_df.empty:
            message += f"\n🌏 해외물 ESG 채권 현황\n"
            
            # 활성/만기 구분
            if '상태' in overseas_df.columns:
                active_count = len(overseas_df[overseas_df['상태'] == '활성'])
                expired_count = len(overseas_df[overseas_df['상태'] == '만기/상환'])
                message += f"• 활성 채권: {active_count}개\n"
                message += f"• 만기/상환: {expired_count}개\n"
                
                # 최근 발행 채권 (활성 채권 중에서)
                active_df = overseas_df[overseas_df['상태'] == '활성'].copy()
            else:
                active_df = overseas_df.copy()
                message += f"• 총 {len(overseas_df)}개 채권\n"
            
            # 최근 발행 채권 (발행연월 기준)
            if not active_df.empty:
                active_df['발행연월_dt'] = pd.to_datetime(
                    active_df['발행연월'].astype(str).str[:4] + '-' + 
                    active_df['발행연월'].astype(str).str[5:7] + '-01',
                    errors='coerce'
                )
                
                # 최근 6개월 이내 발행
                six_months_ago = today - timedelta(days=180)
                recent_overseas = active_df[active_df['발행연월_dt'] >= six_months_ago]
                
                if len(recent_overseas) > 0:
                    message += f"• 최근 6개월 발행: {len(recent_overseas)}개\n"
                    
                    # 최근 발행 발행기관별로 표시 (최대 5개)
                    recent_overseas_sorted = recent_overseas.sort_values('발행연월_dt', ascending=False)
                    message += "\n최근 발행 기관:\n"
                    
                    # 발행기관별로 그룹화
                    recent_issuers = recent_overseas_sorted.groupby('발행기관').agg({
                        '채권유형': lambda x: ', '.join(x.unique()),
                        '발행금액': lambda x: ', '.join(x),
                        '발행연월': 'count'
                    }).reset_index()
                    
                    recent_issuers.columns = ['발행기관', '채권유형', '발행금액', '건수']
                    
                    for idx, row in recent_issuers.head(5).iterrows():
                        if row['건수'] > 1:
                            message += f"  - {row['발행기관']} ({row['건수']}건)\n"
                        else:
                            message += f"  - {row['발행기관']} {row['채권유형']} ({row['발행금액']})\n"
                    
                    if len(recent_issuers) > 5:
                        message += f"  ... 외 {len(recent_issuers) - 5}개 기관\n"
                
                # 새로 추가된 채권 확인 (이전 수집 대비)
                new_bonds = active_df[active_df['조회일자'] == active_df['조회일자'].max()]
                if len(new_bonds) > 0 and len(active_df) > len(new_bonds):
                    message += f"\n🆕 신규 추가: {len(new_bonds)}개\n"
        
        # 전체 통계
        message += f"\n📈 전체 현황:\n"
        message += f"• 국내 ESG 채권: {domestic_df['표준코드'].nunique():,}개\n"
        
        # 국내 발행기관 수
        if not domestic_df.empty:
            domestic_issuers = domestic_df['발행기관'].nunique()
            message += f"  - 발행기관: {domestic_issuers}개\n"
        
        message += f"• 해외물 ESG 채권: {len(overseas_df)}개"
        if '상태' in overseas_df.columns:
            active_count = len(overseas_df[overseas_df['상태'] == '활성'])
            message += f" (활성: {active_count}개)"
        
        # 해외물 발행기관 수
        if not overseas_df.empty:
            overseas_issuers = overseas_df['발행기관'].nunique()
            message += f"\n  - 발행기관: {overseas_issuers}개"
        
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
    
    # 1. 국내 ESG 채권 데이터 수집
    # 날짜 리스트 생성
    if start_date and end_date:
        print(f"\n[국내 채권] 날짜 범위: {start_date} ~ {end_date}")
        dates_list = get_monthly_dates(start_date, end_date)
        print(f"조회할 날짜 ({len(dates_list)}개): {', '.join(dates_list)}")
    else:
        # 날짜 범위가 없으면 오늘 날짜만
        today = datetime.now().strftime('%Y%m%d')
        dates_list = [today]
        print(f"\n[국내 채권] 단일 날짜 조회: {today}")
    
    domestic_data = []
    
    # tqdm으로 진행 상황 표시
    for date in tqdm(dates_list, desc="국내 채권 수집 중"):
        df = scrape_krx_esg_bonds_by_date(date)
        if not df.empty:
            domestic_data.append(df)
            tqdm.write(f"    → {date}: {len(df)}개 채권 수집 완료")
        else:
            tqdm.write(f"    → {date}: 데이터 없음")
        
        # API 부하 방지를 위한 대기
        time.sleep(2)
    
    if domestic_data:
        # 모든 국내 데이터 병합
        domestic_df = pd.concat(domestic_data, ignore_index=True)
        print(f"\n[국내 채권] 총 수집된 데이터: {len(domestic_df)}개")
    else:
        print("\n[국내 채권] 수집된 데이터가 없습니다.")
        domestic_df = pd.DataFrame()
    
    # 2. 해외물 ESG 채권 데이터 수집
    print("\n[해외물 채권] 데이터 수집 중...")
    overseas_df = scrape_krx_overseas_esg_bonds()
    
    if not overseas_df.empty:
        print(f"[해외물 채권] 수집 완료: {len(overseas_df)}개")
    else:
        print("[해외물 채권] 데이터가 없습니다.")
    
    # 데이터가 하나도 없으면 종료
    if domestic_df.empty and overseas_df.empty:
        print("\n수집된 데이터가 없습니다.")
        sys.exit(1)
    
    # Google Sheets 업데이트
    print("\nGoogle Sheets 업데이트 중...")
    update_google_sheets(domestic_df, overseas_df, spreadsheet_id, credentials_json)
    
    # 텔레그램 알림 전송 (GitHub Actions 환경에서만)
    if 'GITHUB_ACTIONS' in os.environ:
        send_telegram_notification(domestic_df, overseas_df)
    
    print("\n✅ 작업이 완료되었습니다.")

if __name__ == "__main__":
    main()
