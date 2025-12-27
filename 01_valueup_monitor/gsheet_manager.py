"""
Google Sheets 관리자
밸류업 공시 목록을 Google Sheets에 기록

Quota 고려사항:
- 읽기: 분당 300회
- 쓰기: 분당 60회
- 배치 업데이트로 API 호출 최소화
"""

import os
import sys
from typing import List, Optional, Dict
from datetime import datetime
import gspread
from google.oauth2.service_account import Credentials

# stdout 버퍼링 해제
sys.stdout.reconfigure(line_buffering=True)


def log(message: str):
    """타임스탬프와 함께 로그 출력"""
    timestamp = datetime.now().strftime("%H:%M:%S")
    print(f"[{timestamp}] {message}", flush=True)


class GSheetManager:
    """Google Sheets 관리자"""
    
    SCOPES = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive'
    ]
    
    # 시트 헤더 정의 (A~J열)
    HEADERS = [
        '번호',           # A
        '공시일자',       # B
        '회사명',         # C
        '종목코드',       # D
        '공시제목',       # E
        '접수번호',       # F
        '원시PDF링크',    # G
        '구글드라이브링크', # H
        '수집일시',       # I
        '아티팩트링크'    # J - GitHub Actions 아티팩트 다운로드 정보
    ]
    
    # 열 인덱스 (1-based)
    COL_ACPTNO = 6          # F열: 접수번호
    COL_GDRIVE_LINK = 8     # H열: 구글드라이브링크
    COL_ARTIFACT_LINK = 10  # J열: 아티팩트링크
    
    def __init__(self, credentials_json: Optional[str] = None, spreadsheet_id: Optional[str] = None):
        """
        초기화
        
        Args:
            credentials_json: 서비스 계정 JSON 파일 경로 또는 JSON 문자열
            spreadsheet_id: 스프레드시트 ID
        """
        self.spreadsheet_id = spreadsheet_id or os.environ.get('VALUEUP_GSPREAD_ID')
        self.client = None
        self.spreadsheet = None
        self._worksheet_cache = {}
        
        # 인증 정보 로드
        creds = None
        if credentials_json:
            if os.path.isfile(credentials_json):
                creds = Credentials.from_service_account_file(credentials_json, scopes=self.SCOPES)
            else:
                import json
                info = json.loads(credentials_json)
                creds = Credentials.from_service_account_info(info, scopes=self.SCOPES)
        else:
            creds_json = os.environ.get('GOOGLE_SERVICE')
            if creds_json:
                import json
                info = json.loads(creds_json)
                creds = Credentials.from_service_account_info(info, scopes=self.SCOPES)
        
        if creds:
            self.client = gspread.authorize(creds)
            if self.spreadsheet_id:
                try:
                    self.spreadsheet = self.client.open_by_key(self.spreadsheet_id)
                    log(f"스프레드시트 연결 성공: {self.spreadsheet.title}")
                except gspread.exceptions.SpreadsheetNotFound:
                    log(f"스프레드시트를 찾을 수 없습니다. ID: {self.spreadsheet_id}")
                    log("  → 서비스 계정에 스프레드시트 공유 권한이 있는지 확인하세요.")
                except gspread.exceptions.APIError as e:
                    log(f"Google Sheets API 오류: {e}")
                except Exception as e:
                    log(f"스프레드시트 열기 실패: {type(e).__name__}: {e}")
    
    def get_or_create_worksheet(self, sheet_name: str = "밸류업공시목록") -> Optional[gspread.Worksheet]:
        """
        워크시트 가져오기 또는 생성
        
        Args:
            sheet_name: 시트 이름
            
        Returns:
            워크시트 또는 None
        """
        if not self.spreadsheet:
            return None
        
        # 캐시 확인
        if sheet_name in self._worksheet_cache:
            return self._worksheet_cache[sheet_name]
        
        try:
            worksheet = self.spreadsheet.worksheet(sheet_name)
            
            # 기존 시트 열/헤더 확장
            self._ensure_columns_and_headers(worksheet)
                    
        except gspread.exceptions.WorksheetNotFound:
            # 새 시트 생성
            worksheet = self.spreadsheet.add_worksheet(
                title=sheet_name,
                rows=2000,  # 넉넉하게 2000행
                cols=len(self.HEADERS)
            )
            # 헤더 설정
            worksheet.update('A1', [self.HEADERS])
            # 헤더 서식 설정 (굵게)
            worksheet.format('A1:J1', {
                'textFormat': {'bold': True},
                'backgroundColor': {'red': 0.9, 'green': 0.9, 'blue': 0.9}
            })
            log(f"새 워크시트 생성: {sheet_name}")
        
        self._worksheet_cache[sheet_name] = worksheet
        return worksheet
    
    def _ensure_columns_and_headers(self, worksheet: gspread.Worksheet):
        """열 수 및 헤더 확인/확장"""
        try:
            current_cols = worksheet.col_count
            required_cols = len(self.HEADERS)
            
            # 열 수가 부족하면 확장
            if current_cols < required_cols:
                worksheet.add_cols(required_cols - current_cols)
                log(f"  시트 열 확장: {current_cols} → {required_cols}")
            
            # 헤더 확인 및 추가
            headers = worksheet.row_values(1)
            if len(headers) < len(self.HEADERS):
                missing_headers = self.HEADERS[len(headers):]
                for idx, header in enumerate(missing_headers):
                    col_num = len(headers) + idx + 1
                    worksheet.update_cell(1, col_num, header)
                log(f"  시트에 새 헤더 추가: {missing_headers}")
                
        except Exception as e:
            log(f"  시트 열/헤더 확장 중 오류: {e}")
    
    def _ensure_row_capacity(self, worksheet: gspread.Worksheet, needed_rows: int):
        """
        필요한 행 수만큼 시트 용량 확보
        
        Args:
            worksheet: 워크시트
            needed_rows: 추가할 행 수
        """
        try:
            current_rows = worksheet.row_count
            # 현재 데이터 행 수 확인
            all_values = worksheet.col_values(1)  # A열 기준
            used_rows = len(all_values)
            
            # 필요한 총 행 수
            required_rows = used_rows + needed_rows + 100  # 여유분 100행
            
            if current_rows < required_rows:
                rows_to_add = required_rows - current_rows
                worksheet.add_rows(rows_to_add)
                log(f"  시트 행 확장: {current_rows} → {required_rows} (+{rows_to_add}행)")
                
        except Exception as e:
            log(f"  시트 행 확장 중 오류: {e}")
    
    def get_existing_acptno_set(self, worksheet: gspread.Worksheet) -> set:
        """
        이미 기록된 접수번호 집합 반환
        
        Args:
            worksheet: 워크시트
            
        Returns:
            접수번호 집합
        """
        try:
            # F열 (접수번호) 전체 가져오기
            acptno_col = worksheet.col_values(self.COL_ACPTNO)
            return set(acptno_col[1:])  # 헤더 제외
        except Exception as e:
            log(f"접수번호 조회 중 오류: {e}")
            return set()
    
    def get_all_data_with_row_numbers(self, worksheet: gspread.Worksheet) -> Dict[str, int]:
        """
        접수번호와 행 번호 매핑 반환 (배치 업데이트용)
        
        Args:
            worksheet: 워크시트
            
        Returns:
            {접수번호: 행번호} 딕셔너리
        """
        try:
            acptno_col = worksheet.col_values(self.COL_ACPTNO)
            return {acptno: row_idx + 1 for row_idx, acptno in enumerate(acptno_col) if acptno}
        except Exception as e:
            log(f"데이터 조회 중 오류: {e}")
            return {}
    
    def append_disclosures(self, disclosures: List[Dict], sheet_name: str = "밸류업공시목록") -> int:
        """
        공시 목록 추가 (배치)
        
        Args:
            disclosures: 공시 정보 딕셔너리 리스트
            sheet_name: 시트 이름
            
        Returns:
            추가된 행 수
        """
        worksheet = self.get_or_create_worksheet(sheet_name)
        if not worksheet:
            return 0
        
        # 이미 존재하는 접수번호 확인
        existing = self.get_existing_acptno_set(worksheet)
        
        # 새로운 항목만 필터링
        new_items = []
        for d in disclosures:
            acptno = str(d.get('접수번호', ''))
            if acptno and acptno not in existing:
                new_items.append(d)
        
        if not new_items:
            log("새로운 공시 항목이 없습니다.")
            return 0
        
        # 행 용량 확보
        self._ensure_row_capacity(worksheet, len(new_items))
        
        # 행 데이터 생성
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        rows = []
        for d in new_items:
            row = [
                d.get('번호', ''),
                d.get('공시일자', ''),
                d.get('회사명', ''),
                d.get('종목코드', ''),
                d.get('공시제목', ''),
                d.get('접수번호', ''),
                d.get('원시PDF링크', ''),
                d.get('구글드라이브링크', ''),
                now,
                ''  # 아티팩트링크 (나중에 업데이트)
            ]
            rows.append(row)
        
        # 배치로 추가 (1회 API 호출)
        try:
            worksheet.append_rows(rows, value_input_option='USER_ENTERED')
            log(f"{len(rows)}건의 새로운 공시 추가 완료")
            return len(rows)
        except Exception as e:
            log(f"행 추가 중 오류: {e}")
            return 0
    
    def batch_update_links(
        self, 
        updates: List[Dict], 
        sheet_name: str = "밸류업공시목록"
    ) -> int:
        """
        여러 행의 링크를 배치로 업데이트 (1회 API 호출)
        
        Args:
            updates: [{'접수번호': str, '구글드라이브링크': str, '아티팩트링크': str}, ...]
            sheet_name: 시트 이름
            
        Returns:
            업데이트된 행 수
        """
        if not updates:
            return 0
        
        worksheet = self.get_or_create_worksheet(sheet_name)
        if not worksheet:
            return 0
        
        # 접수번호 → 행 번호 매핑 조회 (1회 API 호출)
        acptno_to_row = self.get_all_data_with_row_numbers(worksheet)
        
        # 업데이트할 셀 데이터 수집
        batch_data = []
        updated_count = 0
        
        for update in updates:
            acptno = str(update.get('접수번호', ''))
            gdrive_link = update.get('구글드라이브링크', '')
            artifact_link = update.get('아티팩트링크', '')
            
            if acptno not in acptno_to_row:
                log(f"  [WARN] 접수번호 {acptno}를 찾을 수 없음")
                continue
            
            row_num = acptno_to_row[acptno]
            
            # H열 (구글드라이브링크) 업데이트
            if gdrive_link:
                cell_h = f"H{row_num}"
                batch_data.append({
                    'range': cell_h,
                    'values': [[gdrive_link]]
                })
            
            # J열 (아티팩트링크) 업데이트
            if artifact_link:
                cell_j = f"J{row_num}"
                batch_data.append({
                    'range': cell_j,
                    'values': [[artifact_link]]
                })
            
            updated_count += 1
        
        if not batch_data:
            log("업데이트할 데이터가 없습니다.")
            return 0
        
        # 배치 업데이트 (1회 API 호출)
        try:
            worksheet.batch_update(batch_data, value_input_option='USER_ENTERED')
            log(f"  → {updated_count}건 링크 배치 업데이트 완료 (API 호출 1회)")
            return updated_count
        except Exception as e:
            log(f"배치 업데이트 중 오류: {e}")
            return 0
    
    def update_gdrive_link(self, acptno: str, gdrive_link: str, sheet_name: str = "밸류업공시목록") -> bool:
        """
        구글드라이브 링크 업데이트 (단일 - 하위 호환용)
        
        주의: API quota 소모가 큼. batch_update_links 사용 권장.
        
        Args:
            acptno: 접수번호
            gdrive_link: 구글드라이브 링크
            sheet_name: 시트 이름
            
        Returns:
            성공 여부
        """
        result = self.batch_update_links([{
            '접수번호': acptno,
            '구글드라이브링크': gdrive_link
        }], sheet_name)
        return result > 0
    
    def get_items_without_gdrive_link(self, sheet_name: str = "밸류업공시목록") -> List[Dict]:
        """
        구글드라이브 링크가 없는 항목 조회
        
        Args:
            sheet_name: 시트 이름
            
        Returns:
            항목 리스트
        """
        worksheet = self.get_or_create_worksheet(sheet_name)
        if not worksheet:
            return []
        
        try:
            all_records = worksheet.get_all_records()
            return [
                r for r in all_records 
                if r.get('접수번호') and not r.get('구글드라이브링크')
            ]
        except Exception as e:
            log(f"항목 조회 중 오류: {e}")
            return []
    
    def get_items_without_artifact_link(self, sheet_name: str = "밸류업공시목록") -> List[Dict]:
        """
        아티팩트 링크가 없는 항목 조회
        
        Args:
            sheet_name: 시트 이름
            
        Returns:
            항목 리스트
        """
        worksheet = self.get_or_create_worksheet(sheet_name)
        if not worksheet:
            return []
        
        try:
            all_records = worksheet.get_all_records()
            return [
                r for r in all_records 
                if r.get('접수번호') and not r.get('아티팩트링크')
            ]
        except Exception as e:
            log(f"항목 조회 중 오류: {e}")
            return []


def main():
    """테스트용 메인 함수"""
    manager = GSheetManager()
    
    # 테스트 데이터
    test_disclosures = [
        {
            '번호': 1,
            '공시일자': '2025-12-26',
            '회사명': '테스트기업',
            '종목코드': '000000',
            '공시제목': '기업가치 제고 계획(자율공시)',
            '접수번호': 'TEST001',
            '원시PDF링크': 'https://example.com/test.pdf',
            '구글드라이브링크': ''
        }
    ]
    
    count = manager.append_disclosures(test_disclosures)
    log(f"추가된 항목: {count}건")
    
    # 배치 업데이트 테스트
    updates = [
        {
            '접수번호': 'TEST001',
            '구글드라이브링크': 'https://drive.google.com/file/test',
            '아티팩트링크': 'Archive_pdf/20251226_테스트기업_TEST001.pdf'
        }
    ]
    manager.batch_update_links(updates)


if __name__ == "__main__":
    main()
