"""
기업별 분석 결과 저장 관리자
Google Drive에 기업별 스프레드시트를 생성/업데이트

폴더 구조:
01_Valueup_archive/
└── ValueUp_analysis/
    ├── 삼성전자_005930 (Google Spreadsheet)
    │   ├── Summary (기업정보 + 최신 목표 현황)
    │   └── Target_History (목표 이력 추적)
    ├── SK하이닉스_000660 (Google Spreadsheet)
    └── ...

분석 메타정보는 VALUEUP_GSPREAD_ID의 "밸류업공시 목록" 시트 L열부터 관리
"""

import os
import sys
import json
from typing import Optional, Dict, Any, List
from datetime import datetime

import gspread
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

sys.stdout.reconfigure(line_buffering=True)


def log(message: str):
    """타임스탬프와 함께 로그 출력"""
    timestamp = datetime.now().strftime("%H:%M:%S")
    print(f"[{timestamp}] {message}", flush=True)


class CompanySheetManager:
    """기업별 분석 결과 스프레드시트 관리자"""
    
    ANALYSIS_FOLDER_NAME = "ValueUp_analysis"
    
    SCOPES = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive'
    ]
    
    def __init__(self, credentials_json: Optional[str] = None, archive_folder_id: Optional[str] = None):
        """
        초기화
        
        Args:
            credentials_json: 서비스 계정 JSON 문자열
            archive_folder_id: 01_Valueup_archive 폴더 ID
        """
        self.credentials_json = credentials_json or os.environ.get('GOOGLE_SERVICE')
        self.archive_folder_id = archive_folder_id or os.environ.get('VALUEUP_ARCHIVE_ID')
        
        self.gc = None  # gspread 클라이언트
        self.drive_service = None  # Google Drive API
        self.analysis_folder_id = None  # ValueUp_analysis 폴더 ID (캐시)
        
        self._init_clients()
    
    def _init_clients(self):
        """Google API 클라이언트 초기화"""
        if not self.credentials_json:
            log("[ERROR] GOOGLE_SERVICE 환경변수가 설정되지 않았습니다.")
            return
        
        if not self.archive_folder_id:
            log("[WARN] VALUEUP_ARCHIVE_ID 환경변수가 설정되지 않았습니다. 기업별 시트 저장 비활성화.")
            return
        
        try:
            creds_info = json.loads(self.credentials_json)
            creds = Credentials.from_service_account_info(creds_info, scopes=self.SCOPES)
            
            # gspread 클라이언트
            self.gc = gspread.authorize(creds)
            
            # Drive API 클라이언트
            self.drive_service = build('drive', 'v3', credentials=creds)
            
            log("CompanySheetManager 초기화 완료")
        except Exception as e:
            log(f"[ERROR] 클라이언트 초기화 실패: {e}")
    
    def _get_or_create_analysis_folder(self) -> Optional[str]:
        """
        ValueUp_analysis 폴더 ID 조회 또는 생성
        
        Returns:
            폴더 ID 또는 None
        """
        if self.analysis_folder_id:
            return self.analysis_folder_id
        
        if not self.drive_service:
            return None
        
        try:
            # 1. 기존 폴더 검색
            query = (
                f"name = '{self.ANALYSIS_FOLDER_NAME}' and "
                f"'{self.archive_folder_id}' in parents and "
                f"mimeType = 'application/vnd.google-apps.folder' and "
                f"trashed = false"
            )
            
            results = self.drive_service.files().list(
                q=query,
                spaces='drive',
                fields='files(id, name)'
            ).execute()
            
            files = results.get('files', [])
            
            if files:
                self.analysis_folder_id = files[0]['id']
                log(f"기존 {self.ANALYSIS_FOLDER_NAME} 폴더 발견: {self.analysis_folder_id}")
                return self.analysis_folder_id
            
            # 2. 폴더가 없으면 생성
            folder_metadata = {
                'name': self.ANALYSIS_FOLDER_NAME,
                'mimeType': 'application/vnd.google-apps.folder',
                'parents': [self.archive_folder_id]
            }
            
            folder = self.drive_service.files().create(
                body=folder_metadata,
                fields='id'
            ).execute()
            
            self.analysis_folder_id = folder['id']
            log(f"{self.ANALYSIS_FOLDER_NAME} 폴더 생성 완료: {self.analysis_folder_id}")
            return self.analysis_folder_id
            
        except Exception as e:
            log(f"[ERROR] 폴더 조회/생성 실패: {e}")
            return None
    
    def _find_company_spreadsheet(self, company_name: str, stock_code: str) -> Optional[str]:
        """
        기업별 스프레드시트 검색
        
        Args:
            company_name: 기업명
            stock_code: 종목코드
            
        Returns:
            스프레드시트 ID 또는 None
        """
        folder_id = self._get_or_create_analysis_folder()
        if not folder_id:
            return None
        
        # 파일명 형식: "기업명_종목코드"
        file_name = f"{company_name}_{stock_code}"
        
        try:
            query = (
                f"name = '{file_name}' and "
                f"'{folder_id}' in parents and "
                f"mimeType = 'application/vnd.google-apps.spreadsheet' and "
                f"trashed = false"
            )
            
            results = self.drive_service.files().list(
                q=query,
                spaces='drive',
                fields='files(id, name)'
            ).execute()
            
            files = results.get('files', [])
            
            if files:
                return files[0]['id']
            
            return None
            
        except Exception as e:
            log(f"[ERROR] 스프레드시트 검색 실패: {e}")
            return None
    
    def _create_company_spreadsheet(self, company_name: str, stock_code: str, industry: str = "") -> Optional[str]:
        """
        기업별 스프레드시트 신규 생성
        
        Args:
            company_name: 기업명
            stock_code: 종목코드
            industry: 업종
            
        Returns:
            스프레드시트 ID 또는 None
        """
        folder_id = self._get_or_create_analysis_folder()
        if not folder_id:
            return None
        
        file_name = f"{company_name}_{stock_code}"
        
        try:
            # 1. 스프레드시트 생성
            spreadsheet_metadata = {
                'name': file_name,
                'mimeType': 'application/vnd.google-apps.spreadsheet',
                'parents': [folder_id]
            }
            
            file = self.drive_service.files().create(
                body=spreadsheet_metadata,
                fields='id'
            ).execute()
            
            spreadsheet_id = file['id']
            log(f"스프레드시트 생성: {file_name} ({spreadsheet_id})")
            
            # 2. 시트 구조 초기화
            self._init_spreadsheet_structure(spreadsheet_id, company_name, stock_code, industry)
            
            return spreadsheet_id
            
        except Exception as e:
            log(f"[ERROR] 스프레드시트 생성 실패: {e}")
            return None
    
    def _init_spreadsheet_structure(self, spreadsheet_id: str, company_name: str, stock_code: str, industry: str):
        """
        스프레드시트 시트 구조 초기화 (Summary + Target_History)
        
        Target_History는 항목 중심 피벗 구조:
        - 행: 항목 + 세부분류 (현재값, 목표값, 목표연도, 달성률, 전기대비)
        - 열: 보고서일별로 동적 확장
        
        Args:
            spreadsheet_id: 스프레드시트 ID
            company_name: 기업명
            stock_code: 종목코드
            industry: 업종
        """
        try:
            spreadsheet = self.gc.open_by_key(spreadsheet_id)
            
            # 기본 시트 이름 변경 (Sheet1 → Summary)
            worksheet = spreadsheet.sheet1
            worksheet.update_title('Summary')
            
            # Summary 시트 헤더 설정
            summary_headers = [
                ['기업 기본 정보', '', '', ''],
                ['항목', '값', '', ''],
                ['기업명', company_name, '', ''],
                ['종목코드', stock_code, '', ''],
                ['업종', industry, '', ''],
                ['최초 공시일', '', '', ''],
                ['최신 공시일', '', '', ''],
                ['총 보고서 수', '0', '', ''],
                ['', '', '', ''],
                ['최신 목표 현황', '', '', '', '', '', '', ''],
                ['영역', '카테고리', '항목', 'Core', '현재값', '목표값', '목표연도', '비고'],
            ]
            worksheet.update('A1:H11', summary_headers)
            
            # Target_History 시트 생성 (피벗 구조)
            ws_history = spreadsheet.add_worksheet(title='Target_History', rows=500, cols=50)
            
            # 헤더 행 설정 (A~G: 항목 정보, H~: 보고서별 데이터)
            header_row1 = ['', '', '', '', '', '', '', '접수번호']
            header_row2 = ['영역', '카테고리', '항목ID', '항목명', 'Core', '세부분류', 'Level', '보고서일']
            ws_history.update('A1:H1', [header_row1])
            ws_history.update('A2:H2', [header_row2])
            
            log(f"  → 시트 구조 초기화 완료 (Summary, Target_History 피벗)")
            
        except Exception as e:
            log(f"[ERROR] 시트 구조 초기화 실패: {e}")
    
    def get_or_create_company_sheet(self, company_name: str, stock_code: str, industry: str = "") -> Optional[gspread.Spreadsheet]:
        """
        기업별 스프레드시트 조회 또는 생성
        
        Args:
            company_name: 기업명
            stock_code: 종목코드
            industry: 업종
            
        Returns:
            gspread.Spreadsheet 객체 또는 None
        """
        if not self.gc or not self.drive_service:
            log("[WARN] 클라이언트 미초기화 - 기업별 시트 저장 건너뜀")
            return None
        
        # 1. 기존 스프레드시트 검색
        spreadsheet_id = self._find_company_spreadsheet(company_name, stock_code)
        
        # 2. 없으면 생성
        if not spreadsheet_id:
            log(f"기업 스프레드시트 생성 중: {company_name}_{stock_code}")
            spreadsheet_id = self._create_company_spreadsheet(company_name, stock_code, industry)
        else:
            log(f"기존 스프레드시트 발견: {company_name}_{stock_code}")
        
        if not spreadsheet_id:
            return None
        
        try:
            return self.gc.open_by_key(spreadsheet_id)
        except Exception as e:
            log(f"[ERROR] 스프레드시트 열기 실패: {e}")
            return None
    
    def get_company_sheet_url(self, company_name: str, stock_code: str) -> Optional[str]:
        """
        기업별 스프레드시트 URL 반환
        
        Args:
            company_name: 기업명
            stock_code: 종목코드
            
        Returns:
            스프레드시트 URL 또는 None
        """
        spreadsheet_id = self._find_company_spreadsheet(company_name, stock_code)
        if spreadsheet_id:
            return f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}"
        return None
    
    def add_analysis_result(
        self, 
        company_name: str, 
        stock_code: str, 
        acptno: str,
        report_date: str,
        analysis_result: Dict[str, Any],
        industry: str = ""
    ) -> Optional[str]:
        """
        분석 결과를 기업별 스프레드시트에 추가
        
        Args:
            company_name: 기업명
            stock_code: 종목코드
            acptno: 접수번호
            report_date: 보고서 제출일
            analysis_result: 분석 결과 딕셔너리
            industry: 업종
            
        Returns:
            스프레드시트 URL 또는 None
        """
        spreadsheet = self.get_or_create_company_sheet(company_name, stock_code, industry)
        if not spreadsheet:
            return None
        
        try:
            # 1. Target_History에 항목별 이력 추가
            self._add_to_target_history(spreadsheet, acptno, report_date, analysis_result)
            
            # 2. Summary 시트 업데이트
            self._update_summary(spreadsheet, report_date, analysis_result)
            
            log(f"  → 기업별 시트 저장 완료: {company_name}_{stock_code}")
            return spreadsheet.url
            
        except Exception as e:
            log(f"[ERROR] 분석 결과 저장 실패: {e}")
            import traceback
            log(f"  → {traceback.format_exc()[:500]}")
            return None
    
    def _add_to_target_history(
        self, 
        spreadsheet: gspread.Spreadsheet, 
        acptno: str, 
        report_date: str,
        analysis_result: Dict[str, Any]
    ):
        """
        Target_History 시트에 분석 결과 추가 (피벗 구조)
        
        구조:
        - 행: 항목 + 세부분류 (현재값, 목표값, 목표연도, 달성률, 전기대비)
        - 열: 보고서일별로 동적 확장
        
        Args:
            spreadsheet: 스프레드시트 객체
            acptno: 접수번호
            report_date: 보고서 제출일
            analysis_result: 분석 결과
        """
        ws = spreadsheet.worksheet('Target_History')
        
        # 1. 기존 데이터 읽기
        all_values = ws.get_all_values()
        
        if len(all_values) < 2:
            # 헤더가 없으면 초기화
            ws.update('A1:H2', [
                ['', '', '', '', '', '', '', '접수번호'],
                ['영역', '카테고리', '항목ID', '항목명', 'Core', '세부분류', 'Level', '보고서일']
            ])
            all_values = ws.get_all_values()
        
        # 2. 보고서 열 위치 찾기 또는 추가
        header_row1 = all_values[0] if len(all_values) > 0 else []
        header_row2 = all_values[1] if len(all_values) > 1 else []
        
        # H열(인덱스 7)부터 보고서 데이터
        report_col_idx = None
        for col_idx in range(7, len(header_row1)):
            if header_row1[col_idx] == acptno:
                report_col_idx = col_idx
                break
        
        if report_col_idx is None:
            # 새 열 추가
            report_col_idx = max(7, len(header_row1))
            col_letter = self._get_column_letter(report_col_idx + 1)
            ws.update_acell(f'{col_letter}1', acptno)
            ws.update_acell(f'{col_letter}2', report_date)
            log(f"    → 새 보고서 열 추가: {col_letter} ({report_date})")
        
        # 3. 기존 항목 행 매핑 (항목ID → 행 인덱스 딕셔너리)
        # 구조: {항목ID: {세부분류: 행번호}}
        item_row_map = {}
        for row_idx, row in enumerate(all_values[2:], start=3):  # 3행부터 데이터
            if len(row) >= 6:
                item_id = row[2]  # C열: 항목ID
                sub_type = row[5]  # F열: 세부분류
                if item_id:
                    if item_id not in item_row_map:
                        item_row_map[item_id] = {}
                    item_row_map[item_id][sub_type] = row_idx
        
        # 4. 분석 결과 기록
        analysis_items = analysis_result.get('analysis_items', {})
        sub_types = ['현재값', '목표값', '목표연도', '달성률', '전기대비']
        
        updates = []  # batch update용
        new_rows = []  # 새로 추가할 행들
        next_row = len(all_values) + 1
        
        for item_id, item_data in analysis_items.items():
            level = item_data.get('level', 0)
            if level == 0:
                continue
            
            # 값 매핑
            values = {
                '현재값': str(item_data.get('current_value', '')) if item_data.get('current_value') else '',
                '목표값': str(item_data.get('target_value', '')) if item_data.get('target_value') else '',
                '목표연도': str(item_data.get('target_year', '')) if item_data.get('target_year') else '',
                '달성률': '',  # 추후 계산
                '전기대비': ''  # 추후 계산
            }
            
            col_letter = self._get_column_letter(report_col_idx + 1)
            
            if item_id in item_row_map:
                # 기존 항목: 해당 열에 값 업데이트
                for sub_type in sub_types:
                    if sub_type in item_row_map[item_id]:
                        row_num = item_row_map[item_id][sub_type]
                        cell_addr = f'{col_letter}{row_num}'
                        updates.append({'range': cell_addr, 'values': [[values.get(sub_type, '')]]})
            else:
                # 새 항목: 행 그룹 추가
                for sub_type in sub_types:
                    new_row = [
                        item_data.get('area_name', ''),
                        item_data.get('category_name', ''),
                        item_id,
                        item_data.get('item_name', item_id),
                        'Y' if item_data.get('is_core', False) else '',
                        sub_type,
                        level,
                        ''  # 보고서일 열은 빈칸 (헤더에서 관리)
                    ]
                    # 해당 보고서 열에 값 추가
                    while len(new_row) <= report_col_idx:
                        new_row.append('')
                    new_row[report_col_idx] = values.get(sub_type, '')
                    new_rows.append(new_row)
        
        # 5. 업데이트 실행
        if updates:
            ws.batch_update(updates)
            log(f"    → 기존 항목 업데이트: {len(updates)}개 셀")
        
        if new_rows:
            # 새 행들 추가
            start_row = len(all_values) + 1
            end_row = start_row + len(new_rows) - 1
            end_col = max(8, report_col_idx + 1)
            range_str = f'A{start_row}:{self._get_column_letter(end_col)}{end_row}'
            ws.update(range_str, new_rows)
            log(f"    → 새 항목 추가: {len(new_rows)}개 행")
    
    def _get_column_letter(self, col_idx: int) -> str:
        """열 인덱스를 열 문자로 변환 (1=A, 2=B, ..., 27=AA)"""
        result = ""
        while col_idx > 0:
            col_idx, remainder = divmod(col_idx - 1, 26)
            result = chr(65 + remainder) + result
        return result
    
    def _update_summary(
        self, 
        spreadsheet: gspread.Spreadsheet, 
        report_date: str,
        analysis_result: Dict[str, Any]
    ):
        """Summary 시트 업데이트"""
        ws = spreadsheet.worksheet('Summary')
        
        # 최신 공시일 업데이트
        ws.update_acell('B7', report_date)
        
        # 최초 공시일 (비어있으면 설정)
        first_date = ws.acell('B6').value
        if not first_date:
            ws.update_acell('B6', report_date)
        
        # 총 보고서 수 증가
        try:
            current_count = int(ws.acell('B8').value or '0')
            ws.update_acell('B8', str(current_count + 1))
        except:
            ws.update_acell('B8', '1')
        
        # 최신 목표 현황 업데이트 (12행부터)
        analysis_items = analysis_result.get('analysis_items', {})
        
        rows_to_add = []
        for item_id, item_data in analysis_items.items():
            level = item_data.get('level', 0)
            if level == 0:
                continue
            
            row = [
                item_data.get('area_name', ''),
                item_data.get('category_name', ''),
                item_data.get('item_name', item_id),
                'Y' if item_data.get('is_core', False) else '',
                str(item_data.get('current_value', '')) if item_data.get('current_value') else '',
                str(item_data.get('target_value', '')) if item_data.get('target_value') else '',
                str(item_data.get('target_year', '')) if item_data.get('target_year') else '',
                item_data.get('note', '')
            ]
            rows_to_add.append(row)
        
        if rows_to_add:
            # 기존 목표 현황 삭제 후 새로 작성 (12행부터)
            start_row = 12
            end_row = start_row + len(rows_to_add) - 1
            range_str = f'A{start_row}:H{end_row}'
            ws.update(range_str, rows_to_add)


def main():
    """테스트용 메인 함수"""
    manager = CompanySheetManager()
    
    if not manager.gc:
        log("초기화 실패")
        return
    
    # 테스트: 폴더 생성/조회
    folder_id = manager._get_or_create_analysis_folder()
    log(f"Analysis 폴더 ID: {folder_id}")
    
    # 테스트: 스프레드시트 생성/조회
    spreadsheet = manager.get_or_create_company_sheet(
        company_name="테스트기업",
        stock_code="000000",
        industry="테스트업종"
    )
    
    if spreadsheet:
        log(f"스프레드시트 URL: {spreadsheet.url}")


if __name__ == "__main__":
    main()
