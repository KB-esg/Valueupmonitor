"""
Google Sheets 분석 관리자
밸류업 공시 분석 결과를 Google Sheets에 기록
"""

import os
import sys
import json
from typing import List, Dict, Optional, Any, Tuple
from datetime import datetime, timedelta

import gspread
from google.oauth2.service_account import Credentials

from framework_loader import Framework, FrameworkLoader

sys.stdout.reconfigure(line_buffering=True)


def log(message: str):
    """타임스탬프와 함께 로그 출력"""
    timestamp = datetime.now().strftime("%H:%M:%S")
    print(f"[{timestamp}] {message}", flush=True)
    sys.stdout.flush()
    sys.stderr.flush()


class GSheetAnalyzer:
    """Google Sheets 분석 관리자"""
    
    SCOPES = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive'
    ]
    
    # 시트 이름
    SHEET_DISCLOSURES = "밸류업공시목록"
    SHEET_FRAMEWORK = "Framework"
    SHEET_ANALYSIS = "밸류업공시분석"
    
    # 분석 시트 기본 헤더 (기본 정보)
    BASE_HEADERS = [
        '접수번호',      # A
        '회사명',        # B
        '종목코드',      # C
        '공시일자',      # D
        '분석일시',      # E
        '분석상태',      # F - pending, completed, error
        '언급항목수',    # G
        'Core언급수',    # H
        '주요포인트',    # I
    ]
    
    # 각 항목별 접미사
    ITEM_SUFFIXES = ['_level', '_current', '_target', '_year', '_note']
    
    def __init__(
        self, 
        credentials_json: Optional[str] = None, 
        spreadsheet_id: Optional[str] = None
    ):
        """
        초기화
        
        Args:
            credentials_json: 서비스 계정 JSON 문자열 또는 파일 경로
            spreadsheet_id: 스프레드시트 ID
        """
        self.spreadsheet_id = spreadsheet_id or os.environ.get('VALUEUP_GSPREAD_ID')
        self.client = None
        self.spreadsheet = None
        self._worksheet_cache = {}
        self.framework: Optional[Framework] = None
        
        # 인증 정보 로드
        creds = None
        if credentials_json:
            if os.path.isfile(credentials_json):
                creds = Credentials.from_service_account_file(credentials_json, scopes=self.SCOPES)
            else:
                info = json.loads(credentials_json)
                creds = Credentials.from_service_account_info(info, scopes=self.SCOPES)
        else:
            creds_json = os.environ.get('GOOGLE_SERVICE')
            if creds_json:
                info = json.loads(creds_json)
                creds = Credentials.from_service_account_info(info, scopes=self.SCOPES)
        
        if creds:
            self.client = gspread.authorize(creds)
            if self.spreadsheet_id:
                try:
                    self.spreadsheet = self.client.open_by_key(self.spreadsheet_id)
                    log(f"스프레드시트 연결 성공: {self.spreadsheet.title}")
                except gspread.exceptions.SpreadsheetNotFound:
                    log(f"[ERROR] 스프레드시트를 찾을 수 없습니다. ID: {self.spreadsheet_id}")
                except Exception as e:
                    log(f"[ERROR] 스프레드시트 열기 실패: {e}")
    
    def _get_worksheet(self, sheet_name: str) -> Optional[gspread.Worksheet]:
        """워크시트 가져오기 (캐시 사용)"""
        if not self.spreadsheet:
            return None
        
        if sheet_name in self._worksheet_cache:
            return self._worksheet_cache[sheet_name]
        
        try:
            worksheet = self.spreadsheet.worksheet(sheet_name)
            self._worksheet_cache[sheet_name] = worksheet
            return worksheet
        except gspread.exceptions.WorksheetNotFound:
            log(f"[WARN] 워크시트를 찾을 수 없습니다: {sheet_name}")
            return None
    
    def load_framework(self) -> Optional[Framework]:
        """Framework 시트에서 프레임워크 로드"""
        worksheet = self._get_worksheet(self.SHEET_FRAMEWORK)
        if not worksheet:
            return None
        
        try:
            records = worksheet.get_all_records()
            loader = FrameworkLoader()
            self.framework = loader.load_from_records(records)
            return self.framework
        except Exception as e:
            log(f"[ERROR] 프레임워크 로드 실패: {e}")
            return None
    
    def get_pending_disclosures(self, days: int = 7) -> List[Dict]:
        """
        분석 대기 중인 공시 목록 조회
        
        Args:
            days: 최근 N일간의 공시만 조회
            
        Returns:
            분석 대상 공시 리스트
        """
        # 공시 목록 시트
        disclosures_ws = self._get_worksheet(self.SHEET_DISCLOSURES)
        if not disclosures_ws:
            return []
        
        # 분석 시트에서 이미 분석된 접수번호 조회
        analyzed_acptnos = self._get_analyzed_acptnos()
        
        try:
            all_records = disclosures_ws.get_all_records()
            
            # 필터링: 최근 N일 + 분석 안됨 + 아티팩트링크 있음
            cutoff_date = datetime.now() - timedelta(days=days)
            pending = []
            
            for record in all_records:
                acptno = str(record.get('접수번호', '')).strip()
                if not acptno:
                    continue
                
                # 이미 분석됨
                if acptno in analyzed_acptnos:
                    continue
                
                # 공시일자 파싱
                date_str = str(record.get('공시일자', ''))
                try:
                    # 형식: "2025-12-26 09:11:00" 또는 "2025-12-26"
                    if ' ' in date_str:
                        disclosure_date = datetime.strptime(date_str.split(' ')[0], "%Y-%m-%d")
                    else:
                        disclosure_date = datetime.strptime(date_str[:10], "%Y-%m-%d")
                    
                    # 기간 필터
                    if disclosure_date < cutoff_date:
                        continue
                        
                except ValueError:
                    # 날짜 파싱 실패 시 포함
                    pass
                
                # 구글드라이브링크, 아티팩트링크, 원시PDF링크 중 하나 있어야 함
                gdrive_link = record.get('구글드라이브링크', '')
                artifact_link = record.get('아티팩트링크', '')
                raw_pdf_link = record.get('원시PDF링크', '')
                
                if not gdrive_link and not artifact_link and not raw_pdf_link:
                    continue
                
                pending.append(record)
            
            log(f"분석 대기 공시: {len(pending)}건 (전체 {len(all_records)}건 중)")
            return pending
            
        except Exception as e:
            log(f"[ERROR] 공시 목록 조회 실패: {e}")
            return []
    
    def _get_analyzed_acptnos(self) -> set:
        """이미 분석된 접수번호 집합 반환"""
        worksheet = self._get_worksheet(self.SHEET_ANALYSIS)
        if not worksheet:
            return set()
        
        try:
            # A열 (접수번호) 전체 조회
            acptno_col = worksheet.col_values(1)
            return set(acptno_col[1:])  # 헤더 제외
        except Exception as e:
            log(f"[WARN] 분석 완료 목록 조회 실패: {e}")
            return set()
    
    def _generate_headers(self) -> List[str]:
        """분석 시트 헤더 생성"""
        if not self.framework:
            return self.BASE_HEADERS.copy()
        
        headers = self.BASE_HEADERS.copy()
        
        # 각 항목별 컬럼 추가
        for item in self.framework.items:
            for suffix in self.ITEM_SUFFIXES:
                headers.append(f"{item.item_id}{suffix}")
        
        return headers
    
    def _get_or_create_analysis_sheet(self) -> Optional[gspread.Worksheet]:
        """분석 시트 가져오기 또는 생성"""
        if not self.spreadsheet:
            return None
        
        # 캐시 확인
        if self.SHEET_ANALYSIS in self._worksheet_cache:
            return self._worksheet_cache[self.SHEET_ANALYSIS]
        
        try:
            worksheet = self.spreadsheet.worksheet(self.SHEET_ANALYSIS)
            
            # 헤더 확인 및 업데이트
            expected_headers = self._generate_headers()
            current_headers = worksheet.row_values(1)
            
            if len(current_headers) < len(expected_headers):
                # 부족한 헤더 추가
                for i in range(len(current_headers), len(expected_headers)):
                    worksheet.update_cell(1, i + 1, expected_headers[i])
                log(f"  분석 시트 헤더 확장: {len(current_headers)} → {len(expected_headers)}")
            
            self._worksheet_cache[self.SHEET_ANALYSIS] = worksheet
            return worksheet
            
        except gspread.exceptions.WorksheetNotFound:
            # 새 시트 생성
            headers = self._generate_headers()
            
            worksheet = self.spreadsheet.add_worksheet(
                title=self.SHEET_ANALYSIS,
                rows=1000,
                cols=len(headers)
            )
            
            # 헤더 설정
            worksheet.update('A1', [headers])
            
            # 헤더 서식
            header_range = f"A1:{gspread.utils.rowcol_to_a1(1, len(headers))}"
            worksheet.format(header_range, {
                'textFormat': {'bold': True},
                'backgroundColor': {'red': 0.9, 'green': 0.9, 'blue': 0.9}
            })
            
            log(f"  분석 시트 생성 완료: {len(headers)}개 컬럼")
            self._worksheet_cache[self.SHEET_ANALYSIS] = worksheet
            return worksheet
    
    def save_analysis_result(
        self, 
        disclosure: Dict, 
        analysis_result: Dict,
        status: str = "completed"
    ) -> bool:
        """
        분석 결과 저장
        
        Args:
            disclosure: 원본 공시 정보
            analysis_result: Gemini 분석 결과
            status: 분석 상태 (completed, error)
            
        Returns:
            성공 여부
        """
        worksheet = self._get_or_create_analysis_sheet()
        if not worksheet or not self.framework:
            return False
        
        try:
            # 행 데이터 생성
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            # 분석 항목 데이터
            analysis_items = analysis_result.get('analysis_items', {})
            summary = analysis_result.get('summary', {})
            
            # 언급 항목 수 계산
            items_mentioned = sum(
                1 for item_id, data in analysis_items.items()
                if data.get('level', 0) > 0
            )
            core_mentioned = sum(
                1 for item in self.framework.core_items
                if analysis_items.get(item.item_id, {}).get('level', 0) > 0
            )
            
            # 주요 포인트
            highlights = summary.get('key_highlights', [])
            highlights_str = "; ".join(highlights[:3]) if highlights else ""
            
            # 기본 정보
            row = [
                str(disclosure.get('접수번호', '')),
                str(disclosure.get('회사명', '')),
                str(disclosure.get('종목코드', '')),
                str(disclosure.get('공시일자', '')),
                now,
                status,
                items_mentioned,
                core_mentioned,
                highlights_str
            ]
            
            # 각 항목별 데이터 추가
            for item in self.framework.items:
                item_data = analysis_items.get(item.item_id, {})
                row.extend([
                    item_data.get('level', 0),
                    self._format_value(item_data.get('current_value')),
                    self._format_value(item_data.get('target_value')),
                    self._format_value(item_data.get('target_year')),
                    str(item_data.get('note', ''))[:100]  # 100자 제한
                ])
            
            # 행 추가
            worksheet.append_row(row, value_input_option='USER_ENTERED')
            
            log(f"  분석 결과 저장 완료: {disclosure.get('회사명', '')}")
            return True
            
        except Exception as e:
            log(f"[ERROR] 분석 결과 저장 실패: {e}")
            return False
    
    def _format_value(self, value: Any) -> str:
        """값을 시트 저장용 문자열로 변환"""
        if value is None:
            return ""
        if isinstance(value, (int, float)):
            return str(value)
        return str(value)
    
    def save_error_result(self, disclosure: Dict, error_message: str) -> bool:
        """
        분석 오류 결과 저장
        
        Args:
            disclosure: 원본 공시 정보
            error_message: 오류 메시지
            
        Returns:
            성공 여부
        """
        empty_result = {
            'analysis_items': {},
            'summary': {'key_highlights': [f"오류: {error_message}"]}
        }
        return self.save_analysis_result(disclosure, empty_result, status="error")
    
    def get_analysis_summary(self) -> Dict[str, int]:
        """분석 현황 요약"""
        worksheet = self._get_worksheet(self.SHEET_ANALYSIS)
        if not worksheet:
            return {'total': 0, 'completed': 0, 'error': 0}
        
        try:
            status_col = worksheet.col_values(6)  # F열: 분석상태
            statuses = status_col[1:]  # 헤더 제외
            
            return {
                'total': len(statuses),
                'completed': statuses.count('completed'),
                'error': statuses.count('error')
            }
        except Exception as e:
            log(f"[WARN] 요약 조회 실패: {e}")
            return {'total': 0, 'completed': 0, 'error': 0}
    
    def update_estimated_tokens(self, acptno: str, estimated_tokens: int) -> bool:
        """
        밸류업공시목록 시트의 K열(예상토큰수) 업데이트
        
        Args:
            acptno: 접수번호
            estimated_tokens: 예상 토큰 수
            
        Returns:
            성공 여부
        """
        worksheet = self._get_worksheet(self.SHEET_DISCLOSURES)
        if not worksheet:
            return False
        
        try:
            # F열(접수번호)에서 해당 행 찾기
            acptno_col = worksheet.col_values(6)  # F열: 접수번호
            
            row_idx = None
            for i, val in enumerate(acptno_col):
                if str(val).strip() == str(acptno).strip():
                    row_idx = i + 1  # 1-based index
                    break
            
            if not row_idx:
                log(f"  [WARN] 접수번호 {acptno}를 찾을 수 없습니다.")
                return False
            
            # K열 헤더 확인 및 생성
            headers = worksheet.row_values(1)
            if len(headers) < 11:  # K열이 없으면 헤더 추가
                worksheet.update_cell(1, 11, '예상토큰수')
                log("  K열 '예상토큰수' 헤더 추가됨")
            
            # K열 업데이트
            worksheet.update_cell(row_idx, 11, estimated_tokens)
            return True
            
        except Exception as e:
            log(f"  [ERROR] 토큰 수 업데이트 실패: {e}")
            return False
    
    def batch_update_estimated_tokens(self, updates: List[Dict[str, Any]]) -> int:
        """
        여러 공시의 예상 토큰 수 일괄 업데이트
        
        Args:
            updates: [{'접수번호': str, '예상토큰수': int}, ...]
            
        Returns:
            업데이트 성공 건수
        """
        worksheet = self._get_worksheet(self.SHEET_DISCLOSURES)
        if not worksheet:
            log("  [ERROR] 밸류업공시목록 시트를 찾을 수 없습니다.")
            return 0
        
        try:
            # 현재 시트 크기 확인
            current_cols = worksheet.col_count
            log(f"  현재 시트 열 수: {current_cols}")
            
            # K열(11번째)이 필요하므로 열이 부족하면 확장
            if current_cols < 11:
                log(f"  시트 열 확장 중: {current_cols} → 15")
                worksheet.resize(cols=15)
            
            # K열 헤더 확인 및 생성
            headers = worksheet.row_values(1)
            log(f"  현재 헤더 수: {len(headers)}")
            
            # K열 헤더가 없거나 다른 값이면 설정
            if len(headers) < 11 or headers[10] != '예상토큰수':
                worksheet.update_cell(1, 11, '예상토큰수')
                log("  K열 '예상토큰수' 헤더 추가됨")
            
            # F열(접수번호) 전체 조회
            acptno_col = worksheet.col_values(6)
            log(f"  접수번호 데이터 행 수: {len(acptno_col)}")
            
            # 접수번호 → 행 번호 매핑
            acptno_to_row = {}
            for i, val in enumerate(acptno_col):
                if val:
                    acptno_to_row[str(val).strip()] = i + 1
            
            # 일괄 업데이트 준비
            batch_data = []
            for update in updates:
                acptno = str(update.get('접수번호', '')).strip()
                tokens = update.get('예상토큰수', 0)
                
                if acptno in acptno_to_row:
                    row_idx = acptno_to_row[acptno]
                    batch_data.append({
                        'range': f'K{row_idx}',
                        'values': [[tokens]]
                    })
                else:
                    log(f"  [WARN] 접수번호 {acptno}를 시트에서 찾을 수 없음")
            
            if batch_data:
                worksheet.batch_update(batch_data)
                log(f"  예상토큰수 일괄 업데이트: {len(batch_data)}건")
                return len(batch_data)
            
            return 0
            
        except Exception as e:
            log(f"  [ERROR] 일괄 토큰 수 업데이트 실패: {type(e).__name__}: {e}")
            import traceback
            log(f"  {traceback.format_exc()[:300]}")
            return 0
    
    def update_disclosure_analysis_meta(
        self, 
        acptno: str, 
        status: str,
        items_count: int,
        core_count: int,
        company_sheet_url: str = ""
    ) -> bool:
        """
        밸류업공시목록 시트의 L열부터 분석 메타정보 업데이트
        
        컬럼 구조 (L열부터):
        - L: 분석상태 (completed/error)
        - M: 분석일시
        - N: 분석항목수
        - O: Core항목수
        - P: 기업시트링크
        
        Args:
            acptno: 접수번호
            status: 분석상태
            items_count: 분석항목 수
            core_count: Core항목 수
            company_sheet_url: 기업별 시트 URL
            
        Returns:
            성공 여부
        """
        worksheet = self._get_worksheet(self.SHEET_DISCLOSURES)
        if not worksheet:
            return False
        
        try:
            # 접수번호로 행 찾기 (A열)
            acptno_col = worksheet.col_values(1)  # A열
            
            row_idx = None
            for i, val in enumerate(acptno_col):
                if str(val).strip() == str(acptno).strip():
                    row_idx = i + 1
                    break
            
            if not row_idx:
                log(f"  [WARN] 접수번호 {acptno}를 밸류업공시목록에서 찾을 수 없음")
                return False
            
            # L~P열 업데이트
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            update_data = [[status, now, items_count, core_count, company_sheet_url]]
            
            worksheet.update(f'L{row_idx}:P{row_idx}', update_data)
            log(f"    → 밸류업공시목록 메타정보 업데이트 완료 (L~P열)")
            return True
            
        except Exception as e:
            log(f"  [ERROR] 메타정보 업데이트 실패: {e}")
            return False
    
    def update_company_sheet_url(self, acptno: str, company_sheet_url: str) -> bool:
        """
        밸류업공시목록 시트의 P열에 기업시트링크 업데이트
        
        Args:
            acptno: 접수번호
            company_sheet_url: 기업별 시트 URL
            
        Returns:
            성공 여부
        """
        worksheet = self._get_worksheet(self.SHEET_DISCLOSURES)
        if not worksheet:
            return False
        
        try:
            # 접수번호로 행 찾기
            acptno_col = worksheet.col_values(1)
            
            row_idx = None
            for i, val in enumerate(acptno_col):
                if str(val).strip() == str(acptno).strip():
                    row_idx = i + 1
                    break
            
            if not row_idx:
                return False
            
            # P열 업데이트
            worksheet.update_acell(f'P{row_idx}', company_sheet_url)
            return True
            
        except Exception as e:
            log(f"  [ERROR] 기업시트링크 업데이트 실패: {e}")
            return False


def main():
    """테스트용 메인 함수"""
    analyzer = GSheetAnalyzer()
    
    if not analyzer.spreadsheet:
        log("스프레드시트 연결 실패")
        return
    
    # 프레임워크 로드
    framework = analyzer.load_framework()
    if framework:
        log(f"프레임워크 항목 수: {len(framework.items)}")
    
    # 분석 대기 목록
    pending = analyzer.get_pending_disclosures(days=7)
    for item in pending[:5]:
        log(f"  - {item.get('회사명')}: {item.get('접수번호')}")
    
    # 분석 현황
    summary = analyzer.get_analysis_summary()
    log(f"분석 현황: {summary}")


if __name__ == "__main__":
    main()
