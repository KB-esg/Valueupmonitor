"""
Google Sheets 관리자
밸류업 공시 목록을 Google Sheets에 기록
"""

import os
from typing import List, Optional, Dict
from datetime import datetime
import gspread
from google.oauth2.service_account import Credentials


class GSheetManager:
    """Google Sheets 관리자"""
    
    SCOPES = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive'
    ]
    
    # 시트 헤더 정의
    HEADERS = [
        '번호',
        '공시일자',
        '회사명',
        '종목코드',
        '공시제목',
        '접수번호',
        '원시PDF링크',
        '구글드라이브링크',
        '수집일시'
    ]
    
    def __init__(self, credentials_json: Optional[str] = None, spreadsheet_id: Optional[str] = None):
        """
        초기화
        
        Args:
            credentials_json: 서비스 계정 JSON 파일 경로 또는 JSON 문자열
            spreadsheet_id: 스프레드시트 ID
        """
        self.spreadsheet_id = spreadsheet_id or os.environ.get('GSHEET_SPREADSHEET_ID')
        self.client = None
        self.spreadsheet = None
        
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
            creds_json = os.environ.get('GOOGLE_CREDENTIALS')
            if creds_json:
                import json
                info = json.loads(creds_json)
                creds = Credentials.from_service_account_info(info, scopes=self.SCOPES)
        
        if creds:
            self.client = gspread.authorize(creds)
            if self.spreadsheet_id:
                try:
                    self.spreadsheet = self.client.open_by_key(self.spreadsheet_id)
                except Exception as e:
                    print(f"스프레드시트 열기 실패: {e}")
    
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
        
        try:
            worksheet = self.spreadsheet.worksheet(sheet_name)
        except gspread.exceptions.WorksheetNotFound:
            # 새 시트 생성
            worksheet = self.spreadsheet.add_worksheet(
                title=sheet_name,
                rows=1000,
                cols=len(self.HEADERS)
            )
            # 헤더 설정
            worksheet.update('A1', [self.HEADERS])
            # 헤더 서식 설정 (굵게)
            worksheet.format('A1:I1', {
                'textFormat': {'bold': True},
                'backgroundColor': {'red': 0.9, 'green': 0.9, 'blue': 0.9}
            })
        
        return worksheet
    
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
            acptno_col = worksheet.col_values(6)  # 6번째 열 = 접수번호
            return set(acptno_col[1:])  # 헤더 제외
        except Exception as e:
            print(f"접수번호 조회 중 오류: {e}")
            return set()
    
    def append_disclosures(self, disclosures: List[Dict], sheet_name: str = "밸류업공시목록") -> int:
        """
        공시 목록 추가
        
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
            print("새로운 공시 항목이 없습니다.")
            return 0
        
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
                now
            ]
            rows.append(row)
        
        # 배치로 추가
        try:
            worksheet.append_rows(rows, value_input_option='USER_ENTERED')
            print(f"{len(rows)}건의 새로운 공시 추가 완료")
            return len(rows)
        except Exception as e:
            print(f"행 추가 중 오류: {e}")
            return 0
    
    def update_gdrive_link(self, acptno: str, gdrive_link: str, sheet_name: str = "밸류업공시목록") -> bool:
        """
        구글드라이브 링크 업데이트
        
        Args:
            acptno: 접수번호
            gdrive_link: 구글드라이브 링크
            sheet_name: 시트 이름
            
        Returns:
            성공 여부
        """
        worksheet = self.get_or_create_worksheet(sheet_name)
        if not worksheet:
            return False
        
        try:
            # 접수번호로 행 찾기
            cell = worksheet.find(acptno, in_column=6)  # F열
            if cell:
                # H열 (구글드라이브링크) 업데이트
                worksheet.update_cell(cell.row, 8, gdrive_link)
                return True
        except Exception as e:
            print(f"링크 업데이트 중 오류: {e}")
        
        return False
    
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
            print(f"항목 조회 중 오류: {e}")
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
    print(f"추가된 항목: {count}건")


if __name__ == "__main__":
    main()
