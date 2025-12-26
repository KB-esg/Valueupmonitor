"""
KRX Value-Up 모니터 메인 실행 파일
매주 실행하여 새로운 밸류업 공시를 수집하고 Google Drive/Sheets에 저장
"""

import asyncio
import os
import sys
from dataclasses import asdict
from typing import Optional

from krx_valueup_crawler import KRXValueUpCrawler, DisclosureItem
from gdrive_uploader import GDriveUploader
from gsheet_manager import GSheetManager


class ValueUpMonitor:
    """밸류업 공시 모니터"""
    
    def __init__(
        self,
        credentials_json: Optional[str] = None,
        spreadsheet_id: Optional[str] = None,
        gdrive_folder_id: Optional[str] = None,
        days: int = 7
    ):
        """
        초기화
        
        Args:
            credentials_json: 서비스 계정 JSON
            spreadsheet_id: 스프레드시트 ID
            gdrive_folder_id: 구글드라이브 폴더 ID
            days: 조회할 기간(일)
        """
        self.credentials_json = credentials_json or os.environ.get('GOOGLE_CREDENTIALS')
        self.spreadsheet_id = spreadsheet_id or os.environ.get('GSHEET_SPREADSHEET_ID')
        self.gdrive_folder_id = gdrive_folder_id or os.environ.get('GDRIVE_FOLDER_ID')
        self.days = days
        
        # 컴포넌트 초기화
        self.sheet_manager = GSheetManager(
            credentials_json=self.credentials_json,
            spreadsheet_id=self.spreadsheet_id
        )
        self.drive_uploader = GDriveUploader(
            credentials_json=self.credentials_json,
            folder_id=self.gdrive_folder_id
        )
    
    async def run(self) -> dict:
        """
        메인 실행 로직
        
        Returns:
            실행 결과 딕셔너리
        """
        result = {
            'total_found': 0,
            'new_added': 0,
            'pdf_uploaded': 0,
            'errors': []
        }
        
        print("=" * 60)
        print("KRX Value-Up 공시 모니터 시작")
        print("=" * 60)
        
        # 1. KRX에서 공시 목록 크롤링
        print(f"\n[1단계] KRX에서 최근 {self.days}일간 공시 목록 조회 중...")
        
        async with KRXValueUpCrawler(headless=True) as crawler:
            try:
                items = await crawler.get_disclosure_list(days=self.days)
                result['total_found'] = len(items)
                print(f"  → 총 {len(items)}건의 공시 발견")
                
                if not items:
                    print("  → 새로운 공시가 없습니다.")
                    return result
                
                # 2. Google Sheets에 새 항목 추가
                print("\n[2단계] Google Sheets에 공시 목록 기록 중...")
                
                disclosures = [asdict(item) for item in items]
                new_count = self.sheet_manager.append_disclosures(disclosures)
                result['new_added'] = new_count
                print(f"  → {new_count}건의 새로운 공시 추가됨")
                
                if new_count == 0:
                    print("  → 모든 공시가 이미 기록되어 있습니다.")
                    return result
                
                # 3. PDF 다운로드 및 Google Drive 업로드
                print("\n[3단계] PDF 다운로드 및 Google Drive 업로드 중...")
                
                # 구글드라이브 링크가 없는 항목 조회
                pending_items = self.sheet_manager.get_items_without_gdrive_link()
                print(f"  → 업로드 대기 항목: {len(pending_items)}건")
                
                for idx, item in enumerate(pending_items, 1):
                    acptno = item.get('접수번호', '')
                    company = item.get('회사명', '')
                    date = item.get('공시일자', '').replace('-', '')
                    
                    print(f"  [{idx}/{len(pending_items)}] {company} ({acptno})...")
                    
                    try:
                        # PDF 다운로드
                        pdf_data = await crawler.download_pdf(acptno)
                        
                        if pdf_data:
                            # 파일명 생성: 공시일자_회사명_접수번호.pdf
                            filename = f"{date}_{company}_{acptno}.pdf"
                            
                            # Google Drive에 업로드
                            gdrive_link = self.drive_uploader.upload_pdf(pdf_data, filename)
                            
                            if gdrive_link:
                                # 시트에 링크 업데이트
                                self.sheet_manager.update_gdrive_link(acptno, gdrive_link)
                                result['pdf_uploaded'] += 1
                                print(f"      → 업로드 완료: {gdrive_link}")
                            else:
                                result['errors'].append(f"Drive 업로드 실패: {acptno}")
                        else:
                            result['errors'].append(f"PDF 다운로드 실패: {acptno}")
                            
                    except Exception as e:
                        error_msg = f"{acptno}: {str(e)}"
                        result['errors'].append(error_msg)
                        print(f"      → 오류: {e}")
                    
                    # 요청 간 딜레이
                    await asyncio.sleep(2)
                
            except Exception as e:
                result['errors'].append(f"크롤링 오류: {str(e)}")
                print(f"오류 발생: {e}")
        
        # 결과 출력
        print("\n" + "=" * 60)
        print("실행 결과 요약")
        print("=" * 60)
        print(f"  발견된 공시: {result['total_found']}건")
        print(f"  새로 추가됨: {result['new_added']}건")
        print(f"  PDF 업로드: {result['pdf_uploaded']}건")
        if result['errors']:
            print(f"  오류: {len(result['errors'])}건")
            for err in result['errors']:
                print(f"    - {err}")
        print("=" * 60)
        
        return result


async def main():
    """메인 함수"""
    # 환경변수 확인
    required_env = ['GOOGLE_CREDENTIALS', 'GSHEET_SPREADSHEET_ID']
    missing = [e for e in required_env if not os.environ.get(e)]
    
    if missing:
        print(f"필수 환경변수가 설정되지 않았습니다: {', '.join(missing)}")
        print("\n필요한 환경변수:")
        print("  - GOOGLE_CREDENTIALS: 서비스 계정 JSON")
        print("  - GSHEET_SPREADSHEET_ID: 스프레드시트 ID")
        print("  - GDRIVE_FOLDER_ID: (선택) 구글드라이브 폴더 ID")
        sys.exit(1)
    
    # 조회 기간 (기본 7일)
    days = int(os.environ.get('VALUEUP_DAYS', '7'))
    
    monitor = ValueUpMonitor(days=days)
    result = await monitor.run()
    
    # GitHub Actions 출력 설정
    if os.environ.get('GITHUB_OUTPUT'):
        with open(os.environ['GITHUB_OUTPUT'], 'a') as f:
            f.write(f"total_found={result['total_found']}\n")
            f.write(f"new_added={result['new_added']}\n")
            f.write(f"pdf_uploaded={result['pdf_uploaded']}\n")


if __name__ == "__main__":
    asyncio.run(main())
