"""
KRX Value-Up 모니터 메인 실행 파일
매주 실행하여 새로운 밸류업 공시를 수집하고 Google Drive/Sheets에 저장
"""

import argparse
import asyncio
import os
import sys
import json
import re
from dataclasses import asdict
from typing import Optional
from datetime import datetime

# stdout 버퍼링 해제 (GitHub Actions에서 실시간 출력)
sys.stdout.reconfigure(line_buffering=True)

from krx_valueup_crawler import KRXValueUpCrawler, DisclosureItem
from gdrive_uploader import GDriveUploader
from gsheet_manager import GSheetManager


def log(message: str):
    """타임스탬프와 함께 로그 출력 (즉시 flush)"""
    timestamp = datetime.now().strftime("%H:%M:%S")
    print(f"[{timestamp}] {message}", flush=True)


def get_service_account_email() -> str:
    """서비스 계정 이메일 추출"""
    creds_json = os.environ.get('GOOGLE_SERVICE', '')
    if creds_json:
        try:
            info = json.loads(creds_json)
            return info.get('client_email', '(이메일 없음)')
        except json.JSONDecodeError:
            return '(JSON 파싱 실패)'
    return '(GOOGLE_SERVICE 미설정)'


class ValueUpMonitor:
    """밸류업 공시 모니터"""
    
    def __init__(
        self,
        credentials_json: Optional[str] = None,
        spreadsheet_id: Optional[str] = None,
        gdrive_folder_id: Optional[str] = None,
        days: int = 7,
        period: str = None,
        max_pages: int = 10,
        skip_pdf: bool = False
    ):
        """
        초기화
        
        Args:
            credentials_json: 서비스 계정 JSON
            spreadsheet_id: 스프레드시트 ID
            gdrive_folder_id: 구글드라이브 폴더 ID
            days: 조회할 기간(일), period가 None일 때 사용
            period: 기간 버튼 ('1주', '1개월', '3개월', '6개월', '1년', '전체')
            max_pages: 최대 크롤링 페이지 수
            skip_pdf: PDF 다운로드/업로드 건너뛰기
        """
        self.credentials_json = credentials_json or os.environ.get('GOOGLE_SERVICE')
        self.spreadsheet_id = spreadsheet_id or os.environ.get('VALUEUP_GSPREAD_ID')
        self.gdrive_folder_id = gdrive_folder_id or os.environ.get('VALUEUP_ARCHIVE_ID')
        self.days = days
        self.period = period
        self.max_pages = max_pages
        self.skip_pdf = skip_pdf
        
        # 컴포넌트 초기화
        self.sheet_manager = GSheetManager(
            credentials_json=self.credentials_json,
            spreadsheet_id=self.spreadsheet_id
        )
        self.drive_uploader = GDriveUploader(
            credentials_json=self.credentials_json,
            folder_id=self.gdrive_folder_id
        )
        
        # 연결 상태 확인
        self.sheet_ready = self.sheet_manager.spreadsheet is not None
        self.drive_ready = self.drive_uploader.service is not None
    
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
        
        log("=" * 60)
        log("KRX Value-Up 공시 모니터 시작")
        log("=" * 60)
        log(f"서비스 계정: {get_service_account_email()}")
        log(f"스프레드시트 ID: {self.spreadsheet_id}")
        log(f"드라이브 폴더 ID: {self.gdrive_folder_id}")
        log(f"Google Sheets 연결: {'성공' if self.sheet_ready else '실패'}")
        log(f"Google Drive 연결: {'성공' if self.drive_ready else '실패'}")
        
        # 조회 옵션 출력
        if self.period:
            log(f"조회 기간: {self.period}")
        else:
            log(f"조회 기간: 최근 {self.days}일")
        log(f"최대 페이지: {self.max_pages}")
        log(f"PDF 업로드: {'건너뜀' if self.skip_pdf else '활성화'}")
        
        if not self.sheet_ready:
            log("[오류] Google Sheets에 연결할 수 없습니다.")
            log("  → 서비스 계정에 스프레드시트 편집 권한이 있는지 확인하세요.")
            result['errors'].append("Google Sheets 연결 실패")
            return result
        
        # 1. KRX에서 공시 목록 크롤링
        if self.period:
            log(f"[1단계] KRX에서 '{self.period}' 기간 공시 목록 조회 중...")
        else:
            log(f"[1단계] KRX에서 최근 {self.days}일간 공시 목록 조회 중...")
        
        async with KRXValueUpCrawler(headless=True) as crawler:
            try:
                items = await crawler.get_disclosure_list(
                    days=self.days,
                    period=self.period,
                    max_pages=self.max_pages
                )
                result['total_found'] = len(items)
                log(f"  → 총 {len(items)}건의 공시 발견")
                
                if not items:
                    log("  → 새로운 공시가 없습니다.")
                    return result
                
                # 2. Google Sheets에 새 항목 추가
                log("[2단계] Google Sheets에 공시 목록 기록 중...")
                
                disclosures = [asdict(item) for item in items]
                new_count = self.sheet_manager.append_disclosures(disclosures)
                result['new_added'] = new_count
                log(f"  → {new_count}건의 새로운 공시 추가됨")
                
                if new_count == 0:
                    log("  → 모든 공시가 이미 기록되어 있습니다.")
                
                # 3. PDF 다운로드 및 Google Drive 업로드
                if self.skip_pdf:
                    log("[3단계] PDF 업로드 건너뜀 (--skip-pdf 옵션)")
                elif not self.drive_ready:
                    log("[3단계] Google Drive 연결 실패로 PDF 업로드 건너뜀")
                else:
                    log("[3단계] PDF 다운로드 및 Google Drive 업로드 중...")
                    
                    # 구글드라이브 링크가 없는 항목 조회
                    pending_items = self.sheet_manager.get_items_without_gdrive_link()
                    log(f"  → 업로드 대기 항목: {len(pending_items)}건")
                    
                    if not pending_items:
                        log("  → 모든 항목이 이미 업로드되어 있습니다.")
                    
                    for idx, item in enumerate(pending_items, 1):
                        acptno = item.get('접수번호', '')
                        company = item.get('회사명', '')
                        date = item.get('공시일자', '').replace('-', '').replace(' ', '_').replace(':', '')
                        
                        log(f"  [{idx}/{len(pending_items)}] {company} ({acptno})...")
                        
                        try:
                            # PDF 다운로드
                            pdf_data = await crawler.download_pdf(acptno)
                            
                            if pdf_data:
                                # 파일명 생성: 공시일자_회사명_접수번호.pdf
                                safe_company = re.sub(r'[^\w가-힣]', '', company)  # 특수문자 제거
                                filename = f"{date[:8]}_{safe_company}_{acptno}.pdf"
                                
                                # Google Drive에 업로드
                                gdrive_link = self.drive_uploader.upload_pdf(pdf_data, filename)
                                
                                if gdrive_link:
                                    # 시트에 링크 업데이트
                                    self.sheet_manager.update_gdrive_link(acptno, gdrive_link)
                                    result['pdf_uploaded'] += 1
                                    log(f"      → 업로드 완료: {gdrive_link}")
                                else:
                                    result['errors'].append(f"Drive 업로드 실패: {acptno}")
                                    log(f"      → Drive 업로드 실패")
                            else:
                                result['errors'].append(f"PDF 다운로드 실패: {acptno}")
                                log(f"      → PDF 다운로드 실패")
                                
                        except Exception as e:
                            error_msg = f"{acptno}: {str(e)}"
                            result['errors'].append(error_msg)
                            log(f"      → 오류: {e}")
                        
                        # 요청 간 딜레이
                        await asyncio.sleep(2)
                
            except Exception as e:
                result['errors'].append(f"크롤링 오류: {str(e)}")
                log(f"오류 발생: {e}")
        
        # 결과 출력
        log("=" * 60)
        log("실행 결과 요약")
        log("=" * 60)
        log(f"  발견된 공시: {result['total_found']}건")
        log(f"  새로 추가됨: {result['new_added']}건")
        log(f"  PDF 업로드: {result['pdf_uploaded']}건")
        if result['errors']:
            log(f"  오류: {len(result['errors'])}건")
            for err in result['errors']:
                log(f"    - {err}")
        log("=" * 60)
        
        return result


def parse_args():
    """CLI 인자 파싱"""
    parser = argparse.ArgumentParser(
        description='KRX Value-Up 공시 모니터',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
사용 예시:
  # 최근 7일 (기본값)
  python main.py
  
  # 최근 30일
  python main.py --days 30
  
  # 기간 버튼 사용 (3개월)
  python main.py --period 3개월
  
  # 전체 기간 아카이브 (PDF 업로드 포함)
  python main.py --period 전체 --max-pages 20
  
  # 목록만 수집 (PDF 업로드 건너뜀)
  python main.py --period 1년 --skip-pdf
        """
    )
    
    parser.add_argument(
        '--days', '-d',
        type=int,
        default=int(os.environ.get('VALUEUP_DAYS', '7')),
        help='조회할 기간(일), 기본값: 7'
    )
    
    parser.add_argument(
        '--period', '-p',
        type=str,
        choices=['1주', '1개월', '3개월', '6개월', '1년', '2년', '3년', '전체'],
        default=os.environ.get('VALUEUP_PERIOD'),
        help='기간 버튼 선택 (이 옵션 사용 시 --days 무시)'
    )
    
    parser.add_argument(
        '--max-pages', '-m',
        type=int,
        default=int(os.environ.get('VALUEUP_MAX_PAGES', '10')),
        help='최대 크롤링 페이지 수, 기본값: 10'
    )
    
    parser.add_argument(
        '--skip-pdf',
        action='store_true',
        default=os.environ.get('VALUEUP_SKIP_PDF', '').lower() == 'true',
        help='PDF 다운로드/업로드 건너뛰기'
    )
    
    return parser.parse_args()


async def main():
    """메인 함수"""
    args = parse_args()
    
    # 환경변수 확인
    required_env = ['GOOGLE_SERVICE', 'VALUEUP_GSPREAD_ID']
    missing = [e for e in required_env if not os.environ.get(e)]
    
    if missing:
        log(f"필수 환경변수가 설정되지 않았습니다: {', '.join(missing)}")
        log("")
        log("필요한 환경변수:")
        log("  - GOOGLE_SERVICE: 서비스 계정 JSON")
        log("  - VALUEUP_GSPREAD_ID: 스프레드시트 ID")
        log("  - VALUEUP_ARCHIVE_ID: (선택) 구글드라이브 폴더 ID")
        sys.exit(1)
    
    monitor = ValueUpMonitor(
        days=args.days,
        period=args.period,
        max_pages=args.max_pages,
        skip_pdf=args.skip_pdf
    )
    result = await monitor.run()
    
    # GitHub Actions 출력 설정
    if os.environ.get('GITHUB_OUTPUT'):
        with open(os.environ['GITHUB_OUTPUT'], 'a') as f:
            f.write(f"total_found={result['total_found']}\n")
            f.write(f"new_added={result['new_added']}\n")
            f.write(f"pdf_uploaded={result['pdf_uploaded']}\n")


if __name__ == "__main__":
    asyncio.run(main())
