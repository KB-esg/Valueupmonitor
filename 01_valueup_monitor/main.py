"""
KRX Value-Up 모니터 메인 실행 파일
매주 실행하여 새로운 밸류업 공시를 수집하고 Google Sheets에 기록
PDF는 로컬 저장 + Google Drive 업로드 (OAuth2 인증 시)
"""

import argparse
import asyncio
import os
import sys
import json
import re
from dataclasses import asdict
from typing import Optional
from datetime import datetime, timedelta

# stdout 버퍼링 해제 (GitHub Actions에서 실시간 출력)
sys.stdout.reconfigure(line_buffering=True)

from krx_valueup_crawler import KRXValueUpCrawler, DisclosureItem
from gsheet_manager import GSheetManager
from gdrive_uploader import GDriveUploader
from stock_code_mapper import StockCodeMapper


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
    
    # PDF 저장 폴더 (GitHub Actions 아티팩트로 업로드됨)
    PDF_OUTPUT_DIR = "Archive_pdf"
    
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
            skip_pdf: PDF 다운로드 건너뛰기
        """
        self.credentials_json = credentials_json or os.environ.get('GOOGLE_SERVICE')
        self.spreadsheet_id = spreadsheet_id or os.environ.get('VALUEUP_GSPREAD_ID')
        self.gdrive_folder_id = gdrive_folder_id or os.environ.get('VALUEUP_ARCHIVE_ID')
        self.period = period
        self.max_pages = max_pages
        self.skip_pdf = skip_pdf
        
        # period에 따른 effective_days 계산
        if period:
            period_to_days = {
                '1주': 7,
                '1개월': 30,
                '3개월': 90,
                '6개월': 180,
                '1년': 365,
                '2년': 730,
                '3년': 1095,
                '전체': 3650  # 약 10년
            }
            self.days = period_to_days.get(period, days)
        else:
            self.days = days
        
        # Google Sheets 초기화
        self.sheet_manager = GSheetManager(
            credentials_json=self.credentials_json,
            spreadsheet_id=self.spreadsheet_id
        )
        
        # Google Drive 초기화 (OAuth2 우선, 서비스 계정 fallback)
        self.drive_uploader = GDriveUploader(
            folder_id=self.gdrive_folder_id
        )
        
        # 연결 상태 확인
        self.sheet_ready = self.sheet_manager.spreadsheet is not None
        self.drive_ready = self.drive_uploader.service is not None
        
        # PDF 저장 폴더 생성
        os.makedirs(self.PDF_OUTPUT_DIR, exist_ok=True)
        
        # GitHub Actions 환경변수
        self.github_run_id = os.environ.get('GITHUB_RUN_ID', '')
        self.github_repository = os.environ.get('GITHUB_REPOSITORY', '')
    
    def _generate_artifact_info(self, filename: str) -> str:
        """
        GitHub Actions 아티팩트 정보 생성
        
        Args:
            filename: PDF 파일명
            
        Returns:
            아티팩트 정보 문자열 (다음 Action에서 조회 가능)
        """
        # 아티팩트 폴더/파일 경로
        artifact_path = f"Archive_pdf/{filename}"
        
        # GitHub Actions 환경인 경우 실행 정보 포함
        if self.github_run_id and self.github_repository:
            # 아티팩트 다운로드 URL (Actions 페이지)
            actions_url = f"https://github.com/{self.github_repository}/actions/runs/{self.github_run_id}"
            return f"{artifact_path}|run_id:{self.github_run_id}"
        
        return artifact_path
    
    async def run(self) -> dict:
        """
        메인 실행 로직
        
        Returns:
            실행 결과 딕셔너리
        """
        result = {
            'total_found': 0,
            'new_added': 0,
            'pdf_downloaded': 0,
            'pdf_uploaded': 0,
            'errors': []
        }
        
        log("=" * 60)
        log("KRX Value-Up 공시 모니터 시작")
        log("=" * 60)
        log(f"서비스 계정: {get_service_account_email()}")
        log(f"스프레드시트 ID: {self.spreadsheet_id}")
        log(f"드라이브 폴더 ID: {self.gdrive_folder_id or '(미설정)'}")
        log(f"PDF 로컬 저장: {self.PDF_OUTPUT_DIR}/")
        log(f"Google Sheets 연결: {'성공' if self.sheet_ready else '실패'}")
        log(f"Google Drive 연결: {'성공' if self.drive_ready else '실패'}")
        if self.drive_ready:
            log(f"  → 인증 방식: {self.drive_uploader.auth_method}")
        
        # 조회 옵션 출력
        if self.period:
            log(f"조회 기간: {self.period}")
        else:
            log(f"조회 기간: 최근 {self.days}일")
        log(f"최대 페이지: {self.max_pages}")
        log(f"PDF 다운로드: {'건너뜀' if self.skip_pdf else '활성화'}")
        
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
                
                # 2. 종목코드 채우기 (비어있는 경우)
                log("[2단계] 종목코드 조회 중...")
                stock_mapper = StockCodeMapper()
                
                for item in items:
                    if not item.종목코드:
                        code = stock_mapper.get_code(item.회사명)
                        if code:
                            item.종목코드 = code
                            log(f"  → {item.회사명} → {code}")
                        else:
                            log(f"  → {item.회사명} → (종목코드 없음)")
                
                # 3. Google Sheets에 새 항목 추가
                log("[3단계] Google Sheets에 공시 목록 기록 중...")
                
                disclosures = [asdict(item) for item in items]
                new_items = self.sheet_manager.append_disclosures(disclosures)
                new_count = len(new_items)
                result['new_added'] = new_count
                log(f"  → {new_count}건의 새로운 공시 추가됨")
                
                if new_count == 0:
                    log("  → 모든 공시가 이미 기록되어 있습니다.")
                
                # 4. PDF 다운로드 및 저장/업로드
                if self.skip_pdf:
                    log("[4단계] PDF 다운로드 건너뜀 (--skip-pdf 옵션)")
                else:
                    log("[4단계] PDF 다운로드 및 저장 중...")
                    
                    # 이번 크롤링에서 수집한 접수번호 집합 (문자열)
                    crawled_acptno_set = {str(item.접수번호).strip() for item in items}
                    
                    # 구글드라이브링크가 없는 항목 조회 (새 항목 + 기존 실패 항목)
                    all_pending = self.sheet_manager.get_items_without_gdrive_link()
                    
                    # 이번 크롤링 범위 내 항목만 필터링
                    # 접수번호 비교 시 문자열로 정규화 (시트에서 숫자로 저장된 경우 대비)
                    pending_items = []
                    for item in all_pending:
                        sheet_acptno = item.get('접수번호', '')
                        # 숫자인 경우 정수로 변환 후 문자열로 (지수 표기 방지)
                        if isinstance(sheet_acptno, (int, float)):
                            sheet_acptno = str(int(sheet_acptno))
                        else:
                            sheet_acptno = str(sheet_acptno).strip()
                        
                        if sheet_acptno in crawled_acptno_set:
                            pending_items.append(item)
                    
                    skipped = len(all_pending) - len(pending_items)
                    log(f"  → 처리 대상: {len(pending_items)}건 (범위 외 {skipped}건 제외)")
                    
                    if not pending_items:
                        log("  → 처리할 항목이 없습니다.")
                    
                    # 링크 업데이트 정보 수집 (배치용)
                    link_updates = []
                    
                    for idx, item in enumerate(pending_items, 1):
                        acptno = item.get('접수번호', '')
                        company = item.get('회사명', '')
                        date_str = item.get('공시일자', '').replace('-', '').replace(' ', '_').replace(':', '')
                        
                        log(f"  [{idx}/{len(pending_items)}] {company} ({acptno})...")
                        
                        # 공시 날짜 파싱 (월별 폴더용)
                        disclosure_date = None
                        try:
                            date_part = date_str[:8]  # YYYYMMDD
                            if len(date_part) == 8:
                                disclosure_date = datetime.strptime(date_part, "%Y%m%d")
                        except:
                            pass
                        
                        try:
                            # PDF 다운로드
                            pdf_data = await crawler.download_pdf(acptno)
                            
                            if pdf_data:
                                # 파일명 생성: 공시일자_회사명_접수번호.pdf
                                safe_company = re.sub(r'[^\w가-힣]', '', company)
                                filename = f"{date_str[:8]}_{safe_company}_{acptno}.pdf"
                                filepath = os.path.join(self.PDF_OUTPUT_DIR, filename)
                                
                                # 1) 로컬에 PDF 저장 (항상)
                                with open(filepath, 'wb') as f:
                                    f.write(pdf_data)
                                result['pdf_downloaded'] += 1
                                log(f"      → 로컬 저장: {filename} ({len(pdf_data):,} bytes)")
                                
                                # 2) Google Drive 업로드 (가능한 경우)
                                gdrive_link = None
                                if self.drive_ready:
                                    gdrive_link = self.drive_uploader.upload_pdf(
                                        pdf_data, 
                                        filename,
                                        use_monthly_folder=True,
                                        date=disclosure_date
                                    )
                                    if gdrive_link:
                                        result['pdf_uploaded'] += 1
                                        log(f"      → Drive 업로드: {gdrive_link}")
                                
                                # 3) 아티팩트 링크 정보 생성
                                artifact_info = self._generate_artifact_info(filename)
                                
                                # 링크 업데이트 정보 수집 (나중에 배치로 업데이트)
                                link_updates.append({
                                    '접수번호': acptno,
                                    '구글드라이브링크': gdrive_link or f"[로컬저장] {filename}",
                                    '아티팩트링크': artifact_info
                                })
                                
                            else:
                                result['errors'].append(f"PDF 다운로드 실패: {acptno}")
                                log(f"      → PDF 다운로드 실패")
                                
                        except Exception as e:
                            error_msg = f"{acptno}: {str(e)}"
                            result['errors'].append(error_msg)
                            log(f"      → 오류: {e}")
                        
                        # 요청 간 딜레이
                        await asyncio.sleep(2)
                    
                    # 5. 시트에 링크 배치 업데이트 (1회 API 호출)
                    if link_updates:
                        log("[5단계] 시트에 링크 정보 배치 업데이트...")
                        updated = self.sheet_manager.batch_update_links(link_updates)
                        log(f"  → {updated}건 업데이트 완료")
                
            except Exception as e:
                result['errors'].append(f"크롤링 오류: {str(e)}")
                log(f"오류 발생: {e}")
        
        # 결과 출력
        log("=" * 60)
        log("실행 결과 요약")
        log("=" * 60)
        log(f"  발견된 공시: {result['total_found']}건")
        log(f"  새로 추가됨: {result['new_added']}건")
        log(f"  PDF 로컬 저장: {result['pdf_downloaded']}건")
        log(f"  PDF Drive 업로드: {result['pdf_uploaded']}건")
        log(f"  저장 위치: {self.PDF_OUTPUT_DIR}/")
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
  
  # 전체 기간 아카이브
  python main.py --period 전체 --max-pages 20
  
  # 목록만 수집 (PDF 다운로드 건너뜀)
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
        help='PDF 다운로드 건너뛰기'
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
        log("")
        log("선택적 환경변수 (Google Drive 업로드용):")
        log("  - GDRIVE_REFRESH_TOKEN: OAuth2 리프레시 토큰")
        log("  - GDRIVE_CLIENT_ID: OAuth2 클라이언트 ID")
        log("  - GDRIVE_CLIENT_SECRET: OAuth2 클라이언트 시크릿")
        log("  - VALUEUP_ARCHIVE_ID: 업로드할 폴더 ID")
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
            f.write(f"pdf_downloaded={result['pdf_downloaded']}\n")
            f.write(f"pdf_uploaded={result['pdf_uploaded']}\n")


if __name__ == "__main__":
    asyncio.run(main())
