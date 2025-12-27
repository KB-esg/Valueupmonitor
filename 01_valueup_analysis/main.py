"""
밸류업 공시 분석기 메인 실행 파일
Framework 기반으로 PDF를 분석하여 Google Sheets에 기록
"""

import argparse
import asyncio
import os
import sys
import json
from typing import Optional, Dict, Any
from datetime import datetime

# stdout 버퍼링 해제 (GitHub Actions에서 실시간 출력)
sys.stdout.reconfigure(line_buffering=True)

from gsheet_analyzer import GSheetAnalyzer
from pdf_extractor import PDFExtractor
from gemini_analyzer import GeminiAnalyzer
from framework_loader import Framework


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


class ValueUpAnalyzer:
    """밸류업 공시 분석기"""
    
    def __init__(
        self,
        credentials_json: Optional[str] = None,
        spreadsheet_id: Optional[str] = None,
        gemini_api_key: Optional[str] = None,
        days: int = 7,
        max_items: int = 10,
        dry_run: bool = False
    ):
        """
        초기화
        
        Args:
            credentials_json: 서비스 계정 JSON
            spreadsheet_id: 스프레드시트 ID
            gemini_api_key: Gemini API 키
            days: 분석할 공시 기간(일)
            max_items: 최대 분석 항목 수
            dry_run: 테스트 모드 (저장 안함)
        """
        self.credentials_json = credentials_json or os.environ.get('GOOGLE_SERVICE')
        self.spreadsheet_id = spreadsheet_id or os.environ.get('VALUEUP_GSPREAD_ID')
        self.gemini_api_key = gemini_api_key or os.environ.get('GEM_ANALYTIC')
        self.days = days
        self.max_items = max_items
        self.dry_run = dry_run
        
        # 컴포넌트 초기화
        self.sheet_analyzer = GSheetAnalyzer(
            credentials_json=self.credentials_json,
            spreadsheet_id=self.spreadsheet_id
        )
        
        self.pdf_extractor = PDFExtractor()
        
        self.gemini_analyzer = GeminiAnalyzer(
            api_key=self.gemini_api_key
        )
        
        # 연결 상태
        self.sheet_ready = self.sheet_analyzer.spreadsheet is not None
        self.gemini_ready = self.gemini_analyzer.model is not None
        
        # 프레임워크
        self.framework: Optional[Framework] = None
    
    def run(self) -> Dict[str, Any]:
        """
        메인 실행 로직
        
        Returns:
            실행 결과 딕셔너리
        """
        result = {
            'total_pending': 0,
            'analyzed': 0,
            'errors': 0,
            'error_details': []
        }
        
        log("=" * 60)
        log("밸류업 공시 분석기 시작")
        log("=" * 60)
        log(f"서비스 계정: {get_service_account_email()}")
        log(f"스프레드시트 ID: {self.spreadsheet_id}")
        log(f"Gemini API: {'설정됨' if self.gemini_api_key else '미설정'}")
        log(f"분석 기간: 최근 {self.days}일")
        log(f"최대 분석 수: {self.max_items}건")
        log(f"테스트 모드: {'예' if self.dry_run else '아니오'}")
        log(f"Google Sheets 연결: {'성공' if self.sheet_ready else '실패'}")
        log(f"Gemini 모델 연결: {'성공' if self.gemini_ready else '실패'}")
        
        if not self.sheet_ready:
            log("[오류] Google Sheets에 연결할 수 없습니다.")
            result['error_details'].append("Google Sheets 연결 실패")
            return result
        
        if not self.gemini_ready:
            log("[오류] Gemini API에 연결할 수 없습니다.")
            result['error_details'].append("Gemini API 연결 실패")
            return result
        
        # 1. 프레임워크 로드
        log("")
        log("[1단계] 프레임워크 로드 중...")
        self.framework = self.sheet_analyzer.load_framework()
        
        if not self.framework:
            log("[오류] 프레임워크를 로드할 수 없습니다.")
            result['error_details'].append("프레임워크 로드 실패")
            return result
        
        log(f"  → 프레임워크 버전: {self.framework.version}")
        log(f"  → 총 항목: {len(self.framework.items)}개")
        log(f"  → Core 항목: {len(self.framework.core_items)}개")
        
        # 2. 분석 대기 공시 조회
        log("")
        log("[2단계] 분석 대기 공시 조회 중...")
        pending_disclosures = self.sheet_analyzer.get_pending_disclosures(days=self.days)
        result['total_pending'] = len(pending_disclosures)
        
        if not pending_disclosures:
            log("  → 분석 대기 중인 공시가 없습니다.")
            return result
        
        log(f"  → {len(pending_disclosures)}건의 공시 발견")
        
        # 최대 분석 수 제한
        items_to_analyze = pending_disclosures[:self.max_items]
        if len(pending_disclosures) > self.max_items:
            log(f"  → {self.max_items}건만 분석 (나머지는 다음 실행에서)")
        
        # 3. 각 공시 분석
        log("")
        log("[3단계] 공시 분석 시작...")
        
        for idx, disclosure in enumerate(items_to_analyze, 1):
            acptno = disclosure.get('접수번호', '')
            company = disclosure.get('회사명', '')
            
            log("")
            log(f"[{idx}/{len(items_to_analyze)}] {company} ({acptno})")
            
            try:
                # 3-1. PDF 다운로드 및 텍스트 추출
                log("  PDF 다운로드 중...")
                pdf_bytes, pdf_text = self.pdf_extractor.get_pdf_and_text(acptno)
                
                if not pdf_text:
                    log("  [WARN] PDF 텍스트 추출 실패")
                    if not self.dry_run:
                        self.sheet_analyzer.save_error_result(disclosure, "PDF 텍스트 추출 실패")
                    result['errors'] += 1
                    result['error_details'].append(f"{company}: PDF 추출 실패")
                    continue
                
                log(f"  텍스트 추출 완료: {len(pdf_text):,}자")
                
                # 3-2. Gemini 분석
                log("  Gemini 분석 중...")
                analysis_result = self.gemini_analyzer.analyze(
                    pdf_text=pdf_text,
                    company_name=company,
                    framework=self.framework
                )
                
                if not analysis_result:
                    log("  [WARN] Gemini 분석 실패")
                    if not self.dry_run:
                        self.sheet_analyzer.save_error_result(disclosure, "Gemini 분석 실패")
                    result['errors'] += 1
                    result['error_details'].append(f"{company}: Gemini 분석 실패")
                    continue
                
                # 분석 결과 요약 출력
                items = analysis_result.get('analysis_items', {})
                mentioned = sum(1 for d in items.values() if d.get('level', 0) > 0)
                quantitative = sum(1 for d in items.values() if d.get('level', 0) == 2)
                
                log(f"  분석 완료: {mentioned}개 항목 언급 ({quantitative}개 정량적)")
                
                # 3-3. 결과 저장
                if self.dry_run:
                    log("  [DRY-RUN] 저장 건너뜀")
                else:
                    log("  결과 저장 중...")
                    success = self.sheet_analyzer.save_analysis_result(
                        disclosure=disclosure,
                        analysis_result=analysis_result,
                        status="completed"
                    )
                    
                    if success:
                        result['analyzed'] += 1
                    else:
                        result['errors'] += 1
                        result['error_details'].append(f"{company}: 저장 실패")
                
                # API 호출 간 딜레이
                import time
                time.sleep(2)
                
            except Exception as e:
                log(f"  [ERROR] 예외 발생: {e}")
                if not self.dry_run:
                    self.sheet_analyzer.save_error_result(disclosure, str(e))
                result['errors'] += 1
                result['error_details'].append(f"{company}: {str(e)[:50]}")
        
        # 결과 출력
        log("")
        log("=" * 60)
        log("실행 결과 요약")
        log("=" * 60)
        log(f"  분석 대기: {result['total_pending']}건")
        log(f"  분석 완료: {result['analyzed']}건")
        log(f"  오류: {result['errors']}건")
        
        if result['error_details']:
            log("  오류 상세:")
            for err in result['error_details'][:5]:
                log(f"    - {err}")
            if len(result['error_details']) > 5:
                log(f"    ... 외 {len(result['error_details']) - 5}건")
        
        # 전체 분석 현황
        summary = self.sheet_analyzer.get_analysis_summary()
        log("")
        log(f"전체 분석 현황: 완료 {summary['completed']}건, 오류 {summary['error']}건")
        log("=" * 60)
        
        return result


def parse_args():
    """CLI 인자 파싱"""
    parser = argparse.ArgumentParser(
        description='밸류업 공시 분석기',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
사용 예시:
  # 최근 7일 공시 분석 (기본값)
  python main.py
  
  # 최근 30일 공시 분석
  python main.py --days 30
  
  # 최대 5건만 분석
  python main.py --max-items 5
  
  # 테스트 모드 (저장 안함)
  python main.py --dry-run
        """
    )
    
    parser.add_argument(
        '--days', '-d',
        type=int,
        default=int(os.environ.get('ANALYSIS_DAYS', '7')),
        help='분석할 공시 기간(일), 기본값: 7'
    )
    
    parser.add_argument(
        '--max-items', '-m',
        type=int,
        default=int(os.environ.get('ANALYSIS_MAX_ITEMS', '10')),
        help='최대 분석 항목 수, 기본값: 10'
    )
    
    parser.add_argument(
        '--dry-run',
        action='store_true',
        default=os.environ.get('ANALYSIS_DRY_RUN', '').lower() == 'true',
        help='테스트 모드 (결과 저장 안함)'
    )
    
    return parser.parse_args()


def main():
    """메인 함수"""
    args = parse_args()
    
    # 환경변수 확인
    required_env = ['GOOGLE_SERVICE', 'VALUEUP_GSPREAD_ID', 'GEM_ANALYTIC']
    missing = [e for e in required_env if not os.environ.get(e)]
    
    if missing:
        log(f"필수 환경변수가 설정되지 않았습니다: {', '.join(missing)}")
        log("")
        log("필요한 환경변수:")
        log("  - GOOGLE_SERVICE: 서비스 계정 JSON")
        log("  - VALUEUP_GSPREAD_ID: 스프레드시트 ID")
        log("  - GEM_ANALYTIC: Gemini API 키")
        sys.exit(1)
    
    analyzer = ValueUpAnalyzer(
        days=args.days,
        max_items=args.max_items,
        dry_run=args.dry_run
    )
    
    result = analyzer.run()
    
    # GitHub Actions 출력 설정
    if os.environ.get('GITHUB_OUTPUT'):
        with open(os.environ['GITHUB_OUTPUT'], 'a') as f:
            f.write(f"total_pending={result['total_pending']}\n")
            f.write(f"analyzed={result['analyzed']}\n")
            f.write(f"errors={result['errors']}\n")
    
    # 오류 발생 시 종료 코드 1
    if result['errors'] > 0 and result['analyzed'] == 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
