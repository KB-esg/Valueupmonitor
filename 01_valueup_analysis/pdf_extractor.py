"""
PDF 추출기
구글 드라이브에서 PDF 다운로드 및 텍스트 추출
OAuth2 인증 우선, 서비스 계정 fallback

환경변수:
- OAuth2 방식 (개인 드라이브 접근):
  - GDRIVE_REFRESH_TOKEN: 리프레시 토큰
  - GDRIVE_CLIENT_ID: OAuth 클라이언트 ID
  - GDRIVE_CLIENT_SECRET: OAuth 클라이언트 시크릿
  
- 서비스 계정 방식 (fallback):
  - GOOGLE_SERVICE: 서비스 계정 JSON
"""

import os
import sys
import re
import io
import json
import tempfile
from typing import Optional, Tuple
from datetime import datetime

# PDF 텍스트 추출
try:
    import pdfplumber
    HAS_PDFPLUMBER = True
except ImportError:
    HAS_PDFPLUMBER = False

try:
    from PyPDF2 import PdfReader
    HAS_PYPDF2 = True
except ImportError:
    HAS_PYPDF2 = False

# Google Drive API
try:
    from googleapiclient.discovery import build
    from googleapiclient.http import MediaIoBaseDownload
    HAS_GOOGLE_DRIVE = True
except ImportError:
    HAS_GOOGLE_DRIVE = False

sys.stdout.reconfigure(line_buffering=True)


def log(message: str):
    """타임스탬프와 함께 로그 출력"""
    timestamp = datetime.now().strftime("%H:%M:%S")
    print(f"[{timestamp}] {message}", flush=True)
    sys.stdout.flush()
    sys.stderr.flush()


class PDFExtractor:
    """PDF 다운로드 및 텍스트 추출 (OAuth2 + 서비스 계정 지원)"""
    
    SCOPES = [
        'https://www.googleapis.com/auth/drive.readonly',
        'https://www.googleapis.com/auth/drive'
    ]
    
    def __init__(
        self,
        # OAuth2 인증 정보
        refresh_token: Optional[str] = None,
        client_id: Optional[str] = None,
        client_secret: Optional[str] = None,
        # 서비스 계정 인증 정보 (fallback)
        credentials_json: Optional[str] = None,
        temp_dir: Optional[str] = None
    ):
        """
        초기화
        
        Args:
            refresh_token: OAuth2 리프레시 토큰
            client_id: OAuth2 클라이언트 ID
            client_secret: OAuth2 클라이언트 시크릿
            credentials_json: 서비스 계정 JSON 문자열 또는 파일 경로
            temp_dir: 임시 파일 저장 디렉토리 (기본값: 시스템 임시 디렉토리)
        """
        self.temp_dir = temp_dir or tempfile.gettempdir()
        self.drive_service = None
        self.auth_method = None
        
        # PDF 추출 라이브러리 확인
        if HAS_PDFPLUMBER:
            log("PDF 추출 라이브러리: pdfplumber")
        elif HAS_PYPDF2:
            log("PDF 추출 라이브러리: PyPDF2")
        else:
            log("[WARN] PDF 추출 라이브러리가 없습니다.")
        
        if not HAS_GOOGLE_DRIVE:
            log("[WARN] Google Drive API 라이브러리가 없습니다.")
            return
        
        # 환경변수에서 OAuth2 인증 정보 로드
        refresh_token = refresh_token or os.environ.get('GDRIVE_REFRESH_TOKEN')
        client_id = client_id or os.environ.get('GDRIVE_CLIENT_ID')
        client_secret = client_secret or os.environ.get('GDRIVE_CLIENT_SECRET')
        
        # 1. OAuth2 인증 시도 (우선)
        if refresh_token and client_id and client_secret:
            try:
                self._init_oauth2(refresh_token, client_id, client_secret)
                self.auth_method = 'OAuth2'
                log("Google Drive 인증: OAuth2 (개인 계정)")
            except Exception as e:
                log(f"[WARN] OAuth2 인증 실패: {e}")
        
        # 2. 서비스 계정 인증 (fallback)
        if not self.drive_service:
            credentials_json = credentials_json or os.environ.get('GOOGLE_SERVICE')
            if credentials_json:
                try:
                    self._init_service_account(credentials_json)
                    self.auth_method = 'ServiceAccount'
                    log("Google Drive 인증: 서비스 계정")
                except Exception as e:
                    log(f"[WARN] 서비스 계정 인증 실패: {e}")
        
        if self.drive_service:
            log(f"Google Drive API 초기화 완료 ({self.auth_method})")
        else:
            log("[ERROR] Google Drive API 초기화 실패")
    
    def _init_oauth2(self, refresh_token: str, client_id: str, client_secret: str):
        """OAuth2 인증 초기화"""
        from google.oauth2.credentials import Credentials
        from google.auth.transport.requests import Request
        
        creds = Credentials(
            token=None,
            refresh_token=refresh_token,
            client_id=client_id,
            client_secret=client_secret,
            token_uri='https://oauth2.googleapis.com/token'
        )
        
        # 액세스 토큰 갱신
        creds.refresh(Request())
        
        self.drive_service = build('drive', 'v3', credentials=creds)
    
    def _init_service_account(self, credentials_json: str):
        """서비스 계정 인증 초기화"""
        from google.oauth2.service_account import Credentials
        
        if os.path.isfile(credentials_json):
            creds = Credentials.from_service_account_file(credentials_json, scopes=self.SCOPES)
        else:
            info = json.loads(credentials_json)
            creds = Credentials.from_service_account_info(info, scopes=self.SCOPES)
        
        self.drive_service = build('drive', 'v3', credentials=creds)
    
    def extract_file_id_from_url(self, gdrive_url: str) -> Optional[str]:
        """
        구글 드라이브 URL에서 파일 ID 추출
        
        Args:
            gdrive_url: 구글 드라이브 공유 링크
            
        Returns:
            파일 ID 또는 None
        """
        if not gdrive_url:
            return None
        
        # 패턴들: 
        # https://drive.google.com/file/d/FILE_ID/view?usp=drivesdk
        # https://drive.google.com/open?id=FILE_ID
        patterns = [
            r'/file/d/([a-zA-Z0-9_-]+)',
            r'id=([a-zA-Z0-9_-]+)',
            r'/d/([a-zA-Z0-9_-]+)'
        ]
        
        for pattern in patterns:
            match = re.search(pattern, gdrive_url)
            if match:
                return match.group(1)
        
        return None
    
    def download_pdf_from_gdrive(self, file_id: str) -> Optional[bytes]:
        """
        구글 드라이브에서 PDF 다운로드
        
        Args:
            file_id: 파일 ID
            
        Returns:
            PDF 바이트 데이터 또는 None
        """
        if not self.drive_service:
            log("  [ERROR] Google Drive 서비스가 초기화되지 않았습니다.")
            return None
        
        try:
            # 파일 메타데이터 확인
            file_metadata = self.drive_service.files().get(
                fileId=file_id,
                fields='name,mimeType,size'
            ).execute()
            
            file_name = file_metadata.get('name', 'unknown')
            mime_type = file_metadata.get('mimeType', '')
            file_size = file_metadata.get('size', 'unknown')
            
            log(f"  파일 정보: {file_name} ({file_size} bytes)")
            
            # PDF 다운로드
            request = self.drive_service.files().get_media(fileId=file_id)
            
            buffer = io.BytesIO()
            downloader = MediaIoBaseDownload(buffer, request)
            
            done = False
            while not done:
                status, done = downloader.next_chunk()
            
            pdf_bytes = buffer.getvalue()
            
            if pdf_bytes[:4] == b'%PDF':
                log(f"  PDF 다운로드 완료: {len(pdf_bytes):,} bytes")
                return pdf_bytes
            else:
                log(f"  [WARN] PDF가 아닌 파일입니다: {mime_type}")
                return None
                
        except Exception as e:
            log(f"  [ERROR] 드라이브 다운로드 실패: {e}")
            return None
    
    def download_pdf_from_gdrive_url(self, gdrive_url: str) -> Optional[bytes]:
        """
        구글 드라이브 URL에서 PDF 다운로드
        
        Args:
            gdrive_url: 구글 드라이브 공유 링크
            
        Returns:
            PDF 바이트 데이터 또는 None
        """
        file_id = self.extract_file_id_from_url(gdrive_url)
        if not file_id:
            log(f"  [ERROR] 파일 ID를 추출할 수 없습니다: {gdrive_url[:50]}...")
            return None
        
        return self.download_pdf_from_gdrive(file_id)
    
    def extract_text_from_bytes(self, pdf_bytes: bytes) -> str:
        """
        PDF 바이트에서 텍스트 추출
        
        Args:
            pdf_bytes: PDF 바이트 데이터
            
        Returns:
            추출된 텍스트
        """
        # 임시 파일에 저장
        temp_path = os.path.join(
            self.temp_dir, 
            f"temp_{datetime.now().strftime('%Y%m%d%H%M%S%f')}.pdf"
        )
        
        try:
            with open(temp_path, 'wb') as f:
                f.write(pdf_bytes)
            
            text = self.extract_text_from_file(temp_path)
            return text
            
        finally:
            # 임시 파일 삭제
            if os.path.exists(temp_path):
                os.remove(temp_path)
    
    def extract_text_from_file(self, pdf_path: str) -> str:
        """
        PDF 파일에서 텍스트 추출
        
        Args:
            pdf_path: PDF 파일 경로
            
        Returns:
            추출된 텍스트
        """
        text_parts = []
        
        # pdfplumber 시도 (더 나은 테이블 추출)
        if HAS_PDFPLUMBER:
            try:
                with pdfplumber.open(pdf_path) as pdf:
                    for i, page in enumerate(pdf.pages):
                        page_text = page.extract_text()
                        if page_text:
                            text_parts.append(f"[페이지 {i+1}]\n{page_text}")
                        
                        # 테이블 추출 시도
                        tables = page.extract_tables()
                        for table_idx, table in enumerate(tables):
                            if table:
                                table_text = self._format_table(table)
                                if table_text:
                                    text_parts.append(f"[테이블 {table_idx+1}]\n{table_text}")
                
                if text_parts:
                    full_text = "\n\n".join(text_parts)
                    log(f"  텍스트 추출 완료 (pdfplumber): {len(full_text):,} 글자")
                    return full_text
                    
            except Exception as e:
                log(f"  [WARN] pdfplumber 추출 실패: {e}")
        
        # PyPDF2 fallback
        if HAS_PYPDF2:
            try:
                with open(pdf_path, 'rb') as f:
                    reader = PdfReader(f)
                    for i, page in enumerate(reader.pages):
                        page_text = page.extract_text()
                        if page_text:
                            text_parts.append(f"[페이지 {i+1}]\n{page_text}")
                
                if text_parts:
                    full_text = "\n\n".join(text_parts)
                    log(f"  텍스트 추출 완료 (PyPDF2): {len(full_text):,} 글자")
                    return full_text
                    
            except Exception as e:
                log(f"  [WARN] PyPDF2 추출 실패: {e}")
        
        log("  [ERROR] 텍스트 추출 실패")
        return ""
    
    def _format_table(self, table: list) -> str:
        """
        테이블 데이터를 텍스트로 포맷
        
        Args:
            table: 2차원 리스트 형태의 테이블 데이터
            
        Returns:
            포맷된 테이블 텍스트
        """
        if not table:
            return ""
        
        lines = []
        for row in table:
            if row:
                cells = [str(cell).strip() if cell else "" for cell in row]
                if any(cells):  # 빈 행 제외
                    lines.append(" | ".join(cells))
        
        return "\n".join(lines)
    
    def get_pdf_and_text_from_gdrive(self, gdrive_url: str) -> Tuple[Optional[bytes], str]:
        """
        구글 드라이브에서 PDF 다운로드 및 텍스트 추출
        
        PDF 직접 전달을 우선하므로, 텍스트 추출 실패해도 pdf_bytes는 반환
        
        Args:
            gdrive_url: 구글 드라이브 공유 링크
            
        Returns:
            (PDF 바이트, 추출된 텍스트) 튜플
            - PDF 다운로드 성공 시: (bytes, text) - text는 빈 문자열일 수 있음
            - PDF 다운로드 실패 시: (None, "")
        """
        pdf_bytes = self.download_pdf_from_gdrive_url(gdrive_url)
        
        if not pdf_bytes:
            log("  [ERROR] PDF 다운로드 실패 - 구글드라이브 링크 또는 인증 확인 필요")
            return None, ""
        
        # 텍스트 추출 시도 (fallback용)
        log("  텍스트 추출 중 (fallback용)...")
        text = ""
        try:
            text = self.extract_text_from_bytes(pdf_bytes)
            if text:
                log(f"  텍스트 추출 성공: {len(text):,}자")
            else:
                log("  [WARN] 텍스트 추출 결과 없음 (이미지 PDF일 수 있음)")
                log("  → PDF 직접 전달로 분석 시도 예정")
        except Exception as e:
            log(f"  [WARN] 텍스트 추출 실패: {e}")
            log("  → PDF 직접 전달로 분석 시도 예정")
        
        # 텍스트 추출 실패해도 pdf_bytes는 반환 (Gemini가 직접 읽을 수 있음)
        return pdf_bytes, text
    
    def estimate_tokens(self, pdf_bytes: Optional[bytes] = None, text: Optional[str] = None) -> int:
        """
        PDF/텍스트의 예상 토큰 수 추정
        
        Gemini 토큰 계산 기준:
        - 텍스트: 한글 1글자 ≈ 2-3 토큰, 영문 4글자 ≈ 1 토큰
        - 이미지 PDF: 페이지당 약 258 토큰
        
        Args:
            pdf_bytes: PDF 바이너리 데이터
            text: 추출된 텍스트 (있으면 텍스트 기반 추정)
            
        Returns:
            예상 토큰 수
        """
        # 1. 텍스트가 있으면 텍스트 기반 추정
        if text and len(text) > 100:
            # 한글/영문 혼합 기준: 평균 2 토큰/글자
            estimated = int(len(text) * 2.0)
            return estimated
        
        # 2. PDF 바이트가 있으면 페이지 수 기반 추정 (이미지 PDF 가정)
        if pdf_bytes:
            page_count = self._count_pdf_pages(pdf_bytes)
            if page_count > 0:
                # 이미지 PDF: 페이지당 258 토큰 + 여유분
                estimated = page_count * 300
                return estimated
            
            # 페이지 수 확인 실패 시 바이트 기반 추정
            # 일반적으로 PDF 1KB ≈ 50-100 토큰
            estimated = int(len(pdf_bytes) / 1024 * 75)
            return estimated
        
        return 0
    
    def _count_pdf_pages(self, pdf_bytes: bytes) -> int:
        """
        PDF 페이지 수 확인
        
        Args:
            pdf_bytes: PDF 바이너리 데이터
            
        Returns:
            페이지 수 (실패 시 0)
        """
        temp_path = os.path.join(
            self.temp_dir, 
            f"temp_count_{datetime.now().strftime('%Y%m%d%H%M%S%f')}.pdf"
        )
        
        try:
            with open(temp_path, 'wb') as f:
                f.write(pdf_bytes)
            
            if HAS_PYPDF2:
                with open(temp_path, 'rb') as f:
                    reader = PdfReader(f)
                    return len(reader.pages)
            
            if HAS_PDFPLUMBER:
                with pdfplumber.open(temp_path) as pdf:
                    return len(pdf.pages)
            
            return 0
            
        except Exception as e:
            log(f"  [WARN] 페이지 수 확인 실패: {e}")
            return 0
            
        finally:
            if os.path.exists(temp_path):
                os.remove(temp_path)
    
    def get_pdf_info(self, gdrive_url: str) -> Dict[str, Any]:
        """
        PDF 정보 조회 (다운로드 + 텍스트 추출 + 토큰 추정)
        
        Args:
            gdrive_url: 구글 드라이브 공유 링크
            
        Returns:
            {
                'pdf_bytes': bytes or None,
                'text': str,
                'estimated_tokens': int,
                'page_count': int,
                'file_size': int
            }
        """
        result = {
            'pdf_bytes': None,
            'text': '',
            'estimated_tokens': 0,
            'page_count': 0,
            'file_size': 0
        }
        
        pdf_bytes, text = self.get_pdf_and_text_from_gdrive(gdrive_url)
        
        if pdf_bytes:
            result['pdf_bytes'] = pdf_bytes
            result['text'] = text
            result['file_size'] = len(pdf_bytes)
            result['page_count'] = self._count_pdf_pages(pdf_bytes)
            result['estimated_tokens'] = self.estimate_tokens(pdf_bytes, text)
        
        return result


def parse_artifact_link(artifact_link: str) -> Tuple[str, str]:
    """
    아티팩트 링크 파싱
    
    Args:
        artifact_link: 예: "Archive_pdf/20251226_한미반도체_20251226000082.pdf|run_id:20532076409"
        
    Returns:
        (파일 경로, run_id) 튜플
    """
    if not artifact_link:
        return "", ""
    
    parts = artifact_link.split('|')
    file_path = parts[0].strip() if parts else ""
    
    run_id = ""
    if len(parts) > 1:
        run_id_part = parts[1].strip()
        if run_id_part.startswith('run_id:'):
            run_id = run_id_part.split(':')[1]
    
    return file_path, run_id


def main():
    """테스트용 메인 함수"""
    extractor = PDFExtractor()
    
    # 테스트 (실제 구글 드라이브 링크로 테스트)
    test_url = "https://drive.google.com/file/d/1UYnBc930Kcv-PaskjmHO6i04eH7z3J4C/view?usp=drivesdk"
    
    log(f"테스트 URL: {test_url}")
    pdf_bytes, text = extractor.get_pdf_and_text_from_gdrive(test_url)
    
    if pdf_bytes:
        log(f"PDF 크기: {len(pdf_bytes):,} bytes")
        log(f"텍스트 길이: {len(text):,} 글자")
        log(f"\n텍스트 미리보기 (처음 500자):\n{text[:500]}")
    else:
        log("PDF 다운로드 실패")


if __name__ == "__main__":
    main()
