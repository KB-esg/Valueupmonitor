"""
PDF 추출기
KRX에서 PDF 다운로드 및 텍스트 추출
"""

import os
import sys
import tempfile
import requests
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

sys.stdout.reconfigure(line_buffering=True)


def log(message: str):
    """타임스탬프와 함께 로그 출력"""
    timestamp = datetime.now().strftime("%H:%M:%S")
    print(f"[{timestamp}] {message}", flush=True)


class PDFExtractor:
    """PDF 다운로드 및 텍스트 추출"""
    
    # KRX PDF 다운로드 URL 템플릿
    KRX_PDF_URL = "https://kind.krx.co.kr/common/pdfDownload.do"
    
    # 요청 헤더
    HEADERS = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'application/pdf,*/*',
        'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
        'Referer': 'https://kind.krx.co.kr/'
    }
    
    def __init__(self, temp_dir: Optional[str] = None):
        """
        초기화
        
        Args:
            temp_dir: 임시 파일 저장 디렉토리 (기본값: 시스템 임시 디렉토리)
        """
        self.temp_dir = temp_dir or tempfile.gettempdir()
        self.session = requests.Session()
        self.session.headers.update(self.HEADERS)
        
        # PDF 추출 라이브러리 확인
        if HAS_PDFPLUMBER:
            log("PDF 추출 라이브러리: pdfplumber")
        elif HAS_PYPDF2:
            log("PDF 추출 라이브러리: PyPDF2")
        else:
            log("[WARN] PDF 추출 라이브러리가 없습니다. pdfplumber 또는 PyPDF2를 설치하세요.")
    
    def download_pdf_from_krx(self, acptno: str) -> Optional[bytes]:
        """
        KRX에서 PDF 다운로드
        
        Args:
            acptno: 접수번호
            
        Returns:
            PDF 바이트 데이터 또는 None
        """
        try:
            params = {
                'method': 'pdfDown',
                'acptNo': acptno
            }
            
            response = self.session.get(
                self.KRX_PDF_URL,
                params=params,
                timeout=60
            )
            
            if response.status_code == 200:
                content_type = response.headers.get('Content-Type', '')
                
                if 'pdf' in content_type.lower() or response.content[:4] == b'%PDF':
                    log(f"  PDF 다운로드 완료: {len(response.content):,} bytes")
                    return response.content
                else:
                    log(f"  [WARN] PDF가 아닌 응답: {content_type[:50]}")
                    return None
            else:
                log(f"  [ERROR] HTTP {response.status_code}")
                return None
                
        except requests.RequestException as e:
            log(f"  [ERROR] 다운로드 실패: {e}")
            return None
    
    def download_pdf_from_url(self, url: str) -> Optional[bytes]:
        """
        URL에서 PDF 다운로드
        
        Args:
            url: PDF URL
            
        Returns:
            PDF 바이트 데이터 또는 None
        """
        try:
            response = self.session.get(url, timeout=60)
            
            if response.status_code == 200:
                if response.content[:4] == b'%PDF':
                    log(f"  PDF 다운로드 완료: {len(response.content):,} bytes")
                    return response.content
                else:
                    log(f"  [WARN] PDF가 아닌 응답")
                    return None
            else:
                log(f"  [ERROR] HTTP {response.status_code}")
                return None
                
        except requests.RequestException as e:
            log(f"  [ERROR] 다운로드 실패: {e}")
            return None
    
    def extract_text_from_bytes(self, pdf_bytes: bytes) -> str:
        """
        PDF 바이트에서 텍스트 추출
        
        Args:
            pdf_bytes: PDF 바이트 데이터
            
        Returns:
            추출된 텍스트
        """
        # 임시 파일에 저장
        temp_path = os.path.join(self.temp_dir, f"temp_{datetime.now().strftime('%Y%m%d%H%M%S')}.pdf")
        
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
    
    def get_pdf_and_text(self, acptno: str) -> Tuple[Optional[bytes], str]:
        """
        PDF 다운로드 및 텍스트 추출을 한번에 수행
        
        Args:
            acptno: 접수번호
            
        Returns:
            (PDF 바이트, 추출된 텍스트) 튜플
        """
        pdf_bytes = self.download_pdf_from_krx(acptno)
        
        if pdf_bytes:
            text = self.extract_text_from_bytes(pdf_bytes)
            return pdf_bytes, text
        
        return None, ""


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
    
    # 테스트 (실제 접수번호로 테스트)
    test_acptno = "20251226000082"
    
    log(f"테스트 접수번호: {test_acptno}")
    pdf_bytes, text = extractor.get_pdf_and_text(test_acptno)
    
    if pdf_bytes:
        log(f"PDF 크기: {len(pdf_bytes):,} bytes")
        log(f"텍스트 길이: {len(text):,} 글자")
        log(f"\n텍스트 미리보기 (처음 500자):\n{text[:500]}")
    else:
        log("PDF 다운로드 실패")


if __name__ == "__main__":
    main()
