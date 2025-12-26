"""
Google Drive 업로더 (OAuth2 지원)
개인 마이 드라이브에 PDF 파일 업로드

인증 방식:
1. OAuth2 (권장): 개인 계정의 마이 드라이브에 업로드
2. 서비스 계정: 공유 드라이브에만 업로드 가능

환경변수:
- OAuth2 방식:
  - GDRIVE_REFRESH_TOKEN: 리프레시 토큰
  - GDRIVE_CLIENT_ID: OAuth 클라이언트 ID
  - GDRIVE_CLIENT_SECRET: OAuth 클라이언트 시크릿
  
- 서비스 계정 방식 (fallback):
  - GOOGLE_SERVICE: 서비스 계정 JSON
"""

import os
import io
import sys
from typing import Optional
from datetime import datetime

# stdout 버퍼링 해제
sys.stdout.reconfigure(line_buffering=True)


def log(message: str):
    """타임스탬프와 함께 로그 출력"""
    timestamp = datetime.now().strftime("%H:%M:%S")
    print(f"[{timestamp}] {message}", flush=True)


class GDriveUploader:
    """Google Drive 업로더 (OAuth2 + 서비스 계정 지원)"""
    
    SCOPES = [
        'https://www.googleapis.com/auth/drive.file',
        'https://www.googleapis.com/auth/drive'
    ]
    
    def __init__(
        self,
        folder_id: Optional[str] = None,
        # OAuth2 인증 정보
        refresh_token: Optional[str] = None,
        client_id: Optional[str] = None,
        client_secret: Optional[str] = None,
        # 서비스 계정 인증 정보 (fallback)
        credentials_json: Optional[str] = None
    ):
        """
        초기화
        
        Args:
            folder_id: 업로드할 폴더 ID (없으면 루트)
            refresh_token: OAuth2 리프레시 토큰
            client_id: OAuth2 클라이언트 ID
            client_secret: OAuth2 클라이언트 시크릿
            credentials_json: 서비스 계정 JSON (fallback)
        """
        self.folder_id = folder_id or os.environ.get('VALUEUP_ARCHIVE_ID')
        self.service = None
        self.auth_method = None
        
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
                log(f"OAuth2 인증 실패: {e}")
        
        # 2. 서비스 계정 인증 (fallback)
        if not self.service:
            credentials_json = credentials_json or os.environ.get('GOOGLE_SERVICE')
            if credentials_json:
                try:
                    self._init_service_account(credentials_json)
                    self.auth_method = 'ServiceAccount'
                    log("Google Drive 인증: 서비스 계정 (공유 드라이브만 가능)")
                except Exception as e:
                    log(f"서비스 계정 인증 실패: {e}")
    
    def _init_oauth2(self, refresh_token: str, client_id: str, client_secret: str):
        """OAuth2 인증 초기화"""
        from google.oauth2.credentials import Credentials
        from google.auth.transport.requests import Request
        from googleapiclient.discovery import build
        
        creds = Credentials(
            token=None,
            refresh_token=refresh_token,
            client_id=client_id,
            client_secret=client_secret,
            token_uri='https://oauth2.googleapis.com/token'
        )
        
        # 액세스 토큰 갱신
        creds.refresh(Request())
        
        self.service = build('drive', 'v3', credentials=creds)
    
    def _init_service_account(self, credentials_json: str):
        """서비스 계정 인증 초기화"""
        from google.oauth2.service_account import Credentials
        from googleapiclient.discovery import build
        import json
        
        if os.path.isfile(credentials_json):
            creds = Credentials.from_service_account_file(credentials_json, scopes=self.SCOPES)
        else:
            info = json.loads(credentials_json)
            creds = Credentials.from_service_account_info(info, scopes=self.SCOPES)
        
        self.service = build('drive', 'v3', credentials=creds)
    
    def upload_pdf(self, pdf_data: bytes, filename: str, folder_id: Optional[str] = None) -> Optional[str]:
        """
        PDF 파일 업로드
        
        Args:
            pdf_data: PDF 바이너리 데이터
            filename: 저장할 파일명
            folder_id: 업로드할 폴더 ID (없으면 기본 폴더)
            
        Returns:
            업로드된 파일의 웹 링크 또는 None
        """
        from googleapiclient.http import MediaIoBaseUpload
        
        if not self.service:
            log("Google Drive 서비스가 초기화되지 않았습니다.")
            return None
        
        target_folder = folder_id or self.folder_id
        
        file_metadata = {
            'name': filename,
            'mimeType': 'application/pdf'
        }
        
        if target_folder:
            file_metadata['parents'] = [target_folder]
        
        media = MediaIoBaseUpload(
            io.BytesIO(pdf_data),
            mimetype='application/pdf',
            resumable=True
        )
        
        try:
            # supportsAllDrives: 서비스 계정의 공유 드라이브 지원
            file = self.service.files().create(
                body=file_metadata,
                media_body=media,
                fields='id, webViewLink',
                supportsAllDrives=True
            ).execute()
            
            file_id = file.get('id')
            web_link = file.get('webViewLink')
            
            # 파일 공유 설정 (링크가 있는 모든 사용자가 볼 수 있도록)
            try:
                self.service.permissions().create(
                    fileId=file_id,
                    body={
                        'type': 'anyone',
                        'role': 'reader'
                    },
                    supportsAllDrives=True
                ).execute()
            except Exception as perm_error:
                # 권한 설정 실패해도 업로드는 성공한 것으로 처리
                log(f"  권한 설정 경고 (무시 가능): {perm_error}")
            
            return web_link
            
        except Exception as e:
            error_msg = str(e)
            log(f"파일 업로드 중 오류: {e}")
            
            # 스토리지 할당량 오류 안내
            if 'storageQuotaExceeded' in error_msg or 'storage quota' in error_msg.lower():
                log("  → 서비스 계정은 개인 드라이브에 업로드할 수 없습니다.")
                log("  → OAuth2 인증을 사용하거나 공유 드라이브를 이용하세요.")
            
            return None
    
    def check_file_exists(self, filename: str, folder_id: Optional[str] = None) -> bool:
        """
        파일 존재 여부 확인
        
        Args:
            filename: 파일명
            folder_id: 폴더 ID
            
        Returns:
            존재 여부
        """
        if not self.service:
            return False
        
        target_folder = folder_id or self.folder_id
        
        query = f"name = '{filename}' and trashed = false"
        if target_folder:
            query += f" and '{target_folder}' in parents"
        
        try:
            results = self.service.files().list(
                q=query,
                spaces='drive',
                fields='files(id, name)',
                supportsAllDrives=True,
                includeItemsFromAllDrives=True
            ).execute()
            
            return len(results.get('files', [])) > 0
            
        except Exception as e:
            log(f"파일 확인 중 오류: {e}")
            return False
    
    def create_folder(self, folder_name: str, parent_id: Optional[str] = None) -> Optional[str]:
        """
        폴더 생성
        
        Args:
            folder_name: 폴더명
            parent_id: 상위 폴더 ID
            
        Returns:
            생성된 폴더 ID 또는 None
        """
        if not self.service:
            return None
        
        file_metadata = {
            'name': folder_name,
            'mimeType': 'application/vnd.google-apps.folder'
        }
        
        if parent_id:
            file_metadata['parents'] = [parent_id]
        
        try:
            folder = self.service.files().create(
                body=file_metadata,
                fields='id',
                supportsAllDrives=True
            ).execute()
            
            return folder.get('id')
            
        except Exception as e:
            log(f"폴더 생성 중 오류: {e}")
            return None


def main():
    """테스트용 메인 함수"""
    uploader = GDriveUploader()
    
    if not uploader.service:
        log("Google Drive 서비스 초기화 실패")
        log("")
        log("OAuth2 인증 환경변수 확인:")
        log("  - GDRIVE_REFRESH_TOKEN")
        log("  - GDRIVE_CLIENT_ID")
        log("  - GDRIVE_CLIENT_SECRET")
        return
    
    log(f"인증 방식: {uploader.auth_method}")
    log(f"대상 폴더 ID: {uploader.folder_id or '(루트)'}")
    
    # 테스트용 PDF 데이터
    test_pdf = b'%PDF-1.4\n1 0 obj\n<</Type/Catalog>>\nendobj\ntrailer<</Root 1 0 R>>'
    
    link = uploader.upload_pdf(test_pdf, 'test_valueup.pdf')
    if link:
        log(f"업로드 성공: {link}")
    else:
        log("업로드 실패")


if __name__ == "__main__":
    main()
