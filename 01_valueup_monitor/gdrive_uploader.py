"""
Google Drive 업로더
밸류업 PDF 파일을 Google Drive에 업로드 (공유 드라이브 지원)
"""

import os
import io
import sys
from typing import Optional
from datetime import datetime
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload

# stdout 버퍼링 해제
sys.stdout.reconfigure(line_buffering=True)


def log(message: str):
    """타임스탬프와 함께 로그 출력"""
    timestamp = datetime.now().strftime("%H:%M:%S")
    print(f"[{timestamp}] {message}", flush=True)


class GDriveUploader:
    """Google Drive 업로더 (공유 드라이브 지원)"""
    
    SCOPES = [
        'https://www.googleapis.com/auth/drive.file',
        'https://www.googleapis.com/auth/drive'
    ]
    
    def __init__(self, credentials_json: Optional[str] = None, folder_id: Optional[str] = None):
        """
        초기화
        
        Args:
            credentials_json: 서비스 계정 JSON 파일 경로 또는 JSON 문자열
            folder_id: 업로드할 폴더 ID (공유 드라이브 폴더 권장)
        """
        self.folder_id = folder_id or os.environ.get('VALUEUP_ARCHIVE_ID')
        self.service = None
        
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
            # 환경변수에서 로드
            creds_json = os.environ.get('GOOGLE_SERVICE')
            if creds_json:
                import json
                info = json.loads(creds_json)
                creds = Credentials.from_service_account_info(info, scopes=self.SCOPES)
        
        if creds:
            self.service = build('drive', 'v3', credentials=creds)
    
    def upload_pdf(self, pdf_data: bytes, filename: str, folder_id: Optional[str] = None) -> Optional[str]:
        """
        PDF 파일 업로드 (공유 드라이브 지원)
        
        서비스 계정은 개인 드라이브에 업로드할 수 없으므로
        반드시 공유 드라이브(Shared Drive) 폴더에 업로드해야 합니다.
        
        Args:
            pdf_data: PDF 바이너리 데이터
            filename: 저장할 파일명
            folder_id: 업로드할 폴더 ID (공유 드라이브 폴더)
            
        Returns:
            업로드된 파일의 웹 링크 또는 None
        """
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
            # supportsAllDrives=True: 공유 드라이브 지원
            file = self.service.files().create(
                body=file_metadata,
                media_body=media,
                fields='id, webViewLink',
                supportsAllDrives=True  # 공유 드라이브 지원
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
                    supportsAllDrives=True  # 공유 드라이브 지원
                ).execute()
            except Exception as perm_error:
                # 권한 설정 실패해도 업로드는 성공한 것으로 처리
                log(f"권한 설정 경고 (무시): {perm_error}")
            
            return web_link
            
        except Exception as e:
            error_msg = str(e)
            log(f"파일 업로드 중 오류: {e}")
            
            # 스토리지 할당량 오류인 경우 안내 메시지
            if 'storageQuotaExceeded' in error_msg or 'storage quota' in error_msg.lower():
                log("  → 서비스 계정은 개인 드라이브에 업로드할 수 없습니다.")
                log("  → 공유 드라이브(Shared Drive)를 사용하세요.")
                log("  → VALUEUP_ARCHIVE_ID가 공유 드라이브 폴더 ID인지 확인하세요.")
            
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
                supportsAllDrives=True,  # 공유 드라이브 지원
                includeItemsFromAllDrives=True  # 공유 드라이브 항목 포함
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
                supportsAllDrives=True  # 공유 드라이브 지원
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
        return
    
    log("Google Drive 연결 성공")
    log(f"대상 폴더 ID: {uploader.folder_id}")
    
    # 테스트용 PDF 데이터
    test_pdf = b'%PDF-1.4\n1 0 obj\n<</Type/Catalog>>\nendobj\ntrailer<</Root 1 0 R>>'
    
    link = uploader.upload_pdf(test_pdf, 'test_valueup.pdf')
    if link:
        log(f"업로드 성공: {link}")
    else:
        log("업로드 실패")
        log("참고: 서비스 계정은 공유 드라이브에만 업로드 가능합니다.")


if __name__ == "__main__":
    main()
