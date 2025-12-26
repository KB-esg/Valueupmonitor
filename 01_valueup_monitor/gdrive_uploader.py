"""
Google Drive 업로더
밸류업 PDF 파일을 Google Drive에 업로드
"""

import os
import io
from typing import Optional
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload


class GDriveUploader:
    """Google Drive 업로더"""
    
    SCOPES = [
        'https://www.googleapis.com/auth/drive.file',
        'https://www.googleapis.com/auth/drive'
    ]
    
    def __init__(self, credentials_json: Optional[str] = None, folder_id: Optional[str] = None):
        """
        초기화
        
        Args:
            credentials_json: 서비스 계정 JSON 파일 경로 또는 JSON 문자열
            folder_id: 업로드할 폴더 ID (없으면 루트)
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
        PDF 파일 업로드
        
        Args:
            pdf_data: PDF 바이너리 데이터
            filename: 저장할 파일명
            folder_id: 업로드할 폴더 ID (없으면 기본 폴더)
            
        Returns:
            업로드된 파일의 웹 링크 또는 None
        """
        if not self.service:
            print("Google Drive 서비스가 초기화되지 않았습니다.")
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
            file = self.service.files().create(
                body=file_metadata,
                media_body=media,
                fields='id, webViewLink'
            ).execute()
            
            # 파일 공유 설정 (링크가 있는 모든 사용자가 볼 수 있도록)
            self.service.permissions().create(
                fileId=file['id'],
                body={
                    'type': 'anyone',
                    'role': 'reader'
                }
            ).execute()
            
            return file.get('webViewLink')
            
        except Exception as e:
            print(f"파일 업로드 중 오류: {e}")
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
                fields='files(id, name)'
            ).execute()
            
            return len(results.get('files', [])) > 0
            
        except Exception as e:
            print(f"파일 확인 중 오류: {e}")
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
                fields='id'
            ).execute()
            
            return folder.get('id')
            
        except Exception as e:
            print(f"폴더 생성 중 오류: {e}")
            return None


def main():
    """테스트용 메인 함수"""
    uploader = GDriveUploader()
    
    # 테스트용 PDF 데이터
    test_pdf = b'%PDF-1.4 test'
    
    link = uploader.upload_pdf(test_pdf, 'test_valueup.pdf')
    if link:
        print(f"업로드 성공: {link}")
    else:
        print("업로드 실패")


if __name__ == "__main__":
    main()
