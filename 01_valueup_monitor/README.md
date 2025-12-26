# KRX Value-Up 공시 모니터

KRX KIND 사이트에서 기업가치 제고(밸류업) 공시를 자동으로 수집하고, PDF를 Google Drive에 업로드하며, 목록을 Google Sheets에 기록하는 자동화 도구입니다.

## 기능

- **공시 목록 크롤링**: Playwright를 사용하여 KRX KIND 밸류업 공시 페이지에서 공시 목록 수집
- **PDF 다운로드**: 각 공시의 PDF 파일 자동 다운로드
- **Google Drive 업로드**: 다운로드한 PDF를 Google Drive에 자동 업로드
- **Google Sheets 기록**: 공시 목록을 스프레드시트에 누적 기록
- **중복 방지**: 이미 수집된 공시는 자동으로 건너뜀

## 파일 구조

```
01_valueup_monitor/
├── __init__.py              # 패키지 초기화
├── krx_valueup_crawler.py   # KRX 크롤러 (Playwright)
├── gdrive_uploader.py       # Google Drive 업로더
├── gsheet_manager.py        # Google Sheets 관리자
├── main.py                  # 메인 실행 파일
└── README.md

requirements/
└── requirements_valueup_read.txt

.github/workflows/
└── valueup_monitor_1.yml    # GitHub Actions 워크플로우
```

## 설정

### 1. Google Cloud 서비스 계정 설정

1. [Google Cloud Console](https://console.cloud.google.com/)에서 프로젝트 생성
2. Google Sheets API 및 Google Drive API 활성화
3. 서비스 계정 생성 및 JSON 키 다운로드
4. 해당 서비스 계정 이메일을 스프레드시트와 Drive 폴더에 편집자로 공유

### 2. Google Sheets 준비

1. 새 스프레드시트 생성
2. 스프레드시트 ID 복사 (URL에서 `/d/` 와 `/edit` 사이의 문자열)
3. 서비스 계정에 편집 권한 부여

### 3. GitHub Secrets 설정

```
GOOGLE_SERVICE        # 서비스 계정 JSON 전체 (한 줄로)
VALUEUP_GSPREAD_ID    # 스프레드시트 ID
VALUEUP_ARCHIVE_ID    # Google Drive 폴더 ID
```

## Google Sheets 구조

| 열 | 필드명 | 설명 |
|---|--------|------|
| A | 번호 | 공시 순번 |
| B | 공시일자 | 공시 게시일 |
| C | 회사명 | 기업명 |
| D | 종목코드 | 주식 종목코드 |
| E | 공시제목 | 공시 제목 |
| F | 접수번호 | KRX 접수번호 (고유식별자) |
| G | 원시PDF링크 | KRX PDF 다운로드 링크 |
| H | 구글드라이브링크 | 업로드된 PDF 링크 |
| I | 수집일시 | 데이터 수집 시각 |

## 실행 방법

### 로컬 실행

```bash
# 의존성 설치
pip install -r requirements/requirements_valueup_read.txt
playwright install chromium

# 환경변수 설정
export GOOGLE_SERVICE='{"type": "service_account", ...}'
export VALUEUP_GSPREAD_ID='your-spreadsheet-id'
export VALUEUP_ARCHIVE_ID='your-folder-id'

# 실행
cd 01_valueup_monitor
python main.py
```

### GitHub Actions

- **자동 실행**: 매주 일요일 자정(UTC) = 월요일 오전 9시(KST)
- **수동 실행**: Actions 탭에서 "Run workflow" 클릭

## 라이선스

MIT License
