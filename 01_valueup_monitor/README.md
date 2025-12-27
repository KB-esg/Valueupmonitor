# KRX Value-Up 공시 모니터

KRX KIND 사이트에서 기업가치 제고(밸류업) 공시를 자동으로 수집하고, Google Sheets에 기록하며, PDF를 다운로드하는 도구입니다.

## 주요 기능

- **공시 목록 크롤링**: KRX KIND 밸류업 공시 페이지에서 공시 목록 수집
- **조회 기간 기준 조기 종료**: 설정된 기간 외 공시가 발견되면 크롤링 자동 종료
- **종목코드 자동 조회**: 회사명으로 6자리 종목코드 자동 매핑 (pykrx/KRX API)
- **Google Sheets 연동**: 배치 업데이트로 API quota 절약, 시트 행/열 자동 확장
- **PDF 다운로드**: 기업이 제출한 원본 첨부 PDF 다운로드 (첨부문서 우선)
- **Google Drive 업로드**: OAuth2 인증으로 개인 드라이브에 월별 폴더 구조로 저장
- **GitHub Actions 아티팩트**: PDF 파일 90일 보관, 시트에 아티팩트 정보 기록

## 파일 구조

```
01_valueup_monitor/
├── main.py                 # 메인 실행 파일
├── krx_valueup_crawler.py  # KRX 밸류업 공시 크롤러 (Playwright)
├── gsheet_manager.py       # Google Sheets 관리 (배치 업데이트, 자동 확장)
├── gdrive_uploader.py      # Google Drive 업로더 (OAuth2 지원)
├── stock_code_mapper.py    # 종목코드 조회 모듈
├── OAUTH_SETUP_GUIDE.md    # OAuth2 설정 가이드
├── README.md               # 이 파일
└── Archive_pdf/            # PDF 저장 폴더 (자동 생성)

.github/workflows/
└── valueup_monitor.yml     # GitHub Actions 워크플로우

requirements/
└── requirements_valueup_read.txt  # Python 의존성
```

## 실행 흐름

```
[1단계] KRX에서 공시 목록 조회
    ├── 날짜 범위 설정 (days 또는 period)
    ├── 조회 기간 외 공시 다수 발견 시 조기 종료
    ↓
[2단계] 종목코드 조회 (회사명 → 종목코드)
    ↓
[3단계] Google Sheets에 공시 목록 기록
    ├── 시트 행/열 부족 시 자동 확장
    ├── 배치 추가 (1회 API 호출)
    ↓
[4단계] PDF 다운로드 및 저장
    ├── 조회 기간 내 공시만 처리
    ├── 로컬: Archive_pdf/
    └── Drive: PDF_archive/YY_MM/ (OAuth2 설정 시)
    ↓
[5단계] 시트에 링크 정보 배치 업데이트 (1회 API 호출)
    ├── H열: 구글드라이브링크
    └── J열: 아티팩트링크
```

## Google Sheets 구조

| 열 | 헤더 | 설명 |
|----|------|------|
| A | 번호 | 순번 |
| B | 공시일자 | YYYY-MM-DD HH:MM:SS |
| C | 회사명 | 기업명 |
| D | 종목코드 | 6자리 종목코드 (자동 조회) |
| E | 공시제목 | 공시 제목 |
| F | 접수번호 | KRX 접수번호 (14자리) |
| G | 원시PDF링크 | KRX 직접 다운로드 URL |
| H | 구글드라이브링크 | Drive 링크 또는 [로컬저장] 파일명 |
| I | 수집일시 | 데이터 수집 시각 |
| J | 아티팩트링크 | GitHub Actions 아티팩트 정보 |

### 시트 자동 확장

- **열 부족**: 기존 시트가 9열이면 10열로 자동 확장
- **행 부족**: 데이터 추가 전 필요한 행 수 + 100행 여유분 확보
- **새 시트 생성**: 2000행 × 10열로 생성

## Google Drive 폴더 구조

OAuth2 설정 시 자동 생성:

```
01_Valueup_archive (VALUEUP_ARCHIVE_ID)
└── PDF_archive
    ├── 25_11
    │   └── 20251115_삼성전자_20251115000123.pdf
    └── 25_12
        ├── 20251224_감성코퍼레이션_20251224000527.pdf
        └── 20251226_한미반도체_20251226000082.pdf
```

## 환경변수

### 필수

| 변수명 | 설명 |
|--------|------|
| `GOOGLE_SERVICE` | 서비스 계정 JSON (Sheets 접근용) |
| `VALUEUP_GSPREAD_ID` | Google Sheets 스프레드시트 ID |

### 선택 (Google Drive 업로드용)

| 변수명 | 설명 |
|--------|------|
| `GDRIVE_REFRESH_TOKEN` | OAuth2 리프레시 토큰 |
| `GDRIVE_CLIENT_ID` | OAuth2 클라이언트 ID |
| `GDRIVE_CLIENT_SECRET` | OAuth2 클라이언트 시크릿 |
| `VALUEUP_ARCHIVE_ID` | Drive 업로드 폴더 ID |

### 실행 옵션

| 변수명 | 기본값 | 설명 |
|--------|--------|------|
| `VALUEUP_DAYS` | 7 | 조회 기간 (일) |
| `VALUEUP_PERIOD` | - | 기간 버튼 (1주, 1개월, 3개월 등) |
| `VALUEUP_MAX_PAGES` | 10 | 최대 크롤링 페이지 수 |
| `VALUEUP_SKIP_PDF` | false | PDF 다운로드 건너뛰기 |
| `VALUEUP_DEBUG` | false | 디버그 모드 |

## 설치 및 실행

### 로컬 실행

```bash
# 의존성 설치
pip install -r requirements/requirements_valueup_read.txt

# Playwright 브라우저 설치
playwright install chromium

# 환경변수 설정
export GOOGLE_SERVICE='{"type": "service_account", ...}'
export VALUEUP_GSPREAD_ID='your_spreadsheet_id'

# 실행
cd 01_valueup_monitor
python main.py --days 7
```

### CLI 옵션

```bash
# 최근 7일 (기본값)
python main.py

# 최근 30일
python main.py --days 30

# 기간 버튼 사용 (3개월)
python main.py --period 3개월

# 전체 기간 아카이브
python main.py --period 전체 --max-pages 50

# 목록만 수집 (PDF 다운로드 건너뜀)
python main.py --period 1년 --skip-pdf
```

### GitHub Actions

1. **Secrets 설정** (Settings → Secrets and variables → Actions)
   - `GOOGLE_SERVICE`: 서비스 계정 JSON
   - `VALUEUP_GSPREAD_ID`: 스프레드시트 ID

2. **수동 실행**
   - Actions → KRX Value-Up Monitor → Run workflow
   - 기간, 페이지 수, PDF 옵션 선택

3. **스케줄 실행**
   - 매주 월요일 오전 9시 (KST) 자동 실행

## 조기 종료 조건

조회 기간 외 공시가 발견되면 크롤링을 조기 종료합니다:

```
페이지 3 파싱 중...
  발견: 20건
    [SKIP] 파트론 - 2025-12-08 (기간 외)
    [SKIP] 이녹스첨단소재 - 2025-12-05 (기간 외)
  필터 후: 5건 추가 (제외: 15건)
  조회 기간 외 공시 다수 발견, 크롤링 종료
```

**종료 조건:**
- 페이지의 절반 이상이 기간 외 공시인 경우
- 전체 페이지가 기간 외인 경우 즉시 종료

## API Quota 최적화

Google Sheets API quota를 절약하기 위한 배치 처리:

| 작업 | 이전 | 이후 |
|------|------|------|
| 공시 추가 | 건별 호출 | `append_rows()` 1회 |
| 링크 업데이트 | 건별 `update_cell()` | `batch_update()` 1회 |
| 시트 확장 | 오류 발생 후 처리 | 사전 용량 확보 |

## OAuth2 설정 (Google Drive 업로드)

서비스 계정은 개인 드라이브에 업로드할 수 없습니다. 개인 드라이브 업로드를 위해 OAuth2 설정이 필요합니다.

자세한 설정 방법은 [OAUTH_SETUP_GUIDE.md](OAUTH_SETUP_GUIDE.md)를 참조하세요.

**요약:**
1. Google Cloud Console에서 OAuth 클라이언트 ID 생성 (웹 애플리케이션)
2. OAuth Playground에서 리프레시 토큰 획득
3. GitHub Secrets에 토큰 저장

## PDF 다운로드 방법

첨부 PDF를 우선으로 다운로드합니다:

1. **첨부문서 드롭다운** → 기타공시첨부서류 선택 → PDF 링크 추출 (우선)
2. `filedownload('pdf')` JavaScript 호출 (fallback)
3. PDF 버튼 클릭 (fallback)

## 필터링

다음 공시는 자동으로 제외됩니다:
- "예고" 포함 공시 (예: 기업가치 제고 계획 예고)
- "안내공시" 포함 공시

## 아티팩트 정보

J열에 저장되는 아티팩트 정보 형식:
```
Archive_pdf/20251226_한미반도체_20251226000082.pdf|run_id:12345678901
```

다음 Action에서 아티팩트 조회:
```python
# J열 파싱
filepath, run_info = artifact_link.split('|')
run_id = run_info.replace('run_id:', '')

# GitHub API로 아티팩트 다운로드
# GET /repos/{owner}/{repo}/actions/runs/{run_id}/artifacts
```

## 의존성

```
playwright>=1.40.0       # 브라우저 자동화
gspread>=5.12.0          # Google Sheets API
google-auth>=2.25.0      # Google 인증
google-auth-oauthlib>=1.2.0
google-api-python-client>=2.111.0
python-dateutil>=2.8.2
requests>=2.31.0
aiohttp>=3.9.0           # 비동기 HTTP
pykrx>=1.0.45            # 종목코드 조회
```

## 문제 해결

### "Range exceeds grid limits"
- 시트 열/행 수 부족
- 자동 확장 기능으로 해결됨 (v2.0+)

### "스프레드시트를 찾을 수 없습니다"
- 서비스 계정 이메일에 스프레드시트 편집 권한 부여 필요
- 스프레드시트 ID 확인

### "Storage quota exceeded" (Google Drive)
- 서비스 계정은 개인 드라이브에 업로드 불가
- OAuth2 설정 필요 (OAUTH_SETUP_GUIDE.md 참조)

### PDF 다운로드 실패
- 디버그 아티팩트 확인 (`krx_debug`)
- 스크린샷으로 페이지 상태 확인

### 종목코드 조회 실패
- pykrx 또는 KRX API 네트워크 문제
- 비상장 기업인 경우 조회 불가

## 라이선스

MIT License
