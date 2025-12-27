# KRX Value-Up 공시 분석기

KRX 밸류업 공시 PDF를 LLM(Claude/Gemini)으로 분석하여 기업별 목표와 이행 현황을 추적하는 도구입니다.

## 주요 기능

- **LLM 기반 PDF 분석**: Claude Haiku(기본) 또는 Gemini Flash로 밸류업 보고서 분석
- **45개 Framework 항목**: 수익성, 주주환원, 재무건전성, 성장성, 밸류에이션 등 체계적 분석
- **기업별 팔로우업**: 동일 기업의 시간에 따른 목표 변화 추적
- **피벗 구조 이력**: 항목별로 보고서일에 따른 변화를 가로로 한눈에 파악
- **Google Sheets 연동**: 분석 결과 자동 저장 및 메타정보 관리

## 파일 구조

```
01_valueup_analysis/
├── main.py                    # 메인 실행 파일 (오케스트레이터)
├── claude_analyzer.py         # Claude API 분석기 (기본)
├── gemini_analyzer.py         # Gemini API 분석기 (대체)
├── gsheet_analyzer.py         # Google Sheets 분석 결과 관리
├── company_sheet_manager.py   # 기업별 스프레드시트 관리
├── pdf_extractor.py           # PDF 다운로드 및 텍스트 추출
├── framework_loader.py        # 분석 Framework 로더
└── README.md                  # 이 파일

.github/workflows/
└── valueup_analysis.yml       # GitHub Actions 워크플로우
```

## 실행 흐름

```
[1단계] Framework 로드 (45개 분석 항목)
    ↓
[2단계] 분석 대기 공시 조회 (밸류업공시목록 시트)
    ↓
[3단계] PDF 토큰 산정 및 캐싱
    ↓
[4단계] LLM 분석 (텍스트 우선, PDF fallback)
    │
    ├─ [4-1] 밸류업공시분석 시트에 결과 저장
    │
    ├─ [4-2] 기업별 스프레드시트에 이력 저장 (피벗 구조)
    │         └─ 01_Valueup_archive/ValueUp_analysis/기업명_종목코드
    │
    └─ [4-3] 밸류업공시목록 시트 L~P열에 메타정보 업데이트
```

## 데이터 저장 구조

### 1. 밸류업공시목록 시트 (L~P열)

| 열 | 필드명 | 설명 |
|----|--------|------|
| L | 분석상태 | completed / error |
| M | 분석일시 | 분석 완료 시각 |
| N | 분석항목수 | 언급된 항목 수 |
| O | Core항목수 | Core 항목 중 언급된 수 |
| P | 기업시트링크 | 기업별 스프레드시트 URL |

### 2. 기업별 스프레드시트 (Google Drive)

```
01_Valueup_archive/
└── ValueUp_analysis/
    ├── 삼성전자_005930
    │   ├── Summary (기업정보 + 최신 목표 현황)
    │   └── Target_History (목표 이력 - 피벗 구조)
    ├── SK하이닉스_000660
    └── ...
```

#### Target_History 피벗 구조

| 영역 | 카테고리 | 항목ID | 항목명 | Core | 세부분류 | Level | 보고서일 | 2024-06-15 | 2024-09-20 | 2024-12-20 |
|------|----------|--------|--------|------|----------|-------|----------|------------|------------|------------|
| 수익성 | 자본수익률 | ROE | ROE | Y | 현재값 | 2 | | 7.5% | 8.0% | 8.5% |
| 수익성 | 자본수익률 | ROE | ROE | Y | 목표값 | 2 | | 12% | 12% | **15%** |
| 수익성 | 자본수익률 | ROE | ROE | Y | 목표연도 | 2 | | 2026 | 2026 | **2027** |

→ 동일 항목의 시간에 따른 변화를 가로로 한눈에 파악 가능

## 환경변수

### 필수

| 변수명 | 설명 |
|--------|------|
| `GOOGLE_SERVICE` | 서비스 계정 JSON |
| `VALUEUP_GSPREAD_ID` | Google Sheets 스프레드시트 ID |
| `ANT_ANALYTIC` | Claude API 키 (기본 분석기) |

### 선택

| 변수명 | 설명 |
|--------|------|
| `GEM_ANALYTIC` | Gemini API 키 (대체 분석기) |
| `ANALYZER_TYPE` | 분석기 선택: `claude`(기본) / `gemini` |
| `VALUEUP_ARCHIVE_ID` | 기업별 시트 저장 폴더 ID |
| `GDRIVE_REFRESH_TOKEN` | OAuth2 리프레시 토큰 |
| `GDRIVE_CLIENT_ID` | OAuth2 클라이언트 ID |
| `GDRIVE_CLIENT_SECRET` | OAuth2 클라이언트 시크릿 |

### 실행 옵션

| 변수명 | 기본값 | 설명 |
|--------|--------|------|
| `VALUEUP_DAYS` | 7 | 분석 대상 기간 (일) |
| `VALUEUP_PERIOD` | - | 기간 버튼 (1주, 1개월, 3개월 등) |
| `VALUEUP_MAX_ITEMS` | 10 | 최대 분석 건수 |
| `VALUEUP_DRY_RUN` | false | 테스트 모드 (저장 안함) |

## 설치 및 실행

### 로컬 실행

```bash
# 의존성 설치
pip install gspread google-auth google-auth-oauthlib google-api-python-client
pip install pdfplumber anthropic

# 환경변수 설정
export GOOGLE_SERVICE='{"type": "service_account", ...}'
export VALUEUP_GSPREAD_ID='your_spreadsheet_id'
export ANT_ANALYTIC='sk-ant-...'

# 실행
cd 01_valueup_analysis
python main.py --days 7 --max-items 5
```

### CLI 옵션

```bash
# 최근 7일, 최대 10건 분석 (기본값)
python main.py

# 최근 30일 분석
python main.py --days 30

# 기간 버튼 사용 (3개월)
python main.py --period 3개월

# 최대 5건만 분석
python main.py --max-items 5

# 테스트 모드 (저장 안함)
python main.py --dry-run

# Gemini 분석기 사용
ANALYZER_TYPE=gemini python main.py
```

### GitHub Actions

1. **Secrets 설정** (Settings → Secrets and variables → Actions)
   - `GOOGLE_SERVICE`: 서비스 계정 JSON
   - `VALUEUP_GSPREAD_ID`: 스프레드시트 ID
   - `ANT_ANALYTIC`: Claude API 키
   - `VALUEUP_ARCHIVE_ID`: (선택) 기업별 시트 저장 폴더 ID

2. **수동 실행**
   - Actions → Value-Up Analysis (Claude) → Run workflow
   - 기간, 분석 건수, 분석기 선택

3. **스케줄 실행**
   - 매일 오전 10시 (KST) 자동 실행

## LLM 분석기 비교

| 항목 | Claude Haiku (기본) | Gemini Flash |
|------|---------------------|--------------|
| 모델 | claude-3-5-haiku-20241022 | gemini-2.0-flash |
| 가격 | $0.25/$1.25 per 1M tokens | Free tier 불안정 |
| 안정성 | 안정적 | Rate limit 빈번 |
| 컨텍스트 | 200K tokens | 1M tokens |

## Framework 분석 항목 (45개)

### 영역별 구성

| 영역 | 항목 수 | Core 항목 예시 |
|------|---------|----------------|
| 수익성 | 10개 | ROE, ROIC |
| 주주환원 | 8개 | 배당성향, 자사주 매입 |
| 재무건전성 | 7개 | 부채비율 |
| 성장성 | 8개 | 매출성장률 |
| 밸류에이션 | 8개 | PBR, PER |
| 기타 | 4개 | 지배구조 |

### 분석 Level

| Level | 의미 |
|-------|------|
| 0 | 언급 없음 |
| 1 | 단순 언급 (목표 없음) |
| 2 | 구체적 목표 제시 |
| 3 | 목표 + 실행계획 |

## 비용 추정

### Claude Haiku

- Input: $0.25 / 1M tokens
- Output: $1.25 / 1M tokens
- 분석 1건: ~12K input, ~2K output
- **월 100건: ~$0.55**

## 문제 해결

### "Claude API 연결 실패"
- `ANT_ANALYTIC` 환경변수 확인
- API 키 유효성 확인: https://console.anthropic.com/

### "기업별 시트 저장 비활성화"
- `VALUEUP_ARCHIVE_ID` 환경변수 미설정 시 정상 동작
- 기업별 시트 저장이 필요하면 폴더 ID 설정

### "Rate Limit 초과"
- Claude: 분당 15회, 100만 토큰 제한
- 연속 3회 실패 시 자동 중단
- 다음 실행에서 재시도

### "PDF 다운로드 실패"
- OAuth2 토큰 만료 확인
- Google Drive 접근 권한 확인

## 의존성

```
# Google APIs
gspread>=5.12.0
google-auth>=2.25.0
google-auth-oauthlib>=1.1.0
google-api-python-client>=2.111.0

# PDF Processing
pdfplumber>=0.10.3

# LLM APIs
anthropic>=0.39.0    # Claude API (기본)
# google-genai>=0.3.0  # Gemini API (선택)
```

## 라이선스

MIT License
