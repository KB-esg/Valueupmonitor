# Google Drive OAuth2 설정 가이드

GitHub Actions에서 개인 Google Drive에 파일을 업로드하려면 OAuth2 인증이 필요합니다.  
이 가이드는 **웹 브라우저만으로** 토큰을 획득하는 방법을 설명합니다.

---

## 1단계: Google Cloud Console 설정

### 1.1 OAuth 동의 화면 설정

1. [Google Cloud Console](https://console.cloud.google.com) 접속
2. 기존 프로젝트 선택 (서비스 계정과 같은 프로젝트)
3. **API 및 서비스** → **OAuth 동의 화면**
4. 사용자 유형: **외부** 선택 → **만들기**
5. 앱 정보 입력:
   - 앱 이름: `ValueUp Monitor` (아무거나)
   - 사용자 지원 이메일: 본인 이메일
   - 개발자 연락처: 본인 이메일
6. **저장 후 계속**
7. 범위(Scopes) 페이지: **범위 추가 또는 삭제** 클릭
   - `https://www.googleapis.com/auth/drive.file` 선택 ✅
   - (`drive` 전체 권한은 선택하지 않음 - 심사 필요)
8. **저장 후 계속**
9. 테스트 사용자 페이지: **Add Users** 클릭
   - **본인 Gmail 주소 추가** (중요!)
10. **저장 후 계속** → **대시보드로 돌아가기**

### 1.2 OAuth 클라이언트 ID 생성

1. **API 및 서비스** → **사용자 인증 정보**
2. **+ 사용자 인증 정보 만들기** → **OAuth 클라이언트 ID**
3. 애플리케이션 유형: **웹 애플리케이션**
4. 이름: `ValueUp OAuth` (아무거나)
5. **승인된 리디렉션 URI** 추가:
   ```
   https://developers.google.com/oauthplayground
   ```
6. **만들기** 클릭
7. **클라이언트 ID**와 **클라이언트 보안 비밀번호** 복사해두기

---

## 2단계: OAuth Playground에서 토큰 획득

### 2.1 OAuth Playground 접속

1. [OAuth 2.0 Playground](https://developers.google.com/oauthplayground) 접속

### 2.2 설정

1. 우측 상단 **⚙️ (설정)** 아이콘 클릭
2. **Use your own OAuth credentials** 체크 ✅
3. 아래 정보 입력:
   - **OAuth Client ID**: 1단계에서 복사한 클라이언트 ID
   - **OAuth Client secret**: 1단계에서 복사한 클라이언트 보안 비밀번호
4. **Close** 클릭

### 2.3 API 범위 선택

1. 왼쪽 목록에서 **Drive API v3** 찾기
2. 다음 항목만 체크:
   - ✅ `https://www.googleapis.com/auth/drive.file`
   - ❌ `https://www.googleapis.com/auth/drive` (체크하지 않음)
3. **Authorize APIs** 클릭

### 2.4 Google 계정 인증

1. Google 계정 선택 (테스트 사용자로 등록한 계정)
2. "Google hasn't verified this app" 경고 → **Continue** 클릭
3. 권한 요청 화면 → **Continue** 클릭

### 2.5 토큰 교환

1. **Exchange authorization code for tokens** 클릭
2. 우측에 토큰 정보 표시됨
3. **Refresh token** 값 복사 (중요!)

---

## 3단계: GitHub Secrets 설정

GitHub 저장소 → **Settings** → **Secrets and variables** → **Actions**

다음 3개의 Secret 추가:

| Secret 이름 | 값 |
|-------------|-----|
| `GDRIVE_CLIENT_ID` | 1단계에서 복사한 클라이언트 ID |
| `GDRIVE_CLIENT_SECRET` | 1단계에서 복사한 클라이언트 보안 비밀번호 |
| `GDRIVE_REFRESH_TOKEN` | 2단계에서 복사한 Refresh token |

### 선택 사항

| Secret 이름 | 값 | 설명 |
|-------------|-----|------|
| `VALUEUP_ARCHIVE_ID` | 폴더 ID | 업로드할 폴더 지정 (없으면 루트) |

**폴더 ID 확인 방법:**  
Google Drive에서 폴더 열기 → URL에서 `folders/` 뒤의 문자열
```
https://drive.google.com/drive/folders/1ABC...XYZ
                                        ↑ 이 부분이 폴더 ID
```

---

## 4단계: 테스트

1. GitHub Actions → **KRX Value-Up Monitor** 워크플로우
2. **Run workflow** 클릭
3. 로그에서 확인:
   ```
   Google Drive 인증: OAuth2 (개인 계정)
   ```

---

## 문제 해결

### "Access blocked" 오류
- OAuth 동의 화면에서 **테스트 사용자**에 본인 이메일 추가했는지 확인

### "invalid_grant" 오류
- Refresh token이 만료됨 → 2단계부터 다시 진행
- 앱이 "테스트" 상태면 토큰은 7일 후 만료됨

### 토큰 만료 방지 (중요!)

1. Google Cloud Console → **API 및 서비스** → **OAuth 동의 화면**
2. 상단의 **"앱 게시"** 버튼 클릭
3. 확인 팝업 → **확인**

`drive.file` 범위만 사용하면 **심사 없이 즉시 게시** 가능합니다.

---

## 요약

```
Google Cloud Console          OAuth Playground           GitHub Secrets
┌─────────────────┐          ┌─────────────────┐       ┌─────────────────┐
│ 1. OAuth 동의화면 │          │ 3. 토큰 획득     │       │ 4. Secrets 저장  │
│ 2. 클라이언트 생성│ ───────▶ │    - Client ID  │ ────▶ │ GDRIVE_CLIENT_ID│
│    - Client ID  │          │    - Secret     │       │ GDRIVE_CLIENT_  │
│    - Secret     │          │    - Refresh    │       │   SECRET        │
└─────────────────┘          └─────────────────┘       │ GDRIVE_REFRESH_ │
                                                        │   TOKEN         │
                                                        └─────────────────┘
```
