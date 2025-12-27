"""
KRX Value-Up 공시 크롤러
Playwright 기반으로 KRX KIND 사이트에서 밸류업 공시를 크롤링
"""

import asyncio
import re
import os
import tempfile
from datetime import datetime, timedelta
from dataclasses import dataclass
from typing import List, Optional
from playwright.async_api import async_playwright, Page, Browser


def log(msg: str):
    """실시간 로그 출력 (GitHub Actions 호환)"""
    timestamp = datetime.now().strftime("%H:%M:%S")
    print(f"[{timestamp}] {msg}", flush=True)


@dataclass
class DisclosureItem:
    """공시 항목 데이터 클래스"""
    번호: int
    공시일자: str
    회사명: str
    종목코드: str
    공시제목: str
    접수번호: str  # acptno
    문서번호: str  # docNo
    원시PDF링크: str
    구글드라이브링크: str = ""


class KRXValueUpCrawler:
    """KRX 밸류업 공시 크롤러"""
    
    BASE_URL = "https://kind.krx.co.kr"
    LIST_URL = f"{BASE_URL}/valueup/disclsstat.do?method=valueupDisclsStatMain"
    VIEWER_URL = f"{BASE_URL}/common/disclsviewer.do"
    PDF_DOWNLOAD_URL = f"{BASE_URL}/common/pdfDownload.do"
    
    # 기간 버튼 매핑
    PERIOD_BUTTONS = {
        '1주': '1주',
        '1개월': '1개월',
        '3개월': '3개월',
        '6개월': '6개월',
        '1년': '1년',
        '2년': '2년',
        '3년': '3년',
        '전체': '전체'
    }
    
    def __init__(self, headless: bool = True, debug_dir: Optional[str] = None):
        """
        초기화
        
        Args:
            headless: 헤드리스 모드 여부
            debug_dir: 디버그 파일 저장 디렉토리 (None이면 환경변수에서 읽음)
        """
        self.headless = headless
        # 환경변수에서 디버그 디렉토리 읽기
        if debug_dir is None:
            env_debug = os.environ.get('VALUEUP_DEBUG', 'false').lower()
            if env_debug == 'true':
                self.debug_dir = os.environ.get('VALUEUP_DEBUG_DIR', '/tmp/krx_debug')
            else:
                self.debug_dir = None
        else:
            self.debug_dir = debug_dir
            
        self.browser: Optional[Browser] = None
        self.page: Optional[Page] = None
        
        # 디버그 디렉토리 생성
        if self.debug_dir:
            os.makedirs(self.debug_dir, exist_ok=True)
            log(f"디버그 모드 활성화: {self.debug_dir}")
        
    async def __aenter__(self):
        await self.start()
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
        
    async def start(self):
        """브라우저 시작"""
        log("브라우저 시작 중...")
        self.playwright = await async_playwright().start()
        self.browser = await self.playwright.chromium.launch(headless=self.headless)
        self.context = await self.browser.new_context(
            viewport={"width": 1920, "height": 1080},
            locale="ko-KR",
            accept_downloads=True  # 다운로드 허용
        )
        self.page = await self.context.new_page()
        log("브라우저 시작 완료")
        
    async def close(self):
        """브라우저 종료"""
        log("브라우저 종료 중...")
        if self.browser:
            await self.browser.close()
        if self.playwright:
            await self.playwright.stop()
        log("브라우저 종료 완료")
    
    async def _save_debug_screenshot(self, page: Page, name: str):
        """디버그용 스크린샷 저장"""
        if not self.debug_dir:
            return
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filepath = os.path.join(self.debug_dir, f"{timestamp}_{name}.png")
        try:
            await page.screenshot(path=filepath, full_page=True)
            log(f"  [DEBUG] 스크린샷 저장: {filepath}")
        except Exception as e:
            log(f"  [DEBUG] 스크린샷 저장 실패: {e}")
    
    async def _save_debug_html(self, page: Page, name: str):
        """디버그용 HTML 저장"""
        if not self.debug_dir:
            return
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filepath = os.path.join(self.debug_dir, f"{timestamp}_{name}.html")
        try:
            content = await page.content()
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(content)
            log(f"  [DEBUG] HTML 저장: {filepath}")
        except Exception as e:
            log(f"  [DEBUG] HTML 저장 실패: {e}")
    
    async def _save_debug_js(self, page: Page, name: str, script_selector: str = "script"):
        """디버그용 JavaScript 저장"""
        if not self.debug_dir:
            return
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filepath = os.path.join(self.debug_dir, f"{timestamp}_{name}.js")
        
        try:
            # 모든 스크립트 태그 내용 추출
            scripts = await page.locator(script_selector).all()
            js_content = []
            for i, script in enumerate(scripts):
                try:
                    text = await script.text_content()
                    if text and text.strip():
                        js_content.append(f"// === Script {i+1} ===\n{text}\n")
                except:
                    pass
            
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write('\n'.join(js_content))
            log(f"  [DEBUG] JS 저장: {filepath}")
        except Exception as e:
            log(f"  [DEBUG] JS 저장 실패: {e}")
    
    async def click_period_button(self, period: str) -> bool:
        """
        기간 버튼 클릭
        
        Args:
            period: 기간 문자열 ('1주', '1개월', '3개월', '6개월', '1년', '전체' 등)
            
        Returns:
            성공 여부
        """
        if period not in self.PERIOD_BUTTONS:
            log(f"  지원하지 않는 기간: {period}")
            return False
        
        try:
            # 밸류업 페이지의 기간 버튼 셀렉터 (더 구체적)
            selectors = [
                f'.btn-set.dates a:has-text("{period}")',
                f'ul.btn-set a:has-text("{period}")',
                f'.period-btn:has-text("{period}")',
                f'.search-period a:has-text("{period}")',
                f'a.period:has-text("{period}")',
                f'a:has-text("{period}")',
                f'button:has-text("{period}")',
                f'span:has-text("{period}")',
                f'input[value="{period}"]',
            ]
            
            for selector in selectors:
                btn = self.page.locator(selector).first
                if await btn.count() > 0:
                    await btn.click()
                    log(f"  기간 버튼 클릭: {period}")
                    await asyncio.sleep(2)
                    return True
            
            log(f"  기간 버튼을 찾을 수 없음: {period}")
            return False
            
        except Exception as e:
            log(f"  기간 버튼 클릭 오류: {e}")
            return False
    
    async def click_search_button(self) -> bool:
        """검색 버튼 클릭 - 밸류업 페이지에 특화된 셀렉터 사용"""
        try:
            # 밸류업 페이지의 검색 버튼 셀렉터 (더 구체적인 것부터 시도)
            selectors = [
                # 이미지 버튼 (가장 일반적)
                'img[alt="검색"]',
                'a:has(img[alt="검색"])',
                # 클래스 기반
                'a.search-btn',
                'button.search-btn',
                '.search-btn',
                # 검색 영역 내 버튼
                '.search-area a:has-text("검색")',
                '.search-box a:has-text("검색")',
                'form a:has-text("검색")',
                # input 버튼
                'input[type="button"][value="검색"]',
                'input[type="submit"][value="검색"]',
                # 정확한 텍스트 매칭 (회사별검색 등 제외)
                'a:text-is("검색")',
                'button:text-is("검색")',
            ]
            
            for selector in selectors:
                try:
                    btn = self.page.locator(selector).first
                    if await btn.count() > 0:
                        # 버튼이 보이는지 확인
                        is_visible = await btn.is_visible()
                        if is_visible:
                            await btn.click()
                            log(f"  검색 버튼 클릭 성공: {selector}")
                            await asyncio.sleep(2)
                            return True
                except Exception:
                    continue
            
            log("  검색 버튼을 찾을 수 없음 (검색 없이 계속 진행)")
            return False
            
        except Exception as e:
            log(f"  검색 버튼 클릭 오류: {e}")
            return False
    
    async def get_total_pages(self) -> int:
        """총 페이지 수 추출"""
        try:
            # 페이지네이션에서 마지막 페이지 번호 찾기
            paging = self.page.locator('.paging, .pagination, nav[aria-label*="page"]').first
            if await paging.count() > 0:
                # 마지막 페이지 링크 또는 텍스트 찾기
                last_page = await paging.locator('a:last-child, span:last-child').text_content()
                if last_page:
                    match = re.search(r'\d+', last_page)
                    if match:
                        return int(match.group())
            
            # "전체 N건" 형태에서 추출
            total_text = await self.page.locator('*:has-text("전체")').first.text_content()
            if total_text:
                match = re.search(r'전체\s*(\d+)', total_text)
                if match:
                    total = int(match.group(1))
                    return (total // 15) + 1  # 페이지당 15건 가정
            
            return 1
            
        except Exception as e:
            log(f"  총 페이지 수 추출 오류: {e}")
            return 1
    
    async def go_to_page(self, page_num: int) -> bool:
        """특정 페이지로 이동 - 페이지네이션 영역 내에서만 검색"""
        try:
            # 페이지네이션 영역 내에서만 페이지 번호 찾기 (정확한 텍스트 매칭)
            paging_selectors = [
                f'.paging a:text-is("{page_num}")',
                f'.paging-group a:text-is("{page_num}")',
                f'section.paging-group a:text-is("{page_num}")',
                f'.pagination a:text-is("{page_num}")',
                f'div.paging a:text-is("{page_num}")',
            ]
            
            for selector in paging_selectors:
                try:
                    page_link = self.page.locator(selector).first
                    if await page_link.count() > 0:
                        is_visible = await page_link.is_visible()
                        if is_visible:
                            await page_link.click()
                            log(f"  페이지 {page_num}으로 이동 (셀렉터: {selector})")
                            await asyncio.sleep(2)
                            return True
                except Exception:
                    continue
            
            # JavaScript로 페이지 이동 시도 (fnPageGo 함수 - Colab 코드에서 발견)
            try:
                await self.page.evaluate(f"fnPageGo('{page_num}')")
                log(f"  JavaScript fnPageGo('{page_num}')로 이동")
                await asyncio.sleep(2)
                return True
            except Exception:
                pass
            
            # goPage 함수 시도
            try:
                await self.page.evaluate(f'goPage({page_num})')
                log(f"  JavaScript goPage({page_num})로 이동")
                await asyncio.sleep(2)
                return True
            except Exception:
                pass
            
            log(f"  페이지 {page_num} 링크를 찾을 수 없음 (1페이지만 존재할 수 있음)")
            return False
            
        except Exception as e:
            log(f"  페이지 이동 오류: {e}")
            return False
    
    async def parse_current_page(self) -> List[DisclosureItem]:
        """현재 페이지의 공시 목록 파싱 - 밸류업 페이지 특화"""
        items = []
        
        # 테이블 행 추출 (여러 셀렉터 시도)
        table_selectors = [
            'table.list tbody tr',
            'table tbody tr',
            '.board-list tbody tr',
            '#grid tbody tr',
        ]
        
        rows = []
        for selector in table_selectors:
            rows = await self.page.locator(selector).all()
            if rows:
                log(f"  테이블 셀렉터 사용: {selector}")
                break
        
        log(f"  현재 페이지 행 수: {len(rows)}")
        
        for row_idx, row in enumerate(rows):
            try:
                # 행 전체 HTML 로깅 (디버그용)
                row_html = await row.inner_html()
                if row_idx == 0 and self.debug_dir:
                    log(f"  [DEBUG] 첫 번째 행 HTML (처음 500자): {row_html[:500]}")
                
                cells = await row.locator('td').all()
                if len(cells) < 3:
                    continue
                
                # 접수번호 추출 - 여러 패턴 시도
                접수번호 = ""
                
                # 패턴 1: openDisclsViewer('접수번호')
                match = re.search(r"openDisclsViewer\s*\(\s*['\"]?(\d+)['\"]?", row_html)
                if match:
                    접수번호 = match.group(1)
                
                # 패턴 2: openPop('접수번호') 또는 다른 팝업 함수
                if not 접수번호:
                    match = re.search(r"openPop\s*\(\s*['\"]?(\d+)['\"]?", row_html)
                    if match:
                        접수번호 = match.group(1)
                
                # 패턴 3: acptno=접수번호 또는 acptNo=접수번호
                if not 접수번호:
                    match = re.search(r'acpt[Nn]o[=\'"\s:]+(\d{14,})', row_html)
                    if match:
                        접수번호 = match.group(1)
                
                # 패턴 4: href에서 접수번호 추출
                if not 접수번호:
                    match = re.search(r'href="[^"]*?(\d{14,})[^"]*"', row_html)
                    if match:
                        접수번호 = match.group(1)
                
                # 패턴 5: onclick에서 14자리 이상 숫자 추출
                if not 접수번호:
                    match = re.search(r'onclick="[^"]*?(\d{14,})[^"]*"', row_html)
                    if match:
                        접수번호 = match.group(1)
                
                # 패턴 6: 어떤 속성이든 14자리 숫자 추출
                if not 접수번호:
                    match = re.search(r'[\'"](\d{14,})[\'"]', row_html)
                    if match:
                        접수번호 = match.group(1)
                
                if not 접수번호:
                    log(f"  [SKIP] 행 {row_idx}: 접수번호 추출 실패")
                    continue
                
                log(f"  [FOUND] 접수번호: {접수번호}")
                
                # 번호 (첫 번째 셀)
                번호_text = await cells[0].text_content()
                번호 = int(번호_text.strip()) if 번호_text and 번호_text.strip().isdigit() else 0
                
                # 셀 개수에 따라 다른 파싱 로직
                if len(cells) >= 5:
                    # 일반적인 구조: 번호 | 공시일자 | 회사명 | 종목코드 | 공시제목
                    공시일자 = (await cells[1].text_content() or "").strip()
                    회사명 = (await cells[2].text_content() or "").strip().split()[0] if await cells[2].text_content() else ""
                    종목코드_text = (await cells[3].text_content() or "").strip()
                    공시제목 = (await cells[4].text_content() or "").strip()
                    
                elif len(cells) >= 4:
                    # 대체 구조: 번호 | 공시일자 | 회사명(종목코드) | 공시제목
                    공시일자 = (await cells[1].text_content() or "").strip()
                    회사명_full = (await cells[2].text_content() or "").strip()
                    회사명 = 회사명_full.split()[0] if 회사명_full else ""
                    종목코드_text = 회사명_full
                    공시제목 = (await cells[3].text_content() or "").strip()
                    
                else:
                    # 최소 구조
                    공시일자 = (await cells[1].text_content() or "").strip() if len(cells) > 1 else ""
                    회사명 = ""
                    종목코드_text = ""
                    공시제목 = ""
                
                # "예고" 또는 "안내공시" 포함된 공시 제외
                if "예고" in 공시제목 or "안내공시" in 공시제목:
                    log(f"  [SKIP] 예고/안내공시 제외: {공시제목[:40]}...")
                    continue
                
                # 종목코드 추출 (6자리 숫자)
                종목코드 = ""
                if 종목코드_text:
                    stock_match = re.search(r'[A-Z]?\d{6}', 종목코드_text)
                    if stock_match:
                        종목코드 = stock_match.group()
                
                원시PDF링크 = f"{self.PDF_DOWNLOAD_URL}?method=pdfDown&acptNo={접수번호}"
                
                item = DisclosureItem(
                    번호=번호,
                    공시일자=공시일자,
                    회사명=회사명,
                    종목코드=종목코드,
                    공시제목=공시제목,
                    접수번호=접수번호,
                    문서번호="",
                    원시PDF링크=원시PDF링크
                )
                items.append(item)
                log(f"  [OK] {공시일자} | {회사명} | {공시제목[:30]}...")
                    
            except Exception as e:
                log(f"  행 파싱 오류: {e}")
                continue
        
        return items
    
    async def get_disclosure_list(
        self, 
        days: int = 7, 
        period: Optional[str] = None,
        max_pages: int = 10
    ) -> List[DisclosureItem]:
        """
        공시 목록 조회
        
        Args:
            days: 조회할 기간(일), 기본 7일
            period: 기간 버튼 ('1주', '1개월' 등) - 지정시 days 무시
            max_pages: 최대 크롤링 페이지 수
            
        Returns:
            공시 항목 리스트
        """
        all_items = []
        
        # 페이지 로드
        log(f"공시 목록 페이지 로드 중...")
        await self.page.goto(self.LIST_URL, wait_until="networkidle")
        await asyncio.sleep(3)
        
        # 디버그: 초기 페이지 저장
        await self._save_debug_screenshot(self.page, "01_list_initial")
        await self._save_debug_html(self.page, "01_list_initial")
        await self._save_debug_js(self.page, "01_list_initial")
        
        # 컷오프 날짜 계산 (period 여부와 관계없이)
        # period 사용 시에도 days 기준으로 컷오프 적용
        end_date = datetime.now()
        
        if period:
            # 기간 버튼에 따른 days 계산
            period_to_days = {
                '1주': 7,
                '1개월': 30,
                '3개월': 90,
                '6개월': 180,
                '1년': 365,
                '2년': 730,
                '3년': 1095,
                '전체': 3650  # 약 10년
            }
            effective_days = period_to_days.get(period, days)
            log(f"기간 버튼 클릭: {period} (약 {effective_days}일)")
            await self.click_period_button(period)
            await asyncio.sleep(2)
        else:
            effective_days = days
            # 날짜 범위로 검색
            start_date = end_date - timedelta(days=days)
            log(f"날짜 범위 설정: {start_date.strftime('%Y-%m-%d')} ~ {end_date.strftime('%Y-%m-%d')}")
            
            try:
                start_selectors = ['input#fromDate', 'input[name="fromDate"]', 'input.from-date']
                end_selectors = ['input#toDate', 'input[name="toDate"]', 'input.to-date']
                
                for selector in start_selectors:
                    start_input = self.page.locator(selector).first
                    if await start_input.count() > 0:
                        await start_input.fill(start_date.strftime("%Y-%m-%d"))
                        log(f"  시작일 입력 완료: {selector}")
                        break
                
                for selector in end_selectors:
                    end_input = self.page.locator(selector).first
                    if await end_input.count() > 0:
                        await end_input.fill(end_date.strftime("%Y-%m-%d"))
                        log(f"  종료일 입력 완료: {selector}")
                        break
                
                await self.click_search_button()
                
            except Exception as e:
                log(f"날짜 설정 오류: {e}")
        
        # 컷오프 날짜 설정 (항상 적용)
        cutoff_date = end_date - timedelta(days=effective_days)
        log(f"  컷오프 날짜: {cutoff_date.strftime('%Y-%m-%d')} 이전 공시는 제외")
        
        # 디버그: 검색 후 저장
        await self._save_debug_screenshot(self.page, "02_list_after_search")
        await self._save_debug_html(self.page, "02_list_after_search")
        
        # 페이지별 크롤링
        consecutive_old_count = 0  # 연속으로 오래된 공시가 나온 횟수
        
        for page_num in range(1, max_pages + 1):
            log(f"페이지 {page_num} 파싱 중...")
            
            page_items = await self.parse_current_page()
            log(f"  발견: {len(page_items)}건")
            
            if not page_items:
                log("  더 이상 항목 없음, 종료")
                break
            
            # 날짜 필터링
            filtered_items = []
            old_items_in_page = 0
            
            for item in page_items:
                try:
                    # 날짜 파싱
                    date_str = item.공시일자.replace('.', '-').strip()
                    if ' ' in date_str:
                        date_str = date_str.split(' ')[0]
                    
                    if len(date_str) == 10:
                        item_date = datetime.strptime(date_str, "%Y-%m-%d")
                    elif len(date_str) == 8:
                        item_date = datetime.strptime(date_str, "%Y%m%d")
                    else:
                        # 날짜 파싱 실패시 포함
                        filtered_items.append(item)
                        continue
                    
                    if item_date >= cutoff_date:
                        filtered_items.append(item)
                        consecutive_old_count = 0  # 리셋
                    else:
                        old_items_in_page += 1
                        log(f"    [SKIP] {item.회사명} - {item.공시일자} (기간 외)")
                        
                except Exception as e:
                    # 예외 발생시 포함 (안전)
                    filtered_items.append(item)
            
            all_items.extend(filtered_items)
            log(f"  필터 후: {len(filtered_items)}건 추가 (제외: {old_items_in_page}건)")
            
            # 조기 종료 조건: 페이지의 절반 이상이 기간 외 공시인 경우
            if old_items_in_page > len(page_items) // 2:
                consecutive_old_count += 1
                if consecutive_old_count >= 1:  # 한 페이지라도 절반 이상 기간 외면 종료
                    log(f"  조회 기간 외 공시 다수 발견, 크롤링 종료")
                    break
            else:
                consecutive_old_count = 0
            
            # 전체 페이지가 기간 외인 경우 즉시 종료
            if len(filtered_items) == 0 and len(page_items) > 0:
                log(f"  현재 페이지 전체가 조회 기간 외, 크롤링 종료")
                break
            
            # 다음 페이지로 이동
            if page_num < max_pages:
                if not await self.go_to_page(page_num + 1):
                    break
                await asyncio.sleep(1)
        
        # 중복 접수번호 제거
        seen_acptno = set()
        unique_items = []
        for item in all_items:
            if item.접수번호 not in seen_acptno:
                seen_acptno.add(item.접수번호)
                unique_items.append(item)
        
        if len(unique_items) < len(all_items):
            log(f"  중복 제거: {len(all_items)} → {len(unique_items)}건")
        
        log(f"총 {len(unique_items)}건 수집 완료")
        return unique_items
    
    async def download_pdf(self, acptno: str, doc_no: str = "") -> Optional[bytes]:
        """
        PDF 다운로드 - 첨부문서 PDF 우선
        
        다운로드 순서:
        1. 첨부문서(기타공시첨부서류)에서 PDF 링크 찾기 (기업 제출 원본 PDF)
        2. filedownload('pdf') JavaScript 호출 (본문 PDF - fallback)
        3. PDF 버튼 클릭 (본문 PDF - fallback)
        
        Args:
            acptno: 접수번호
            doc_no: 문서번호 (사용하지 않음, 호환성 유지)
            
        Returns:
            PDF 바이너리 데이터 또는 None
        """
        log(f"  PDF 다운로드 시작: acptno={acptno}")
        
        viewer_url = f"{self.VIEWER_URL}?method=search&acptno={acptno}"
        
        page = await self.context.new_page()
        try:
            # 1. 뷰어 페이지 열기
            await page.goto(viewer_url, wait_until="networkidle")
            await asyncio.sleep(5)  # iframe 로딩 대기
            
            # 디버그: 다운로드 전 상태 저장
            await self._save_debug_screenshot(page, f"pdf_viewer_{acptno}")
            
            # ========== 방법 1: 첨부문서에서 PDF 링크 찾기 (우선!) ==========
            log(f"    [방법1] 첨부문서(기타공시첨부서류)에서 PDF 검색...")
            attach_select = page.locator('select#attachedDoc')
            
            if await attach_select.count() > 0:
                options = await attach_select.locator('option').all()
                log(f"    첨부문서 드롭다운 옵션 수: {len(options)}")
                
                # 옵션 목록 출력 (디버그)
                for opt in options:
                    opt_text = await opt.text_content() or ""
                    opt_value = await opt.get_attribute('value') or ""
                    log(f"      - '{opt_text[:40]}' (value: {opt_value[:30] if opt_value else 'empty'})")
                
                # 기타공시첨부서류 또는 첨부서류가 있는 옵션 찾기
                for option in options:
                    option_text = await option.text_content() or ""
                    option_value = await option.get_attribute('value') or ""
                    
                    # 빈 값이거나 "선택" 옵션은 건너뛰기
                    if not option_value or "선택" in option_text:
                        continue
                    
                    # 첨부서류 관련 옵션인지 확인
                    is_attachment = any(keyword in option_text for keyword in [
                        '첨부', '기타공시', '기타공개', '첨부서류', '첨부문서'
                    ])
                    
                    if not is_attachment:
                        log(f"    건너뜀 (첨부서류 아님): {option_text[:40]}")
                        continue
                    
                    log(f"    첨부서류 선택: {option_text[:50]}...")
                    
                    # 옵션 선택
                    await attach_select.select_option(value=option_value)
                    await asyncio.sleep(3)  # iframe 로딩 대기
                    
                    # 디버그: 첨부서류 선택 후 상태 저장
                    await self._save_debug_screenshot(page, f"pdf_attached_{acptno}")
                    
                    # iframe에서 PDF 링크 찾기
                    pdf_url = await self._find_pdf_in_iframe(page)
                    if pdf_url:
                        log(f"    PDF URL 발견: {pdf_url[:80]}...")
                        pdf_data = await self._download_pdf_from_url(pdf_url)
                        if pdf_data:
                            log(f"    첨부 PDF 다운로드 성공: {len(pdf_data):,} bytes")
                            return pdf_data
                        else:
                            log(f"    첨부 PDF 다운로드 실패, 다음 방법 시도")
                    else:
                        log(f"    iframe에서 PDF 링크를 찾을 수 없음")
            else:
                log(f"    첨부문서 드롭다운(#attachedDoc)을 찾을 수 없음")
            
            # ========== 방법 2: filedownload('pdf') JavaScript 호출 (본문 PDF) ==========
            log(f"    [방법2] filedownload('pdf') 시도 (본문 PDF)...")
            try:
                doc_no_value = await page.evaluate('''() => {
                    const form = document.getElementById("docdownloadform");
                    if (form) {
                        const docNoInput = form.querySelector("#docNo, input[name='docNo']");
                        return docNoInput ? docNoInput.value : "";
                    }
                    return "";
                }''')
                
                if doc_no_value:
                    log(f"    docNo 설정됨: {doc_no_value}")
                    
                    async with page.expect_download(timeout=30000) as download_info:
                        await page.evaluate("filedownload('pdf')")
                    
                    download = await download_info.value
                    log(f"    다운로드 파일: {download.suggested_filename}")
                    
                    with tempfile.NamedTemporaryFile(delete=False, suffix='.pdf') as tmp:
                        tmp_path = tmp.name
                    
                    await download.save_as(tmp_path)
                    
                    with open(tmp_path, 'rb') as f:
                        pdf_data = f.read()
                    
                    os.unlink(tmp_path)
                    
                    if len(pdf_data) > 1000 and pdf_data.startswith(b'%PDF'):
                        log(f"    본문 PDF 다운로드 성공: {len(pdf_data):,} bytes")
                        return pdf_data
                    else:
                        log(f"    filedownload 결과 유효하지 않음")
                else:
                    log(f"    docNo가 설정되지 않음 (본문 없음)")
                    
            except Exception as e:
                log(f"    filedownload 실패: {e}")
            
            # ========== 방법 3: PDF 버튼 직접 클릭 (본문 PDF) ==========
            log(f"    [방법3] PDF 버튼 클릭 시도 (본문 PDF)...")
            try:
                pdf_btn = page.locator('a:has(img[src*="btn_pdf"]), a:has(img[alt*="PDF"])').first
                if await pdf_btn.count() > 0:
                    async with page.expect_download(timeout=30000) as download_info:
                        await pdf_btn.click()
                    
                    download = await download_info.value
                    log(f"    다운로드 파일: {download.suggested_filename}")
                    
                    with tempfile.NamedTemporaryFile(delete=False, suffix='.pdf') as tmp:
                        tmp_path = tmp.name
                    
                    await download.save_as(tmp_path)
                    
                    with open(tmp_path, 'rb') as f:
                        pdf_data = f.read()
                    
                    os.unlink(tmp_path)
                    
                    if len(pdf_data) > 1000 and pdf_data.startswith(b'%PDF'):
                        log(f"    본문 PDF(버튼) 다운로드 성공: {len(pdf_data):,} bytes")
                        return pdf_data
                else:
                    log(f"    PDF 버튼을 찾을 수 없음")
            except Exception as e:
                log(f"    PDF 버튼 클릭 실패: {e}")
            
            log(f"    모든 방법 실패, PDF를 찾을 수 없음")
            await self._save_debug_screenshot(page, f"pdf_not_found_{acptno}")
            return None
            
        except Exception as e:
            log(f"  PDF 다운로드 오류: {e}")
            import traceback
            log(f"  {traceback.format_exc()}")
            return None
        finally:
            await page.close()
    
    async def _find_pdf_in_iframe(self, page: Page) -> Optional[str]:
        """iframe 내부에서 PDF 링크 찾기"""
        try:
            # JavaScript로 iframe 내부의 모든 링크 정보 수집
            result = await page.evaluate('''() => {
                const iframe = document.getElementById('docViewFrm');
                if (!iframe || !iframe.contentDocument) {
                    return { error: 'iframe not accessible', links: [] };
                }
                
                const allLinks = [];
                const links = iframe.contentDocument.querySelectorAll('a');
                
                for (const link of links) {
                    const href = link.getAttribute('href') || '';
                    const text = link.textContent || '';
                    if (href || text) {
                        allLinks.push({
                            href: href,
                            text: text.substring(0, 100),
                            fullHref: link.href  // 브라우저가 해석한 전체 URL
                        });
                    }
                }
                
                // PDF 링크 찾기 - href 또는 fullHref에서
                for (const linkInfo of allLinks) {
                    const href = linkInfo.href.toLowerCase();
                    const fullHref = linkInfo.fullHref.toLowerCase();
                    const text = linkInfo.text.toLowerCase();
                    
                    if (href.includes('.pdf') || fullHref.includes('.pdf') || text.includes('.pdf')) {
                        return {
                            found: true,
                            href: linkInfo.href,
                            fullHref: linkInfo.fullHref,
                            text: linkInfo.text
                        };
                    }
                }
                
                return { found: false, links: allLinks.slice(0, 10) };  // 디버그용 처음 10개 링크
            }''')
            
            if result.get('error'):
                log(f"    iframe 접근 불가: {result['error']}")
                return None
            
            if result.get('found'):
                href = result.get('href', '')
                full_href = result.get('fullHref', '')
                text = result.get('text', '')
                
                log(f"    [DEBUG] PDF 링크 발견:")
                log(f"      - href: {href[:100]}...")
                log(f"      - fullHref: {full_href[:100]}...")
                log(f"      - text: {text[:50]}...")
                
                # fullHref 사용 (브라우저가 해석한 전체 URL)
                if full_href and full_href.startswith('http') and '.pdf' in full_href.lower():
                    return full_href
                
                # href가 전체 URL인 경우
                if href.startswith('http'):
                    return href
                
                # href가 프로토콜 상대 경로인 경우 (//로 시작)
                if href.startswith('//'):
                    return f"https:{href}"
                
                # href가 절대 경로인 경우 (/로 시작)
                if href.startswith('/'):
                    return f"{self.BASE_URL}{href}"
                
                # 상대 경로 - DART 또는 external 경로일 가능성
                # fullHref가 유효하면 사용
                if full_href and full_href.startswith('http'):
                    return full_href
                
                log(f"    [WARN] PDF href 형식 알 수 없음: {href}")
                return None
            else:
                # 디버그: 찾은 링크들 출력
                links = result.get('links', [])
                if links and self.debug_dir:
                    log(f"    [DEBUG] iframe 내 링크 {len(links)}개 (PDF 없음):")
                    for i, link in enumerate(links[:5]):
                        log(f"      {i+1}. href={link.get('href', '')[:50]}, text={link.get('text', '')[:30]}")
                return None
            
        except Exception as e:
            log(f"    iframe PDF 검색 오류: {e}")
            import traceback
            log(f"    {traceback.format_exc()}")
            return None
    
    async def _download_pdf_from_url(self, pdf_url: str) -> Optional[bytes]:
        """URL에서 PDF 다운로드 (aiohttp 사용)"""
        try:
            import aiohttp
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Referer': 'https://kind.krx.co.kr/',
                'Accept': 'application/pdf,*/*',
            }
            
            async with aiohttp.ClientSession() as session:
                async with session.get(pdf_url, headers=headers, timeout=aiohttp.ClientTimeout(total=60)) as response:
                    if response.status == 200:
                        pdf_data = await response.read()
                        
                        # PDF 유효성 검사
                        if len(pdf_data) < 1000:
                            log(f"    PDF가 너무 작음: {len(pdf_data)} bytes")
                            return None
                        
                        if not pdf_data.startswith(b'%PDF'):
                            log(f"    유효하지 않은 PDF 형식 (헤더: {pdf_data[:20]})")
                            return None
                        
                        log(f"    PDF 다운로드 완료: {len(pdf_data)} bytes")
                        return pdf_data
                    else:
                        log(f"    PDF 다운로드 실패: HTTP {response.status}")
                        return None
                        
        except ImportError:
            log(f"    aiohttp 없음, requests 사용...")
            return await self._download_pdf_with_requests(pdf_url)
        except Exception as e:
            log(f"    PDF URL 다운로드 오류: {e}")
            return None
    
    async def _download_pdf_with_requests(self, pdf_url: str) -> Optional[bytes]:
        """requests로 PDF 다운로드 (fallback)"""
        try:
            import requests
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Referer': 'https://kind.krx.co.kr/',
            }
            
            response = requests.get(pdf_url, headers=headers, timeout=60)
            if response.status_code == 200:
                pdf_data = response.content
                
                if len(pdf_data) > 1000 and pdf_data.startswith(b'%PDF'):
                    log(f"    PDF 다운로드 완료 (requests): {len(pdf_data)} bytes")
                    return pdf_data
            
            return None
        except Exception as e:
            log(f"    requests 다운로드 오류: {e}")
            return None


async def main():
    """테스트용 메인 함수"""
    debug_dir = os.environ.get('VALUEUP_DEBUG_DIR', '/tmp/krx_debug')
    
    async with KRXValueUpCrawler(headless=True, debug_dir=debug_dir) as crawler:
        log("=== 공시 목록 조회 테스트 ===")
        items = await crawler.get_disclosure_list(days=7, max_pages=1)
        
        log(f"\n총 {len(items)}건의 공시 발견")
        for item in items[:3]:
            log(f"- {item.공시일자} | {item.회사명} | {item.공시제목}")
            log(f"  접수번호: {item.접수번호}")
        
        # PDF 다운로드 테스트
        if items:
            log(f"\n=== PDF 다운로드 테스트 ===")
            test_item = items[0]
            pdf_data = await crawler.download_pdf(test_item.접수번호)
            if pdf_data:
                log(f"PDF 다운로드 성공: {len(pdf_data)} bytes")
            else:
                log("PDF 다운로드 실패")
        
        log(f"\n디버그 파일 저장 위치: {debug_dir}")


if __name__ == "__main__":
    asyncio.run(main())
