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
                'a.btn-search',
                'button.btn-search',
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
        """특정 페이지로 이동"""
        try:
            # 페이지 번호 링크 클릭
            page_link = self.page.locator(f'a:has-text("{page_num}"), a[href*="page={page_num}"]').first
            if await page_link.count() > 0:
                await page_link.click()
                await asyncio.sleep(2)
                return True
            
            # JavaScript로 페이지 이동 시도
            await self.page.evaluate(f'goPage({page_num})')
            await asyncio.sleep(2)
            return True
            
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
        
        # 기간 설정
        if period:
            log(f"기간 버튼 클릭: {period}")
            await self.click_period_button(period)
            await asyncio.sleep(2)
        else:
            # 날짜 범위로 검색
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days)
            log(f"날짜 범위 설정: {start_date.strftime('%Y-%m-%d')} ~ {end_date.strftime('%Y-%m-%d')}")
            
            try:
                # 날짜 입력 필드 찾기 (여러 셀렉터 시도)
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
                
                # 검색 버튼 클릭 (실패해도 계속 진행)
                await self.click_search_button()
                
            except Exception as e:
                log(f"날짜 설정 오류: {e}")
        
        # 디버그: 검색 후 저장
        await self._save_debug_screenshot(self.page, "02_list_after_search")
        await self._save_debug_html(self.page, "02_list_after_search")
        
        # 페이지별 크롤링
        cutoff_date = datetime.now() - timedelta(days=days) if not period else None
        
        for page_num in range(1, max_pages + 1):
            log(f"페이지 {page_num} 파싱 중...")
            
            page_items = await self.parse_current_page()
            log(f"  발견: {len(page_items)}건")
            
            if not page_items:
                log("  더 이상 항목 없음, 종료")
                break
            
            # 날짜 필터링 (period가 아닌 경우)
            if cutoff_date:
                filtered_items = []
                for item in page_items:
                    try:
                        # 다양한 날짜 형식 처리
                        date_str = item.공시일자.replace('.', '-').strip()
                        if len(date_str) == 10:  # YYYY-MM-DD
                            item_date = datetime.strptime(date_str, "%Y-%m-%d")
                        elif len(date_str) == 8:  # YYYYMMDD
                            item_date = datetime.strptime(date_str, "%Y%m%d")
                        else:
                            filtered_items.append(item)
                            continue
                            
                        if item_date >= cutoff_date:
                            filtered_items.append(item)
                    except:
                        filtered_items.append(item)
                
                all_items.extend(filtered_items)
                
                # 필터링으로 제외된 항목이 있으면 더 이상 진행 불필요
                if len(filtered_items) < len(page_items):
                    log(f"  날짜 기준 도달, 종료")
                    break
            else:
                all_items.extend(page_items)
            
            # 다음 페이지로 이동
            if page_num < max_pages:
                if not await self.go_to_page(page_num + 1):
                    break
                await asyncio.sleep(1)
        
        log(f"총 {len(all_items)}건 수집 완료")
        return all_items
    
    async def get_doc_number(self, acptno: str) -> str:
        """
        공시 상세 페이지에서 문서번호(docNo) 추출
        
        Args:
            acptno: 접수번호
            
        Returns:
            문서번호
        """
        viewer_url = f"{self.VIEWER_URL}?method=search&acptno={acptno}"
        log(f"  문서번호 추출 중: {acptno}")
        
        page = await self.context.new_page()
        try:
            await page.goto(viewer_url, wait_until="networkidle")
            await asyncio.sleep(3)
            
            # 디버그: 뷰어 페이지 저장
            await self._save_debug_screenshot(page, f"viewer_{acptno}")
            await self._save_debug_html(page, f"viewer_{acptno}")
            await self._save_debug_js(page, f"viewer_{acptno}")
            
            # mainDoc select에서 docNo 추출
            # <option value='20251128000575|Y' selected="selected">
            main_doc = page.locator('select#mainDoc option[selected]')
            if await main_doc.count() > 0:
                value = await main_doc.get_attribute('value') or ""
                log(f"    mainDoc value: {value}")
                if '|' in value:
                    return value.split('|')[0]
                return value
            
            # 첫 번째 실제 옵션 (본문선택 제외)
            first_option = page.locator('select#mainDoc option:not([value=""])').first
            if await first_option.count() > 0:
                value = await first_option.get_attribute('value') or ""
                log(f"    first option value: {value}")
                if '|' in value:
                    return value.split('|')[0]
                return value
            
            # hidden input에서 추출
            doc_no_input = page.locator('input#docNo, input[name="docNo"]')
            if await doc_no_input.count() > 0:
                return await doc_no_input.get_attribute('value') or ""
                
        except Exception as e:
            log(f"  문서번호 추출 오류: {e}")
        finally:
            await page.close()
            
        return ""
    
    async def download_pdf(self, acptno: str, doc_no: str = "") -> Optional[bytes]:
        """
        PDF 다운로드 - 뷰어 팝업에서 PDF 버튼 클릭 방식
        
        Args:
            acptno: 접수번호
            doc_no: 문서번호 (없으면 자동 추출)
            
        Returns:
            PDF 바이너리 데이터 또는 None
        """
        log(f"  PDF 다운로드 시작: acptno={acptno}")
        
        # 문서번호 추출
        if not doc_no:
            doc_no = await self.get_doc_number(acptno)
        
        if not doc_no:
            log(f"  문서번호를 찾을 수 없음: acptno={acptno}")
            return None
        
        log(f"    문서번호: {doc_no}")
        
        viewer_url = f"{self.VIEWER_URL}?method=search&acptno={acptno}"
        
        page = await self.context.new_page()
        try:
            # 1. 뷰어 페이지 열기
            await page.goto(viewer_url, wait_until="networkidle")
            await asyncio.sleep(3)
            
            # 디버그: 다운로드 전 상태 저장
            await self._save_debug_screenshot(page, f"pdf_before_{acptno}")
            
            # 2. PDF 버튼 찾기
            # HTML에서: <a href="#viewer" onclick="pdfPrint();return false;"><img src="../images/common/btn_pdf.png" alt="PDF 로 저장" /></a>
            pdf_btn_selectors = [
                'a[onclick*="pdfPrint"]',
                'a:has(img[alt*="PDF"])',
                'img[alt*="PDF"]',
                'a:has-text("PDF")',
            ]
            
            pdf_btn = None
            for selector in pdf_btn_selectors:
                btn = page.locator(selector).first
                if await btn.count() > 0:
                    pdf_btn = btn
                    log(f"    PDF 버튼 발견: {selector}")
                    break
            
            if not pdf_btn:
                log(f"  PDF 버튼을 찾을 수 없음")
                await self._save_debug_screenshot(page, f"pdf_no_button_{acptno}")
                return None
            
            # 3. 직접 다운로드 URL 시도 (pdfPrint 우회)
            # fnPdfJson -> filedownload('pdf') -> /common/pdfDownload.do?method=pdfDown&acptNo=XXX&docNo=YYY
            download_url = f"{self.PDF_DOWNLOAD_URL}?method=pdfDown&acptNo={acptno}&docNo={doc_no}"
            log(f"    직접 다운로드 URL: {download_url}")
            
            download_page = await self.context.new_page()
            try:
                # 다운로드 대기
                async with download_page.expect_download(timeout=60000) as download_info:
                    await download_page.goto(download_url)
                
                download = await download_info.value
                log(f"    다운로드 시작: {download.suggested_filename}")
                
                # 임시 파일로 저장
                with tempfile.NamedTemporaryFile(delete=False, suffix='.pdf') as tmp:
                    tmp_path = tmp.name
                
                await download.save_as(tmp_path)
                
                with open(tmp_path, 'rb') as f:
                    pdf_data = f.read()
                
                os.unlink(tmp_path)
                
                # PDF 유효성 검사 (최소 크기 및 헤더 확인)
                if len(pdf_data) < 1000:
                    log(f"    PDF가 너무 작음: {len(pdf_data)} bytes")
                    return None
                
                if not pdf_data.startswith(b'%PDF'):
                    log(f"    유효하지 않은 PDF 형식")
                    return None
                
                log(f"    PDF 다운로드 완료: {len(pdf_data)} bytes")
                return pdf_data
                
            except Exception as e:
                log(f"    직접 다운로드 실패: {e}")
            finally:
                await download_page.close()
            
            # 4. PDF 버튼 클릭 방식 (fallback)
            log("    PDF 버튼 클릭 방식 시도...")
            try:
                async with page.expect_download(timeout=60000) as download_info:
                    await pdf_btn.click()
                
                download = await download_info.value
                
                with tempfile.NamedTemporaryFile(delete=False, suffix='.pdf') as tmp:
                    tmp_path = tmp.name
                
                await download.save_as(tmp_path)
                
                with open(tmp_path, 'rb') as f:
                    pdf_data = f.read()
                
                os.unlink(tmp_path)
                
                if pdf_data and len(pdf_data) > 1000 and pdf_data.startswith(b'%PDF'):
                    log(f"    PDF 버튼 클릭 성공: {len(pdf_data)} bytes")
                    return pdf_data
                    
            except Exception as e:
                log(f"    PDF 버튼 클릭 실패: {e}")
                await self._save_debug_screenshot(page, f"pdf_click_failed_{acptno}")
            
            return None
            
        except Exception as e:
            log(f"  PDF 다운로드 오류: {e}")
            return None
        finally:
            await page.close()


async def main():
    """테스트용 메인 함수"""
    # 디버그 모드로 실행
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
