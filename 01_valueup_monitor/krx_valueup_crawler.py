"""
KRX Value-Up 공시 크롤러
Playwright 기반으로 KRX KIND 사이트에서 밸류업 공시를 크롤링
"""

import asyncio
import re
import os
import sys
import tempfile
from datetime import datetime, timedelta
from dataclasses import dataclass
from typing import List, Optional
from playwright.async_api import async_playwright, Page, Browser

# stdout 버퍼링 해제
sys.stdout.reconfigure(line_buffering=True)


def log(message: str):
    """타임스탬프와 함께 로그 출력"""
    timestamp = datetime.now().strftime("%H:%M:%S")
    print(f"[{timestamp}] {message}", flush=True)


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
    
    def __init__(self, headless: bool = True):
        self.headless = headless
        self.browser: Optional[Browser] = None
        self.page: Optional[Page] = None
        
    async def __aenter__(self):
        await self.start()
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
        
    async def start(self):
        """브라우저 시작"""
        log("  → Playwright 브라우저 시작 중...")
        self.playwright = await async_playwright().start()
        self.browser = await self.playwright.chromium.launch(headless=self.headless)
        self.context = await self.browser.new_context(
            viewport={"width": 1920, "height": 1080},
            locale="ko-KR"
        )
        self.page = await self.context.new_page()
        log("  → 브라우저 시작 완료")
        
    async def close(self):
        """브라우저 종료"""
        if self.browser:
            await self.browser.close()
        if self.playwright:
            await self.playwright.stop()
    
    async def click_period_button(self, period: str) -> bool:
        """
        기간 버튼 클릭 (1주, 1개월, 3개월, 6개월, 1년, 2년, 3년, 전체)
        
        Args:
            period: 기간 문자열
            
        Returns:
            성공 여부
        """
        try:
            # 기간 버튼 찾기 (a 태그 또는 button)
            period_btn = self.page.locator(f'a:text-is("{period}"), button:text-is("{period}")')
            if await period_btn.count() > 0:
                await period_btn.first.click()
                log(f"  → '{period}' 버튼 클릭 완료")
                await asyncio.sleep(2)
                return True
            else:
                log(f"  → '{period}' 버튼을 찾을 수 없음")
                return False
        except Exception as e:
            log(f"  → 기간 버튼 클릭 오류: {e}")
            return False
    
    async def click_search_button(self) -> bool:
        """검색 버튼 클릭"""
        try:
            # 다양한 검색 버튼 셀렉터 시도
            selectors = [
                'a.btn-search',
                'button.btn-search', 
                'input.btn-search',
                'a:has(img[alt="검색"])',
                'a.search',
                'button:text-is("검색")',
                'input[value="검색"]',
                'a[title="검색"]',
                'img[alt="검색"]'
            ]
            
            for selector in selectors:
                btn = self.page.locator(selector)
                if await btn.count() > 0:
                    await btn.first.click()
                    log(f"  → 검색 버튼 클릭 완료 (selector: {selector})")
                    await asyncio.sleep(3)
                    return True
            
            log("  → 검색 버튼을 찾을 수 없음")
            return False
        except Exception as e:
            log(f"  → 검색 버튼 클릭 오류: {e}")
            return False
    
    async def get_total_pages(self) -> int:
        """총 페이지 수 확인"""
        try:
            # 페이지 정보 텍스트에서 추출 (예: "전체 51건 : 1/4")
            page_info = self.page.locator('div.paging, span.page-info, *:text-matches("전체.*건")')
            if await page_info.count() > 0:
                text = await page_info.first.text_content()
                match = re.search(r'(\d+)/(\d+)', text)
                if match:
                    return int(match.group(2))
            
            # 페이지 번호 링크에서 마지막 페이지 추출
            page_links = await self.page.locator('div.paging a, a.page-link').all()
            max_page = 1
            for link in page_links:
                text = await link.text_content()
                if text and text.strip().isdigit():
                    max_page = max(max_page, int(text.strip()))
            return max_page
        except:
            return 1
    
    async def go_to_page(self, page_num: int) -> bool:
        """특정 페이지로 이동"""
        try:
            # 페이지 번호 링크 클릭
            page_link = self.page.locator(f'div.paging a:text-is("{page_num}"), a.page-link:text-is("{page_num}")')
            if await page_link.count() > 0:
                await page_link.first.click()
                await asyncio.sleep(2)
                return True
            return False
        except Exception as e:
            log(f"  → 페이지 이동 오류: {e}")
            return False

    async def parse_current_page(self) -> List[DisclosureItem]:
        """현재 페이지의 공시 목록 파싱"""
        items = []
        
        # 테이블 행 추출 - 다양한 셀렉터 시도
        rows = await self.page.locator('table.list tbody tr').all()
        if not rows:
            rows = await self.page.locator('table.tbl-list tbody tr').all()
        if not rows:
            rows = await self.page.locator('div.list table tbody tr').all()
        if not rows:
            rows = await self.page.locator('table tbody tr').all()
        
        for row in rows:
            try:
                cells = await row.locator('td').all()
                if len(cells) < 4:
                    continue
                
                # 번호
                번호_text = await cells[0].text_content()
                번호 = int(번호_text.strip()) if 번호_text and 번호_text.strip().isdigit() else 0
                
                # 공시일자
                공시일자 = (await cells[1].text_content() or "").strip()
                
                # 회사명 셀에서 회사명과 종목코드 추출
                회사명_cell = cells[2]
                회사명_full = (await 회사명_cell.text_content() or "").strip()
                
                # 회사명 (첫 번째 단어 또는 링크 텍스트)
                회사명_link = 회사명_cell.locator('a')
                if await 회사명_link.count() > 0:
                    회사명 = (await 회사명_link.first.text_content() or "").strip()
                else:
                    회사명_parts = 회사명_full.split()
                    회사명 = 회사명_parts[0] if 회사명_parts else ""
                
                # 종목코드 추출 (span 또는 텍스트에서)
                종목코드 = ""
                stock_code_match = re.search(r'[A-Z]?\d{6}', 회사명_full)
                if stock_code_match:
                    종목코드 = stock_code_match.group()
                
                # 공시제목 셀
                제목_cell = cells[3]
                공시제목 = (await 제목_cell.text_content() or "").strip()
                
                # 접수번호 추출 - 제목 링크의 onclick에서
                접수번호 = ""
                제목_link = 제목_cell.locator('a')
                if await 제목_link.count() > 0:
                    onclick = await 제목_link.first.get_attribute('onclick') or ""
                    # openDisclsViewer('20251224001157') 형태에서 추출
                    match = re.search(r"openDisclsViewer\s*\(\s*['\"](\d+)['\"]", onclick)
                    if match:
                        접수번호 = match.group(1)
                
                # onclick에서 못 찾으면 행 전체 HTML에서 시도
                if not 접수번호:
                    row_html = await row.inner_html()
                    match = re.search(r"openDisclsViewer\s*\(\s*['\"](\d+)['\"]", row_html)
                    if match:
                        접수번호 = match.group(1)
                    else:
                        # acptno 파라미터에서 추출
                        match = re.search(r'acptno[=\'"\s:]+[\'"]?(\d+)', row_html, re.IGNORECASE)
                        if match:
                            접수번호 = match.group(1)
                
                if 접수번호:
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
                    
            except Exception as e:
                log(f"  → 행 파싱 오류: {e}")
                continue
        
        return items

    async def get_disclosure_list(
        self, 
        days: int = 7,
        period: str = None,
        max_pages: int = 10
    ) -> List[DisclosureItem]:
        """
        공시 목록 조회
        
        Args:
            days: 조회할 기간(일), period가 None일 때 사용
            period: 기간 버튼 ('1주', '1개월', '3개월', '6개월', '1년', '전체' 등)
            max_pages: 최대 크롤링 페이지 수
            
        Returns:
            공시 항목 리스트
        """
        all_items = []
        
        # 페이지 로드
        log("  → 브라우저에서 KRX KIND 페이지 로딩 중...")
        await self.page.goto(self.LIST_URL, wait_until="networkidle", timeout=60000)
        log("  → 페이지 로딩 완료")
        await asyncio.sleep(3)
        
        # 기간 설정
        if period:
            # 기간 버튼 클릭
            log(f"  → 기간 설정: {period}")
            await self.click_period_button(period)
        else:
            # 날짜 직접 입력
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days)
            
            log(f"  → 조회 기간 설정: {start_date.strftime('%Y-%m-%d')} ~ {end_date.strftime('%Y-%m-%d')}")
            
            try:
                # 시작일 입력
                start_input = self.page.locator('input#fromDate, input[name="fromDate"]')
                if await start_input.count() > 0:
                    await start_input.clear()
                    await start_input.fill(start_date.strftime("%Y-%m-%d"))
                    log(f"  → 시작일 입력 완료")
                
                # 종료일 입력
                end_input = self.page.locator('input#toDate, input[name="toDate"]')
                if await end_input.count() > 0:
                    await end_input.clear()
                    await end_input.fill(end_date.strftime("%Y-%m-%d"))
                    log(f"  → 종료일 입력 완료")
                
                # 검색 버튼 클릭
                await self.click_search_button()
                
            except Exception as e:
                log(f"  → 날짜 설정 오류: {e}")
        
        await asyncio.sleep(2)
        
        # 총 페이지 수 확인
        total_pages = await self.get_total_pages()
        log(f"  → 총 {total_pages} 페이지 발견")
        
        # 각 페이지 크롤링
        pages_to_crawl = min(total_pages, max_pages)
        
        for page_num in range(1, pages_to_crawl + 1):
            log(f"  → 페이지 {page_num}/{pages_to_crawl} 파싱 중...")
            
            if page_num > 1:
                if not await self.go_to_page(page_num):
                    log(f"  → 페이지 {page_num} 이동 실패, 크롤링 종료")
                    break
            
            page_items = await self.parse_current_page()
            log(f"  → 페이지 {page_num}: {len(page_items)}건 파싱 완료")
            
            # 날짜 필터링 (period 사용 시에는 건너뜀)
            if not period and days:
                cutoff_date = datetime.now() - timedelta(days=days)
                filtered_items = []
                for item in page_items:
                    try:
                        # 공시일자 파싱 (YYYY-MM-DD HH:MM 형식)
                        date_str = item.공시일자.split()[0] if item.공시일자 else ""
                        item_date = datetime.strptime(date_str, "%Y-%m-%d")
                        if item_date >= cutoff_date:
                            filtered_items.append(item)
                    except:
                        filtered_items.append(item)  # 파싱 실패 시 포함
                
                all_items.extend(filtered_items)
                
                # 날짜가 cutoff 이전이면 더 이상 크롤링 불필요
                if page_items and len(filtered_items) < len(page_items):
                    log(f"  → 기간 외 데이터 도달, 크롤링 종료")
                    break
            else:
                all_items.extend(page_items)
        
        log(f"  → 크롤링 완료: 총 {len(all_items)}건")
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
        
        page = await self.context.new_page()
        try:
            log(f"      → 문서번호 조회 중: {acptno}")
            await page.goto(viewer_url, wait_until="networkidle", timeout=30000)
            await asyncio.sleep(2)
            
            # mainDoc select에서 docNo 추출
            main_doc = page.locator('select#mainDoc option[selected], select#mainDoc option:nth-child(2)')
            if await main_doc.count() > 0:
                value = await main_doc.first.get_attribute('value') or ""
                # value 형식: "20251128000575|Y"
                if '|' in value:
                    doc_no = value.split('|')[0]
                    log(f"      → 문서번호 발견: {doc_no}")
                    return doc_no
                return value
            
            # 또는 hidden input에서 추출
            doc_no_input = page.locator('input#docNo, input[name="docNo"]')
            if await doc_no_input.count() > 0:
                doc_no = await doc_no_input.first.get_attribute('value') or ""
                if doc_no:
                    log(f"      → 문서번호 발견: {doc_no}")
                return doc_no
                
        except Exception as e:
            log(f"      → 문서번호 추출 오류: {e}")
        finally:
            await page.close()
            
        return ""
    
    async def download_pdf(self, acptno: str, doc_no: str = "") -> Optional[bytes]:
        """
        PDF 다운로드
        
        Args:
            acptno: 접수번호
            doc_no: 문서번호 (없으면 자동 추출)
            
        Returns:
            PDF 바이너리 데이터 또는 None
        """
        if not doc_no:
            doc_no = await self.get_doc_number(acptno)
        
        if not doc_no:
            log(f"      → 문서번호를 찾을 수 없음: {acptno}")
            return None
        
        download_url = f"{self.PDF_DOWNLOAD_URL}?method=pdfDown&acptNo={acptno}&docNo={doc_no}"
        
        page = await self.context.new_page()
        try:
            log(f"      → PDF 다운로드 시작...")
            # 다운로드 대기 설정
            async with page.expect_download(timeout=60000) as download_info:
                await page.goto(download_url)
            
            download = await download_info.value
            
            # 임시 파일로 저장 후 읽기
            with tempfile.NamedTemporaryFile(delete=False, suffix='.pdf') as tmp:
                tmp_path = tmp.name
            
            await download.save_as(tmp_path)
            
            with open(tmp_path, 'rb') as f:
                pdf_data = f.read()
            
            os.unlink(tmp_path)
            log(f"      → PDF 다운로드 완료: {len(pdf_data)} bytes")
            return pdf_data
            
        except Exception as e:
            log(f"      → PDF 다운로드 오류: {e}")
            return None
        finally:
            await page.close()


async def main():
    """테스트용 메인 함수"""
    async with KRXValueUpCrawler(headless=True) as crawler:
        # 7일간 데이터 조회
        items = await crawler.get_disclosure_list(days=7)
        
        print(f"총 {len(items)}건의 공시 발견")
        for item in items[:5]:
            print(f"- {item.공시일자} | {item.회사명} | {item.공시제목}")
            print(f"  접수번호: {item.접수번호}")


if __name__ == "__main__":
    asyncio.run(main())
