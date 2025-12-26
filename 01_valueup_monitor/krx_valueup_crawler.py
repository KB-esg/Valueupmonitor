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
        self.playwright = await async_playwright().start()
        self.browser = await self.playwright.chromium.launch(headless=self.headless)
        self.context = await self.browser.new_context(
            viewport={"width": 1920, "height": 1080},
            locale="ko-KR"
        )
        self.page = await self.context.new_page()
        
    async def close(self):
        """브라우저 종료"""
        if self.browser:
            await self.browser.close()
        if self.playwright:
            await self.playwright.stop()
    
    async def get_disclosure_list(self, days: int = 7) -> List[DisclosureItem]:
        """
        공시 목록 조회
        
        Args:
            days: 조회할 기간(일), 기본 7일
            
        Returns:
            공시 항목 리스트
        """
        items = []
        
        # 페이지 로드
        await self.page.goto(self.LIST_URL, wait_until="networkidle")
        await asyncio.sleep(2)
        
        # 기간 설정 (최근 N일)
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)
        
        # 날짜 입력 필드 설정
        try:
            # 시작일 설정
            start_input = self.page.locator('input[name="fromDate"], input#fromDate').first
            if await start_input.count() > 0:
                await start_input.fill(start_date.strftime("%Y-%m-%d"))
            
            # 종료일 설정
            end_input = self.page.locator('input[name="toDate"], input#toDate').first
            if await end_input.count() > 0:
                await end_input.fill(end_date.strftime("%Y-%m-%d"))
            
            # 검색 버튼 클릭
            search_btn = self.page.locator('button:has-text("검색"), a:has-text("검색"), input[value="검색"]').first
            if await search_btn.count() > 0:
                await search_btn.click()
                await asyncio.sleep(2)
        except Exception as e:
            print(f"날짜 설정 중 오류 (무시하고 기본값 사용): {e}")
        
        # 테이블 행 추출
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
                
                # 회사명 및 종목코드
                company_cell = cells[2]
                회사명_full = (await company_cell.text_content() or "").strip()
                
                # 회사명과 종목코드 분리 (예: "삼성전자 005930" 또는 링크에서)
                회사명 = 회사명_full.split()[0] if 회사명_full else ""
                
                # 종목코드 추출 (링크나 배지에서)
                종목코드 = ""
                stock_code_match = re.search(r'[A-Z]?\d{6}', 회사명_full)
                if stock_code_match:
                    종목코드 = stock_code_match.group()
                
                # 공시제목
                공시제목 = (await cells[3].text_content() or "").strip()
                
                # 접수번호 추출 (링크에서)
                link = await row.locator('a[onclick*="acptno"], a[href*="acptno"]').first
                접수번호 = ""
                if await link.count() > 0:
                    onclick = await link.get_attribute('onclick') or ""
                    href = await link.get_attribute('href') or ""
                    
                    acptno_match = re.search(r'acptno[=\'":\s]+(\d+)', onclick + href)
                    if acptno_match:
                        접수번호 = acptno_match.group(1)
                
                if not 접수번호:
                    # 다른 방식으로 접수번호 추출 시도
                    all_text = await row.inner_html()
                    acptno_match = re.search(r'acptno[=\'"\s:]+(\d+)', all_text)
                    if acptno_match:
                        접수번호 = acptno_match.group(1)
                
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
                print(f"행 파싱 중 오류: {e}")
                continue
        
        return items
    
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
            await page.goto(viewer_url, wait_until="networkidle")
            await asyncio.sleep(2)
            
            # mainDoc select에서 docNo 추출
            main_doc = page.locator('select#mainDoc option[selected], select#mainDoc option:nth-child(2)')
            if await main_doc.count() > 0:
                value = await main_doc.get_attribute('value') or ""
                # value 형식: "20251128000575|Y"
                if '|' in value:
                    return value.split('|')[0]
                return value
            
            # 또는 hidden input에서 추출
            doc_no_input = page.locator('input#docNo, input[name="docNo"]')
            if await doc_no_input.count() > 0:
                return await doc_no_input.get_attribute('value') or ""
                
        except Exception as e:
            print(f"문서번호 추출 중 오류: {e}")
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
            print(f"문서번호를 찾을 수 없습니다: acptno={acptno}")
            return None
        
        download_url = f"{self.PDF_DOWNLOAD_URL}?method=pdfDown&acptNo={acptno}&docNo={doc_no}"
        
        page = await self.context.new_page()
        try:
            # 다운로드 대기 설정
            async with page.expect_download() as download_info:
                await page.goto(download_url)
            
            download = await download_info.value
            
            # 임시 파일로 저장 후 읽기
            with tempfile.NamedTemporaryFile(delete=False, suffix='.pdf') as tmp:
                tmp_path = tmp.name
            
            await download.save_as(tmp_path)
            
            with open(tmp_path, 'rb') as f:
                pdf_data = f.read()
            
            os.unlink(tmp_path)
            return pdf_data
            
        except Exception as e:
            print(f"PDF 다운로드 중 오류: {e}")
            return None
        finally:
            await page.close()


async def main():
    """테스트용 메인 함수"""
    async with KRXValueUpCrawler(headless=True) as crawler:
        items = await crawler.get_disclosure_list(days=7)
        
        print(f"총 {len(items)}건의 공시 발견")
        for item in items[:5]:  # 상위 5개만 출력
            print(f"- {item.공시일자} | {item.회사명} | {item.공시제목}")
            print(f"  접수번호: {item.접수번호}")


if __name__ == "__main__":
    asyncio.run(main())
