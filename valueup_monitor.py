import os
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import telegram
import asyncio
import logging
from typing import List, Dict
import re

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class KRXMonitor:
    def __init__(self):
        self.telegram_token = os.environ.get('TELEGRAM_TOKEN')
        self.chat_id = os.environ.get('CHAT_ID')
        if not self.telegram_token or not self.chat_id:
            raise ValueError("환경 변수 TELEGRAM_TOKEN과 CHAT_ID가 필요합니다.")
            
        self.base_url = "https://kind.krx.co.kr/valueup/disclsstat.do?method=valueupDisclsStatMain"
        self.bot = telegram.Bot(token=self.telegram_token)

    def setup_driver(self):
        """Selenium 웹드라이버 설정"""
        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--disable-gpu')
        return webdriver.Chrome(options=chrome_options)

    def get_page_content(self, driver, page: int = 1) -> str:
        """특정 페이지의 컨텐츠 가져오기"""
        if page > 1:
            # 페이지 번호 클릭
            try:
                page_link = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.XPATH, f"//a[@onclick=\"fnPageGo('{page}');return false;\"]"))
                )
                page_link.click()
                # 새 데이터 로딩 대기
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CLASS_NAME, "CI-GRID-BODY-TABLE"))
                )
            except Exception as e:
                logging.error(f"페이지 {page} 이동 실패: {e}")
                return None

        return driver.page_source

    def get_total_pages(self, html_content: str) -> int:
        """총 페이지 수 추출"""
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            info_div = soup.find('div', {'class': 'info', 'type-00': True})
            if info_div:
                match = re.search(r'/(\d+)', info_div.text)
                if match:
                    return int(match.group(1))
            return 1
        except Exception as e:
            logging.error(f"총 페이지 수 추출 실패: {e}")
            return 1

    def parse_disclosures(self, html_content: str, week_ago: datetime) -> tuple[List[Dict], bool]:
        """HTML 컨텐츠에서 공시 정보 파싱"""
        soup = BeautifulSoup(html_content, 'html.parser')
        table = soup.find('table', {'class': 'CI-GRID-BODY-TABLE'})
        
        if not table:
            logging.error("테이블을 찾을 수 없습니다.")
            return [], False

        disclosures = []
        need_next_page = False
        
        rows = table.find_all('tr')
        logging.info(f"총 {len(rows)}개의 행을 찾았습니다.")
        
        for row in rows:
            try:
                cols = row.find_all('td')
                if len(cols) >= 4:
                    date_str = cols[1].text.strip()
                    company = cols[2].find('a', {'id': 'companysum'}).text.strip()
                    title = cols[3].find('a').text.strip()
                    
                    try:
                        disclosure_date = datetime.strptime(date_str, '%Y-%m-%d %H:%M')
                        
                        if disclosure_date >= week_ago:
                            disclosures.append({
                                'date': date_str,
                                'company': company,
                                'title': title
                            })
                            logging.info(f"파싱 성공: {date_str} - {company}")
                        elif len(disclosures) > 0:
                            break
                        
                        if row == rows[-1] and disclosure_date >= week_ago:
                            need_next_page = True
                            
                    except ValueError as e:
                        logging.error(f"날짜 파싱 에러: {date_str} - {e}")
                        continue
                        
            except Exception as e:
                logging.error(f"행 파싱 중 에러 발생: {e}")
                continue

        return disclosures, need_next_page

    async def send_telegram_message(self, message: str):
        """텔레그램으로 메시지 전송"""
        try:
            chat_id = int(self.chat_id)
            await self.bot.send_message(
                chat_id=chat_id,
                text=message,
                parse_mode='HTML',
                disable_web_page_preview=True
            )
            logging.info("텔레그램 메시지 전송 성공")
        except ValueError as e:
            logging.error(f"잘못된 chat_id 형식: {self.chat_id}")
            raise
        except Exception as e:
            logging.error(f"텔레그램 메시지 전송 실패: {e}")
            raise

    def format_message(self, disclosures: List[Dict]) -> str:
        """공시 정보를 텔레그램 메시지 형식으로 변환"""
        if not disclosures:
            return "최근 일주일간 신규 기업가치 제고 계획 공시가 없습니다."

        message = "<b>🔔 최근 일주일 기업가치 제고 계획 공시</b>\n\n"
        
        from itertools import groupby
        from operator import itemgetter
        
        sorted_disclosures = sorted(disclosures, key=itemgetter('date'), reverse=True)
        
        for date, group in groupby(sorted_disclosures, key=itemgetter('date')):
            message += f"📅 <b>{date}</b>\n"
            group_list = list(group)
            for disc in group_list:
                message += f"• {disc['company']}\n"
                message += f"  └ {disc['title']}\n"
            message += "\n"

        message += f"\n총 {len(disclosures)}건의 공시가 있습니다."
        return message

    async def run_weekly_check(self):
        """주간 모니터링 실행"""
        driver = None
        try:
            driver = self.setup_driver()
            driver.get(self.base_url)
            
            # 초기 페이지 로딩 대기
            WebDriverWait(driver, 20).until(
                EC.presence_of_element_located((By.CLASS_NAME, "CI-GRID-BODY-TABLE"))
            )
            
            all_disclosures = []
            page = 1
            week_ago = datetime.now() - timedelta(days=7)
            
            while True:
                html_content = self.get_page_content(driver, page)
                if not html_content:
                    break
                    
                disclosures, need_next_page = self.parse_disclosures(html_content, week_ago)
                all_disclosures.extend(disclosures)
                
                if not need_next_page:
                    break
                    
                total_pages = self.get_total_pages(html_content)
                if page >= total_pages:
                    break
                    
                page += 1
                logging.info(f"다음 페이지({page}) 확인 중...")
            
            logging.info(f"전체 {len(all_disclosures)}개의 공시 수집 완료")
            message = self.format_message(all_disclosures)
            await self.send_telegram_message(message)
            
        except Exception as e:
            error_message = f"모니터링 중 에러 발생: {str(e)}"
            logging.error(error_message)
            try:
                await self.send_telegram_message(f"⚠️ {error_message}")
            except Exception as telegram_error:
                logging.error(f"에러 메시지 전송 실패: {telegram_error}")
        finally:
            if driver:
                driver.quit()

def main():
    monitor = KRXMonitor()
    asyncio.run(monitor.run_weekly_check())

if __name__ == "__main__":
    main()
