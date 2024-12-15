import os
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import telegram
import asyncio
import logging
from typing import List, Dict
import traceback
import re

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
        chrome_options.binary_location = '/usr/bin/chromium-browser'
        
        service = Service('/usr/bin/chromedriver')
        return webdriver.Chrome(service=service, options=chrome_options)

    def extract_rcp_no(self, onclick_attr: str) -> str:
        """공시 상세보기 링크에서 rcpNo 추출"""
        match = re.search(r"openDisclsViewer\('(\d+)'", onclick_attr)
        return match.group(1) if match else None

    def parse_page(self, driver, week_ago: datetime) -> tuple[List[Dict], bool]:
        """현재 페이지의 공시 정보 파싱"""
        disclosures = []
        
        try:
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CLASS_NAME, "list"))
            )
            
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            table = soup.find('table', {'class': 'list'})
            
            if not table:
                logging.error("테이블을 찾을 수 없습니다.")
                return [], False

            rows = table.find('tbody').find_all('tr')
            for row in rows:
                try:
                    cols = row.find_all('td')
                    if len(cols) >= 4:
                        date_str = cols[1].text.strip()
                        company = cols[2].find('a', {'id': 'companysum'}).text.strip()
                        title_link = cols[3].find('a')
                        title = title_link.text.strip()
                        
                        # rcpNo 추출
                        rcp_no = self.extract_rcp_no(title_link['onclick'])
                        disclosure_url = f"https://kind.krx.co.kr/common/disclsviewer.do?method=search&rcpNo={rcp_no}"
                        
                        disclosure_date = datetime.strptime(date_str, '%Y-%m-%d %H:%M')
                        
                        if disclosure_date >= week_ago:
                            disclosures.append({
                                'date': date_str,
                                'company': company,
                                'title': title,
                                'url': disclosure_url
                            })
                            logging.info(f"파싱 성공: {date_str} - {company}")
                        else:
                            return disclosures, False
                            
                except Exception as e:
                    logging.error(f"행 파싱 중 에러: {str(e)}")
                    continue

            return disclosures, True
            
        except Exception as e:
            logging.error(f"페이지 파싱 중 에러: {str(e)}")
            return [], False

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
        except Exception as e:
            logging.error(f"텔레그램 메시지 전송 실패: {str(e)}")
            raise

    def format_message(self, disclosures: List[Dict]) -> str:
        """공시 정보를 텔레그램 메시지 형식으로 변환"""
        if not disclosures:
            return "최근 일주일간 신규 기업가치 제고 계획 공시가 없습니다."

        message = "🔔 최근 일주일 기업가치 제고 계획 공시\n\n"
        
        from itertools import groupby
        from operator import itemgetter
        
        sorted_disclosures = sorted(disclosures, key=itemgetter('date'), reverse=True)
        
        for date, group in groupby(sorted_disclosures, key=itemgetter('date')):
            message += f"📅 {date}\n"
            for disc in list(group):
                message += f"• {disc['company']}\n"
                message += f"  └ <a href='{disc['url']}'>{disc['title']}</a>\n"
            message += "\n"

        message += f"총 {len(disclosures)}건의 공시가 있습니다."
        return message

    async def run_weekly_check(self):
        """주간 모니터링 실행"""
        driver = None
        try:
            driver = self.setup_driver()
            logging.info("Chrome WebDriver 초기화 성공")
            
            driver.get(self.base_url)
            logging.info("페이지 로딩 시작")
            
            week_ago = datetime.now() - timedelta(days=7)
            all_disclosures = []
            page = 1
            
            while True:
                logging.info(f"페이지 {page} 처리 중")
                disclosures, need_next_page = self.parse_page(driver, week_ago)
                all_disclosures.extend(disclosures)
                
                if not need_next_page:
                    break
                    
                try:
                    next_page = driver.find_element(By.XPATH, f"//a[contains(@onclick, \"fnPageGo('{page + 1}')\")]")
                    next_page.click()
                    page += 1
                except:
                    break
            
            logging.info(f"전체 {len(all_disclosures)}개의 공시 수집 완료")
            message = self.format_message(all_disclosures)
            await self.send_telegram_message(message)
            
        except Exception as e:
            error_message = f"에러 발생: {str(e)}"
            logging.error(error_message)
            logging.error(traceback.format_exc())
            await self.send_telegram_message(f"⚠️ {error_message}")
            
        finally:
            if driver:
                driver.quit()

def main():
    monitor = KRXMonitor()
    asyncio.run(monitor.run_weekly_check())

if __name__ == "__main__":
    main()
