import os
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import telegram
import asyncio
import logging
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

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
            
        self.url = "https://kind.krx.co.kr/valueup/disclsstat.do?method=valueupDisclsStatMain#viewer"
        self.bot = telegram.Bot(token=self.telegram_token)

    def setup_driver(self):
        """Selenium 웹드라이버 설정"""
        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--disable-gpu')
        chrome_options.binary_location = "/usr/bin/google-chrome"
        
        service = Service(ChromeDriverManager().install())
        return webdriver.Chrome(service=service, options=chrome_options)

    def get_this_week_disclosures(self):
        """이번 주의 신규 공시 데이터 수집"""
        driver = self.setup_driver()
        try:
            driver.get(self.url)
            
            # 페이지 로딩 대기
            WebDriverWait(driver, 20).until(
                EC.presence_of_element_located((By.CLASS_NAME, "CI-GRID-BODY-TABLE"))
            )
            
            # 테이블 데이터 가져오기
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            table = soup.find('table', {'class': 'CI-GRID-BODY-TABLE'})
            
            if not table:
                logging.error("테이블을 찾을 수 없습니다.")
                return []

            # 이번 주의 날짜 범위 계산
            today = datetime.now()
            start_of_week = today - timedelta(days=today.weekday())
            end_of_week = start_of_week + timedelta(days=4)  # 금요일까지

            disclosures = []
            rows = table.find_all('tr')
            
            for row in rows:
                cols = row.find_all('td')
                if len(cols) >= 2:
                    company = cols[0].text.strip()
                    date_str = cols[1].text.strip()
                    
                    try:
                        disclosure_date = datetime.strptime(date_str, '%Y/%m/%d')
                        if start_of_week <= disclosure_date <= end_of_week:
                            disclosures.append({
                                'company': company,
                                'date': date_str
                            })
                    except ValueError as e:
                        logging.error(f"날짜 파싱 에러: {e}")
                        continue

            return disclosures

        except Exception as e:
            logging.error(f"데이터 수집 중 에러 발생: {e}")
            return []
            
        finally:
            driver.quit()

    async def send_telegram_message(self, message):
        """텔레그램으로 메시지 전송"""
        try:
            await self.bot.send_message(chat_id=self.chat_id, text=message, parse_mode='HTML')
            logging.info("텔레그램 메시지 전송 성공")
        except Exception as e:
            logging.error(f"텔레그램 메시지 전송 실패: {e}")

    async def run_weekly_check(self):
        """주간 모니터링 실행"""
        logging.info("주간 모니터링 시작")
        
        disclosures = self.get_this_week_disclosures()
        
        if not disclosures:
            message = "이번 주 신규 기업가치 제고 계획 공시가 없습니다."
        else:
            message = "<b>이번 주 신규 기업가치 제고 계획 공시</b>\n\n"
            for disc in disclosures:
                message += f"회사명: {disc['company']}\n"
                message += f"공시일자: {disc['date']}\n\n"

        await self.send_telegram_message(message)
        logging.info("주간 모니터링 완료")

def main():
    monitor = KRXMonitor()
    asyncio.run(monitor.run_weekly_check())

if __name__ == "__main__":
    main()
