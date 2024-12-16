import os
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import telegram
import asyncio
import logging
from datetime import datetime
import re

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class MSITMonitor:
    def __init__(self):
        self.telegram_token = os.environ.get('TELCO_NEWS_TOKEN')
        self.chat_id = os.environ.get('TELCO_NEWS_TESTER')
        if not self.telegram_token or not self.chat_id:
            raise ValueError("환경 변수 TELCO_NEWS_TOKEN과 TELCO_NEWS_TESTER가 필요합니다.")
            
        self.url = "https://www.msit.go.kr/bbs/list.do?sCode=user&mPid=74&mId=99"
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

    def check_telco_news(self, title: str) -> bool:
        """통신 서비스 가입 현황 관련 뉴스인지 확인"""
        return "통신 서비스 가입 현황" in title

    def is_today(self, date_str: str) -> bool:
        """게시물이 오늘 날짜인지 확인"""
        try:
            # 날짜 형식 정규화
            date_str = date_str.replace(',', ' ').strip()
            try:
                # "YYYY. MM. DD" 형식 시도
                post_date = datetime.strptime(date_str, '%Y. %m. %d').date()
            except ValueError:
                try:
                    # "MMM DD YYYY" 형식 시도
                    post_date = datetime.strptime(date_str, '%b %d %Y').date()
                except ValueError:
                    # "YYYY-MM-DD" 형식 시도
                    post_date = datetime.strptime(date_str, '%Y-%m-%d').date()
            
            today = datetime.now().date()
            logging.info(f"게시물 날짜 확인: {post_date} vs {today}")
            return post_date == today
        except Exception as e:
            logging.error(f"날짜 파싱 에러: {str(e)}")
            return False

    def has_next_page(self, driver) -> bool:
        """다음 페이지 존재 여부 확인"""
        try:
            current_page = int(driver.find_element(By.CSS_SELECTOR, "a.page-link[aria-current='page']").text)
            next_page_link = driver.find_elements(By.CSS_SELECTOR, f"a.page-link[href*='pageIndex={current_page + 1}']")
            return len(next_page_link) > 0
        except Exception as e:
            logging.error(f"다음 페이지 확인 중 에러: {str(e)}")
            return False

    def go_to_next_page(self, driver) -> bool:
        """다음 페이지로 이동"""
        try:
            current_page = int(driver.find_element(By.CSS_SELECTOR, "a.page-link[aria-current='page']").text)
            next_page = driver.find_element(By.CSS_SELECTOR, f"a.page-link[href*='pageIndex={current_page + 1}']")
            next_page.click()
            WebDriverWait(driver, 10).until(
                EC.staleness_of(driver.find_element(By.CSS_SELECTOR, "div.board_list"))
            )
            return True
        except Exception as e:
            logging.error(f"다음 페이지 이동 중 에러: {str(e)}")
            return False

    def parse_page(self, driver) -> tuple[list, bool]:
        """현재 페이지의 뉴스 정보 파싱"""
        telco_news = []
        continue_search = True
        
        try:
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CLASS_NAME, "board_list"))
            )
            
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            news_items = soup.find_all('div', {'class': 'toggle'})
            
            for item in news_items:
                # thead는 건너뛰기
                if 'thead' in item.get('class', []):
                    continue
                    
                try:
                    date_elem = item.find('div', {'class': 'date', 'aria-label': '등록일'})
                    if not date_elem:
                        date_elem = item.find('div', {'class': 'date'})
                    if not date_elem:
                        continue
                        
                    date_str = date_elem.text.strip()
                    if not date_str or date_str == '등록일':
                        continue
                        
                    logging.info(f"Found date string: {date_str}")
                    
                if not self.is_today(date_str):
                    continue_search = False
                    break
                
                title_elem = item.find('p', {'class': 'title'})
                if title_elem and self.check_telco_news(title_elem.text.strip()):
                    title = title_elem.text.strip()
                    dept = item.find('dd', {'id': lambda x: x and 'td_CHRG_DEPT_NM' in x}).text.strip()
                    telco_news.append({
                        'title': title,
                        'date': date_str,
                        'department': dept
                    })
            
            return telco_news, continue_search
            
        except Exception as e:
            logging.error(f"페이지 파싱 중 에러: {str(e)}")
            return [], False

    async def send_telegram_message(self, news_items: list):
        """텔레그램으로 메시지 전송"""
        if not news_items:
            return
            
        try:
            message = "📱 통신서비스 가입 현황 업데이트\n\n"
            
            for news in news_items:
                message += f"📅 {news['date']}\n"
                message += f"📑 {news['title']}\n"
                message += f"🏢 {news['department']}\n\n"

            chat_id = int(self.chat_id)
            await self.bot.send_message(
                chat_id=chat_id,
                text=message,
                parse_mode='HTML'
            )
            logging.info("텔레그램 메시지 전송 성공")
        except Exception as e:
            logging.error(f"텔레그램 메시지 전송 실패: {str(e)}")
            raise

    async def run_daily_check(self):
        """일일 모니터링 실행"""
        driver = None
        try:
            driver = self.setup_driver()
            logging.info("Chrome WebDriver 초기화 성공")
            
            driver.get(self.url)
            logging.info("페이지 로딩 완료")
            
            all_news = []
            continue_search = True
            
            while continue_search:
                news_items, should_continue = self.parse_page(driver)
                all_news.extend(news_items)
                
                if not should_continue:
                    break
                    
                if self.has_next_page(driver):
                    if not self.go_to_next_page(driver):
                        break
                else:
                    break
            
            if all_news:
                await self.send_telegram_message(all_news)
            
        except Exception as e:
            error_message = f"에러 발생: {str(e)}"
            logging.error(error_message)
            await self.send_telegram_message([{
                'title': error_message,
                'date': datetime.now().strftime('%Y. %m. %d'),
                'department': 'System'
            }])
            
        finally:
            if driver:
                driver.quit()

def main():
    monitor = MSITMonitor()
    asyncio.run(monitor.run_daily_check())

if __name__ == "__main__":
    main()
