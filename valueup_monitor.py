import os
import requests
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
            
        self.base_url = "https://kind.krx.co.kr/valueup/disclsstat.do"
        self.bot = telegram.Bot(token=self.telegram_token)

    def fetch_krx_data(self, page: int = 1) -> str:
        """KRX 웹사이트에서 데이터 가져오기"""
        params = {
            "method": "valueupDisclsStatMain",
            "currentPageSize": "15",
            "pageIndex": str(page)
        }
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        try:
            response = requests.get(self.base_url, params=params, headers=headers)
            response.raise_for_status()
            return response.text
        except Exception as e:
            logging.error(f"KRX 데이터 요청 실패 (페이지 {page}): {e}")
            raise

    def get_total_pages(self, html_content: str) -> int:
        """총 페이지 수 추출"""
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            info_div = soup.find('div', {'class': 'info', 'type': '00'})
            if info_div:
                # "1/6" 형태에서 총 페이지 수 추출
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
        table = soup.find('table', {'class': 'list type-00 mt10'})
        
        if not table:
            logging.error("테이블을 찾을 수 없습니다.")
            return [], False

        disclosures = []
        need_next_page = False
        
        # tbody 내의 모든 tr 태그 찾기
        rows = table.find('tbody').find_all('tr')
        
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
                            # 일주일 이전 데이터가 나오면 중단
                            break
                        
                        # 마지막 행이 일주일 이내라면 다음 페이지 필요
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
            for disc in group:
                message += f"• {disc['company']}\n"
                message += f"  └ {disc['title']}\n"
            message += "\n"

        message += f"\n총 {len(disclosures)}건의 공시가 있습니다."
        return message

    async def run_weekly_check(self):
        """주간 모니터링 실행"""
        try:
            all_disclosures = []
            page = 1
            week_ago = datetime.now() - timedelta(days=7)
            
            while True:
                html_content = self.fetch_krx_data(page)
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

def main():
    monitor = KRXMonitor()
    asyncio.run(monitor.run_weekly_check())

if __name__ == "__main__":
    main()
