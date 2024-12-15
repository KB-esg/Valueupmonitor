import os
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import telegram
import asyncio
import logging
from typing import List, Dict

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
            
        self.url = "https://kind.krx.co.kr/valueup/disclsstat.do?method=valueupDisclsStatMain"
        self.bot = telegram.Bot(token=self.telegram_token)

    def parse_disclosures(self, html_content: str) -> List[Dict]:
        """HTML 컨텐츠에서 공시 정보 파싱"""
        soup = BeautifulSoup(html_content, 'html.parser')
        table = soup.find('table', {'class': 'list'})
        if not table:
            logging.error("테이블을 찾을 수 없습니다.")
            return []

        # 최근 일주일 날짜 범위 계산
        today = datetime.now()
        week_ago = today - timedelta(days=7)

        disclosures = []
        rows = table.find('tbody').find_all('tr')
        
        for row in rows:
            cols = row.find_all('td')
            if len(cols) >= 4:  # 번호, 공시일자, 회사명, 공시제목 컬럼 확인
                try:
                    # 공시일자 파싱
                    date_str = cols[1].text.strip()
                    disclosure_date = datetime.strptime(date_str, '%Y-%m-%d %H:%M')
                    
                    # 최근 일주일 데이터만 필터링
                    if disclosure_date >= week_ago:
                        company = cols[2].find('a').text.strip()  # 회사명에서 a 태그 내용만 추출
                        title = cols[3].find('a').text.strip()
                        
                        disclosures.append({
                            'date': date_str,
                            'company': company,
                            'title': title
                        })
                except (ValueError, AttributeError) as e:
                    logging.error(f"데이터 파싱 에러: {e}")
                    continue

        return disclosures

    async def send_telegram_message(self, message: str):
        """텔레그램으로 메시지 전송"""
        try:
            await self.bot.send_message(
                chat_id=self.chat_id,
                text=message,
                parse_mode='HTML',
                disable_web_page_preview=True
            )
            logging.info("텔레그램 메시지 전송 성공")
        except Exception as e:
            logging.error(f"텔레그램 메시지 전송 실패: {e}")
            raise

    def format_message(self, disclosures: List[Dict]) -> str:
        """공시 정보를 텔레그램 메시지 형식으로 변환"""
        if not disclosures:
            return "최근 일주일간 신규 기업가치 제고 계획 공시가 없습니다."

        message = "<b>🔔 최근 일주일 기업가치 제고 계획 공시</b>\n\n"
        
        # 날짜별로 그룹화
        from itertools import groupby
        from operator import itemgetter
        
        # 날짜로 정렬
        sorted_disclosures = sorted(disclosures, key=itemgetter('date'), reverse=True)
        
        for date, group in groupby(sorted_disclosures, key=itemgetter('date')):
            message += f"📅 <b>{date}</b>\n"
            for disc in group:
                message += f"• {disc['company']}\n"
                message += f"  └ {disc['title']}\n"
            message += "\n"

        return message

    async def run_weekly_check(self):
        """주간 모니터링 실행"""
        try:
            # HTML 파일 읽기 (실제 운영 시에는 requests.get(self.url) 사용)
            with open('sample.html', 'r', encoding='utf-8') as f:
                html_content = f.read()
            
            disclosures = self.parse_disclosures(html_content)
            message = self.format_message(disclosures)
            await self.send_telegram_message(message)
            
        except Exception as e:
            error_message = f"모니터링 중 에러 발생: {str(e)}"
            logging.error(error_message)
            await self.send_telegram_message(f"⚠️ {error_message}")

def main():
    monitor = KRXMonitor()
    asyncio.run(monitor.run_weekly_check())

if __name__ == "__main__":
    main()
