import os
from telegram import Bot, Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes
import asyncio
import logging
from datetime import datetime

# 로깅 설정
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# 제거할 문자열
REMOVE_STRINGS = [
    "☞ KB증권 통신 텔레그램 채널 바로가기 < https://bit.ly/BaseStation >",
    "☞무료수신거부 0808886611"
]

class TelcoNewsForwarder:
    def __init__(self):
        self.token = os.environ.get('TELCO_NEWS_TOKEN')
        self.receive_chat_id = os.environ.get('TELCO_NEWS_TESTER')
        self.broadcast_chat_ids = [
            os.environ.get('TELCO_NEWS_TESTER'),
            os.environ.get('TELCO_NEWS_TESTER')
        ]
        
        if not all([self.token, self.receive_chat_id] + self.broadcast_chat_ids):
            raise ValueError("필요한 환경 변수가 설정되지 않았습니다.")
            
        self.bot = Bot(token=self.token)

    def clean_message(self, message: str) -> str:
        """지정된 문자열을 제거하는 함수"""
        for remove_str in REMOVE_STRINGS:
            message = message.replace(remove_str, '').strip()
        return message

    async def forward_messages(self):
        """메시지를 수신하고 수정하여 다른 채널에 전달"""
        try:
            # 최근 메시지 가져오기
            async with self.bot:
                messages = await self.bot.get_updates()
                if messages:
                    latest_message = messages[-1].message
                    if latest_message and latest_message.chat.id == int(self.receive_chat_id):
                        # 메시지 정제
                        cleaned_message = self.clean_message(latest_message.text)
                        
                        # 원본 메시지 수정
                        await self.bot.edit_message_text(
                            chat_id=self.receive_chat_id,
                            message_id=latest_message.message_id,
                            text=cleaned_message
                        )
                        
                        # 브로드캐스트 채널로 전달
                        for chat_id in self.broadcast_chat_ids:
                            await self.bot.send_message(
                                chat_id=int(chat_id),
                                text=cleaned_message,
                                parse_mode='HTML'
                            )
                        
                        logger.info("메시지 전달 및 수정 완료")
                    else:
                        logger.info("처리할 새 메시지가 없습니다.")
                        
        except Exception as e:
            logger.error(f"에러 발생: {str(e)}")
            raise

async def main():
    forwarder = TelcoNewsForwarder()
    await forwarder.forward_messages()

if __name__ == "__main__":
    asyncio.run(main())
