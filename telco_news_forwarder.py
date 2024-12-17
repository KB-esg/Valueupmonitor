import os
from telegram import Bot
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
        # 환경 변수 가져오기
        self.token = os.getenv('TELEGRAM_TOKEN')
        self.receive_chat_id = os.getenv('TELCO_NEWS_RECEIVE')
        self.broadcast_chat_ids = [
            os.getenv('TELCO_NEWS_BROADCAST_1'),
            os.getenv('TELCO_NEWS_BROADCAST_2')
        ]
        
        # 환경 변수 확인 및 로깅
        logger.info(f"Token 존재 여부: {bool(self.token)}")
        logger.info(f"수신 채널 ID 존재 여부: {bool(self.receive_chat_id)}")
        logger.info(f"브로드캐스트 채널 1 존재 여부: {bool(self.broadcast_chat_ids[0])}")
        logger.info(f"브로드캐스트 채널 2 존재 여부: {bool(self.broadcast_chat_ids[1])}")
        
        if not self.token:
            raise ValueError("TELEGRAM_TOKEN이 설정되지 않았습니다.")
        if not self.receive_chat_id:
            raise ValueError("TELCO_NEWS_RECEIVE가 설정되지 않았습니다.")
        if not all(self.broadcast_chat_ids):
            raise ValueError("TELCO_NEWS_BROADCAST_1 또는 TELCO_NEWS_BROADCAST_2가 설정되지 않았습니다.")
            
        self.bot = Bot(token=self.token)

    def clean_message(self, message: str) -> str:
        """지정된 문자열을 제거하는 함수"""
        if not message:
            return ""
        
        for remove_str in REMOVE_STRINGS:
            message = message.replace(remove_str, '').strip()
        return message

    async def forward_messages(self):
        """메시지를 수신하고 수정하여 다른 채널에 전달"""
        try:
            # 최근 메시지 가져오기
            updates = await self.bot.get_updates()
            
            if not updates:
                logger.info("처리할 메시지가 없습니다.")
                return
                
            latest_message = updates[-1].message
            if not latest_message:
                logger.info("최근 메시지를 찾을 수 없습니다.")
                return
                
            if str(latest_message.chat.id) != self.receive_chat_id:
                logger.info(f"메시지가 지정된 채널({self.receive_chat_id})에서 오지 않았습니다.")
                return
                
            # 메시지 정제
            original_text = latest_message.text
            cleaned_message = self.clean_message(original_text)
            
            if original_text == cleaned_message:
                logger.info("제거할 문자열이 없습니다.")
                return
                
            logger.info("원본 메시지 수정 시도...")
            
            # 원본 메시지 수정
            try:
                await self.bot.edit_message_text(
                    chat_id=self.receive_chat_id,
                    message_id=latest_message.message_id,
                    text=cleaned_message
                )
                logger.info("원본 메시지 수정 완료")
            except Exception as e:
                logger.error(f"메시지 수정 중 에러: {str(e)}")
            
            # 브로드캐스트 채널로 전달
            for chat_id in self.broadcast_chat_ids:
                try:
                    await self.bot.send_message(
                        chat_id=int(chat_id),
                        text=cleaned_message
                    )
                    logger.info(f"채널 {chat_id}로 메시지 전달 완료")
                except Exception as e:
                    logger.error(f"채널 {chat_id}로 메시지 전달 중 에러: {str(e)}")
                    
        except Exception as e:
            logger.error(f"전체 프로세스 중 에러 발생: {str(e)}")
            raise

async def main():
    try:
        forwarder = TelcoNewsForwarder()
        await forwarder.forward_messages()
    except Exception as e:
        logger.error(f"메인 함수 실행 중 에러: {str(e)}")
        raise

if __name__ == "__main__":
    asyncio.run(main())
