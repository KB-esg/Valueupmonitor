import os
from telegram import Bot
import asyncio
import logging
from datetime import datetime, timedelta

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
        self.token = os.getenv('TELEGRAM_TOKEN')
        self.receive_chat_id = os.getenv('TELCO_NEWS_RECEIVE')
        self.broadcast_chat_ids = [
            os.getenv('TELCO_NEWS_BROADCAST_1'),
            os.getenv('TELCO_NEWS_BROADCAST_2')
        ]
        
        logger.info(f"Token 존재 여부: {bool(self.token)}")
        logger.info(f"수신 채널 ID: {self.receive_chat_id}")
        logger.info(f"브로드캐스트 채널 IDs: {self.broadcast_chat_ids}")
        
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

    async def check_bot_permissions(self):
        """봇의 권한을 확인하는 함수"""
        try:
            # 봇 정보 가져오기
            bot_info = await self.bot.get_me()
            logger.info(f"봇 정보: {bot_info.first_name} (@{bot_info.username})")
            
            # 수신 채널 정보 확인
            try:
                chat = await self.bot.get_chat(self.receive_chat_id)
                logger.info(f"수신 채널 정보: {chat.title} (type: {chat.type})")
            except Exception as e:
                logger.error(f"수신 채널 접근 실패: {str(e)}")
            
            # 브로드캐스트 채널 정보 확인
            for chat_id in self.broadcast_chat_ids:
                try:
                    chat = await self.bot.get_chat(chat_id)
                    logger.info(f"브로드캐스트 채널 정보: {chat.title} (type: {chat.type})")
                except Exception as e:
                    logger.error(f"브로드캐스트 채널 {chat_id} 접근 실패: {str(e)}")
                    
        except Exception as e:
            logger.error(f"봇 권한 확인 중 에러: {str(e)}")

    async def forward_messages(self):
        """메시지를 수신하고 수정하여 다른 채널에 전달"""
        try:
            # 봇 권한 확인
            await self.check_bot_permissions()
            
            # 최근 메시지 가져오기 (offset과 limit 설정)
            logger.info("메시지 업데이트 확인 중...")
            updates = await self.bot.get_updates(offset=-1, limit=1, timeout=30)
            logger.info(f"받은 업데이트 수: {len(updates)}")
            
            if not updates:
                logger.info("처리할 업데이트가 없습니다.")
                return
                
            for update in updates:
                logger.info(f"업데이트 타입: {update.message and 'message' or 'other'}")
                if not update.message:
                    continue
                    
                message = update.message
                logger.info(f"메시지 정보: chat_id={message.chat.id}, message_id={message.message_id}")
                
                if str(message.chat.id) != self.receive_chat_id:
                    logger.info(f"다른 채널의 메시지입니다: {message.chat.id}")
                    continue
                
                # 메시지가 24시간 이내인지 확인
                message_time = message.date.replace(tzinfo=None)
                if datetime.utcnow() - message_time > timedelta(hours=24):
                    logger.info("24시간이 지난 메시지입니다.")
                    continue
                
                # 메시지 정제
                original_text = message.text
                cleaned_message = self.clean_message(original_text)
                
                if original_text == cleaned_message:
                    logger.info("제거할 문자열이 없습니다.")
                    continue
                
                # 원본 메시지 수정
                try:
                    await self.bot.edit_message_text(
                        chat_id=self.receive_chat_id,
                        message_id=message.message_id,
                        text=cleaned_message
                    )
                    logger.info("원본 메시지 수정 완료")
                except Exception as e:
                    logger.error(f"메시지 수정 중 에러: {str(e)}")
                
                # 브로드캐스트 채널로 전달
                for chat_id in self.broadcast_chat_ids:
                    try:
                        sent_msg = await self.bot.send_message(
                            chat_id=int(chat_id),
                            text=cleaned_message
                        )
                        logger.info(f"채널 {chat_id}로 메시지 전달 완료: {sent_msg.message_id}")
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
