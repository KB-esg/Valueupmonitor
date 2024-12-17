import os
from telegram import Bot
import asyncio
import logging
from datetime import datetime, timedelta

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

REMOVE_STRINGS = [
    "☞ KB증권 통신 텔레그램 채널 바로가기 < https://bit.ly/BaseStation >",
    "☞무료수신거부 0808886611"
]

class TelcoNewsForwarder:
    def __init__(self):
        self.token = os.getenv('TELCO_NEWS_TOKEN')
        self.receive_chat_id = os.getenv('TELCO_NEWS_RECEIVE')
        self.broadcast_chat_ids = [
            os.getenv('TELCO_NEWS_BROADCAST_1'),
            os.getenv('TELCO_NEWS_BROADCAST_2')
        ]
        
        logger.info(f"Token 존재 여부: {bool(self.token)}")
        logger.info(f"수신 채널 ID: {self.receive_chat_id}")
        logger.info(f"브로드캐스트 채널 IDs: {self.broadcast_chat_ids}")
        
        if not self.token:
            raise ValueError("TELCO_NEWS_TOKEN이 설정되지 않았습니다.")
        if not self.receive_chat_id:
            raise ValueError("TELCO_NEWS_RECEIVE가 설정되지 않았습니다.")
        if not all(self.broadcast_chat_ids):
            raise ValueError("TELCO_NEWS_BROADCAST_1 또는 TELCO_NEWS_BROADCAST_2가 설정되지 않았습니다.")
            
        self.bot = Bot(token=self.token)

    def clean_message(self, message: str) -> str:
        """지정된 문자열을 제거하는 함수"""
        if not message:
            return ""
        
        cleaned = message
        for remove_str in REMOVE_STRINGS:
            cleaned = cleaned.replace(remove_str, '').strip()
        return cleaned

    async def process_updates(self):
        """메시지 업데이트를 처리하는 함수"""
        try:
            updates = await self.bot.get_updates(limit=100)
            logger.info(f"총 {len(updates)}개의 업데이트를 받았습니다.")
            
            relevant_messages = []
            for update in updates:
                # 메시지 추출 (channel_post 또는 일반 message)
                message = update.channel_post or update.message
                if not message or not message.text:
                    continue

                logger.info(f"메시지 검사 중: chat_id={message.chat.id}, message_id={message.message_id}")
                logger.info(f"메시지 내용: {message.text[:100]}...")  # 처음 100자만 로깅

                # 24시간 이내 메시지인지 확인
                message_time = message.date.replace(tzinfo=None)
                time_diff = datetime.utcnow() - message_time
                logger.info(f"메시지 시간: {message_time}, 경과 시간: {time_diff}")
                
                if time_diff > timedelta(hours=24):
                    logger.info("24시간이 지난 메시지입니다.")
                    continue

                # 제거할 문자열이 있는지 확인
                original_text = message.text
                cleaned_text = self.clean_message(original_text)
                
                if original_text != cleaned_text:
                    # 채널 메시지가 아니더라도 내용이 맞으면 처리
                    relevant_messages.append((message, cleaned_text))
                    logger.info(f"처리 대상 메시지 발견: {message.message_id}")
                    logger.info(f"원본 텍스트: {original_text[:100]}...")
                    logger.info(f"정제된 텍스트: {cleaned_text[:100]}...")
            
            if not relevant_messages:
                logger.info("처리할 메시지를 찾지 못했습니다.")
                return None, None

            # 가장 최근 메시지 선택
            latest_message = max(relevant_messages, key=lambda x: x[0].date)
            logger.info(f"가장 최근 메시지 선택: {latest_message[0].message_id}")
            
            return latest_message

        except Exception as e:
            logger.error(f"업데이트 처리 중 에러: {str(e)}")
            return None, None

    async def forward_messages(self):
        """메시지를 수신하고 수정하여 다른 채널에 전달"""
        try:
            logger.info("메시지 업데이트 확인 중...")
            result = await self.process_updates()
            
            if not result:
                logger.info("처리할 메시지가 없습니다.")
                return
                
            original_message, cleaned_text = result
            
            # 원본 메시지 수정 먼저 시도
            try:
                await self.bot.edit_message_text(
                    chat_id=original_message.chat.id,
                    message_id=original_message.message_id,
                    text=cleaned_text
                )
                logger.info(f"원본 메시지 {original_message.message_id} 수정 완료")
            except Exception as e:
                logger.error(f"메시지 수정 중 에러: {str(e)}")
            
            # 브로드캐스트 채널로 전달
            for chat_id in self.broadcast_chat_ids:
                try:
                    sent_msg = await self.bot.send_message(
                        chat_id=int(chat_id),
                        text=cleaned_text
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
