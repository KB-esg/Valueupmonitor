"""Telco News Forwarder

- 광고 문구 제거 후 원본 메시지를 수정하고 브로드캐스트 채널로 전달
- Google Sheets(Archive_arg 시트)에서 최근 24시간 기사 다이제스트를 추출해 함께 전송
- Google 서비스 계정 키는
  1) GitHub Action Secret `MSIT_GSPREAD_REF` (JSON 문자열)
  2) 환경 변수 `GOOGLE_APPLICATION_CREDENTIALS` (로컬 개발 시 파일 경로)
  둘 중 하나에서 로드한다.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List, Tuple

import gspread
from google.oauth2.service_account import Credentials
from telegram import Bot

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# -------- 설정 상수 --------
REMOVE_STRINGS = [
    "☞ KB증권 통신 텔레그램 채널 바로가기 < https://bit.ly/BaseStation >",
    "☞무료수신거부 0808886611",
]

SHEET_NAME = "Archive_arg"
SECONDS_IN_DAY = 86_400


class TelcoNewsForwarder:
    def __init__(self) -> None:
        # --- Telegram 관련 환경 변수 ---
        self.token: str | None = os.getenv("TELCO_NEWS_TOKEN")
        self.receive_chat_id: str | None = os.getenv("TELCO_NEWS_RECEIVE")
        self.broadcast_chat_ids: List[int] = [
            int(os.getenv("TELCO_NEWS_BROADCAST_1", "0")),
            int(os.getenv("TELCO_NEWS_BROADCAST_2", "0")),
        ]

        # --- Google Sheet 환경 변수 ---
        self.spreadsheet_id: str | None = os.getenv("TELCO_ARTICLE_ID")

        self._validate_env()

        # Telegram Bot 초기화
        self.bot = Bot(token=self.token)

        # gspread 초기화
        self.gc = self._init_gspread_client()

    # ------------------------------------------------------------------
    # ENV 검사
    # ------------------------------------------------------------------
    def _validate_env(self) -> None:
        if not self.token:
            raise ValueError("TELCO_NEWS_TOKEN이 설정되지 않았습니다.")
        if not self.receive_chat_id:
            raise ValueError("TELCO_NEWS_RECEIVE가 설정되지 않았습니다.")
        if not all(self.broadcast_chat_ids):
            raise ValueError("TELCO_NEWS_BROADCAST_[1|2]가 설정되지 않았습니다.")
        if not self.spreadsheet_id:
            raise ValueError("TELCO_ARTICLE_ID가 설정되지 않았습니다.")

    # ------------------------------------------------------------------
    # gspread 클라이언트 초기화
    # ------------------------------------------------------------------
    @staticmethod
    def _init_gspread_client():
        scopes = [
            "https://www.googleapis.com/auth/spreadsheets.readonly",
            "https://www.googleapis.com/auth/drive.readonly",
        ]

        raw_json = os.getenv("MSIT_GSPREAD_REF")
        if raw_json:
            logger.info("Google creds loaded from MSIT_GSPREAD_REF secret")
            creds_dict = json.loads(raw_json)
            creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
        else:
            path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
            if not path or not Path(path).is_file():
                raise ValueError(
                    "MSIT_GSPREAD_REF 또는 GOOGLE_APPLICATION_CREDENTIALS 중 하나가 필요합니다."
                )
            logger.info(f"Google creds loaded from file: {path}")
            creds = Credentials.from_service_account_file(path, scopes=scopes)

        return gspread.authorize(creds)

    # ------------------------------------------------------------------
    # 메시지 정제
    # ------------------------------------------------------------------
    @staticmethod
    def clean_message(message: str | None) -> str:
        if not message:
            return ""
        cleaned = message
        for s in REMOVE_STRINGS:
            cleaned = cleaned.replace(s, "").strip()
        return cleaned

    # ------------------------------------------------------------------
    # gspread → 최근 24h 기사
    # ------------------------------------------------------------------
    async def fetch_recent_articles(self) -> List[Tuple[str, str]]:
        """(title, url) 리스트 반환"""

        def _blocking_io():
            sh = self.gc.open_by_key(self.spreadsheet_id)  # Spreadsheet load
            ws = sh.worksheet(SHEET_NAME)
            rows = ws.get_all_records()  # List[Dict[str, Any]]
            now_utc = datetime.now(tz=timezone.utc)
            recent: List[Tuple[str, str]] = []

            for row in rows:
                # 헤더 정규화: 소문자 + 공백→언더스코어
                norm = {k.strip().lower().replace(" ", "_"): v for k, v in row.items()}
                ts_raw = str(norm.get("timestamp", "")).strip()
                title = str(norm.get("title", "")).strip()
                url = str(norm.get("url", "")).strip()

                if not (ts_raw and title and url):
                    continue

                # Timestamp 파싱 (시트는 "YYYY-MM-DD HH:MM" 형식, KST)
                try:
                    ts_kst = datetime.strptime(ts_raw, "%Y-%m-%d %H:%M")
                    ts_utc = ts_kst.replace(tzinfo=timezone.utc) - timedelta(hours=9)
                except ValueError:
                    logger.debug(f"Timestamp parse 실패: {ts_raw}")
                    continue

                if (now_utc - ts_utc).total_seconds() <= SECONDS_IN_DAY:
                    recent.append((title, url))

            # 최신순 정렬 (시트 자체가 오래된 행 위에 있을 수 있으므로)
            return recent[::-1]

        return await asyncio.to_thread(_blocking_io)

    # ------------------------------------------------------------------
    # 기사 다이제스트 전송
    # ------------------------------------------------------------------
    @staticmethod
    def _escape_html(text: str) -> str:
        return (
            text.replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
        )

    async def send_article_digest(self) -> None:
        articles = await self.fetch_recent_articles()
        if not articles:
            logger.info("최근 24h 신규 기사가 없습니다.")
            return

        lines = [
            f"📑 <a href=\"{url}\">{self._escape_html(title)}</a>"
            for title, url in articles
        ]
        message = "📰 <b>지난 24시간 Telecom Articles</b>\n" + "\n".join(lines)

        for chat_id in self.broadcast_chat_ids:
            try:
                await self.bot.send_message(
                    chat_id=chat_id,
                    text=message,
                    parse_mode="HTML",
                    disable_web_page_preview=True,
                )
                logger.info(f"Digest 전송 성공 → {chat_id}")
            except Exception as e:
                logger.error(f"Digest 전송 실패({chat_id}): {e}")

    # ------------------------------------------------------------------
    # Telegram 업데이트 처리 (광고 제거 & 포워드)
    # ------------------------------------------------------------------
    async def process_updates(self):
        try:
            updates = await self.bot.get_updates(limit=100)
            logger.info(f"총 {len(updates)}개의 업데이트 수신")

            relevant = []
            for update in updates:
                message = update.channel_post or update.message
                if not (message and message.text):
                    continue
                if str(message.chat.id) != str(self.receive_chat_id):
                    continue

                # 24h 필터
                if (datetime.now(tz=timezone.utc) - message.date).total_seconds() > SECONDS_IN_DAY:
                    continue

                cleaned = self.clean_message(message.text)
                if cleaned != message.text:
                    relevant.append((message, cleaned))

            if not relevant:
                return None
            # 최신 메시지 1건만
            return max(relevant, key=lambda mc: mc[0].date)
        except Exception as e:
            logger.error(f"process_updates 오류: {e}")
            return None

    # ------------------------------------------------------------------
    # 메시지 수정 + 전달 + 기사 다이제스트
    # ------------------------------------------------------------------
    async def forward_messages(self):
        result = await self.process_updates()
        if result:
            original, cleaned = result
            # 원본 메시지 수정
            try:
                await self.bot.edit_message_text(
                    chat_id=original.chat.id,
                    message_id=original.message_id,
                    text=cleaned,
                    parse_mode=None,
                )
                logger.info(f"메시지 수정 완료: {original.message_id}")
            except Exception as e:
                logger.warning(f"메시지 수정 실패: {e}")

            # 브로드캐스트 채널로 전송
            for chat_id in self.broadcast_chat_ids:
                try:
                    await self.bot.send_message(
                        chat_id=chat_id,
                        text=cleaned,
                        parse_mode=None,
                    )
                    logger.info(f"포워드 성공 → {chat_id}")
                except Exception as e:
                    logger.error(f"포워드 실패({chat_id}): {e}")

        # ---- 기사 다이제스트 전송 ----
        await self.send_article_digest()


async def main():
    forwarder = TelcoNewsForwarder()
    await forwarder.forward_messages()


if __name__ == "__main__":
    asyncio.run(main())
