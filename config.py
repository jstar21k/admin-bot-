from __future__ import annotations

import os
from dataclasses import dataclass

from dotenv import load_dotenv


def _require_env(name: str) -> str:
    value = os.getenv(name)
    if value is None or not value.strip():
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value.strip()


def _parse_chat_id(name: str) -> int:
    value = _require_env(name)
    try:
        return int(value)
    except ValueError as exc:
        raise RuntimeError(
            f"{name} must be a numeric Telegram chat ID (for example: -1001234567890)."
        ) from exc


@dataclass(frozen=True)
class Settings:
    bot_token: str
    mongo_uri: str
    mongo_db_name: str
    intake_channel_id: int
    thumbnail_source_channel_id: int
    storage_channel_id: int
    confirmation_text: str
    media_group_stabilize_seconds: float
    thumbnail_to_video_delay_seconds: float
    processor_poll_interval_seconds: float
    log_level: str

    @classmethod
    def from_env(cls) -> "Settings":
        load_dotenv()
        return cls(
            bot_token=_require_env("BOT_TOKEN"),
            mongo_uri=_require_env("MONGO_URI"),
            mongo_db_name=os.getenv("MONGO_DB_NAME", "queue_controller_bot").strip(),
            intake_channel_id=_parse_chat_id("INTAKE_CHANNEL_ID"),
            thumbnail_source_channel_id=_parse_chat_id("THUMBNAIL_SOURCE_CHANNEL_ID"),
            storage_channel_id=_parse_chat_id("STORAGE_CHANNEL_ID"),
            confirmation_text=os.getenv("CONFIRMATION_TEXT", "post done").strip().lower(),
            media_group_stabilize_seconds=float(
                os.getenv("MEDIA_GROUP_STABILIZE_SECONDS", "2")
            ),
            thumbnail_to_video_delay_seconds=float(
                os.getenv("THUMBNAIL_TO_VIDEO_DELAY_SECONDS", "3")
            ),
            processor_poll_interval_seconds=float(
                os.getenv("PROCESSOR_POLL_INTERVAL_SECONDS", "2")
            ),
            log_level=os.getenv("LOG_LEVEL", "INFO").strip().upper(),
        )
