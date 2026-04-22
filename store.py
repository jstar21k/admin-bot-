from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from motor.motor_asyncio import AsyncIOMotorDatabase
from pymongo import ASCENDING, ReturnDocument
from pymongo.errors import DuplicateKeyError
from telegram import Message


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class QueueStore:
    def __init__(self, db: AsyncIOMotorDatabase) -> None:
        self._groups = db.intake_groups
        self._queue = db.queue_posts

    async def ensure_indexes(self) -> None:
        await self._groups.create_index("group_key", unique=True)
        await self._queue.create_index("group_key", unique=True)
        await self._queue.create_index(
            [("status", ASCENDING), ("created_at", ASCENDING)],
            name="queue_status_created_idx",
        )
        await self._queue.create_index(
            "processing_slot",
            unique=True,
            partialFilterExpression={"processing_slot": "active"},
            name="single_processing_slot_idx",
        )

    async def upsert_intake_message(self, message: Message) -> str:
        group_key = self._build_group_key(message)
        update_fields: dict[str, Any] = {
            "group_key": group_key,
            "source_chat_id": message.chat_id,
            "media_group_id": message.media_group_id,
            "updated_at": utc_now(),
        }

        if message.photo:
            update_fields["thumbnail_message_id"] = message.message_id

        if message.video:
            update_fields["video_message_id"] = message.message_id

        await self._groups.update_one(
            {"group_key": group_key},
            {
                "$set": update_fields,
                "$setOnInsert": {
                    "created_at": utc_now(),
                    "queued_at": None,
                },
            },
            upsert=True,
        )
        return group_key

    async def recover_complete_groups(self) -> int:
        created = 0
        async for group in self._groups.find(
            {
                "thumbnail_message_id": {"$exists": True},
                "video_message_id": {"$exists": True},
                "queued_at": None,
            }
        ):
            created += int(await self.create_queue_item(group["group_key"]))
        return created

    async def create_queue_item(self, group_key: str) -> bool:
        group = await self._groups.find_one({"group_key": group_key})
        if not group:
            return False
        if not group.get("thumbnail_message_id") or not group.get("video_message_id"):
            return False

        now = utc_now()
        result = await self._queue.update_one(
            {"group_key": group_key},
            {
                "$setOnInsert": {
                    "group_key": group_key,
                    "source_chat_id": group["source_chat_id"],
                    "media_group_id": group.get("media_group_id"),
                    "thumbnail_message_id": group["thumbnail_message_id"],
                    "video_message_id": group["video_message_id"],
                    "status": "pending",
                    "processing_step": None,
                    "processing_slot": None,
                    "waiting_for_confirmation": False,
                    "attempt_count": 0,
                    "last_error": None,
                    "thumbnail_forwarded_message_id": None,
                    "storage_forwarded_message_id": None,
                    "created_at": now,
                    "updated_at": now,
                    "thumbnail_sent_at": None,
                    "video_sent_at": None,
                    "confirmed_at": None,
                }
            },
            upsert=True,
        )
        if result.upserted_id:
            await self._groups.update_one(
                {"group_key": group_key},
                {"$set": {"queued_at": now}},
            )
            return True
        return False

    async def get_active_post(self) -> dict[str, Any] | None:
        return await self._queue.find_one({"processing_slot": "active"})

    async def claim_next_pending(self) -> dict[str, Any] | None:
        candidate = await self._queue.find_one(
            {"status": "pending"},
            sort=[("created_at", ASCENDING)],
        )
        if not candidate:
            return None

        now = utc_now()
        try:
            claimed = await self._queue.find_one_and_update(
                {"_id": candidate["_id"], "status": "pending"},
                {
                    "$set": {
                        "status": "processing",
                        "processing_step": "claimed",
                        "processing_slot": "active",
                        "updated_at": now,
                    },
                    "$inc": {"attempt_count": 1},
                },
                return_document=ReturnDocument.AFTER,
            )
            return claimed
        except DuplicateKeyError:
            return None

    async def mark_thumbnail_sent(
        self, post_id: Any, forwarded_message_id: int | None
    ) -> None:
        await self._queue.update_one(
            {"_id": post_id},
            {
                "$set": {
                    "processing_step": "thumbnail_sent",
                    "thumbnail_sent_at": utc_now(),
                    "thumbnail_forwarded_message_id": forwarded_message_id,
                    "updated_at": utc_now(),
                }
            },
        )

    async def mark_video_sent_and_waiting(
        self, post_id: Any, forwarded_message_id: int | None
    ) -> None:
        await self._queue.update_one(
            {"_id": post_id},
            {
                "$set": {
                    "processing_step": "waiting_confirmation",
                    "video_sent_at": utc_now(),
                    "storage_forwarded_message_id": forwarded_message_id,
                    "waiting_for_confirmation": True,
                    "updated_at": utc_now(),
                }
            },
        )

    async def mark_processing_retry(self, post_id: Any, error_message: str) -> None:
        await self._queue.update_one(
            {"_id": post_id},
            {
                "$set": {
                    "status": "pending",
                    "processing_step": None,
                    "processing_slot": None,
                    "waiting_for_confirmation": False,
                    "last_error": error_message,
                    "updated_at": utc_now(),
                }
            },
        )

    async def mark_active_done(self, confirmation_message_id: int | None) -> dict[str, Any] | None:
        return await self._queue.find_one_and_update(
            {
                "processing_slot": "active",
                "status": "processing",
                "waiting_for_confirmation": True,
            },
            {
                "$set": {
                    "status": "done",
                    "processing_step": "done",
                    "processing_slot": None,
                    "waiting_for_confirmation": False,
                    "confirmed_at": utc_now(),
                    "confirmation_message_id": confirmation_message_id,
                    "updated_at": utc_now(),
                }
            },
            return_document=ReturnDocument.BEFORE,
        )

    async def get_counts(self) -> dict[str, int]:
        pending = await self._queue.count_documents({"status": "pending"})
        processing = await self._queue.count_documents({"status": "processing"})
        done = await self._queue.count_documents({"status": "done"})
        return {"pending": pending, "processing": processing, "done": done}

    @staticmethod
    def _build_group_key(message: Message) -> str:
        if message.media_group_id:
            return f"{message.chat_id}:{message.media_group_id}"
        return f"{message.chat_id}:single:{message.message_id}"
