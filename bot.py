from __future__ import annotations

import asyncio
import logging
from contextlib import suppress

from motor.motor_asyncio import AsyncIOMotorClient
from telegram import Message, Update
from telegram.ext import Application, ContextTypes, TypeHandler
from telegram.error import TelegramError

from config import Settings
from store import QueueStore


LOGGER = logging.getLogger("queue_controller_bot")


class QueueController:
    def __init__(self, application: Application, settings: Settings, store: QueueStore) -> None:
        self._application = application
        self._settings = settings
        self._store = store
        self._processor_lock = asyncio.Lock()
        self._shutdown = asyncio.Event()
        self._intake_finalize_tasks: dict[str, asyncio.Task[None]] = {}
        self._poller_task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        await self._store.ensure_indexes()
        recovered = await self._store.recover_complete_groups()
        if recovered:
            LOGGER.info("Recovered %s complete intake groups into the queue", recovered)
        self._poller_task = self._application.create_task(self._processor_poller())
        self.kick_processor()

    async def stop(self) -> None:
        self._shutdown.set()
        if self._poller_task:
            self._poller_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._poller_task

        tasks = list(self._intake_finalize_tasks.values())
        self._intake_finalize_tasks.clear()
        for task in tasks:
            task.cancel()
        for task in tasks:
            with suppress(asyncio.CancelledError):
                await task

    def kick_processor(self) -> None:
        self._application.create_task(self._process_once())

    async def handle_update(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        del context
        message = update.channel_post or update.message
        if not message:
            return

        if message.chat_id == self._settings.intake_channel_id:
            await self._handle_intake_message(message)
            return

        if message.chat_id == self._settings.storage_channel_id:
            await self._handle_storage_confirmation(message)

    async def _handle_intake_message(self, message: Message) -> None:
        if not (message.photo or message.video):
            return

        group_key = await self._store.upsert_intake_message(message)
        self._schedule_group_finalize(group_key)

    def _schedule_group_finalize(self, group_key: str) -> None:
        previous_task = self._intake_finalize_tasks.pop(group_key, None)
        if previous_task:
            previous_task.cancel()

        task = self._application.create_task(self._finalize_group_after_delay(group_key))
        self._intake_finalize_tasks[group_key] = task

    async def _finalize_group_after_delay(self, group_key: str) -> None:
        try:
            await asyncio.sleep(self._settings.media_group_stabilize_seconds)
            created = await self._store.create_queue_item(group_key)
            if created:
                counts = await self._store.get_counts()
                LOGGER.info(
                    "Queued post %s. Pending=%s Processing=%s Done=%s",
                    group_key,
                    counts["pending"],
                    counts["processing"],
                    counts["done"],
                )
                self.kick_processor()
        except asyncio.CancelledError:
            raise
        finally:
            self._intake_finalize_tasks.pop(group_key, None)

    async def _handle_storage_confirmation(self, message: Message) -> None:
        text = (message.text or "").strip().lower()
        if text != self._settings.confirmation_text:
            return

        completed = await self._store.mark_active_done(message.message_id)
        if not completed:
            LOGGER.info("Confirmation received but no active post is waiting")
            return

        LOGGER.info("Post %s marked done after storage confirmation", completed["group_key"])
        self.kick_processor()

    async def _processor_poller(self) -> None:
        while not self._shutdown.is_set():
            await self._process_once()
            try:
                await asyncio.wait_for(
                    self._shutdown.wait(),
                    timeout=self._settings.processor_poll_interval_seconds,
                )
            except asyncio.TimeoutError:
                continue

    async def _process_once(self) -> None:
        if self._processor_lock.locked():
            return

        async with self._processor_lock:
            active = await self._store.get_active_post()
            if active and active.get("waiting_for_confirmation"):
                return

            post = active or await self._store.claim_next_pending()
            if not post:
                return

            try:
                if not post.get("thumbnail_sent_at"):
                    thumbnail_result = await self._application.bot.copy_message(
                        chat_id=self._settings.thumbnail_source_channel_id,
                        from_chat_id=post["source_chat_id"],
                        message_id=post["thumbnail_message_id"],
                    )
                    await self._store.mark_thumbnail_sent(
                        post["_id"], thumbnail_result.message_id
                    )
                    await asyncio.sleep(self._settings.thumbnail_to_video_delay_seconds)

                if not post.get("waiting_for_confirmation"):
                    video_result = await self._application.bot.copy_message(
                        chat_id=self._settings.storage_channel_id,
                        from_chat_id=post["source_chat_id"],
                        message_id=post["video_message_id"],
                    )
                    await self._store.mark_video_sent_and_waiting(
                        post["_id"], video_result.message_id
                    )

                LOGGER.info(
                    "Post %s moved to storage and is waiting for '%s'",
                    post["group_key"],
                    self._settings.confirmation_text,
                )
            except TelegramError as exc:
                LOGGER.exception("Telegram error while processing %s", post["group_key"])
                await self._store.mark_processing_retry(post["_id"], str(exc))
            except Exception as exc:
                LOGGER.exception("Unexpected error while processing %s", post["group_key"])
                await self._store.mark_processing_retry(post["_id"], str(exc))


async def _post_init(application: Application) -> None:
    controller: QueueController = application.bot_data["controller"]
    await controller.start()


async def _post_shutdown(application: Application) -> None:
    controller: QueueController = application.bot_data["controller"]
    await controller.stop()
    mongo_client: AsyncIOMotorClient = application.bot_data["mongo_client"]
    mongo_client.close()


def main() -> None:
    settings = Settings.from_env()
    logging.basicConfig(
        level=getattr(logging, settings.log_level, logging.INFO),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

    mongo_client = AsyncIOMotorClient(settings.mongo_uri)
    store = QueueStore(mongo_client[settings.mongo_db_name])

    application = (
        Application.builder()
        .token(settings.bot_token)
        .post_init(_post_init)
        .post_shutdown(_post_shutdown)
        .build()
    )

    controller = QueueController(application, settings, store)
    application.bot_data["controller"] = controller
    application.bot_data["mongo_client"] = mongo_client
    application.add_handler(TypeHandler(Update, controller.handle_update))

    LOGGER.info("Starting queue controller bot")
    application.run_polling(drop_pending_updates=False, allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
