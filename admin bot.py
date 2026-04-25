# -*- coding: utf-8 -*-
import asyncio
import html
import io
import logging
import os
import secrets
import socket
import tempfile
import uuid
from contextlib import suppress
from datetime import datetime, timedelta, timezone
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import ReturnDocument
from pymongo.errors import DuplicateKeyError
from telegram import (
    Update, Bot, InlineKeyboardButton, InlineKeyboardMarkup,
    BotCommand, BotCommandScopeDefault, InputFile
)
from telegram.error import Conflict
from telegram.ext import (
    ApplicationBuilder, CommandHandler, MessageHandler,
    CallbackQueryHandler, ContextTypes, filters
)
from telegram.constants import ChatMemberStatus

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

# ━━━ CONFIG ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
BOT_TOKEN = os.environ.get("BOT_TOKEN")
ADMIN_USER_ID = int(os.environ.get("ADMIN_USER_ID", 0))
MONGODB_URI = os.environ.get("MONGODB_URI")
STORAGE_CHANNEL_ID = int(os.environ.get("STORAGE_CHANNEL_ID", 0))
POST_CHANNEL_ID = int(os.environ.get("POST_CHANNEL_ID", 0))  # channel where bot posts thumbnails
GATEWAY_URL = os.environ.get("GATEWAY_URL", "https://vidplays.in/")
FORCE_JOIN_CHANNEL = "link69_viral"  # without @
HOW_TO_OPEN_LINK = "https://t.me/c/2047194577/41"  # Instructions for opening links
INSTANCE_LOCK_ID = "admin_bot_polling_lock"
INSTANCE_ID = os.environ.get("INSTANCE_ID") or f"{socket.gethostname()}-{uuid.uuid4().hex[:8]}"
try:
    SCHEDULE_POLL_SECONDS = max(5, int(os.environ.get("SCHEDULE_POLL_SECONDS", "15")))
except ValueError:
    SCHEDULE_POLL_SECONDS = 15
DISPLAY_TIMEZONE = timezone(timedelta(hours=5, minutes=30))
try:
    INSTANCE_LOCK_TTL_SECONDS = max(60, int(os.environ.get("INSTANCE_LOCK_TTL_SECONDS", "180")))
except ValueError:
    INSTANCE_LOCK_TTL_SECONDS = 180

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# ━━━ DATABASE ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
client = AsyncIOMotorClient(MONGODB_URI)
db = client['tg_bot_pro_db']
files_col = db['files']
users_col = db['users']
logs_col = db['downloads']
runtime_col = db['runtime']
scheduled_posts_col = db['scheduled_posts']

# ━━━ PRELOADED CAPTIONS ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
CAPTIONS = [
"Seedha dil pe lagega! 💘🔥 Dekh ke batao kaisa laga?",
"Raat ki neend uddane wali clip 😈💦 Full HD quality!",
"Ye dekh ke phone side mein rakh dena... control nahi hoga! 🥵",
"Bina sound ke mat dekhna! 🎧 Awaz mein jaadu hai 😏",
"Itna bold content TG pe? 😲 Screenshot mat lena, bas dekho!",
"Subah uthte hi ye dekh liya toh din ban jayega! ☀️😈",
"Aankhein phati ki phati reh jayengi! 👀🔥 End miss mat karna.",
"Ye wali movement next level hai! 🌊💃 Rate karo 1-10?",
"Private collection se nikala hai... special for you! 🤫💎",
"Dil ki dhadkan tez kar dega! ❤️‍🔥 Headphones recommended.",
"Ye video save kar lo, baad mein delete ho jayega! ⏳🏃‍♂️",
"Log pooch rahe hain 'Ye kaun hai?' 😏 Comment mein guess karo!",
"Galti se forward mat karna family group mein! 🙈🚫",
"Isse zyada hot aur kya ho sakta hai? 🤯🔥 Challenge accepted?",
"Sirf close friends ke liye... par tumhare liye public kiya! 😈💌",
"Kal raat viral hua tha, ab yahan available! 🔥📲",
"Agar ye pasand aaya toh '🔥' react karo! Let's see power! 👇",
"Ye angle kisi ne nahi dekha hoga! 📸😲 Unique clip!",
"Thoda sa naughty, thoda sa crazy! 😜💦 Perfect combo.",
"Apne best friend ko bhejo jo single hai! 😂👇 Tag him!",
]

# ━━━ PENDING POST STATE (in-memory) ━━━━━━━━━━━━━━━━━━━━━━━━━
# When media lands in storage, this dict holds the pending post info
# until the matched storage thumbnail or later preview flow completes.
_pending_post = {}  # user_id -> {token, name, duration, thumb, caption, preview_msg_id}
_storage_thumbnail_candidate = None  # last unmatched storage-channel image for the single-post flow

SCHEDULE_OPTIONS = (
    ("10m", 10 * 60),
    ("30m", 30 * 60),
    ("2h", 2 * 60 * 60),
    ("6h", 6 * 60 * 60),
    ("12h", 12 * 60 * 60),
    ("24h", 24 * 60 * 60),
)
SCHEDULE_LABELS = {seconds: label for label, seconds in SCHEDULE_OPTIONS}
SCHEDULE_LIST_LIMIT = 10
AUTO_BATCH_SLOTS = (
    (6, 0, "6:00 AM"),
    (11, 0, "11:00 AM"),
    (16, 0, "4:00 PM"),
    (21, 0, "9:00 PM"),
)
AUTO_BATCH_LIMIT = 10
AUTO_BATCH_LOOKAHEAD_DAYS = 21
CAPTION_ROTATION_STATE_ID = "caption_rotation_state"

CAPTIONS = [
    "End tak dekhoge toh hairan reh jaoge! 😲🔥",
    "Ye miss mat karna... last second mein twist hai! 🤯",
    "Kya aapne wo detail notice ki? 🧐 Dobara dekho!",
    "Sirf 10% log hi iska matlab samajh paaye. 🤫",
    "Wait for it... 🎧 Sound on karke dekho!",
    "Ye clip viral kyun ho rahi hai? Reason jaan lo. 👇",
    "Aankhon pe yakeen nahi hoga! 👀✨ Must watch.",
    "Galti se bhi skip mat karna! ⏳ Value hai isme.",
    "Isse pehle delete ho jaye, save kar lo! 💾🏃‍♂️",
    "Log pooch rahe hain 'Ye kaise kiya?' 😏 Secret revealed.",
    "4K clarity ka asli maza! 🎥💎 Headphones recommended.",
    "Editing next level hai! 🎬 Rate karo 1-10?",
    "Vibes check! ✨ Agar pasand aaye toh 🔥 react karo.",
    "Crystal clear audio + Visuals. 🎧😍 Perfect combo.",
    "Cinematic feel ghar baithe. 🌆🔥 Dekh ke batao.",
    "Best quality version yahan available hai! 📲💯",
    "Thoda unique, thoda crazy! 😜💦 Enjoy!",
    "Apne best friend ke saath share karo! 😂👇 Tag him.",
    "Subah ki shuruwat isi ke saath! ☀️😈 Day made.",
    "Private collection se... special for subscribers! 🤫💎",
    "No words, just vibes. 🔥👇",
    "Save for later! ⏳ You'll need this.",
    "Double tap if you agree! ❤️👇",
    "Comment 'YES' agar full video chahiye! 💬🔥",
    "Link bio/page par hai, jaldi grab karo! 🏃‍♂️💨",
    "Seedha dil pe lagega! 💘🔥 Dekh ke batao kaisa laga?",
    "Raat ki neend uddane wali clip 😈💦 Full HD quality!",
    "Ye dekh ke phone side mein rakh dena... control nahi hoga! 🥵",
    "Bina sound ke mat dekhna! 🎧 Awaz mein jaadu hai 😏",
    "Itna bold content TG pe? 😲 Screenshot mat lena, bas dekho!",
    "Aankhein phati ki phati reh jayengi! 👀🔥 End miss mat karna.",
    "Ye wali movement next level hai! 🌊💃 Rate karo 1-10?",
    "Dil ki dhadkan tez kar dega! ❤️‍🔥 Headphones recommended.",
    "Kal raat viral hua tha, ab yahan available! 🔥📲",
    "Ye angle kisi ne nahi dekha hoga! 📸😲 Unique clip!",
]

CENSOR_STYLE = os.environ.get("CENSOR_STYLE", "blur").strip().lower()
try:
    CENSOR_THRESHOLD = float(os.environ.get("CENSOR_THRESHOLD", "0.15"))
except ValueError:
    CENSOR_THRESHOLD = 0.15

CENSOR_LABELS = {
    "FEMALE_BREAST_EXPOSED",
    "FEMALE_GENITALIA_EXPOSED",
    "MALE_GENITALIA_EXPOSED",
    "BUTTOCKS_EXPOSED",
    "ANUS_EXPOSED",
    "MALE_BREAST_EXPOSED",
}

LABEL_OVAL_SIZE = {
    "FEMALE_BREAST_EXPOSED": 0.85,
    "FEMALE_GENITALIA_EXPOSED": 1.10,
    "MALE_GENITALIA_EXPOSED": 1.10,
    "BUTTOCKS_EXPOSED": 1.00,
    "ANUS_EXPOSED": 1.00,
    "MALE_BREAST_EXPOSED": 0.80,
}

_nude_detector = None


def _lock_document(now: datetime) -> dict:
    return {
        "_id": INSTANCE_LOCK_ID,
        "instance_id": INSTANCE_ID,
        "host": socket.gethostname(),
        "updated_at": now,
        "expires_at": now + timedelta(seconds=INSTANCE_LOCK_TTL_SECONDS),
    }


async def acquire_instance_lock() -> bool:
    now = datetime.now(timezone.utc)
    document = _lock_document(now)

    current = await runtime_col.find_one({"_id": INSTANCE_LOCK_ID})
    if current:
        expires_at = current.get("expires_at")
        if (
            current.get("instance_id") != INSTANCE_ID
            and isinstance(expires_at, datetime)
            and expires_at > now
        ):
            logging.error(
                "Another bot instance is already active on %s until %s (instance=%s).",
                current.get("host", "unknown-host"),
                expires_at.isoformat(),
                current.get("instance_id", "unknown"),
            )
            return False

        await runtime_col.replace_one({"_id": INSTANCE_LOCK_ID}, document, upsert=True)
        return True

    try:
        await runtime_col.insert_one(document)
        return True
    except DuplicateKeyError:
        logging.error("Could not acquire the bot instance lock because another worker created it first.")
        return False


async def renew_instance_lock() -> bool:
    now = datetime.now(timezone.utc)
    result = await runtime_col.update_one(
        {"_id": INSTANCE_LOCK_ID, "instance_id": INSTANCE_ID},
        {"$set": _lock_document(now)},
    )
    return result.modified_count == 1


async def release_instance_lock() -> None:
    await runtime_col.delete_one({"_id": INSTANCE_LOCK_ID, "instance_id": INSTANCE_ID})


async def keep_instance_lock_alive(application) -> None:
    interval_seconds = max(30, INSTANCE_LOCK_TTL_SECONDS // 3)
    while True:
        await asyncio.sleep(interval_seconds)
        renewed = await renew_instance_lock()
        if not renewed:
            logging.error("Lost the single-instance lock; stopping the bot to avoid duplicate polling.")
            await application.stop()
            return


# ━━━ HELPERS ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def generate_token():
    return secrets.token_urlsafe(8)[:10]


def format_duration(seconds):
    if not seconds:
        return "N/A"
    total = int(seconds)
    if total >= 3600:
        h, rem = divmod(total, 3600)
        m, s = divmod(rem, 60)
        return f"{h}:{m:02d}:{s:02d}"
    m, s = divmod(total, 60)
    return f"{m}:{s:02d}"


def admin_kb():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📊 Statistics", callback_data="stats")],
        [InlineKeyboardButton("🔌 System Status", callback_data="status")],
        [InlineKeyboardButton("🔄 Refresh", callback_data="refresh")],
    ])


def preview_kb():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Send Now", callback_data="pc_send"),
         InlineKeyboardButton("🔄 New Caption", callback_data="pc_rot")],
        [InlineKeyboardButton("🖼 New Thumb", callback_data="pc_rethumb"),
         InlineKeyboardButton("❌ Cancel", callback_data="pc_cancel")],
    ])


def get_channel_kb(link: str):
    """Get keyboard for channel posts."""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("▶️ Watch Now", url=link)],
    ])


def admin_kb():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Statistics", callback_data="stats")],
        [InlineKeyboardButton("Scheduled Posts", callback_data="sched_list")],
    ])


def preview_kb():
    return None


def scheduled_list_kb():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Refresh", callback_data="sched_refresh"),
         InlineKeyboardButton("Back", callback_data="sched_back")],
    ])


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


async def pick_next_caption(exclude: str | None = None) -> str:
    available = list(CAPTIONS)
    if exclude and len(available) > 1:
        available = [caption for caption in available if caption != exclude]

    state = await runtime_col.find_one({"_id": CAPTION_ROTATION_STATE_ID})
    remaining = [
        caption for caption in (state or {}).get("remaining", [])
        if caption in available
    ]
    if not remaining:
        remaining = available[:]
        secrets.SystemRandom().shuffle(remaining)

    caption = remaining.pop(0)
    await runtime_col.update_one(
        {"_id": CAPTION_ROTATION_STATE_ID},
        {
            "$set": {
                "remaining": remaining,
                "updated_at": utc_now(),
            }
        },
        upsert=True,
    )
    return caption


def format_schedule_time(value: datetime | None) -> str:
    if not isinstance(value, datetime):
        return "N/A"
    return value.astimezone(DISPLAY_TIMEZONE).strftime("%d %b %I:%M %p IST")


def iter_upcoming_batch_slots(start_time: datetime | None = None):
    current_time = start_time or utc_now()
    current_local = current_time.astimezone(DISPLAY_TIMEZONE)
    start_date = current_local.date()

    for day_offset in range(AUTO_BATCH_LOOKAHEAD_DAYS):
        slot_date = start_date + timedelta(days=day_offset)
        for hour, minute, label in AUTO_BATCH_SLOTS:
            slot_local = datetime(
                slot_date.year,
                slot_date.month,
                slot_date.day,
                hour,
                minute,
                tzinfo=DISPLAY_TIMEZONE,
            )
            slot_utc = slot_local.astimezone(timezone.utc)
            if slot_utc > current_time:
                yield slot_utc, label


async def count_batch_slot_posts(scheduled_for: datetime) -> int:
    return await scheduled_posts_col.count_documents(
        {
            "status": {"$in": ["scheduled", "posting"]},
            "scheduled_for": scheduled_for,
        }
    )


async def get_next_auto_schedule_slot() -> tuple[datetime, str, int]:
    for scheduled_for, batch_label in iter_upcoming_batch_slots():
        booked_count = await count_batch_slot_posts(scheduled_for)
        if booked_count < AUTO_BATCH_LIMIT:
            return scheduled_for, batch_label, booked_count + 1
    raise RuntimeError("No free auto-schedule batch slot found in the next 21 days.")


def get_post_link(post_data: dict) -> str:
    return post_data.get("link") or f"{GATEWAY_URL}?token={post_data['token']}"


def build_post_caption(post_data: dict) -> str:
    return f"{post_data['caption']}\n\nâ± Duration: {post_data['duration']}"


def is_storage_thumbnail_post(post) -> bool:
    if post.photo:
        return True
    mime_type = (getattr(post.document, "mime_type", None) or "").lower()
    return bool(post.document and mime_type.startswith("image/"))


def get_storage_thumbnail_post_file_id(post) -> str | None:
    if post.photo:
        return post.photo[-1].file_id
    if is_storage_thumbnail_post(post):
        return post.document.file_id
    return None


def is_matching_storage_thumbnail(media_message_id: int | None, thumb_message_id: int | None) -> bool:
    if not isinstance(media_message_id, int) or not isinstance(thumb_message_id, int):
        return False
    return abs(media_message_id - thumb_message_id) == 1


def get_post_media(post_data: dict):
    if post_data.get("thumbnail_file_id"):
        return post_data["thumbnail_file_id"]
    if post_data.get("thumb"):
        return post_data["thumb"]
    if post_data.get("thumb_bytes"):
        return build_thumb_inputfile(post_data["thumb_bytes"])
    return None


async def send_public_post(bot: Bot, post_data: dict):
    link = get_post_link(post_data)
    caption = build_post_caption(post_data)
    target_chat_id = POST_CHANNEL_ID or ADMIN_USER_ID
    media = get_post_media(post_data)

    if media:
        sent_message = await bot.send_photo(
            chat_id=target_chat_id,
            photo=media,
            caption=caption,
            parse_mode="HTML",
            reply_markup=get_channel_kb(link),
        )
    else:
        text = caption if POST_CHANNEL_ID else f"ðŸ“ <b>Post:</b>\n\n{caption}"
        sent_message = await bot.send_message(
            chat_id=target_chat_id,
            text=text,
            parse_mode="HTML",
            reply_markup=get_channel_kb(link),
        )

    return sent_message, target_chat_id


async def create_scheduled_post(pending: dict, delay_seconds: int) -> datetime:
    now = utc_now()
    scheduled_for = now + timedelta(seconds=delay_seconds)
    await scheduled_posts_col.insert_one({
        "token": pending["token"],
        "file_name": pending["name"],
        "caption": pending["caption"],
        "duration": pending["duration"],
        "thumbnail_file_id": pending.get("thumb"),
        "link": get_post_link(pending),
        "status": "scheduled",
        "delay_seconds": delay_seconds,
        "delay_label": SCHEDULE_LABELS.get(delay_seconds, f"{delay_seconds}s"),
        "scheduled_for": scheduled_for,
        "created_at": now,
        "updated_at": now,
        "sent_at": None,
        "failed_at": None,
        "last_error": None,
        "target_chat_id": POST_CHANNEL_ID or ADMIN_USER_ID,
        "target_message_id": None,
    })
    return scheduled_for


async def create_auto_scheduled_post(pending: dict) -> tuple[datetime, str, int]:
    now = utc_now()
    scheduled_for, batch_label, batch_position = await get_next_auto_schedule_slot()
    delay_seconds = max(0, int((scheduled_for - now).total_seconds()))
    delay_label = f"Batch {batch_label} #{batch_position}"
    await scheduled_posts_col.insert_one({
        "token": pending["token"],
        "file_name": pending["name"],
        "caption": pending["caption"],
        "duration": pending["duration"],
        "thumbnail_file_id": pending.get("thumb"),
        "link": get_post_link(pending),
        "status": "scheduled",
        "delay_seconds": delay_seconds,
        "delay_label": delay_label,
        "scheduled_for": scheduled_for,
        "created_at": now,
        "updated_at": now,
        "sent_at": None,
        "failed_at": None,
        "last_error": None,
        "target_chat_id": POST_CHANNEL_ID or ADMIN_USER_ID,
        "target_message_id": None,
    })
    return scheduled_for, delay_label, batch_position


async def claim_due_scheduled_post() -> dict | None:
    now = utc_now()
    return await scheduled_posts_col.find_one_and_update(
        {
            "status": "scheduled",
            "scheduled_for": {"$lte": now},
        },
        {
            "$set": {
                "status": "posting",
                "updated_at": now,
                "posting_started_at": now,
            }
        },
        sort=[("scheduled_for", 1), ("created_at", 1)],
        return_document=ReturnDocument.BEFORE,
    )


async def mark_scheduled_post_sent(post_id, sent_message_id: int | None, target_chat_id: int) -> None:
    now = utc_now()
    await scheduled_posts_col.update_one(
        {"_id": post_id},
        {
            "$set": {
                "status": "sent",
                "sent_at": now,
                "updated_at": now,
                "target_message_id": sent_message_id,
                "target_chat_id": target_chat_id,
                "last_error": None,
            }
        },
    )


async def mark_scheduled_post_failed(post_id, error_message: str) -> None:
    now = utc_now()
    await scheduled_posts_col.update_one(
        {"_id": post_id},
        {
            "$set": {
                "status": "failed",
                "failed_at": now,
                "updated_at": now,
                "last_error": error_message,
            }
        },
    )


async def publish_due_scheduled_posts(bot: Bot) -> None:
    while True:
        scheduled_post = await claim_due_scheduled_post()
        if not scheduled_post:
            return

        try:
            sent_message, target_chat_id = await send_public_post(bot, scheduled_post)
            await mark_scheduled_post_sent(
                scheduled_post["_id"],
                getattr(sent_message, "message_id", None),
                target_chat_id,
            )
            logging.info(
                "Scheduled post sent for token %s at %s",
                scheduled_post.get("token"),
                format_schedule_time(scheduled_post.get("scheduled_for")),
            )
        except Exception as exc:
            error_message = str(exc)
            await mark_scheduled_post_failed(scheduled_post["_id"], error_message)
            logging.exception("Scheduled post failed for token %s", scheduled_post.get("token"))
            with suppress(Exception):
                await bot.send_message(
                    chat_id=ADMIN_USER_ID,
                    text=(
                        "âŒ <b>Scheduled post failed.</b>\n\n"
                        f"ðŸ“ <code>{html.escape(scheduled_post.get('file_name', 'Post'))}</code>\n"
                        f"â° <code>{format_schedule_time(scheduled_post.get('scheduled_for'))}</code>\n"
                        f"ðŸª² <code>{html.escape(error_message)}</code>"
                    ),
                    parse_mode="HTML",
                )


async def scheduled_post_poller(application) -> None:
    while True:
        try:
            await publish_due_scheduled_posts(application.bot)
        except asyncio.CancelledError:
            raise
        except Exception:
            logging.exception("Scheduled post poller crashed during a cycle")
        await asyncio.sleep(SCHEDULE_POLL_SECONDS)


async def ensure_runtime_indexes() -> None:
    await scheduled_posts_col.create_index("token", unique=True, name="scheduled_token_unique_idx")
    await scheduled_posts_col.create_index(
        [("status", 1), ("scheduled_for", 1)],
        name="scheduled_status_due_idx",
    )


async def get_database_status_label() -> str:
    try:
        await client.admin.command("ping")
        return "Connected"
    except Exception:
        return "Disconnected"


async def build_next_batch_lines(limit: int = 4) -> list[str]:
    lines = []
    for index, (scheduled_for, batch_label) in enumerate(iter_upcoming_batch_slots(), start=1):
        booked_count = await count_batch_slot_posts(scheduled_for)
        lines.append(
            f"{index}. <code>{format_schedule_time(scheduled_for)}</code> | "
            f"<code>{booked_count}/{AUTO_BATCH_LIMIT}</code> | "
            f"<code>{html.escape(batch_label)}</code>"
        )
        if index >= limit:
            break
    return lines


async def build_admin_home_text() -> str:
    db_status = await get_database_status_label()
    pending_count = await scheduled_posts_col.count_documents({"status": "scheduled"})
    failed_count = await scheduled_posts_col.count_documents({"status": "failed"})
    next_post = await scheduled_posts_col.find_one(
        {"status": "scheduled"},
        sort=[("scheduled_for", 1)],
    )

    lines = [
        "<b>JSTAR PRO ADMIN PANEL</b>",
        "",
        f"System status: <code>Running</code>",
        f"MongoDB: <code>{db_status}</code>",
        f"Pending scheduled posts: <code>{pending_count}</code>",
        f"Failed scheduled posts: <code>{failed_count}</code>",
    ]

    if next_post:
        lines.extend([
            f"Next scheduled post: <code>{format_schedule_time(next_post['scheduled_for'])}</code>",
            f"Next file: <code>{html.escape(next_post.get('file_name', 'Post'))}</code>",
        ])
    else:
        lines.append("Next scheduled post: <code>None</code>")

    batch_lines = await build_next_batch_lines()
    if batch_lines:
        lines.extend([
            "",
            "<b>Upcoming batch slots</b>",
            *batch_lines,
        ])

    lines.extend([
        "",
        "Auto-schedule batches: <code>6 AM, 11 AM, 4 PM, 9 PM</code>",
        "Upload a file to storage to start a new post.",
    ])
    return "\n".join(lines)


async def build_scheduled_posts_text(limit: int = SCHEDULE_LIST_LIMIT) -> str:
    pending_count = await scheduled_posts_col.count_documents({"status": "scheduled"})
    sent_count = await scheduled_posts_col.count_documents({"status": "sent"})
    failed_count = await scheduled_posts_col.count_documents({"status": "failed"})
    posts = await scheduled_posts_col.find(
        {"status": "scheduled"},
        sort=[("scheduled_for", 1)],
    ).to_list(length=limit)

    lines = [
        "<b>Scheduled Posts</b>",
        "",
        f"Pending: <code>{pending_count}</code>",
        f"Sent: <code>{sent_count}</code>",
        f"Failed: <code>{failed_count}</code>",
        "",
    ]

    if not posts:
        lines.append("No pending scheduled posts.")
        return "\n".join(lines)

    for index, post in enumerate(posts, start=1):
        file_name = html.escape(post.get("file_name", "Post"))[:40]
        delay_label = html.escape(post.get("delay_label", "saved"))
        lines.append(
            f"{index}. <code>{file_name}</code>\n"
            f"   {format_schedule_time(post.get('scheduled_for'))} | {delay_label}"
        )

    if pending_count > limit:
        lines.extend([
            "",
            f"Showing first <code>{limit}</code> pending posts.",
        ])

    return "\n".join(lines)


async def auto_schedule_pending_post(
    bot: Bot,
    pending: dict,
    pending_user_id: int | None = None,
) -> tuple[datetime, str]:
    scheduled_for, delay_label, _batch_position = await create_auto_scheduled_post(pending)
    if pending_user_id is not None:
        _pending_post.pop(pending_user_id, None)
    if STORAGE_CHANNEL_ID:
        try:
            await bot.send_message(chat_id=STORAGE_CHANNEL_ID, text="post done")
        except Exception:
            logging.exception(
                "Failed to send post completion signal for token %s",
                pending.get("token"),
            )
    return scheduled_for, delay_label


async def send_auto_schedule_confirmation(bot: Bot, pending: dict, scheduled_for: datetime, delay_label: str) -> None:
    try:
        await bot.send_message(
            chat_id=ADMIN_USER_ID,
            text=(
                "⏰ <b>Post scheduled automatically.</b>\n\n"
                f"Batch: <code>{html.escape(delay_label)}</code>\n"
                f"Time: <code>{format_schedule_time(scheduled_for)}</code>\n"
                f"Thumbnail saved: <code>{'yes' if pending.get('thumb') else 'no'}</code>"
            ),
            parse_mode="HTML",
        )
    except Exception:
        logging.exception(
            "Failed to send auto-schedule confirmation for token %s",
            pending.get("token"),
        )


def get_nude_detector():
    global _nude_detector
    if _nude_detector is None:
        try:
            from nudenet import NudeDetector
            _nude_detector = NudeDetector()
            logging.info("NudeDetector loaded successfully.")
        except Exception as e:
            logging.warning(f"NudeDetector unavailable: {e}")
            _nude_detector = False
    return _nude_detector or None


def skin_blur_oval(img, cx, cy, rw, rh, blur_radius=35):
    from PIL import Image, ImageDraw, ImageFilter

    x1 = max(0, cx - rw)
    y1 = max(0, cy - rh)
    x2 = min(img.width, cx + rw)
    y2 = min(img.height, cy + rh)
    if x2 <= x1 or y2 <= y1 or (x2 - x1) < 4 or (y2 - y1) < 4:
        return img

    region = img.crop((x1, y1, x2, y2))
    blurred = region.copy()
    for _ in range(10):
        blurred = blurred.filter(ImageFilter.GaussianBlur(radius=blur_radius))

    mask = Image.new("L", (x2 - x1, y2 - y1), 0)
    ImageDraw.Draw(mask).ellipse([0, 0, (x2 - x1) - 1, (y2 - y1) - 1], fill=255)
    mask = mask.filter(ImageFilter.GaussianBlur(radius=max(4, min(x2 - x1, y2 - y1) // 8)))

    composite = region.copy()
    composite.paste(blurred, (0, 0), mask)
    output = img.copy()
    output.paste(composite, (x1, y1))
    return output


def pixelate_oval(img, cx, cy, rw, rh, block_size=12):
    from PIL import Image, ImageDraw, ImageFilter

    x1 = max(0, cx - rw)
    y1 = max(0, cy - rh)
    x2 = min(img.width, cx + rw)
    y2 = min(img.height, cy + rh)
    if x2 <= x1 or y2 <= y1:
        return img

    width = x2 - x1
    height = y2 - y1
    mask = Image.new("L", (width, height), 0)
    ImageDraw.Draw(mask).ellipse([0, 0, width - 1, height - 1], fill=255)
    mask = mask.filter(ImageFilter.GaussianBlur(radius=max(4, min(width, height) // 8)))

    region = img.crop((x1, y1, x2, y2))
    mosaic = region.resize(
        (max(1, width // block_size), max(1, height // block_size)),
        Image.BOX,
    ).resize((width, height), Image.NEAREST)

    composite = region.copy()
    composite.paste(mosaic, (0, 0), mask)
    output = img.copy()
    output.paste(composite, (x1, y1))
    return output


def black_oval(img, cx, cy, rw, rh):
    from PIL import ImageDraw

    output = img.copy()
    ImageDraw.Draw(output).ellipse(
        [
            max(0, cx - rw),
            max(0, cy - rh),
            min(img.width, cx + rw),
            min(img.height, cy + rh),
        ],
        fill=(0, 0, 0),
    )
    return output


def apply_censor(img, det_box, label, style):
    x, y, w, h = [int(value) for value in det_box]
    cx = x + w // 2
    cy = y + h // 2
    size = LABEL_OVAL_SIZE.get(label, 0.90)
    rw = max(8, int((w / 2) * size))
    rh = max(8, int((h / 2) * size))

    if style == "pixelate":
        return pixelate_oval(img, cx, cy, rw, rh)
    if style == "black":
        return black_oval(img, cx, cy, rw, rh)
    return skin_blur_oval(img, cx, cy, rw, rh)


def censor_thumbnail_bytes(image_bytes: bytes) -> bytes:
    try:
        from PIL import Image
    except Exception as e:
        logging.warning(f"Pillow unavailable, skipping thumbnail censor: {e}")
        return image_bytes

    detector = get_nude_detector()
    if detector is None:
        return image_bytes

    try:
        with Image.open(io.BytesIO(image_bytes)) as opened:
            image = opened.convert("RGB")
    except Exception as e:
        logging.warning(f"Failed to open thumbnail image: {e}")
        return image_bytes

    temp_path = None
    try:
        with tempfile.NamedTemporaryFile(suffix=".jpg", delete=False) as temp_file:
            temp_path = temp_file.name
            image.save(temp_file, format="JPEG", quality=95)

        detections = detector.detect(temp_path) or []
        censored = image.copy()

        for detection in detections:
            label = detection.get("class", "")
            score = detection.get("score", 0)
            box = detection.get("box", [])
            if label not in CENSOR_LABELS:
                continue
            if score < CENSOR_THRESHOLD or len(box) != 4:
                continue
            censored = apply_censor(censored, box, label, CENSOR_STYLE)

        output = io.BytesIO()
        censored.save(output, format="JPEG", quality=95)
        return output.getvalue()
    except Exception as e:
        logging.warning(f"Failed to censor thumbnail: {e}")
        return image_bytes
    finally:
        if temp_path and os.path.exists(temp_path):
            try:
                os.remove(temp_path)
            except OSError:
                pass


def build_thumb_inputfile(image_bytes: bytes) -> InputFile:
    return InputFile(io.BytesIO(image_bytes), filename="censored_thumb.jpg")


def get_thumb_media(pending: dict):
    if pending.get("thumb"):
        return pending["thumb"]
    if pending.get("thumb_bytes"):
        return build_thumb_inputfile(pending["thumb_bytes"])
    return None


async def send_pending_preview(bot: Bot, pending: dict):
    preview_msg = await bot.send_photo(
        chat_id=ADMIN_USER_ID,
        photo=get_thumb_media(pending),
        caption=build_post_caption(pending),
        parse_mode="HTML",
    )
    if preview_msg.photo:
        pending['thumb'] = preview_msg.photo[-1].file_id
    pending['preview_msg_id'] = preview_msg.message_id
    pending['preview_chat_id'] = preview_msg.chat_id
    pending.pop('awaiting_storage_thumb', None)


async def is_joined(bot: Bot, user_id: int) -> bool:
    """Smart join check: API first, DB fallback if API fails.
    Once a user is verified as joined, save to DB so they
    never get asked again (even if API acts up).

    IMPORTANT: If API check fails (e.g., bot not admin), we trust
    the user's claim to have joined to avoid blocking real users.
    """
    # Step 1: Try Telegram API
    try:
        member = await bot.get_chat_member(
            chat_id=f"@{FORCE_JOIN_CHANNEL}", user_id=user_id
        )
        joined = member.status in (
            ChatMemberStatus.MEMBER,
            ChatMemberStatus.ADMINISTRATOR,
            ChatMemberStatus.OWNER,
        )
        # Save result to DB (cache for next time)
        await users_col.update_one(
            {"user_id": user_id},
            {"$set": {"channel_joined": joined}},
            upsert=True,
        )
        return joined
    except Exception as e:
        logging.warning(f"get_chat_member failed: {e}")
        # API check failed - this usually means:
        # 1. Bot is not admin of the channel
        # 2. Privacy restrictions
        # 3. Rate limiting
        # Don't block user - check DB cache only
        pass

    # Step 2: API failed → check DB cache
    user = await users_col.find_one({"user_id": user_id})
    if user and user.get("channel_joined"):
        return True  # Was verified before, trust the cache

    # API failed AND not in cache - user claims they joined, so trust them
    # This prevents blocking users when bot verification doesn't work
    logging.info(f"User {user_id}: API check failed, trusting user's claim of joining")
    return True


# ━━━ AUTO-DELETE JOB ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

async def auto_delete(context: ContextTypes.DEFAULT_TYPE):
    chat_id, file_msg_id, warn_msg_id = context.job.data
    try:
        await context.bot.delete_message(chat_id=chat_id, message_id=file_msg_id)
        await context.bot.delete_message(chat_id=chat_id, message_id=warn_msg_id)
    except Exception as e:
        logging.warning(f"Auto-delete skipped: {e}")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  COMMAND: /start
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    await users_col.update_one(
        {"user_id": user.id},
        {"$set": {"last_seen": utc_now(), "name": user.full_name}},
        upsert=True,
    )

    # ── /start <token> → deliver file with force-join check ──
    if context.args:
        token = context.args[0]
        file_data = await files_col.find_one({"token": token})

        if not file_data:
            await update.message.reply_text("❌ Invalid or expired link.")
            return

        joined = await is_joined(context.bot, user.id)
        if not joined:
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("📺 Join Channel", url=f"https://t.me/{FORCE_JOIN_CHANNEL}")],
                [InlineKeyboardButton("✅ I've Joined", callback_data="check_join")],
            ])
            await update.message.reply_text(
                "🔒 <b>Access Denied!</b>\n\n"
                "You must join our channel to get the file.\n"
                "Join below, then tap <b>I've Joined</b> 👇",
                reply_markup=kb,
                parse_mode="HTML",
            )
            context.user_data['pending_token'] = token
            return

        # Save joined status in DB so we don't ask again
        await users_col.update_one(
            {"user_id": user.id},
            {"$set": {"channel_joined": True}},
            upsert=True,
        )
        await deliver_file(update, context, file_data)
        return

    # ── Normal /start (no token) ──
    if user.id == ADMIN_USER_ID:
        await update.message.reply_text(
            await build_admin_home_text(),
            reply_markup=admin_kb(),
            parse_mode="HTML",
        )
        return

    # Check DB cache first (instant, no API call)
    user_data = await users_col.find_one({"user_id": user.id})
    if user_data and user_data.get("channel_joined"):
        await update.message.reply_text(
            "👋 Welcome back!\n\nSend me a link to get your file.",
            parse_mode="HTML",
        )
        return

    # Not cached → do full API check
    joined = await is_joined(context.bot, user.id)
    if joined:
        await update.message.reply_text(
            "👋 Welcome back!\n\nSend me a link to get your file.",
            parse_mode="HTML",
        )
    else:
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("📺 Join Channel", url=f"https://t.me/{FORCE_JOIN_CHANNEL}")],
            [InlineKeyboardButton("✅ I've Joined", callback_data="check_join")],
        ])
        await update.message.reply_text(
            "👋 Welcome!\n\n"
            "🔒 <b>Join our channel first</b> to access files.\n"
            "Join below, then tap <b>I've Joined</b> 👇",
            reply_markup=kb,
            parse_mode="HTML",
        )


# ━━━ DELIVER FILE ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

async def deliver_file(update: Update, context: ContextTypes.DEFAULT_TYPE, file_data: dict):
    """Send file to user, update stats, schedule auto-delete."""
    user_id = update.effective_user.id
    token = file_data.get('token')

    try:
        fname = file_data.get('file_name', 'Video')
        caption = f"🎥 <b>File:</b> {fname}\n🚀 <b>Delivered by @{FORCE_JOIN_CHANNEL}</b>"

        file_msg = await context.bot.copy_message(
            chat_id=user_id,
            from_chat_id=STORAGE_CHANNEL_ID,
            message_id=int(file_data['storage_msg_id']),
            caption=caption,
            parse_mode="HTML",
        )

        # Stats - track download with user_id (exclude admin from counting)
        await files_col.update_one({"token": token}, {"$inc": {"total_downloads": 1}})
        await logs_col.insert_one({
            "token": token,
            "user_id": user_id,  # Track user for analytics
            "is_admin": user_id == ADMIN_USER_ID,  # Mark admin downloads
            "time": utc_now()
        })

        # Warning + auto-delete after 10 min (send directly to user, not reply)
        warn_msg = await context.bot.send_message(
            chat_id=user_id,
            text="⚠️ <b>Save to Saved Messages now!</b>\n"
                 "This file will be deleted in <b>10 minutes</b>.",
            parse_mode="HTML",
        )
        if context.job_queue:
            context.job_queue.run_once(
                auto_delete, 600,
                [user_id, file_msg.message_id, warn_msg.message_id],
                chat_id=user_id,
            )
        else:
            logging.warning(
                "Job queue is unavailable; auto-delete was skipped for user %s",
                user_id,
            )

    except Exception as e:
        await context.bot.send_message(
            chat_id=user_id,
            text=f"❌ <b>Error:</b> {str(e)}",
            parse_mode="HTML"
        )
        logging.error(f"Delivery failed: {e}")


# ━━━ FORCE JOIN CHECK ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

async def force_join_check(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """After user clicks 'I've Joined', verify and deliver file."""
    q = update.callback_query
    await q.answer()

    user_id = update.effective_user.id
    pending_token = context.user_data.get('pending_token')

    joined = await is_joined(context.bot, user_id)
    if not joined:
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("📺 Join Channel", url=f"https://t.me/{FORCE_JOIN_CHANNEL}")],
            [InlineKeyboardButton("✅ I've Joined", callback_data="check_join")],
        ])
        await q.edit_message_text(
            "❌ <b>You haven't joined yet!</b>\n\n"
            "Join the channel first, then click below:",
            reply_markup=kb,
            parse_mode="HTML",
        )
        return

    # User is joined → save to DB so never asked again
    await users_col.update_one(
        {"user_id": user_id},
        {"$set": {"channel_joined": True}},
        upsert=True,
    )

    if pending_token:
        file_data = await files_col.find_one({"token": pending_token})
        if file_data:
            await q.edit_message_text(
                "✅ <b>Verified!</b> Delivering your file...",
                parse_mode="HTML",
            )
            await deliver_file(update, context, file_data)
            context.user_data.pop('pending_token', None)
            return

    await q.edit_message_text(
        "✅ <b>Welcome!</b>\n\nNow send me your link to get the file.",
        parse_mode="HTML",
    )


# ━━━ ADMIN CALLBACK BUTTONS ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

async def admin_buttons(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    if query.data == "stats":
        # Get current time for today calculations
        today_start = datetime.now(timezone.utc).replace(
            hour=0, minute=0, second=0, microsecond=0
        )

        # Total links count
        total_links = await files_col.count_documents({})

        # Total users count
        total_users = await users_col.count_documents({})

        # New users today
        new_users_today = await users_col.count_documents({
            "last_seen": {"$gte": today_start}
        })

        # Total downloads (all)
        agg_all = await logs_col.aggregate(
            [{"$group": {"_id": None, "dl": {"$sum": 1}}}]
        ).to_list(1)
        total_dl_all = agg_all[0]['dl'] if agg_all else 0

        # User downloads only (exclude admin)
        agg_users = await logs_col.aggregate([
            {"$match": {"is_admin": {"$ne": True}}},  # Exclude admin
            {"$group": {"_id": None, "dl": {"$sum": 1}}}
        ]).to_list(1)
        total_dl_users = agg_users[0]['dl'] if agg_users else 0

        # Downloads today (user only, exclude admin)
        today_dl = await logs_col.count_documents({
            "time": {"$gte": today_start},
            "is_admin": {"$ne": True}  # Exclude admin
        })

        # New users who joined today (based on last_seen >= today)
        # This is same as new_users_today

        await query.edit_message_text(
            f"📊 <b>DETAILED ANALYTICS</b>\n\n"
            f"👥 Total Users: <code>{total_users}</code>\n"
            f"👥 New Today: <code>{new_users_today}</code>\n"
            f"🔗 Total Links: <code>{total_links}</code>\n"
            f"📥 Downloads (Users): <code>{total_dl_users}</code>\n"
            f"📥 Downloads (All incl. Admin): <code>{total_dl_all}</code>\n"
            f"📅 Downloads Today: <code>{today_dl}</code>",
            reply_markup=admin_kb(), parse_mode="HTML",
        )

    elif query.data == "status":
        try:
            await client.admin.command('ping')
            db_st = "✅ Connected"
        except Exception:
            db_st = "❌ Disconnected"
        await query.edit_message_text(
            f"🔌 <b>SYSTEM STATUS</b>\n\n"
            f"🗄 MongoDB: <code>{db_st}</code>\n"
            f"🛰 Bot: <code>✅ Running</code>",
            reply_markup=admin_kb(), parse_mode="HTML",
        )

    elif query.data == "refresh":
        await query.edit_message_text("✅ Refreshed!", reply_markup=admin_kb())


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  STORAGE UPLOAD → AUTO-LINK + STORAGE THUMBNAIL
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

async def on_storage_upload(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle one storage media upload and one matching storage thumbnail image."""
    global _storage_thumbnail_candidate
    post = update.channel_post
    if not post or post.chat_id != STORAGE_CHANNEL_ID:
        return

    if is_storage_thumbnail_post(post):
        thumb_file_id = get_storage_thumbnail_post_file_id(post)
        if not thumb_file_id:
            logging.warning("Storage thumbnail message %s has no usable file_id.", post.message_id)
            return

        pending = _pending_post.get(ADMIN_USER_ID)
        if pending and pending.get('awaiting_storage_thumb'):
            if not is_matching_storage_thumbnail(pending.get('storage_msg_id'), post.message_id):
                logging.warning(
                    "Storage thumbnail message %s did not clearly match storage media message %s for token %s; leaving post pending.",
                    post.message_id,
                    pending.get('storage_msg_id'),
                    pending.get('token'),
                )
                return

            pending['thumb'] = thumb_file_id
            _storage_thumbnail_candidate = None

            try:
                await context.bot.send_message(
                    chat_id=ADMIN_USER_ID,
                    text="🖼 <b>Matching thumbnail found in the storage channel.</b>\nPreview is ready below.",
                    parse_mode="HTML",
                )
                await send_pending_preview(context.bot, pending)
                scheduled_for, delay_label = await auto_schedule_pending_post(
                    context.bot,
                    pending,
                    pending_user_id=ADMIN_USER_ID,
                )
                await send_auto_schedule_confirmation(context.bot, pending, scheduled_for, delay_label)
                _pending_post.pop(ADMIN_USER_ID, None)
            except Exception:
                logging.exception(
                    "Failed to build preview after matching storage thumbnail for token %s",
                    pending.get('token'),
                )
            return

        if pending:
            logging.warning(
                "Ignoring storage thumbnail message %s because token %s is already in preview/schedule flow.",
                post.message_id,
                pending.get('token'),
            )
            return

        if _storage_thumbnail_candidate:
            logging.warning(
                "Replacing unmatched storage thumbnail message %s with newer message %s.",
                _storage_thumbnail_candidate.get('message_id'),
                post.message_id,
            )

        _storage_thumbnail_candidate = {
            'message_id': post.message_id,
            'file_id': thumb_file_id,
        }
        logging.info("Stored storage thumbnail candidate from message %s.", post.message_id)
        return

    att = post.effective_attachment
    if not att or isinstance(att, list):
        return

    # ── Extract file name (fix: video objects may not have file_name) ──
    if post.video:
        file_name = getattr(post.video, 'file_name', None) or "New_Video"
        video_duration = post.video.duration or 0
    elif post.document:
        file_name = getattr(post.document, 'file_name', None) or "New_File"
        video_duration = getattr(post.document, 'duration', None) or 0
    elif post.audio:
        file_name = getattr(post.audio, 'file_name', None) or "New_Audio"
        video_duration = getattr(post.audio, 'duration', None) or 0
    else:
        file_name = "New_Upload"
        video_duration = 0

    # ── Save to DB ──
    token = generate_token()
    await files_col.insert_one({
        "file_name": file_name,
        "token": token,
        "storage_msg_id": post.message_id,
        "video_duration": video_duration,
        "created_at": utc_now(),
        "total_downloads": 0,
    })

    link = f"{GATEWAY_URL}?token={token}"
    pending = {
        'token': token,
        'name': file_name,
        'duration': format_duration(video_duration),
        'link': link,
        'caption': await pick_next_caption(),
        'storage_msg_id': post.message_id,
        'awaiting_storage_thumb': True,
    }
    _pending_post[ADMIN_USER_ID] = pending

    if _storage_thumbnail_candidate:
        if is_matching_storage_thumbnail(post.message_id, _storage_thumbnail_candidate.get('message_id')):
            pending['thumb'] = _storage_thumbnail_candidate['file_id']
            _storage_thumbnail_candidate = None

            try:
                await context.bot.send_message(
                    chat_id=ADMIN_USER_ID,
                    text=(
                        f"🚀 <b>Auto-Link Created!</b>\n\n"
                        f"📁 <code>{file_name}</code>\n"
                        f"⏱ <code>{format_duration(video_duration)}</code>\n"
                        f"🔗 <code>{link}</code>\n\n"
                        f"🖼 <b>Thumbnail taken from the matching storage channel image.</b>"
                    ),
                    parse_mode="HTML",
                )
                await send_pending_preview(context.bot, pending)
                scheduled_for, delay_label = await auto_schedule_pending_post(
                    context.bot,
                    pending,
                    pending_user_id=ADMIN_USER_ID,
                )
                await send_auto_schedule_confirmation(context.bot, pending, scheduled_for, delay_label)
                _pending_post.pop(ADMIN_USER_ID, None)
            except Exception:
                logging.exception(
                    "Failed to reuse matching storage thumbnail for token %s",
                    token,
                )
            return

        logging.warning(
            "Storage thumbnail message %s did not clearly match new storage media message %s for token %s; post will stay pending.",
            _storage_thumbnail_candidate.get('message_id'),
            post.message_id,
            token,
        )
        _storage_thumbnail_candidate = None

    # ── Send link to admin and wait for matching storage thumbnail ──
    try:
        await context.bot.send_message(
            chat_id=ADMIN_USER_ID,
            text=(
                f"🚀 <b>Auto-Link Created!</b>\n\n"
                f"📁 <code>{file_name}</code>\n"
                f"⏱ <code>{format_duration(video_duration)}</code>\n"
                f"🔗 <code>{link}</code>\n\n"
                f"🖼 <b>Waiting for the matching thumbnail image from the storage channel.</b>\n"
                f"The post will stay pending until it arrives there.\n"
                f"(or send /skip to post without thumbnail)"
            ),
            parse_mode="HTML",
        )
    except Exception:
        logging.error("Failed to notify admin.")
        _pending_post.pop(ADMIN_USER_ID, None)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  ADMIN SENDS THUMBNAIL (photo in private chat)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

async def on_admin_photo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin sent a photo — check if there's a pending post waiting for thumbnail."""
    user_id = update.effective_user.id
    if user_id != ADMIN_USER_ID:
        return

    pending = _pending_post.get(user_id)
    if not pending:
        return  # No pending post, ignore

    if not update.message.photo:
        return  # Not a photo

    # ── Save thumbnail, generate caption, show preview ──
    photo_file = await context.bot.get_file(update.message.photo[-1].file_id)
    pending['thumb_bytes'] = censor_thumbnail_bytes(
        bytes(await photo_file.download_as_bytearray())
    )
    pending['caption'] = await pick_next_caption()

    link = f"{GATEWAY_URL}?token={pending['token']}"
    cap = f"{pending['caption']}\n\n⏱ Duration: {pending['duration']}"

    preview_msg = await update.message.reply_photo(
        photo=build_thumb_inputfile(pending['thumb_bytes']),
        caption=cap,
        parse_mode="HTML",
    )
    if preview_msg.photo:
        pending['thumb'] = preview_msg.photo[-1].file_id
    pending['preview_msg_id'] = preview_msg.message_id
    pending['preview_chat_id'] = preview_msg.chat_id
    try:
        scheduled_for, delay_label = await auto_schedule_pending_post(
            context.bot,
            pending,
            pending_user_id=user_id,
        )
        await send_auto_schedule_confirmation(context.bot, pending, scheduled_for, delay_label)
        _pending_post.pop(user_id, None)
    except DuplicateKeyError:
        await update.message.reply_text("❌ This post is already scheduled.", parse_mode="HTML")
    except Exception as exc:
        await update.message.reply_text(
            f"❌ Could not auto-schedule this post: {exc}",
            parse_mode="HTML",
        )


# ━━━ /skip COMMAND — skip thumbnail, post with caption only ━━━

async def skip_thumb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id != ADMIN_USER_ID:
        return

    pending = _pending_post.get(user_id)
    if not pending:
        await update.message.reply_text("❌ No pending post to skip.")
        return

    link = f"{GATEWAY_URL}?token={pending['token']}"
    cap = f"{secrets.choice(CAPTIONS)}\n\n⏱ Duration: {pending['duration']}"

    # Post directly to channel with BOTH buttons
    if POST_CHANNEL_ID:
        try:
            await context.bot.send_message(
                chat_id=POST_CHANNEL_ID,
                text=cap,
                reply_markup=get_channel_kb(link),
                parse_mode="HTML",
            )
            await update.message.reply_text(
                "✅ <b>Posted to channel!</b>",
                parse_mode="HTML",
            )
        except Exception as e:
            await update.message.reply_text(
                f"❌ Failed to post: {e}\nCheck POST_CHANNEL_ID & bot admin rights.",
                parse_mode="HTML",
            )
    else:
        await context.bot.send_message(
            chat_id=ADMIN_USER_ID,
            text=f"📝 <b>Post:</b>\n\n{cap}",
            reply_markup=get_channel_kb(link),
            parse_mode="HTML",
        )
        await update.message.reply_text(
            "✅ <b>Done!</b> POST_CHANNEL_ID not set — sent to you.\nSet it in Railway to auto-post.",
            parse_mode="HTML",
        )
    _pending_post.pop(user_id, None)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  POST PREVIEW CALLBACKS (Send Now / Rotate / New Thumb / Cancel)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

async def post_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    user_id = update.effective_user.id

    pending = _pending_post.get(user_id)
    if not pending:
        await q.answer("❌ Session expired.", show_alert=True)
        return

    # ── SEND NOW → post directly to channel ──
    if q.data == "pc_send":
        link = f"{GATEWAY_URL}?token={pending['token']}"
        cap = f"{pending['caption']}\n\n⏱ Duration: {pending['duration']}"

        # Remove buttons from preview
        try:
            await q.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass

        # Post directly to channel with BOTH buttons
        if POST_CHANNEL_ID:
            try:
                await context.bot.send_photo(
                    chat_id=POST_CHANNEL_ID,
                    photo=get_thumb_media(pending),
                    caption=cap,
                    parse_mode="HTML",
                    reply_markup=get_channel_kb(link),
                )
                await q.message.reply_text(
                    "✅ <b>Posted to channel!</b>",
                    parse_mode="HTML",
                )
            except Exception as e:
                await q.message.reply_text(
                    f"❌ Failed to post: {e}\nCheck POST_CHANNEL_ID & bot admin rights.",
                    parse_mode="HTML",
                )
        else:
            # Fallback: send to admin if POST_CHANNEL_ID not set
            await context.bot.send_photo(
                chat_id=ADMIN_USER_ID,
                photo=get_thumb_media(pending),
                caption=cap,
                parse_mode="HTML",
                reply_markup=get_channel_kb(link),
            )
            await q.message.reply_text(
                "✅ <b>Done!</b> POST_CHANNEL_ID not set — sent to you.\nSet it in Railway to auto-post.",
                parse_mode="HTML",
            )
        _pending_post.pop(user_id, None)

    # ── NEW CAPTION ──
    elif q.data == "pc_rot":
        pending['caption'] = secrets.choice(
            [c for c in CAPTIONS if c != pending['caption']]
        )
        cap = f"{pending['caption']}\n\n⏱ Duration: {pending['duration']}"
        try:
            await q.edit_message_caption(
                caption=cap,
                parse_mode="HTML",
                reply_markup=preview_kb(),
            )
        except Exception:
            pass

    # ── NEW THUMBNAIL ──
    elif q.data == "pc_rethumb":
        try:
            await q.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass
        await q.message.reply_text(
            "🖼 Send me a <b>new thumbnail</b>:\n(or /skip to post without)",
            parse_mode="HTML",
        )

    # ── CANCEL ──
    elif q.data == "pc_cancel":
        try:
            await q.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass
        await q.message.reply_text("❌ Post cancelled.", parse_mode="HTML")
        _pending_post.pop(user_id, None)


# ━━━ MAIN ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

async def safe_query_edit(query, text: str, reply_markup) -> None:
    try:
        await query.edit_message_text(
            text,
            reply_markup=reply_markup,
            parse_mode="HTML",
        )
    except Exception as exc:
        if "message is not modified" not in str(exc).lower():
            raise


async def admin_buttons(update: Update, context: ContextTypes.DEFAULT_TYPE):
    del context
    query = update.callback_query
    await query.answer()

    if query.data == "stats":
        today_start = utc_now().replace(hour=0, minute=0, second=0, microsecond=0)
        total_links = await files_col.count_documents({})
        total_users = await users_col.count_documents({})
        new_users_today = await users_col.count_documents({
            "last_seen": {"$gte": today_start}
        })

        agg_all = await logs_col.aggregate(
            [{"$group": {"_id": None, "dl": {"$sum": 1}}}]
        ).to_list(1)
        total_dl_all = agg_all[0]['dl'] if agg_all else 0

        agg_users = await logs_col.aggregate([
            {"$match": {"is_admin": {"$ne": True}}},
            {"$group": {"_id": None, "dl": {"$sum": 1}}}
        ]).to_list(1)
        total_dl_users = agg_users[0]['dl'] if agg_users else 0

        today_dl = await logs_col.count_documents({
            "time": {"$gte": today_start},
            "is_admin": {"$ne": True}
        })

        await safe_query_edit(
            query,
            f"📊 <b>DETAILED ANALYTICS</b>\n\n"
            f"👥 Total Users: <code>{total_users}</code>\n"
            f"👥 New Today: <code>{new_users_today}</code>\n"
            f"🔗 Total Links: <code>{total_links}</code>\n"
            f"📥 Downloads (Users): <code>{total_dl_users}</code>\n"
            f"📥 Downloads (All incl. Admin): <code>{total_dl_all}</code>\n"
            f"📅 Downloads Today: <code>{today_dl}</code>",
            admin_kb(),
        )
        return

    if query.data in {"sched_list", "sched_refresh"}:
        await safe_query_edit(
            query,
            await build_scheduled_posts_text(),
            scheduled_list_kb(),
        )
        return

    if query.data in {"status", "sched_back", "refresh"}:
        await safe_query_edit(
            query,
            await build_admin_home_text(),
            admin_kb(),
        )


async def skip_thumb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id != ADMIN_USER_ID:
        return

    pending = _pending_post.get(user_id)
    if not pending:
        await update.message.reply_text("❌ No pending post to skip.")
        return

    pending['caption'] = secrets.choice(CAPTIONS)

    try:
        await send_public_post(context.bot, pending)
        if POST_CHANNEL_ID:
            await update.message.reply_text(
                "✅ <b>Posted to channel!</b>",
                parse_mode="HTML",
            )
        else:
            await update.message.reply_text(
                "✅ <b>Done!</b> POST_CHANNEL_ID not set - sent to you.\nSet it in Railway to auto-post.",
                parse_mode="HTML",
            )
        if STORAGE_CHANNEL_ID:
            with suppress(Exception):
                await context.bot.send_message(chat_id=STORAGE_CHANNEL_ID, text="post done")
    except Exception as e:
        await update.message.reply_text(
            f"❌ Failed to post: {e}\nCheck POST_CHANNEL_ID and bot admin rights.",
            parse_mode="HTML",
        )

    _pending_post.pop(user_id, None)


async def post_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    user_id = update.effective_user.id

    pending = _pending_post.get(user_id)
    if not pending:
        await q.answer("Session expired.", show_alert=True)
        return

    if q.data == "pc_send":
        try:
            await q.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass

        try:
            await send_public_post(context.bot, pending)
            if POST_CHANNEL_ID:
                await q.message.reply_text(
                    "✅ <b>Posted to channel!</b>",
                    parse_mode="HTML",
                )
            else:
                await q.message.reply_text(
                    "✅ <b>Done!</b> POST_CHANNEL_ID not set - sent to you.\nSet it in Railway to auto-post.",
                    parse_mode="HTML",
                )
            if STORAGE_CHANNEL_ID:
                with suppress(Exception):
                    await context.bot.send_message(chat_id=STORAGE_CHANNEL_ID, text="post done")
        except Exception as e:
            await q.message.reply_text(
                f"❌ Failed to post: {e}\nCheck POST_CHANNEL_ID and bot admin rights.",
                parse_mode="HTML",
            )

        _pending_post.pop(user_id, None)
        return

    if q.data.startswith("pc_delay_"):
        try:
            delay_seconds = int(q.data.split("_")[-1])
        except ValueError:
            await q.answer("Invalid schedule.", show_alert=True)
            return

        try:
            scheduled_for = await create_scheduled_post(pending, delay_seconds)
        except DuplicateKeyError:
            await q.answer("This post is already scheduled.", show_alert=True)
            return
        except Exception as exc:
            await q.answer("Failed to save schedule.", show_alert=True)
            await q.message.reply_text(
                f"❌ Could not schedule this post: {exc}",
                parse_mode="HTML",
            )
            return

        _pending_post.pop(user_id, None)

        try:
            await q.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass

        await q.message.reply_text(
            "⏰ <b>Post scheduled.</b>\n\n"
            f"Delay: <code>{html.escape(SCHEDULE_LABELS.get(delay_seconds, str(delay_seconds)))}</code>\n"
            f"Time: <code>{format_schedule_time(scheduled_for)}</code>\n"
            f"Thumbnail saved: <code>{'yes' if pending.get('thumb') else 'no'}</code>",
            parse_mode="HTML",
        )
        return

    if q.data == "pc_rot":
        pending['caption'] = secrets.choice(
            [c for c in CAPTIONS if c != pending['caption']]
        )
        try:
            await q.edit_message_caption(
                caption=build_post_caption(pending),
                parse_mode="HTML",
                reply_markup=preview_kb(),
            )
        except Exception:
            pass
        return

    if q.data == "pc_rethumb":
        try:
            await q.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass
        await q.message.reply_text(
            "Send me a <b>new thumbnail</b>:\n(or /skip to post without)",
            parse_mode="HTML",
        )
        return

    if q.data == "pc_cancel":
        try:
            await q.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass
        await q.message.reply_text("❌ Post cancelled.", parse_mode="HTML")
        _pending_post.pop(user_id, None)


async def on_startup(application) -> None:
    await ensure_runtime_indexes()
    application.bot_data["scheduled_post_task"] = asyncio.create_task(
        scheduled_post_poller(application)
    )


async def admin_buttons(update: Update, context: ContextTypes.DEFAULT_TYPE):
    del context
    query = update.callback_query
    await query.answer()

    if query.data == "stats":
        today_start = utc_now().replace(hour=0, minute=0, second=0, microsecond=0)
        total_links = await files_col.count_documents({})
        total_users = await users_col.count_documents({})
        new_users_today = await users_col.count_documents({
            "last_seen": {"$gte": today_start}
        })

        agg_all = await logs_col.aggregate(
            [{"$group": {"_id": None, "dl": {"$sum": 1}}}]
        ).to_list(1)
        total_dl_all = agg_all[0]['dl'] if agg_all else 0

        agg_users = await logs_col.aggregate([
            {"$match": {"is_admin": {"$ne": True}}},
            {"$group": {"_id": None, "dl": {"$sum": 1}}}
        ]).to_list(1)
        total_dl_users = agg_users[0]['dl'] if agg_users else 0

        today_dl = await logs_col.count_documents({
            "time": {"$gte": today_start},
            "is_admin": {"$ne": True}
        })

        await safe_query_edit(
            query,
            f"📊 <b>DETAILED ANALYTICS</b>\n\n"
            f"👥 Total Users: <code>{total_users}</code>\n"
            f"👥 New Today: <code>{new_users_today}</code>\n"
            f"🔗 Total Links: <code>{total_links}</code>\n"
            f"📥 Downloads (Users): <code>{total_dl_users}</code>\n"
            f"📥 Downloads (All incl. Admin): <code>{total_dl_all}</code>\n"
            f"📅 Downloads Today: <code>{today_dl}</code>",
            admin_kb(),
        )
        return

    if query.data in {"status", "sched_back", "refresh"}:
        await safe_query_edit(
            query,
            await build_admin_home_text(),
            admin_kb(),
        )
        return

    if query.data in {"sched_list", "sched_refresh"}:
        await safe_query_edit(
            query,
            await build_scheduled_posts_text(),
            scheduled_list_kb(),
        )


async def skip_thumb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id != ADMIN_USER_ID:
        return

    pending = _pending_post.get(user_id)
    if not pending:
        await update.message.reply_text("❌ No pending post to skip.")
        return

    pending['caption'] = await pick_next_caption()

    try:
        scheduled_for, delay_label = await auto_schedule_pending_post(
            context.bot,
            pending,
            pending_user_id=user_id,
        )
        await update.message.reply_text(
            "⏰ <b>Post scheduled automatically.</b>\n\n"
            f"Batch: <code>{html.escape(delay_label)}</code>\n"
            f"Time: <code>{format_schedule_time(scheduled_for)}</code>\n"
            f"Thumbnail saved: <code>{'yes' if pending.get('thumb') else 'no'}</code>",
            parse_mode="HTML",
        )
    except Exception as exc:
        await update.message.reply_text(
            f"❌ Could not auto-schedule this post: {exc}",
            parse_mode="HTML",
        )

    _pending_post.pop(user_id, None)


async def post_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    user_id = update.effective_user.id

    pending = _pending_post.get(user_id)
    if not pending:
        await q.answer("Session expired.", show_alert=True)
        return

    if q.data == "pc_send" or q.data.startswith("pc_delay_"):
        try:
            await q.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass

        try:
            scheduled_for, delay_label = await auto_schedule_pending_post(
                context.bot,
                pending,
                pending_user_id=user_id,
            )
            await q.message.reply_text(
                "⏰ <b>Post scheduled automatically.</b>\n\n"
                f"Batch: <code>{html.escape(delay_label)}</code>\n"
                f"Time: <code>{format_schedule_time(scheduled_for)}</code>\n"
                f"Thumbnail saved: <code>{'yes' if pending.get('thumb') else 'no'}</code>",
                parse_mode="HTML",
            )
        except Exception as exc:
            await q.message.reply_text(
                f"❌ Could not auto-schedule this post: {exc}",
                parse_mode="HTML",
            )

        _pending_post.pop(user_id, None)
        return

    if q.data == "pc_rot":
        pending['caption'] = await pick_next_caption(exclude=pending.get('caption'))
        try:
            await q.edit_message_caption(
                caption=build_post_caption(pending),
                parse_mode="HTML",
            )
        except Exception:
            pass
        return

    if q.data == "pc_rethumb":
        try:
            await q.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass
        await q.message.reply_text(
            "Send me a <b>new thumbnail</b>:\n(or /skip to post without)",
            parse_mode="HTML",
        )
        return

    if q.data == "pc_cancel":
        try:
            await q.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass
        await q.message.reply_text("❌ Post cancelled.", parse_mode="HTML")
        _pending_post.pop(user_id, None)


async def on_startup(application) -> None:
    await ensure_runtime_indexes()
    try:
        await application.bot.set_my_commands(
            [BotCommand("start", "Open the bot dashboard")],
            scope=BotCommandScopeDefault(),
        )
    except Exception:
        logging.exception("Failed to register bot commands during startup.")
    application.bot_data["scheduled_post_task"] = asyncio.create_task(
        scheduled_post_poller(application)
    )


async def on_shutdown(application) -> None:
    scheduled_task = application.bot_data.get("scheduled_post_task")
    if scheduled_task:
        scheduled_task.cancel()
        with suppress(asyncio.CancelledError):
            await scheduled_task
    client.close()


if __name__ == '__main__':
    app = (
        ApplicationBuilder()
        .token(BOT_TOKEN)
        .post_init(on_startup)
        .post_shutdown(on_shutdown)
        .build()
    )

    # Commands
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("skip", skip_thumb))

    # Force-join verify callback
    app.add_handler(CallbackQueryHandler(force_join_check, pattern="^check_join$"))

    # Admin panel buttons (stats/status/scheduled/refresh)
    app.add_handler(
        CallbackQueryHandler(
            admin_buttons,
            pattern="^(stats|status|refresh|sched_list|sched_refresh|sched_back)$",
        )
    )

    # Post preview buttons (send/rotate/rethumb/cancel)
    app.add_handler(CallbackQueryHandler(post_callback, pattern="^pc_"))

    # Admin sends photo → check if pending post needs thumbnail
    app.add_handler(MessageHandler(
        filters.Chat(ADMIN_USER_ID) & filters.PHOTO & ~filters.UpdateType.CHANNEL_POST,
        on_admin_photo,
    ))

    # Storage channel upload → auto-link + storage thumbnail matching
    app.add_handler(MessageHandler(
        filters.Chat(STORAGE_CHANNEL_ID) & (filters.PHOTO | filters.VIDEO | filters.Document.ALL | filters.AUDIO),
        on_storage_upload,
    ))

    print("🚀 JSTAR PRO Bot is Live...")
    app.run_polling()
