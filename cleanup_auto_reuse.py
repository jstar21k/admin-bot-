"""
Remove only pending auto-reuse schedule rows from the admin bot database.

Default mode is dry-run. Set CLEAN_AUTO_REUSE_APPLY=true to apply.
This does not touch fresh uploads or already-sent channel posts.
"""

import os
import sys

from pymongo import MongoClient


def env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


APPLY = env_bool("CLEAN_AUTO_REUSE_APPLY", False)
MONGODB_URI = os.getenv("MONGODB_URI") or os.getenv("MONGO_URI")
ADMIN_DB_NAME = os.getenv("ADMIN_DB_NAME", "tg_bot_pro_db")

REUSED_SCHEDULE_FILTER = {
    "status": "scheduled",
    "$or": [
        {"schedule_source": "auto_reuse"},
        {"reused_from_token": {"$exists": True, "$ne": None}},
    ],
}


def require_config() -> None:
    if not MONGODB_URI:
        raise SystemExit("Missing required env: MONGODB_URI or MONGO_URI")


def main() -> int:
    require_config()
    client = MongoClient(MONGODB_URI)
    db = client[ADMIN_DB_NAME]
    scheduled_posts = db["scheduled_posts"]
    files = db["files"]

    reused_posts = list(
        scheduled_posts.find(
            REUSED_SCHEDULE_FILTER,
            {"token": 1, "scheduled_for": 1, "file_name": 1},
        )
    )
    tokens = [post["token"] for post in reused_posts if post.get("token")]
    file_filter = {"token": {"$in": tokens}} if tokens else {"_id": {"$exists": False}}

    print("Mode:", "APPLY" if APPLY else "DRY-RUN")
    print("Admin DB:", ADMIN_DB_NAME)
    print("Pending auto-reuse scheduled posts:", len(reused_posts))
    print("Matching reused file rows:", files.count_documents(file_filter))

    if reused_posts:
        print("\nFirst matching tokens:")
        for post in reused_posts[:10]:
            print("-", post.get("token"), "|", post.get("scheduled_for"), "|", post.get("file_name"))

    if not APPLY:
        print("\nNo changes made. Re-run with CLEAN_AUTO_REUSE_APPLY=true to delete these rows.")
        return 0

    schedule_result = scheduled_posts.delete_many(REUSED_SCHEDULE_FILTER)
    file_result = files.delete_many(file_filter)
    print("\nDeleted pending auto-reuse scheduled posts:", schedule_result.deleted_count)
    print("Deleted matching reused file rows:", file_result.deleted_count)
    return 0


if __name__ == "__main__":
    sys.exit(main())
