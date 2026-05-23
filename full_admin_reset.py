"""
Full fresh reset for the admin bot database.

Default mode is dry-run. Set ADMIN_FULL_RESET_APPLY=true on Railway to apply.

This clears documents from known admin collections with delete_many({}) instead
of dropping the database, so collection/index structure can be recreated safely.
"""

import os
import sys
from typing import Iterable

from pymongo import MongoClient


def env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


APPLY = env_bool("ADMIN_FULL_RESET_APPLY", False)
MONGODB_URI = os.getenv("MONGODB_URI") or os.getenv("MONGO_URI")
ADMIN_DB_NAME = os.getenv("ADMIN_DB_NAME", "tg_bot_pro_db")

ADMIN_COLLECTIONS = ("files", "scheduled_posts", "users", "downloads", "runtime")


def require_config() -> None:
    if not MONGODB_URI:
        raise SystemExit("Missing required env: MONGODB_URI or MONGO_URI")


def clear_collections(db, collection_names: Iterable[str], label: str) -> None:
    for name in collection_names:
        collection = db[name]
        count = collection.count_documents({})
        if APPLY:
            result = collection.delete_many({})
            print(f"DELETED {label}.{name}: {result.deleted_count}")
        else:
            print(f"DRY-RUN delete {label}.{name}: would delete {count}")


def main() -> int:
    require_config()
    client = MongoClient(MONGODB_URI)
    db = client[ADMIN_DB_NAME]

    print("Mode:", "APPLY" if APPLY else "DRY-RUN")
    print("Admin DB:", ADMIN_DB_NAME)
    print("Admin collections:", ", ".join(ADMIN_COLLECTIONS))

    clear_collections(db, ADMIN_COLLECTIONS, ADMIN_DB_NAME)

    if not APPLY:
        print("\nNo changes made. Re-run with ADMIN_FULL_RESET_APPLY=true to clear these documents.")
    else:
        print("\nAdmin DB full reset complete. Start admin-bot, then queue-bot.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
