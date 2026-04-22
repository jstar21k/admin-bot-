# Telegram Admin Bot

Telegram bot for Railway that:

- stores uploaded files in a storage channel,
- creates token-based download links,
- checks force-join before delivery,
- posts previews to a channel,
- can schedule approved posts for fixed delays,
- tracks users and downloads in MongoDB,
- optionally censors thumbnails with Pillow + NudeNet.

## Files you need in GitHub

Keep these in the repo:

- `admin bot.py`
- `requirements.txt`
- `Procfile`
- `runtime.txt`
- `.gitignore`
- `README.md`
- `.env.example`

## Environment variables

Set these in Railway service variables:

- `BOT_TOKEN`: Telegram bot token from BotFather.
- `ADMIN_USER_ID`: Your Telegram numeric user ID.
- `MONGODB_URI`: MongoDB connection string.
- `STORAGE_CHANNEL_ID`: Channel ID where uploaded files are stored.
- `POST_CHANNEL_ID`: Channel ID where public posts are sent.
- `GATEWAY_URL`: Your site or bot link base used to build `?token=...` URLs.
- `CENSOR_STYLE`: Optional. `blur`, `pixelate`, or `black`.
- `CENSOR_THRESHOLD`: Optional detection threshold, for example `0.15`.
- `SCHEDULE_POLL_SECONDS`: Optional. How often the bot checks MongoDB for due scheduled posts. Default is `15`.

If you want the 10-minute auto-delete warning to actually delete files later, make sure Railway installs the dependencies from `requirements.txt`, which now includes `APScheduler` for the Telegram job queue.

## Scheduled posting

After the admin sends a thumbnail and sees the preview, the bot now shows inline delay buttons:

- `10m`
- `30m`
- `2h`
- `6h`
- `12h`
- `24h`

When you tap one, the bot saves the post in MongoDB and publishes it later from the same worker. The saved schedule includes the generated caption, link, and the Telegram thumbnail `file_id`, so scheduled posts survive bot restarts.

The admin panel also includes a `Scheduled Posts` button to inspect pending items and refresh the queue view.

## Important hardcoded values

These are still inside `admin bot.py`, so change them in code if needed:

- `FORCE_JOIN_CHANNEL`
- `HOW_TO_OPEN_LINK`

## Railway setup

1. Push this folder to GitHub.
2. In Railway, create a new project from that GitHub repo.
3. Add all environment variables from `.env.example`.
4. Deploy as a worker using the included `Procfile`.

The start command used by Railway is:

```text
python "admin bot.py"
```

## Telegram setup

- Add the bot as admin in the storage channel.
- Add the bot as admin in the post channel.
- If you want join verification to work reliably, add the bot to the force-join channel with enough rights to check members.

## Local run

```bash
pip install -r requirements.txt
python "admin bot.py"
```

The bot now attempts to load a local `.env` file automatically for local testing.
