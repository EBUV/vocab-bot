# config.py
import os

BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is not set. Set env var BOT_TOKEN or in config.py.")

DB_PATH = "vocab2.db"
WEBHOOK_PATH = f"/webhook/{BOT_TOKEN}"


# intervals for current user (loaded from Google Sheets)
INTERVALS_PATH = "intervals.json"

# >>> NEW: settings for morning mistakes cron
# Long random string, желательно поменять на свою
CRON_SECRET = os.getenv("CRON_SECRET", "CHANGE_ME_TO_SOMETHING_RANDOM")

# Telegram ID чата, куда отправлять утренний список ошибок
MORNING_CHAT_ID = int(os.getenv("MORNING_CHAT_ID", "518129411"))
