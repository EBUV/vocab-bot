# config.py
import os

BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is not set. Please set env var BOT_TOKEN.")

DB_PATH = "vocab2.db"
WEBHOOK_PATH = f"/webhook/{BOT_TOKEN}"
