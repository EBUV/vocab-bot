# main.py

import asyncio
from fastapi import FastAPI, Request
from aiogram import Bot, Dispatcher, types
from aiogram.enums import ParseMode
from aiogram.filters import CommandStart, Command
from aiogram.client.session.aiohttp import AiohttpSession

from config import BOT_TOKEN, WEBHOOK_PATH
from db import init_db, add_dummy_words_if_empty, get_next_word, increment_progress

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is not set. Set env var BOT_TOKEN or in config.py.")

# Создаём бота и диспетчер
session = AiohttpSession()
bot = Bot(token=BOT_TOKEN, session=session)
dp = Dispatcher()

app = FastAPI()


# --- Handlers бота ---

@dp.message(CommandStart())
async def cmd_start(message: types.Message):
    text = (
        "Привет! 👋\n\n"
        "Я бот для тренировки немецких слов.\n"
        "Команда /next покажет тебе следующее слово."
    )
    await message.answer(text)


@dp.message(Command("next"))
async def cmd_next(message: types.Message):
    row = await get_next_word()
    if not row:
        await message.answer("В базе пока нет слов 🙈")
        return

    word_id = row["id"]
    progress = row["progress"]
    question = row["question"]
    answer = row["answer"]
    example = row["example"]

    text = (
        f"❓ {question}\n"
        f"✅ *{answer}*\n"
        f"📈 Прогресс: {progress}"
    )
    if example:
        text += f"\n\n💬 Beispiel:\n_{example}_"

    await increment_progress(word_id)

    await message.answer(text, parse_mode=ParseMode.MARKDOWN)


# --- FastAPI маршруты ---

@app.on_event("startup")
async def on_startup():
    # Инициализируем БД
    await init_db()
    await add_dummy_words_if_empty()
    print("DB initialized")

@app.get("/")
async def root():
    return {"status": "ok", "message": "vocab-bot is running"}

@app.post(WEBHOOK_PATH)
async def telegram_webhook(request: Request):
    """Эндпоинт, который будет вызывать Telegram."""
    data = await request.json()
    update = types.Update.model_validate(data)
    await dp.feed_update(bot, update)
    return {"ok": True}
