# main.py

from fastapi import FastAPI, Request
from aiogram import Bot, Dispatcher, types
from aiogram.enums import ParseMode
from aiogram.filters import CommandStart, Command
from aiogram.client.session.aiohttp import AiohttpSession

from config import BOT_TOKEN, WEBHOOK_PATH
from pydantic import BaseModel
from typing import List, Optional
from db import (
    init_db,
    add_dummy_words_if_empty,
    get_next_word,
    increment_progress,
    replace_all_words,
    get_all_progress,
    Word,
)


# Проверяем токен
if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is not set. Set env var BOT_TOKEN or in config.py.")

# --- Инициализация бота и FastAPI-приложения ---

session = AiohttpSession()
bot = Bot(token=BOT_TOKEN, session=session)
dp = Dispatcher()

app = FastAPI()
# --- Pydantic-модели для sync-эндпоинтов ---

class WordIn(BaseModel):
    sheet_row: int
    progress: int
    question: str
    answer: str
    example: Optional[str] = None


class SyncWordsRequest(BaseModel):
    words: List[WordIn]


# --- Хендлеры бота ---

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

    # пока считаем, что показ карточки = плюс к прогрессу
    await increment_progress(word_id)

    await message.answer(text, parse_mode=ParseMode.MARKDOWN)


# --- Хуки FastAPI ---

@app.on_event("startup")
async def on_startup():
    # Инициализация БД при старте сервиса
    await init_db()
    await add_dummy_words_if_empty()
    print("DB initialized")


@app.get("/")
async def root():
    return {"status": "ok", "message": "vocab-bot is running"}

@app.post("/sync/words")
async def sync_words(payload: SyncWordsRequest):
    """
    Полностью заменяет содержимое таблицы words данными из Google Sheets.

    Ожидаемый формат JSON:
    {
      "words": [
        {
          "sheet_row": 2,
          "progress": 0,
          "question": "дом (по-немецки?)",
          "answer": "das Haus",
          "example": "Das Haus ist groß."
        },
        ...
      ]
    }
    """
    # переводим Pydantic-модели в наш dataclass Word
    words = [
        Word(
            sheet_row=w.sheet_row,
            progress=w.progress,
            question=w.question,
            answer=w.answer,
            example=w.example,
        )
        for w in payload.words
    ]

    await replace_all_words(words)
    return {"status": "ok", "count": len(words)}

@app.get("/sync/progress")
async def sync_progress():
    """
    Возвращает список {sheet_row, progress} для экспорта в Google Sheets.

    Пример ответа:
    {
      "status": "ok",
      "items": [
        {"sheet_row": 2, "progress": 5},
        {"sheet_row": 3, "progress": 1},
        ...
      ]
    }
    """
    items = await get_all_progress()
    return {"status": "ok", "items": items}


@app.post(WEBHOOK_PATH)
async def telegram_webhook(request: Request):
    """Эндпоинт, который будет вызывать Telegram."""
    data = await request.json()
    update = types.Update.model_validate(data)
    await dp.feed_update(bot, update)
    return {"ok": True}
