# main.py

from fastapi import FastAPI, Request
from aiogram import Bot, Dispatcher, F, types
from aiogram.enums import ParseMode
from aiogram.filters import CommandStart, Command
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

from config import BOT_TOKEN, WEBHOOK_PATH
from pydantic import BaseModel
from typing import List, Optional

from db import (
    init_db,
    add_dummy_words_if_empty,
    get_next_word,
    increment_progress,
    decrement_progress,
    replace_all_words,
    get_all_progress,
    Word,
    get_word_by_id,
)


# --- Проверка токена ---

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is not set. Set env var BOT_TOKEN.")


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


# --- Вспомогательная функция: текст вопроса + клавиатура ---

def build_question_message(row) -> tuple[str, InlineKeyboardMarkup]:
    """Готовим текст вопроса и клавиатуру для одного слова."""
    word_id = row["id"]
    progress = row["progress"]
    question = row["question"]

    text = (
        f"❓ *Question*\n"
        f"{question}\n\n"
        f"📈 Current progress for this word: {progress}"
    )

    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="✅ I know",
                    callback_data=f"ans:{word_id}:know",
                ),
                InlineKeyboardButton(
                    text="❌ I don't know",
                    callback_data=f"ans:{word_id}:dont",
                ),
            ]
        ]
    )

    return text, keyboard


# --- Хендлеры бота ---

@dp.message(CommandStart())
async def cmd_start(message: types.Message):
    text = (
        "Hi! 👋\n\n"
        "I'm a bot for training German vocabulary.\n"
        "Send /next to get the first card.\n\n"
        "For each card choose:\n"
        "• ✅ *I know* – if you remember the word\n"
        "• ❌ *I don't know* – if you don't.\n\n"
        "Based on your answers, words you know worse will appear more often."
    )
    await message.answer(text, parse_mode=ParseMode.MARKDOWN)


@dp.message(Command("next"))
async def cmd_next(message: types.Message):
    row = await get_next_word()
    if not row:
        await message.answer("There are no words in the database yet 🙈")
        return

    text, keyboard = build_question_message(row)
    await message.answer(text, parse_mode=ParseMode.MARKDOWN, reply_markup=keyboard)


@dp.callback_query(F.data.startswith("ans:"))
async def handle_answer(callback: types.CallbackQuery):
    """Пользователь нажал: I know / I don't know."""
    try:
        _, word_id_str, verdict = callback.data.split(":")
        word_id = int(word_id_str)
    except Exception:
        await callback.answer("Something went wrong 🤷‍♂️", show_alert=False)
        return

    row = await get_word_by_id(word_id)
    if not row:
        await callback.answer("Word not found in the database.", show_alert=True)
        return

    old_progress = row["progress"]

    # Обновляем прогресс
    if verdict == "know":
        await increment_progress(word_id)
        new_progress = old_progress + 1
        verdict_text = "✅ You marked this word as *known*."
    else:
        await decrement_progress(word_id)
        new_progress = max(0, old_progress - 1)
        verdict_text = "❌ You marked this word as *not known*."

    question = row["question"]
    answer = row["answer"]
    example = row["example"]

    # Текст по предыдущему слову
    result_part = (
        f"{verdict_text}\n\n"
        f"❓ *Previous question*\n{question}\n\n"
        f"✅ *Answer*\n{answer}\n\n"
        f"📈 New progress: {new_progress}"
    )
    if example:
        result_part += f"\n\n💬 *Example*\n_{example}_"

    # Готовим следующее слово
    next_row = await get_next_word()
    if not next_row:
        # Больше слов нет
        final_text = result_part + "\n\nNo more words in the database."
        try:
            await callback.message.edit_reply_markup(reply_markup=None)
        except Exception:
            pass
        await callback.message.answer(final_text, parse_mode=ParseMode.MARKDOWN)
        await callback.answer()
        return

    next_text, next_keyboard = build_question_message(next_row)

    full_text = result_part + "\n\n---\n\n➡ *Next card:*\n\n" + next_text

    # Убираем клавиатуру с предыдущего сообщения
    try:
        await callback.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass

    # Отправляем новое сообщение с ответом + новым вопросом
    await callback.message.answer(
        full_text,
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=next_keyboard,
    )

    await callback.answer()


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


# --- Эндпоинты для синхронизации с Google Sheets ---

@app.post("/sync/words")
async def sync_words(payload: SyncWordsRequest):
    """
    Полностью заменяет содержимое таблицы words данными из Google Sheets.
    """
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
    """
    items = await get_all_progress()
    return {"status": "ok", "items": items}


# --- Webhook для Telegram ---

@app.post(WEBHOOK_PATH)
async def telegram_webhook(request: Request):
    """Эндпоинт, который будет вызывать Telegram."""
    data = await request.json()
    update = types.Update.model_validate(data)
    await dp.feed_update(bot, update)
    return {"ok": True}
