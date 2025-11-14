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
    get_next_word,
    increment_progress_and_update_due,
    decrement_progress,
    replace_all_words,
    get_all_progress,
    get_due_count,
    Word,
    get_word_by_id,
    log_mistake,
    get_last_mistakes,
    get_users_with_mistakes,
)

# Проверяем токен
if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is not set. Set env var BOT_TOKEN or in config.py.")

session = AiohttpSession()
bot = Bot(token=BOT_TOKEN, session=session)
dp = Dispatcher()

app = FastAPI()

# Последнее слово пользователя (для "I was wrong")
user_last_word: dict[int, int] = {}


# ----- Pydantic модели -----

class WordIn(BaseModel):
    sheet_row: int
    progress: int
    question: str
    answer: str
    example: Optional[str] = None
    # миллисекунды (Date.now()), можем не передавать
    last_success_ts_ms: Optional[int] = None


class SyncWordsRequest(BaseModel):
    words: List[WordIn]


# ----- Вспомогательные функции -----

def build_question_message(row, due_count: int) -> tuple[str, InlineKeyboardMarkup]:
    word_id = row["id"]
    progress = row["progress"]
    question = row["question"]

    text = (
        f"❓ {question}\n\n"
        f"📈 Current progress: {progress}\n"
        f"📚 Words due now: {due_count}"
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
                InlineKeyboardButton(
                    text="↩️ I was wrong",
                    callback_data="ans:fix",
                ),
            ]
        ]
    )

    return text, keyboard


async def send_mistakes_to_user(user_id: int, limit: int = 50):
    """Отправить пользователю последние ошибки."""
    rows = await get_last_mistakes(user_id, limit=limit)
    if not rows:
        await bot.send_message(user_id, "No mistakes logged yet ✅")
        return

    # Заголовок
    await bot.send_message(user_id, "Words you should review:\n")

    # Каждое слово отдельным сообщением: вопрос, 2 пустых строки, ответ
    for row in rows:
        q = row["question"]
        a = row["answer"]
        text = f"{q}\n\n\n{a}"  # две пустые строки между
        await bot.send_message(user_id, text)


# ----- Хендлеры бота -----

@dp.message(CommandStart())
async def cmd_start(message: types.Message):
    text = (
        "Hi! 👋\n\n"
        "I'm a bot for training German vocabulary.\n"
        "Send /next to get the first card.\n\n"
        "For each card choose:\n"
        "• ✅ *I know* – if you remember the word\n"
        "• ❌ *I don't know* – if you don't\n"
        "• ↩️ *I was wrong* – if you realise your last answer was wrong.\n\n"
        "You can also use /mistakes to see your latest mistakes."
    )
    await message.answer(text, parse_mode=ParseMode.MARKDOWN)


@dp.message(Command("next"))
async def cmd_next(message: types.Message):
    row = await get_next_word()
    if not row:
        await message.answer("There are no words in the database yet 🙈")
        return

    due_count = await get_due_count()
    text, keyboard = build_question_message(row, due_count)
    await message.answer(text, parse_mode=ParseMode.MARKDOWN, reply_markup=keyboard)


@dp.message(Command("mistakes"))
async def cmd_mistakes(message: types.Message):
    await send_mistakes_to_user(message.from_user.id, limit=50)


@dp.callback_query(F.data.startswith("ans"))
async def handle_answer(callback: types.CallbackQuery):
    data = callback.data
    user_id = callback.from_user.id

    # ----- "I was wrong" -----
    if data == "ans:fix":
        last_id = user_last_word.get(user_id)
        if not last_id:
            await callback.answer("No previous word to fix.", show_alert=False)
            return

        row = await get_word_by_id(last_id)
        if not row:
            await callback.answer("Previous word not found.", show_alert=False)
            return

        old_progress = row["progress"]
        await decrement_progress(last_id)
        await log_mistake(user_id, last_id)
        new_progress = max(0, old_progress - 1)

        text = (
            "🔁 Previous word corrected.\n"
            f"📉 Progress -1 = {new_progress}"
        )
        await callback.message.answer(text)
        await callback.answer()
        return

    # ----- I know / I don't know -----
    try:
        _, word_id_str, verdict = data.split(":")
        word_id = int(word_id_str)
    except Exception:
        await callback.answer("Something went wrong 🤷‍♂️", show_alert=False)
        return

    row = await get_word_by_id(word_id)
    if not row:
        await callback.answer("Word not found in the database.", show_alert=True)
        return

    user_last_word[user_id] = word_id

    old_progress = row["progress"]

    if verdict == "know":
        delta = 1
        new_progress = await increment_progress_and_update_due(word_id)
    else:  # dont know
        delta = -1
        await decrement_progress(word_id)
        await log_mistake(user_id, word_id)
        new_progress = max(0, old_progress - 1)

    sign = "+" if delta > 0 else "-"

    question = row["question"]
    answer = row["answer"]
    example = row["example"]

    prev_part = f"{question}\n\n{answer}"
    if example:
        prev_part += f"\n\n{example}"
    prev_part += f"\n\n📈 Progress {sign}1 = {new_progress}"

    next_row = await get_next_word()
    if not next_row:
        final_text = prev_part + "\n\nNo more words in the database."
        try:
            await callback.message.edit_reply_markup(reply_markup=None)
        except Exception:
            pass
        await callback.message.answer(final_text, parse_mode=ParseMode.MARKDOWN)
        await callback.answer()
        return

    due_count = await get_due_count()
    next_text, next_keyboard = build_question_message(next_row, due_count)

    full_text = prev_part + "\n\n---\n\n" + next_text

    try:
        await callback.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass

    await callback.message.answer(
        full_text,
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=next_keyboard,
    )

    await callback.answer()


# ----- FastAPI hooks -----

@app.on_event("startup")
async def on_startup():
    await init_db()
    print("DB initialized")


@app.get("/")
async def root():
    return {"status": "ok", "message": "vocab-bot is running"}


# ----- Синхронизация с Google Sheets -----

@app.post("/sync/words")
async def sync_words(payload: SyncWordsRequest):
    """
    Импорт из Google Sheets.

    last_success_ts_ms — в миллисекундах (Date.now()),
    внутри переводим в секунды.
    """
    words: List[Word] = []
    for w in payload.words:
        if w.last_success_ts_ms is not None:
            last_success_sec = int(w.last_success_ts_ms // 1000)
        else:
            last_success_sec = None

        words.append(
            Word(
                sheet_row=w.sheet_row,
                progress=w.progress,
                question=w.question,
                answer=w.answer,
                example=w.example,
                last_success_ts=last_success_sec,
            )
        )

    await replace_all_words(words)
    return {"status": "ok", "count": len(words)}


@app.get("/sync/progress")
async def sync_progress():
    """
    Экспорт в Google Sheets.

    last_success_ts возвращаем в миллисекундах
    (чтобы можно было прямо писать Date.now() в ячейку).
    """
    items = await get_all_progress()
    out = []
    for item in items:
        ts = item["last_success_ts"]
        if ts is not None:
            ts_ms = int(ts * 1000)
        else:
            ts_ms = None
        out.append(
            {
                "sheet_row": item["sheet_row"],
                "progress": item["progress"],
                "last_success_ts_ms": ts_ms,
            }
        )

    return {"status": "ok", "items": out}


# ----- Webhook для Telegram -----

@app.post(WEBHOOK_PATH)
async def telegram_webhook(request: Request):
    data = await request.json()
    update = types.Update.model_validate(data)
    await dp.feed_update(bot, update)
    return {"ok": True}


# ----- Эндпоинт для ежедневного дайджеста ошибок -----

@app.get("/cron/daily_mistakes")
async def cron_daily_mistakes():
    user_ids = await get_users_with_mistakes()
    for uid in user_ids:
        await send_mistakes_to_user(uid, limit=50)
    return {"status": "ok", "users_notified": len(user_ids)}
