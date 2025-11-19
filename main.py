# main.py
import logging

logging.basicConfig(level=logging.INFO)

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
    replace_all_mistakes,
    get_all_progress,
    get_all_mistakes_for_sync,
    get_due_count,
    Word,
    get_word_by_id,
    log_mistake,
    get_last_mistakes,
    get_users_with_mistakes,
    get_stats,
)

# ----- ACCESS CONTROL -----

ALLOWED_USER_IDS = {518129411}  # your Telegram user ID


def is_allowed(user_id: int) -> bool:
    return user_id in ALLOWED_USER_IDS


# ----- TEXT SANITIZING (remove problematic chars for Telegram) -----

# удаляем только "странные" управляющие символы, но оставляем \t, \n, \r
CODES_TO_REMOVE = {c for c in range(0, 32) if c not in (9, 10, 13)}
CODES_TO_REMOVE.add(127)  # DEL

# иногда проблемы создают спец. разделители строк из Юникода
UNICODE_BAD_CODES = {0x2028, 0x2029}


def sanitize_text(text: str) -> str:
    """Remove characters that Telegram may not like (control chars etc.)."""
    if not text:
        return text
    result_chars = []
    for ch in text:
        code = ord(ch)
        if code in CODES_TO_REMOVE or code in UNICODE_BAD_CODES:
            continue
        result_chars.append(ch)
    return "".join(result_chars)


def escape_markdown(text: str) -> str:
    """
    Аккуратно экранируем спецсимволы Markdown, чтобы Телега не ругалась.
    Используем Markdown V2-синтаксис.
    """
    if not text:
        return text
    special = r"_*[]()~`>#+-=|{}.!\\"  # набор спецсимволов для MarkdownV2
    escaped = []
    for ch in text:
        if ch in special:
            escaped.append("\\" + ch)
        else:
            escaped.append(ch)
    return "".join(escaped)


async def safe_answer_message(msg: types.Message, text: str, **kwargs):
    """
    Пытаемся отправить с MarkdownV2.
    Если падает – логируем и пробуем без форматирования.
    """
    try:
        safe_text = sanitize_text(text)
        md_text = escape_markdown(safe_text)
        return await msg.answer(
            md_text,
            parse_mode=ParseMode.MARKDOWN_V2,
            **kwargs,
        )
    except Exception:
        logging.exception("Failed to send markdown message, retrying without markdown")
        try:
            safe_text = sanitize_text(text)
            return await msg.answer(safe_text, **kwargs)
        except Exception:
            logging.exception("Failed to send plain text message as well")
            return None


# ----- BOT & APP SETUP -----

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is not set. Set env var BOT_TOKEN or in config.py.")

session = AiohttpSession()
bot = Bot(token=BOT_TOKEN, session=session)
dp = Dispatcher()

app = FastAPI()

# Store last answered word per user (for "I was wrong")
user_last_word: dict[int, int] = {}


# ----- Pydantic models for sync endpoints -----


class WordIn(BaseModel):
    sheet_row: int
    progress: int
    question: str
    answer: str
    example: Optional[str] = None
    # milliseconds (Date.now()), may be omitted
    last_success_ts_ms: Optional[int] = None
    # total mistakes count for this word (column I)
    mistakes_count: Optional[int] = 0


class MistakeLogIn(BaseModel):
    user_id: int
    ts_ms: int           # timestamp in milliseconds (Date.now)
    question: str
    answer: str


class SyncWordsRequest(BaseModel):
    words: List[WordIn]
    mistakes_log: Optional[List[MistakeLogIn]] = None


# ----- Helper functions -----


def build_question_message(row, due_count: int) -> tuple[str, InlineKeyboardMarkup]:
    """Build the question text and inline keyboard for a single word."""
    word_id = row["id"]
    progress = row["progress"]
    question = row["question"]

    text = (
        f"❓ {question}\n\n"
        f"📈 Current progress: {progress}\n"
        f"📚 Words due now: {due_count}"
    )
    text = sanitize_text(text)

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


async def send_mistakes_to_user(user_id: int, limit: int = 80):
    """
    Отправляет пользователю его последние `limit` ошибок
    в порядке от старых к новым.
    """
    rows = await get_last_mistakes(user_id, limit=limit)
    if not rows:
        await bot.send_message(user_id, "No mistakes logged yet ✅")
        return

    # Header message
    await bot.send_message(user_id, "Words you should review:\n")

    # Каждое слово отдельным сообщением: вопрос, две пустые строки, ответ
    for row in rows:
        q = row["question"]
        a = row["answer"]
        text = f"{q}\n\n\n{a}"
        text = sanitize_text(text)
        await bot.send_message(user_id, text)


# ----- Bot handlers -----


@dp.message(CommandStart())
async def cmd_start(message: types.Message):
    if not is_allowed(message.from_user.id):
        await message.answer("Sorry, this bot is currently in private beta.")
        return

    text = (
        "Hi! 👋\n\n"
        "I'm a bot for training German vocabulary.\n"
        "Use /next to get the first card.\n\n"
        "For each card choose:\n"
        "• ✅ *I know* – if you remember the word\n"
        "• ❌ *I don't know* – if you don't\n"
        "• ↩️ *I was wrong* – if you realise your last answer was wrong.\n\n"
        "You can also use:\n"
        "• /mistakes – to see your latest mistakes\n"
        "• /stats – to see your current statistics."
    )
    await safe_answer_message(message, text)


@dp.message(Command("next"))
async def cmd_next(message: types.Message):
    if not is_allowed(message.from_user.id):
        await message.answer("Sorry, this bot is currently in private beta.")
        return

    row = await get_next_word()
    if not row:
        await message.answer("There are no words in the database yet 🙈")
        return

    due_count = await get_due_count()
    text, keyboard = build_question_message(row, due_count)
    await safe_answer_message(message, text, reply_markup=keyboard)


@dp.message(Command("mistakes"))
async def cmd_mistakes(message: types.Message):
    if not is_allowed(message.from_user.id):
        await message.answer("Sorry, this bot is currently in private beta.")
        return

    await send_mistakes_to_user(message.from_user.id, limit=80)


@dp.message(Command("stats"))
async def cmd_stats(message: types.Message):
    """Show basic learning statistics."""
    user_id = message.from_user.id
    if not is_allowed(user_id):
        await message.answer("Sorry, this bot is currently in private beta.")
        return

    s = await get_stats(user_id)

    text = (
        "📊 *Your stats*\n\n"
        f"• Total words in deck: *{s['total_words']}*\n"
        f"• Words due now: *{s['due_now']}*\n"
        f"• Well-known words (progress ≥ 5): *{s['well_known']}*\n"
        f"• Total mistakes logged: *{s['mistakes_total']}*"
    )

    await safe_answer_message(message, text)


@dp.callback_query(F.data.startswith("ans"))
async def handle_answer(callback: types.CallbackQuery):
    user_id = callback.from_user.id

    if not is_allowed(user_id):
        await callback.answer("Access denied.", show_alert=True)
        return

    data = callback.data

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

        old_progress = int(row["progress"])
        new_progress = await decrement_progress(last_id)
        await log_mistake(user_id, last_id)

        delta = old_progress - new_progress
        if delta < 0:
            delta = 0  # на всякий случай
        text = (
            "🔁 Previous word corrected.\n"
            f"📉 Progress -{delta} = {new_progress}"
        )
        await safe_answer_message(callback.message, text)
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

    old_progress = int(row["progress"])

    if verdict == "know":
        new_progress = await increment_progress_and_update_due(word_id)
        delta = new_progress - old_progress  # должна быть 1
    else:  # "dont"
        new_progress = await decrement_progress(word_id)
        await log_mistake(user_id, word_id)
        delta = new_progress - old_progress  # будет отрицательным

    # текстовое представление изменения прогресса
    if delta >= 0:
        sign_part = f"+{delta}"
    else:
        sign_part = f"{delta}"  # уже со знаком минус

    question = row["question"]
    answer = row["answer"]
    example = row["example"]

    prev_part = f"{question}\n\n{answer}"
    if example:
        prev_part += f"\n\n{example}"
    prev_part += f"\n\n📈 Progress {sign_part} = {new_progress}"
    prev_part = sanitize_text(prev_part)

    next_row = await get_next_word()
    if not next_row:
        final_text = prev_part + "\n\nNo more words in the database."
        final_text = sanitize_text(final_text)
        try:
            await callback.message.edit_reply_markup(reply_markup=None)
        except Exception:
            pass
        await safe_answer_message(callback.message, final_text)
        await callback.answer()
        return

    due_count = await get_due_count()
    next_text, next_keyboard = build_question_message(next_row, due_count)

    full_text = prev_part + "\n\n---\n\n" + next_text
    full_text = sanitize_text(full_text)

    try:
        await callback.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass

    await safe_answer_message(
        callback.message,
        full_text,
        reply_markup=next_keyboard,
    )

    await callback.answer()


# ----- FastAPI lifecycle -----


@app.on_event("startup")
async def on_startup():
    await init_db()
    print("DB initialized")


@app.get("/")
async def root():
    return {"status": "ok", "message": "vocab-bot is running"}


# ----- Sync endpoints for Google Sheets -----


@app.post("/sync/words")
async def sync_words(payload: SyncWordsRequest):
    """
    Import from Google Sheets.

    last_success_ts_ms is given in milliseconds (Date.now()).
    Inside we store last_success_ts in seconds and compute next_due_ts.
    mistakes_log: full mistakes history from Log2.
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
                mistakes_count=w.mistakes_count or 0,
            )
        )

    # rebuild words
    await replace_all_words(words)

    # rebuild mistakes log (if provided)
    # формат: (user_id, question, answer, ts_sec)
    entries: list[tuple[int, str, str, int]] = []
    if payload.mistakes_log:
        for m in payload.mistakes_log:
            ts_sec = int(m.ts_ms // 1000)
            entries.append(
                (
                    m.user_id,
                    m.question,
                    m.answer,
                    ts_sec,
                )
            )

    await replace_all_mistakes(entries)
    return {"status": "ok", "count": len(words), "mistakes": len(entries)}


@app.get("/sync/progress")
async def sync_progress():
    """
    Export to Google Sheets.

    - items: per-word progress + last_success_ts_ms + mistakes_count
    - mistakes_log: full mistakes history (Log2 sheet)
    """
    word_items_raw = await get_all_progress()
    items = []
    for item in word_items_raw:
        ts = item["last_success_ts"]
        if ts is not None:
            ts_ms = int(ts * 1000)
        else:
            ts_ms = None
        items.append(
            {
                "sheet_row": item["sheet_row"],
                "progress": item["progress"],
                "last_success_ts_ms": ts_ms,
                "mistakes_count": item["mistakes_count"],
            }
        )

    mistakes_raw = await get_all_mistakes_for_sync()
    mistakes_out = []
    for row in mistakes_raw:
        mistakes_out.append(
            {
                "user_id": row["user_id"],
                "ts_ms": int(row["ts"] * 1000),
                "question": row["question"],
                "answer": row["answer"],
            }
        )

    return {"status": "ok", "items": items, "mistakes_log": mistakes_out}


# ----- Telegram webhook -----

@app.post(WEBHOOK_PATH)
async def telegram_webhook(request: Request):
    data = await request.json()
    update = types.Update.model_validate(data)
    await dp.feed_update(bot, update)
    return {"ok": True}


# ----- Daily mistakes cron endpoint -----


@app.get("/cron/daily_mistakes")
async def cron_daily_mistakes():
    """
    Endpoint to be called by an external scheduler (cron).
    For each user who has mistakes logged, send them last N mistakes.
    """
    user_ids = await get_users_with_mistakes()
    for uid in user_ids:
        if is_allowed(uid):
            await send_mistakes_to_user(uid, limit=80)
    return {"status": "ok", "users_notified": len(user_ids)}
