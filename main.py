import logging

logging.basicConfig(level=logging.INFO)

from fastapi import FastAPI, Request, HTTPException
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

# Remove most control chars, but keep \t, \n, \r
CODES_TO_REMOVE = {c for c in range(0, 32) if c not in (9, 10, 13)}
CODES_TO_REMOVE.add(127)  # DEL

# Rare Unicode line/paragraph separators
UNICODE_BAD_CODES = {0x2028, 0x2029}


def sanitize_text(text: str) -> str:
    if not text:
        return text
    res = []
    for ch in text:
        code = ord(ch)
        if code in CODES_TO_REMOVE or code in UNICODE_BAD_CODES:
            continue
        res.append(ch)
    return "".join(res)


def escape_markdown(text: str) -> str:
    """Escape MarkdownV2 special characters."""
    if not text:
        return text
    special = r"_*[]()~`>#+-=|{}.!\\"  # Telegram MarkdownV2 special chars
    out = []
    for ch in text:
        if ch in special:
            out.append("\\" + ch)
        else:
            out.append(ch)
    return "".join(out)


async def safe_answer_message(msg: types.Message, text: str, **kwargs):
    """
    Try to send with MarkdownV2, fall back to plain text if it fails.
    """
    try:
        safe = sanitize_text(text)
        md = escape_markdown(safe)
        return await msg.answer(md, parse_mode=ParseMode.MARKDOWN_V2, **kwargs)
    except Exception:
        logging.exception("Failed to send markdown message, retrying without markdown")
        try:
            safe = sanitize_text(text)
            return await msg.answer(safe, **kwargs)
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
    # Date.now() in ms
    last_success_ts_ms: Optional[int] = None
    # mistakes counter from column I
    mistakes_count: Optional[int] = 0


class MistakeLogIn(BaseModel):
    user_id: int
    sheet_row: int
    ts_ms: int  # timestamp in milliseconds


class SyncWordsRequest(BaseModel):
    words: List[WordIn]
    mistakes_log: Optional[List[MistakeLogIn]] = None


# ----- Helper functions -----

def build_question_message(row, due_count: int) -> tuple[str, InlineKeyboardMarkup]:
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


async def send_mistakes_to_user(user_id: int, limit: int = 60):
    """
    Send last mistakes to a user as separate messages.
    `get_last_mistakes` уже отдаёт их в нужном порядке (старые → новые).
    """
    rows = await get_last_mistakes(user_id, limit=limit)
    if not rows:
        await bot.send_message(user_id, "No mistakes logged yet ✅")
        return

    await bot.send_message(user_id, "Words you should review:\n")

    for row in rows:
        q = row["question"]
        a = row["answer"]
        text = sanitize_text(f"{q}\n\n\n{a}")
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
    await message.answer(sanitize_text(text), parse_mode=ParseMode.MARKDOWN)


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

    await send_mistakes_to_user(message.from_user.id, limit=60)


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

    await message.answer(sanitize_text(text), parse_mode=ParseMode.MARKDOWN)


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

        old_progress = row["progress"]
        await decrement_progress(last_id)       # внутри БД обнуляем last_success_ts
        await log_mistake(user_id, last_id)
        step = 2 if old_progress > 6 else 1
        new_progress = max(0, old_progress - step)

        text = sanitize_text(
            "🔁 Previous word corrected.\n"
            f"📉 Progress -{step} = {new_progress}"
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
    else:
        # "I don't know" → уменьшаем прогресс, сбрасываем таймер, пишем ошибку
        delta = -1
        await decrement_progress(word_id)       # внутри БД обнуляем last_success_ts
        await log_mistake(user_id, word_id)
        step = 2 if old_progress > 6 else 1
        new_progress = max(0, old_progress - step)

    sign = "+" if delta > 0 else "-"

    question = row["question"]
    answer = row["answer"]
    example = row["example"]

    prev_part = f"{question}\n\n{answer}"
    if example:
        prev_part += f"\n\n{example}"
    prev_part += f"\n\n📈 Progress {sign}{abs(delta)} = {new_progress}"
    prev_part = sanitize_text(prev_part)

    next_row = await get_next_word()
    if not next_row:
        final_text = sanitize_text(prev_part + "\n\nNo more words in the database.")
        try:
            await callback.message.edit_reply_markup(reply_markup=None)
        except Exception:
            pass
        await callback.message.answer(final_text)
        await callback.answer()
        return

    due_count = await get_due_count()
    next_text, next_keyboard = build_question_message(next_row, due_count)

    full_text = sanitize_text(prev_part + "\n\n---\n\n" + next_text)

    try:
        await callback.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass

    await callback.message.answer(full_text, reply_markup=next_keyboard)
    await callback.answer()


# ----- FastAPI lifecycle -----

@app.on_event("startup")
async def on_startup():
    await init_db()
    logging.info("DB initialized")


@app.get("/")
async def root():
    return {"status": "ok", "message": "vocab-bot is running"}


# ----- Sync endpoints for Google Sheets -----

@app.post("/sync/words")
async def sync_words(payload: SyncWordsRequest):
    """
    Import from Google Sheets:
    - words: основной словарь
    - mistakes_log: полный лог ошибок (Log2)
    """
    try:
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

        # Полностью пересобираем таблицу слов
        await replace_all_words(words)

        # Полностью пересобираем лог ошибок
        entries: list[tuple[int, int, int]] = []
        if payload.mistakes_log:
            for m in payload.mistakes_log:
                ts_sec = int(m.ts_ms // 1000)
                entries.append((m.user_id, m.sheet_row, ts_sec))

        await replace_all_mistakes(entries)

        return {"status": "ok", "count": len(words), "mistakes": len(entries)}
    except Exception as e:
        logging.exception("sync_words failed")
        # Это сообщение теперь увидишь в алерте Гугл-таблицы
        raise HTTPException(status_code=500, detail=f"sync_words error: {e}")


@app.get("/sync/progress")
async def sync_progress():
    """
    Export to Google Sheets:
    - items: прогресс по словам + last_success_ts_ms + mistakes_count
    - mistakes_log: полный лог ошибок для листа Log2
    """
    try:
        word_items_raw = await get_all_progress()
        items = []
        for item in word_items_raw:
            ts = item["last_success_ts"]
            ts_ms = int(ts * 1000) if ts is not None else None
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
                    "sheet_row": row["sheet_row"],
                    "ts_ms": int(row["ts"] * 1000),
                    "question": row["question"],
                    "answer": row["answer"],
                }
            )

        return {"status": "ok", "items": items, "mistakes_log": mistakes_out}
    except Exception as e:
        logging.exception("sync_progress failed")
        raise HTTPException(status_code=500, detail=f"sync_progress error: {e}")


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
    user_ids = await get_users_with_mistakes()
    for uid in user_ids:
        if is_allowed(uid):
            await send_mistakes_to_user(uid, limit=60)
    return {"status": "ok", "users_notified": len(user_ids)}
