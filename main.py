import logging
import json

logging.basicConfig(level=logging.INFO)

from fastapi import FastAPI, Request
from aiogram import Bot, Dispatcher, F, types
from aiogram.enums import ParseMode
from aiogram.filters import CommandStart, Command
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

from config import BOT_TOKEN, WEBHOOK_PATH, INTERVALS_PATH
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
    get_intervals_table,   # <-- вот это добавить
)


# ----- ACCESS CONTROL -----

ALLOWED_USER_IDS = {518129411}


def is_allowed(user_id: int) -> bool:
    return user_id in ALLOWED_USER_IDS


# ----- TEXT SANITIZING (remove problematic chars for Telegram) -----

CODES_TO_REMOVE = {c for c in range(0, 32) if c not in (9, 10, 13)}
CODES_TO_REMOVE.add(127)
UNICODE_BAD_CODES = {0x2028, 0x2029}


def sanitize_text(text: str) -> str:
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
    if not text:
        return text
    special = r"_*[]()~`>#+-=|{}.!\\"
    escaped = []
    for ch in text:
        if ch in special:
            escaped.append("\\" + ch)
        else:
            escaped.append(ch)
    return "".join(escaped)


async def safe_answer_message(msg: types.Message, text: str, **kwargs):
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
# Store current question for typed answers
user_current_word: dict[int, int] = {}


# ----- Pydantic models for sync endpoints -----

class WordIn(BaseModel):
    sheet_row: int
    progress: int
    question: str
    answer: str
    example: Optional[str] = None
    last_success_ts_ms: Optional[int] = None
    mistakes_count: Optional[int] = 0


class MistakeLogIn(BaseModel):
    user_id: int
    ts_ms: int
    question: str
    answer: str


class SyncWordsRequest(BaseModel):
    words: List[WordIn]
    mistakes_log: Optional[List[MistakeLogIn]] = None
    intervals_minutes: Optional[List[int]] = None  # уровни 1..12 из листа bot


# ----- Helper functions -----

def normalize_answer(s: str) -> str:
    """
    Нормализация для сравнения ответов:
    - обрезаем пробелы по краям
    - схлопываем множественные пробелы
    - убираем конечные . ? !
    - приводим к нижнему регистру
    """
    if s is None:
        return ""

    s = " ".join(s.strip().split())

    while s and s[-1] in ".!?":
        s = s[:-1].rstrip()

    return s.lower()


def distance_leq1(a: str, b: str) -> int:
    """
    Возвращает 0, 1 или 2:
      0 – строки совпадают;
      1 – можно привести одну к другой одной операцией
          вставки/удаления/замены символа;
      2 – расстояние > 1.
    """
    if a == b:
        return 0

    la, lb = len(a), len(b)
    if abs(la - lb) > 1:
        return 2

    if la == lb:
        mismatches = 0
        for ca, cb in zip(a, b):
            if ca != cb:
                mismatches += 1
                if mismatches > 1:
                    return 2
        return mismatches

    if la > lb:
        a, b = b, a
        la, lb = lb, la

    i = j = 0
    mismatches = 0
    while i < la and j < lb:
        if a[i] == b[j]:
            i += 1
            j += 1
        else:
            mismatches += 1
            j += 1
            if mismatches > 1:
                return 2

    mismatches += (lb - j)
    return 1 if mismatches <= 1 else 2


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


async def send_mistakes_to_user(user_id: int, limit: int = 80):
    rows = await get_last_mistakes(user_id, limit=limit)
    if not rows:
        await bot.send_message(user_id, "No mistakes logged yet ✅")
        return

    await bot.send_message(user_id, "Words you should review:\n")

    for row in rows:
        q = row["question"]
        a = row["answer"]
        text = f"{q}\n\n\n{a}"
        text = sanitize_text(text)
        await bot.send_message(user_id, text)


def format_progress_change(old_progress: int, new_progress: int) -> str:
    if new_progress == old_progress:
        return f"📈 Progress = {new_progress}"

    delta = new_progress - old_progress
    sign = "+" if delta > 0 else "-"
    magnitude = abs(delta)
    arrow = "📈" if delta > 0 else "📉"
    return f"{arrow} Progress {sign}{magnitude} = {new_progress}"


async def ask_next_card(msg: types.Message, user_id: int):
    row = await get_next_word()
    if not row:
        await msg.answer("There are no words in the database yet 🙈")
        return

    due_count = await get_due_count()
    text, keyboard = build_question_message(row, due_count)
    user_current_word[user_id] = row["id"]
    await safe_answer_message(msg, text, reply_markup=keyboard)


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
        "You can also:\n"
        "• type the answer as text – I'll check it;\n"
        "• use /mistakes – to see your latest mistakes;\n"
        "• use /stats – to see your current statistics."
    )
    await safe_answer_message(message, text)


@dp.message(Command("next"))
async def cmd_next(message: types.Message):
    if not is_allowed(message.from_user.id):
        await message.answer("Sorry, this bot is currently in private beta.")
        return

    await ask_next_card(message, message.from_user.id)

@dp.message(Command("intervals"))
async def cmd_intervals(message: types.Message):
    """Показать текущие интервалы повторения в минутах для уровней 1–12."""
    user_id = message.from_user.id
    if not is_allowed(user_id):
        await message.answer("Sorry, this bot is currently in private beta.")
        return

    intervals = get_intervals_table(max_level=12)

    lines = ["📅 *Current intervals (minutes):*"]
    # уровень 0 – “всегда сейчас”, покажем отдельно
    zero_minutes = intervals.get(0)
    if zero_minutes is not None:
        lines.append(f"0 → всегда сейчас (внутри как {zero_minutes} мин)")

    for level in range(1, 13):
        minutes = intervals.get(level, 0)
        lines.append(f"{level} → {minutes}")

    text = "\n".join(lines)
    await safe_answer_message(message, text)

@dp.message(Command("mistakes"))
async def cmd_mistakes(message: types.Message):
    if not is_allowed(message.from_user.id):
        await message.answer("Sorry, this bot is currently in private beta.")
        return

    await send_mistakes_to_user(message.from_user.id, limit=80)


@dp.message(Command("stats"))
async def cmd_stats(message: types.Message):
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

    # "I was wrong"
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
        new_progress = await decrement_progress(last_id)
        await log_mistake(user_id, last_id)

        progress_text = format_progress_change(old_progress, new_progress)
        text = f"🔁 Previous word corrected.\n{progress_text}"

        await safe_answer_message(callback.message, text)
        await callback.answer()
        return

    # I know / I don't know
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
    user_current_word[user_id] = word_id

    old_progress = row["progress"]

    if verdict == "know":
        new_progress = await increment_progress_and_update_due(word_id)
    else:
        new_progress = await decrement_progress(word_id)
        await log_mistake(user_id, word_id)

    progress_text = format_progress_change(old_progress, new_progress)

    question = row["question"]
    answer = row["answer"]
    example = row["example"]

    prev_part = f"{question}\n\n{answer}"
    if example:
        prev_part += f"\n\n{example}"
    prev_part += f"\n\n{progress_text}"
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
    user_current_word[user_id] = next_row["id"]

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


# ----- Typed answers handler -----

@dp.message()
async def handle_typed_answer(message: types.Message):
    user_id = message.from_user.id

    if message.text and message.text.startswith("/"):
        return

    if not is_allowed(user_id):
        await message.answer("Sorry, this bot is currently in private beta.")
        return

    word_id = user_current_word.get(user_id)
    if not word_id:
        await message.answer("I don't know which card you are answering. Send /next first.")
        return

    row = await get_word_by_id(word_id)
    if not row:
        await message.answer("Word not found in the database. Try /next.")
        return

    user_last_word[user_id] = word_id

    user_answer_raw = message.text or ""
    correct_raw = row["answer"] or ""

    user_norm = normalize_answer(user_answer_raw)
    correct_norm = normalize_answer(correct_raw)

    dist = distance_leq1(user_norm, correct_norm)

    old_progress = row["progress"]

    if dist == 0:
        new_progress = await increment_progress_and_update_due(word_id)
        progress_text = format_progress_change(old_progress, new_progress)
        reply = (
            "✅ Correct!\n\n"
            f"Your answer: {user_answer_raw}\n"
            f"Correct answer: {correct_raw}\n\n"
            f"{progress_text}"
        )
    elif dist == 1:
        new_progress = await increment_progress_and_update_due(word_id)
        progress_text = format_progress_change(old_progress, new_progress)
        reply = (
            "🟡 Almost correct (one small typo).\n\n"
            f"Your answer: {user_answer_raw}\n"
            f"Correct answer: {correct_raw}\n\n"
            f"{progress_text}"
        )
    else:
        new_progress = await decrement_progress(word_id)
        await log_mistake(user_id, word_id)
        progress_text = format_progress_change(old_progress, new_progress)
        reply = (
            "❌ Not correct.\n\n"
            f"Your answer: {user_answer_raw}\n"
            f"Correct answer: {correct_raw}\n\n"
            f"{progress_text}"
        )

    await safe_answer_message(message, reply)
    await ask_next_card(message, user_id)


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

    last_success_ts_ms – в мс (Date.now()).
    Внутри храним last_success_ts в секундах и считаем next_due_ts.
    mistakes_log – полный лог ошибок (Log2).
    intervals_minutes – интервалы уровней 1..12 из листа bot.
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

    # сохраняем интервалы в JSON (используется db.progress_to_minutes)
    if payload.intervals_minutes:
        try:
            data = {str(i + 1): int(payload.intervals_minutes[i])
                    for i in range(len(payload.intervals_minutes))}
            with open(INTERVALS_PATH, "w", encoding="utf-8") as f:
                json.dump(data, f)
        except Exception:
            logging.exception("Failed to save intervals file")

    await replace_all_words(words)

    # пересобираем mistakes (если передан лог)
    entries: list[tuple[int, str, str, int]] = []
    if payload.mistakes_log:
        for m in payload.mistakes_log:
            ts_sec = int(m.ts_ms // 1000)
            entries.append(
                (m.user_id, m.question, m.answer, ts_sec)
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
    user_ids = await get_users_with_mistakes()
    for uid in user_ids:
        if is_allowed(uid):
            await send_mistakes_to_user(uid, limit=80)
    return {"status": "ok", "users_notified": len(user_ids)}
