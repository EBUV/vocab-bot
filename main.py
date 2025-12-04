# main.py
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
    get_intervals_table,
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
# Store current question for typed answers / commands
user_current_word: dict[int, int] = {}


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
    ts_ms: int  # timestamp in milliseconds (Date.now)
    question: str
    answer: str


class SyncWordsRequest(BaseModel):
    words: List[WordIn]
    mistakes_log: Optional[List[MistakeLogIn]] = None
    # интервалы повторения в минутах для уровней 1..12
    intervals_minutes: Optional[List[int]] = None


# ----- Helper functions -----


def normalize_answer(s: str) -> str:
    """
    Нормализация для сравнения ответов:
    - обрезаем пробелы по краям
    - схлопываем множественные пробелы
    - убираем ТОЛЬКО конечные . ? !
    - приводим к нижнему регистру
    """
    if s is None:
        return ""

    # убираем лишние пробелы
    s = " ".join(s.strip().split())

    # убираем все точки/вопросительные/восклицательные в КОНЦЕ
    while s and s[-1] in ".!?":
        s = s[:-1].rstrip()

    # нижний регистр
    return s.lower()


def distance_leq1(a: str, b: str) -> int:
    """
    Возвращает 0, 1 или 2:
      0 – строки совпадают;
      1 – можно привести одну к другой одной операцией
          вставки/удаления/замены символа;
      2 – расстояние > 1 (или явно больше).
    """
    if a == b:
        return 0

    la, lb = len(a), len(b)
    if abs(la - lb) > 1:
        return 2

    # случай одинаковой длины → считаем количество несовпадений
    if la == lb:
        mismatches = 0
        for ca, cb in zip(a, b):
            if ca != cb:
                mismatches += 1
                if mismatches > 1:
                    return 2
        return mismatches  # 1, потому что 0 уже вернули раньше

    # делаем так, чтобы a была не длиннее b
    if la > lb:
        a, b = b, a
        la, lb = lb, la

    # теперь len(b) = len(a)+1
    i = j = 0
    mismatches = 0
    while i < la and j < lb:
        if a[i] == b[j]:
            i += 1
            j += 1
        else:
            mismatches += 1
            j += 1  # пропускаем один символ в более длинной строке
            if mismatches > 1:
                return 2

    # возможный «хвост» в b
    mismatches += (lb - j)
    if mismatches <= 1:
        return mismatches
    return 2


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
    """Send last mistakes (oldest first) to a user as separate messages."""
    rows = await get_last_mistakes(user_id, limit=limit)
    if not rows:
        await bot.send_message(user_id, "No mistakes logged yet ✅")
        return

    # Header message
    await bot.send_message(user_id, "Words you should review:\n")

    for row in rows:
        q = row["question"]
        a = row["answer"]
        text = f"{q}\n\n\n{a}"  # две пустые строки между вопросом и ответом
        text = sanitize_text(text)
        await bot.send_message(user_id, text)


def format_progress_change(old_progress: int, new_progress: int) -> str:
    """
    Формирует кусок текста вида '📈 Progress +1 = 6' или '📉 Progress -2 = 5'.
    """
    if new_progress == old_progress:
        return f"📈 Progress = {new_progress}"

    delta = new_progress - old_progress
    sign = "+" if delta > 0 else "-"
    magnitude = abs(delta)
    arrow = "📈" if delta > 0 else "📉"
    return f"{arrow} Progress {sign}{magnitude} = {new_progress}"


async def ask_next_card(msg: types.Message, user_id: int):
    """Выдаём следующую карточку пользователю."""
    row = await get_next_word()
    if not row:
        await msg.answer("There are no words in the database yet 🙈")
        return

    due_count = await get_due_count()
    text, keyboard = build_question_message(row, due_count)
    user_current_word[user_id] = row["id"]
    await safe_answer_message(msg, text, reply_markup=keyboard)


# общий обработчик для простых вердиктов по командам /iknow и /idontknow
async def process_verdict_for_current(message: types.Message, verdict: str):
    user_id = message.from_user.id

    if not is_allowed(user_id):
        await message.answer("Sorry, this bot is currently in private beta.")
        try:
            await message.delete()
        except Exception:
            pass
        return

    word_id = user_current_word.get(user_id)
    if not word_id:
        await message.answer("I don't know which card you are answering. Send /next first.")
        try:
            await message.delete()
        except Exception:
            pass
        return

    row = await get_word_by_id(word_id)
    if not row:
        await message.answer("Word not found in the database. Try /next.")
        try:
            await message.delete()
        except Exception:
            pass
        return

    user_last_word[user_id] = word_id
    old_progress = row["progress"]

    if verdict == "know":
        new_progress = await increment_progress_and_update_due(word_id)
    else:  # "dont"
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
        await safe_answer_message(message, final_text)
    else:
        due_count = await get_due_count()
        next_text, next_keyboard = build_question_message(next_row, due_count)
        user_current_word[user_id] = next_row["id"]
        full_text = prev_part + "\n\n---\n\n" + next_text
        full_text = sanitize_text(full_text)
        await safe_answer_message(message, full_text, reply_markup=next_keyboard)

    # удаляем команду пользователя
    try:
        await message.delete()
    except Exception:
        pass


async def process_fix_for_last(message: types.Message):
    """Обработка команды /iwaswrong (аналог кнопки I was wrong)."""
    user_id = message.from_user.id

    if not is_allowed(user_id):
        await message.answer("Sorry, this bot is currently in private beta.")
        try:
            await message.delete()
        except Exception:
            pass
        return

    last_id = user_last_word.get(user_id)
    if not last_id:
        await message.answer("No previous word to fix.")
        try:
            await message.delete()
        except Exception:
            pass
        return

    row = await get_word_by_id(last_id)
    if not row:
        await message.answer("Previous word not found.")
        try:
            await message.delete()
        except Exception:
            pass
        return

    old_progress = row["progress"]
    new_progress = await decrement_progress(last_id)
    await log_mistake(user_id, last_id)

    progress_text = format_progress_change(old_progress, new_progress)
    text = f"🔁 Previous word corrected.\n{progress_text}"
    await safe_answer_message(message, text)

    try:
        await message.delete()
    except Exception:
        pass


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
        "• use /iknow, /idontknow, /iwaswrong instead of buttons;\n"
        "• use /mistakes – to see your latest mistakes;\n"
        "• use /stats – to see your current statistics;\n"
        "• use /intervals – to see current repetition intervals."
    )
    await safe_answer_message(message, text)


@dp.message(Command("next"))
async def cmd_next(message: types.Message):
    if not is_allowed(message.from_user.id):
        await message.answer("Sorry, this bot is currently in private beta.")
        return

    await ask_next_card(message, message.from_user.id)


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


@dp.message(Command("intervals"))
async def cmd_intervals(message: types.Message):
    """Показать текущие интервалы в минутах для уровней 1–12."""
    user_id = message.from_user.id
    if not is_allowed(user_id):
        await message.answer("Sorry, this bot is currently in private beta.")
        return

    table = get_intervals_table()
    lines = []
    for lvl in range(1, 13):
        minutes = table.get(lvl)
        if minutes is None:
            continue
        lines.append(f"{lvl}: {minutes} min")

    if not lines:
        text = "No intervals configured."
    else:
        text = "⏱ Current intervals (minutes):\n" + "\n".join(lines)

    await safe_answer_message(message, text)


# --- команды, эквивалентные кнопкам ---

@dp.message(Command("iknow"))
async def cmd_iknow(message: types.Message):
    await process_verdict_for_current(message, "know")


@dp.message(Command("idontknow"))
async def cmd_idontknow(message: types.Message):
    await process_verdict_for_current(message, "dont")


@dp.message(Command("iwaswrong"))
async def cmd_iwaswrong(message: types.Message):
    await process_fix_for_last(message)


# ----- Callback-handler для inline-кнопок -----


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
        new_progress = await decrement_progress(last_id)
        await log_mistake(user_id, last_id)

        progress_text = format_progress_change(old_progress, new_progress)
        text = f"🔁 Previous word corrected.\n{progress_text}"

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
    user_current_word[user_id] = word_id

    old_progress = row["progress"]

    if verdict == "know":
        new_progress = await increment_progress_and_update_due(word_id)
    else:  # "dont"
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
    """
    Обрабатываем текстовые ответы пользователя:
    - если это команда ("/...") – ничего не делаем, отдадим другим хендлерам;
    - иначе считаем, что это ответ на последнюю карточку.
    """
    user_id = message.from_user.id

    # не ломаем команды
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

    user_last_word[user_id] = word_id  # чтобы после текстового ответа можно было нажать "I was wrong"

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

    # после ответа сразу выдаём следующую карточку
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

    last_success_ts_ms is given in milliseconds (Date.now()).
    Inside we store last_success_ts in seconds and compute next_due_ts.
    mistakes_log: full mistakes history from Log2.
    intervals_minutes: custom intervals from the 'bot' sheet.
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

    # сохраняем интервалы в файл, чтобы db.progress_to_minutes их использовал
    if payload.intervals_minutes:
        data = {i + 1: int(payload.intervals_minutes[i]) for i in range(len(payload.intervals_minutes))}
        # уровень 0: всегда "должник" → 1 минута
        data[0] = 1
        with open(INTERVALS_PATH, "w", encoding="utf-8") as f:
            json.dump(data, f)

    # rebuild words
    await replace_all_words(words)

    # rebuild mistakes log (если передан)
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
