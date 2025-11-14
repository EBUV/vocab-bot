import aiosqlite
from dataclasses import dataclass
from typing import Optional, List
import time

from config import DB_PATH

# Max knowledge level
MAX_LEVEL = 12

# Spaced repetition intervals in minutes for each level
INTERVAL_MINUTES = {
    0: 1,
    1: 1,
    2: 30,
    3: 240,
    4: 1440,
    5: 2880,
    6: 5760,
    7: 11520,
    8: 23040,
    9: 46080,
    10: 92160,
    11: 138240,
    12: 213120,
}


@dataclass
class Word:
    sheet_row: int
    progress: int
    question: str
    answer: str
    example: Optional[str] = None
    # seconds since epoch, may be None
    last_success_ts: Optional[int] = None


async def init_db():
    """Create tables (if not exist) and add missing columns."""
    async with aiosqlite.connect(DB_PATH) as db:
        # Main words table
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS words (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sheet_row INTEGER NOT NULL,
                progress INTEGER NOT NULL DEFAULT 0,
                question TEXT NOT NULL,
                answer TEXT NOT NULL,
                example TEXT,
                last_success_ts INTEGER,
                next_due_ts INTEGER
            )
            """
        )
        # Mistakes log
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS mistakes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                word_id INTEGER NOT NULL,
                ts INTEGER NOT NULL
            )
            """
        )
        await db.commit()

        # Migration for old schemas
        cursor = await db.execute("PRAGMA table_info(words)")
        columns_info = await cursor.fetchall()
        await cursor.close()
        existing_cols = {row[1] for row in columns_info}

        if "last_success_ts" not in existing_cols:
            await db.execute("ALTER TABLE words ADD COLUMN last_success_ts INTEGER")
        if "next_due_ts" not in existing_cols:
            await db.execute("ALTER TABLE words ADD COLUMN next_due_ts INTEGER")
        await db.commit()


def _get_interval_minutes(progress: int) -> int:
    """Return SRS interval in minutes for a given progress level."""
    if progress >= MAX_LEVEL:
        progress = MAX_LEVEL
    if progress < 0:
        progress = 0
    return INTERVAL_MINUTES.get(progress, INTERVAL_MINUTES[MAX_LEVEL])


async def replace_all_words(words: List[Word]):
    """
    Replace all rows in `words` with data from Google Sheets.

    last_success_ts is given in seconds (or None).
    next_due_ts is recalculated from progress and last_success_ts.
    """
    async with aiosqlite.connect(DB_PATH) as db:
        # Fully rebuild dictionary; clear mistakes log as well
        await db.execute("DELETE FROM words")
        await db.execute("DELETE FROM mistakes")

        for w in words:
            if w.last_success_ts is not None:
                interval_minutes = _get_interval_minutes(w.progress)
                next_due_ts = w.last_success_ts + interval_minutes * 60
            else:
                next_due_ts = None

            await db.execute(
                """
                INSERT INTO words (
                    sheet_row, progress, question, answer, example,
                    last_success_ts, next_due_ts
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    w.sheet_row,
                    w.progress,
                    w.question,
                    w.answer,
                    w.example,
                    w.last_success_ts,
                    next_due_ts,
                ),
            )

        await db.commit()


async def get_next_word(now_ts: Optional[int] = None):
    """Return next word according to SRS logic."""
    if now_ts is None:
        now_ts = int(time.time())

    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row

        # 1) Due words (or never scheduled)
        cursor = await db.execute(
            """
            SELECT id, sheet_row, progress, question, answer, example
            FROM words
            WHERE next_due_ts IS NULL
               OR next_due_ts <= ?
            ORDER BY RANDOM()
            LIMIT 1
            """,
            (now_ts,),
        )
        row = await cursor.fetchone()
        await cursor.close()

        if row is not None:
            return row

        # 2) Next 100 closest words by due time
        cursor = await db.execute(
            """
            SELECT id, sheet_row, progress, question, answer, example
            FROM words
            WHERE next_due_ts IS NOT NULL
            ORDER BY next_due_ts ASC
            LIMIT 100
            """
        )
        rows = await cursor.fetchall()
        await cursor.close()

        if not rows:
            return None

        import random
        return random.choice(rows)


async def get_due_count(now_ts: Optional[int] = None) -> int:
    """How many words are currently due for review."""
    if now_ts is None:
        now_ts = int(time.time())

    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            """
            SELECT COUNT(*)
            FROM words
            WHERE next_due_ts IS NULL
               OR next_due_ts <= ?
            """,
            (now_ts,),
        )
        (count,) = await cursor.fetchone()
        await cursor.close()
    return int(count)


async def increment_progress_and_update_due(word_id: int) -> int:
    """
    Increase word's progress level and recalculate next_due_ts.
    Returns the new progress value.
    """
    now_ts = int(time.time())

    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row

        cursor = await db.execute(
            "SELECT progress FROM words WHERE id = ?",
            (word_id,),
        )
        row = await cursor.fetchone()
        await cursor.close()
        if row is None:
            return 0

        old_progress = row["progress"]
        new_progress = old_progress + 1
        if new_progress > MAX_LEVEL:
            new_progress = MAX_LEVEL

        interval_minutes = _get_interval_minutes(new_progress)
        next_due_ts = now_ts + interval_minutes * 60

        await db.execute(
            """
            UPDATE words
            SET progress = ?, last_success_ts = ?, next_due_ts = ?
            WHERE id = ?
            """,
            (new_progress, now_ts, next_due_ts, word_id),
        )
        await db.commit()

    return new_progress


async def decrement_progress(word_id: int):
    """Decrease progress by 1 (not below 0). Do not touch due timestamps."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT progress FROM words WHERE id = ?",
            (word_id,),
        )
        row = await cursor.fetchone()
        await cursor.close()
        if row is None:
            return

        old_progress = row["progress"]
        new_progress = old_progress - 1
        if new_progress < 0:
            new_progress = 0

        await db.execute(
            "UPDATE words SET progress = ? WHERE id = ?",
            (new_progress, word_id),
        )
        await db.commit()


async def get_word_by_id(word_id: int):
    """Return a single word row by id."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            """
            SELECT id, sheet_row, progress, question, answer, example
            FROM words
            WHERE id = ?
            """,
            (word_id,),
        )
        row = await cursor.fetchone()
        await cursor.close()
    return row


async def get_all_progress():
    """For exporting back to Google Sheets: sheet_row + progress + last_success_ts."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            """
            SELECT sheet_row, progress, last_success_ts
            FROM words
            ORDER BY sheet_row ASC
            """
        )
        rows = await cursor.fetchall()
        await cursor.close()

    return [
        {
            "sheet_row": row["sheet_row"],
            "progress": row["progress"],
            "last_success_ts": row["last_success_ts"],
        }
        for row in rows
    ]


async def get_stats(user_id: int) -> dict:
    """
    Return basic statistics for /stats command:

    - total_words: total count of words in deck
    - due_now: currently due words
    - well_known: words with progress >= 5
    - mistakes_total: total mistakes logged for this user
    """
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row

        # total words
        cursor = await db.execute("SELECT COUNT(*) FROM words")
        (total_words,) = await cursor.fetchone()
        await cursor.close()

        # well-known words (progress >= 5, arbitrary threshold)
        cursor = await db.execute(
            "SELECT COUNT(*) FROM words WHERE progress >= ?",
            (5,),
        )
        (well_known,) = await cursor.fetchone()
        await cursor.close()

        # total mistakes for this user
        cursor = await db.execute(
            "SELECT COUNT(*) FROM mistakes WHERE user_id = ?",
            (user_id,),
        )
        (mistakes_total,) = await cursor.fetchone()
        await cursor.close()

    due_now = await get_due_count()

    return {
        "total_words": int(total_words),
        "due_now": int(due_now),
        "well_known": int(well_known),
        "mistakes_total": int(mistakes_total),
    }


# ---------- MISTAKES LOG ----------

async def log_mistake(user_id: int, word_id: int):
    """Insert mistake record (I don't know / I was wrong)."""
    now_ts = int(time.time())
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """
            INSERT INTO mistakes (user_id, word_id, ts)
            VALUES (?, ?, ?)
            """,
            (user_id, word_id, now_ts),
        )
        await db.commit()


async def get_last_mistakes(user_id: int, limit: int = 50):
    """Fetch last mistakes for the given user (question + answer)."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            """
            SELECT w.question, w.answer
            FROM mistakes m
            JOIN words w ON m.word_id = w.id
            WHERE m.user_id = ?
            ORDER BY m.ts DESC
            LIMIT ?
            """,
            (user_id, limit),
        )
        rows = await cursor.fetchall()
        await cursor.close()
    return rows


async def get_users_with_mistakes():
    """Return list of user_ids that have at least one logged mistake."""
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "SELECT DISTINCT user_id FROM mistakes"
        )
        rows = await cursor.fetchall()
        await cursor.close()
    return [row[0] for row in rows]
