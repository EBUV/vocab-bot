# db.py
import time
from dataclasses import dataclass
from typing import Optional, List, Dict, Any

import aiosqlite

from config import DB_PATH


# --- spaced repetition delays (in minutes) by progress level ---
SPACED_REPETITION_DELAYS = {
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


def _calc_next_due_ts(progress: int, last_success_ts: Optional[int]) -> Optional[int]:
    if last_success_ts is None or progress <= 0:
        return None
    delay_min = SPACED_REPETITION_DELAYS.get(progress, SPACED_REPETITION_DELAYS[12])
    return last_success_ts + delay_min * 60


@dataclass
class Word:
    sheet_row: int
    progress: int
    question: str
    answer: str
    example: Optional[str] = None
    last_success_ts: Optional[int] = None
    mistakes_count: int = 0


async def init_db() -> None:
    """Create tables if they don't exist."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS words (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sheet_row INTEGER NOT NULL UNIQUE,
                progress INTEGER NOT NULL DEFAULT 0,
                question TEXT NOT NULL,
                answer TEXT NOT NULL,
                example TEXT,
                last_success_ts INTEGER,
                next_due_ts INTEGER,
                mistakes_count INTEGER NOT NULL DEFAULT 0
            )
            """
        )
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS mistakes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                sheet_row INTEGER NOT NULL,
                ts INTEGER NOT NULL
            )
            """
        )
        await db.commit()


# ---------- WORDS ----------


async def replace_all_words(words: List[Word]) -> None:
    """Completely replace all word records (used when importing from Google Sheets)."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM words")
        for w in words:
            next_due_ts = _calc_next_due_ts(w.progress, w.last_success_ts)
            await db.execute(
                """
                INSERT INTO words
                    (sheet_row, progress, question, answer, example,
                     last_success_ts, next_due_ts, mistakes_count)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    w.sheet_row,
                    w.progress,
                    w.question,
                    w.answer,
                    w.example,
                    w.last_success_ts,
                    next_due_ts,
                    w.mistakes_count or 0,
                ),
            )
        await db.commit()


async def get_next_word() -> Optional[Dict[str, Any]]:
    """Return one word to ask next, using spaced-repetition logic."""
    now = int(time.time())

    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row

        # 1) words that are due now (or never answered yet)
        cur = await db.execute(
            """
            SELECT *
            FROM words
            WHERE next_due_ts IS NULL OR next_due_ts <= ?
            ORDER BY
                CASE WHEN next_due_ts IS NULL THEN 0 ELSE 1 END,
                next_due_ts
            LIMIT 100
            """,
            (now,),
        )
        rows = await cur.fetchall()

        if not rows:
            # 2) otherwise take nearest upcoming words
            cur = await db.execute(
                """
                SELECT *
                FROM words
                ORDER BY
                    CASE WHEN next_due_ts IS NULL THEN 0 ELSE 1 END,
                    next_due_ts
                LIMIT 100
                """
            )
            rows = await cur.fetchall()

        if not rows:
            return None

        # simple choice: first row (можно сделать random.choice(rows) если хочешь)
        row = rows[0]
        return dict(row)


async def get_word_by_id(word_id: int) -> Optional[Dict[str, Any]]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT * FROM words WHERE id = ?", (word_id,))
        row = await cur.fetchone()
        return dict(row) if row else None


async def increment_progress_and_update_due(word_id: int) -> int:
    """Increase progress, set last_success_ts=now and recalc next_due_ts. Return new progress."""
    now = int(time.time())

    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT progress FROM words WHERE id = ?", (word_id,))
        row = await cur.fetchone()
        if not row:
            return 0

        progress = row["progress"] + 1
        next_due_ts = _calc_next_due_ts(progress, now)

        await db.execute(
            """
            UPDATE words
            SET progress = ?, last_success_ts = ?, next_due_ts = ?
            WHERE id = ?
            """,
            (progress, now, next_due_ts, word_id),
        )
        await db.commit()
        return progress


async def decrement_progress(word_id: int) -> None:
    """Decrease progress by 1 (not below 0) and make word due immediately."""
    now = int(time.time())
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT progress FROM words WHERE id = ?", (word_id,))
        row = await cur.fetchone()
        if not row:
            return
        progress = max(0, row["progress"] - 1)
        next_due_ts = _calc_next_due_ts(progress, now)

        await db.execute(
            """
            UPDATE words
            SET progress = ?, next_due_ts = ?
            WHERE id = ?
            """,
            (progress, next_due_ts, word_id),
        )
        await db.commit()


async def get_due_count() -> int:
    now = int(time.time())
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT COUNT(*) FROM words WHERE next_due_ts IS NULL OR next_due_ts <= ?",
            (now,),
        )
        (count,) = await cur.fetchone()
        return count


async def get_all_progress() -> List[Dict[str, Any]]:
    """Return data for sync to Google Sheets: sheet_row, progress, last_success_ts, mistakes_count."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            """
            SELECT sheet_row, progress, last_success_ts, mistakes_count
            FROM words
            ORDER BY sheet_row
            """
        )
        rows = await cur.fetchall()
        return [dict(r) for r in rows]


# ---------- MISTAKES ----------


async def log_mistake(user_id: int, word_id: int) -> None:
    """Log a mistake for given user and word, and increment mistakes_count."""
    now = int(time.time())
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row

        # find sheet_row for this word
        cur = await db.execute("SELECT sheet_row FROM words WHERE id = ?", (word_id,))
        row = await cur.fetchone()
        if not row:
            return
        sheet_row = row["sheet_row"]

        await db.execute(
            "INSERT INTO mistakes (user_id, sheet_row, ts) VALUES (?, ?, ?)",
            (user_id, sheet_row, now),
        )
        await db.execute(
            "UPDATE words SET mistakes_count = mistakes_count + 1 WHERE id = ?",
            (word_id,),
        )
        await db.commit()


async def get_last_mistakes(user_id: int, limit: int = 50) -> List[Dict[str, Any]]:
    """Get last N mistakes for a user (joined with word text)."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            """
            SELECT m.sheet_row, m.ts, w.question, w.answer
            FROM mistakes m
            JOIN words w ON w.sheet_row = m.sheet_row
            WHERE m.user_id = ?
            ORDER BY m.ts DESC
            LIMIT ?
            """,
            (user_id, limit),
        )
        rows = await cur.fetchall()
        return [dict(r) for r in rows]


async def get_users_with_mistakes() -> List[int]:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT DISTINCT user_id FROM mistakes")
        rows = await cur.fetchall()
        return [r[0] for r in rows]


async def get_stats(user_id: int) -> Dict[str, int]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row

        cur = await db.execute("SELECT COUNT(*) FROM words")
        (total_words,) = await cur.fetchone()

        now = int(time.time())
        cur = await db.execute(
            "SELECT COUNT(*) FROM words WHERE next_due_ts IS NULL OR next_due_ts <= ?",
            (now,),
        )
        (due_now,) = await cur.fetchone()

        cur = await db.execute(
            "SELECT COUNT(*) FROM words WHERE progress >= 5"
        )
        (well_known,) = await cur.fetchone()

        cur = await db.execute(
            "SELECT COUNT(*) FROM mistakes WHERE user_id = ?",
            (user_id,),
        )
        (mistakes_total,) = await cur.fetchone()

        return {
            "total_words": total_words,
            "due_now": due_now,
            "well_known": well_known,
            "mistakes_total": mistakes_total,
        }


async def get_all_mistakes_for_sync() -> List[Dict[str, Any]]:
    """Return full mistakes log for syncing with Google Sheets (Log2)."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            """
            SELECT m.user_id, m.sheet_row, m.ts, w.question, w.answer
            FROM mistakes m
            JOIN words w ON w.sheet_row = m.sheet_row
            ORDER BY m.ts ASC
            """
        )
        rows = await cur.fetchall()
        return [dict(r) for r in rows]


async def replace_all_mistakes(entries: List[tuple[int, int, int]]) -> None:
    """
    Completely replace mistakes table (used when importing from Google Sheets).

    entries: list of (user_id, sheet_row, ts)
    """
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM mistakes")
        for user_id, sheet_row, ts in entries:
            await db.execute(
                "INSERT INTO mistakes (user_id, sheet_row, ts) VALUES (?, ?, ?)",
                (user_id, sheet_row, ts),
            )
        await db.commit()
