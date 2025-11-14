# db.py
import time
import random
from dataclasses import dataclass
from typing import List, Optional, Dict, Any

import aiosqlite

from config import DB_PATH


# ---------- Dataclass ----------

@dataclass
class Word:
    sheet_row: int
    progress: int
    question: str
    answer: str
    example: Optional[str] = None
    last_success_ts: Optional[int] = None  # seconds since epoch
    mistake_count: int = 0


# ---------- Spaced-repetition schedule ----------

# minutes for each progress level
_INTERVAL_MINUTES = {
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
}

_LAST_LEVEL_MINUTES = 213120  # level 12+


def _interval_seconds(progress: int) -> int:
    minutes = _INTERVAL_MINUTES.get(progress, _LAST_LEVEL_MINUTES)
    return minutes * 60


# ---------- DB init & migrations ----------

async def init_db() -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS words (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                sheet_row       INTEGER NOT NULL,
                progress        INTEGER NOT NULL DEFAULT 0,
                question        TEXT NOT NULL,
                answer          TEXT NOT NULL,
                example         TEXT,
                last_success_ts INTEGER,
                next_due_ts     INTEGER,
                mistake_count   INTEGER NOT NULL DEFAULT 0
            );
            """
        )

        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS mistakes (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id    INTEGER NOT NULL,
                word_id    INTEGER NOT NULL,
                created_at INTEGER NOT NULL,
                FOREIGN KEY(word_id) REFERENCES words(id)
            );
            """
        )

        # simple defensive migration in case table already existed without new columns
        await _ensure_column(db, "words", "last_success_ts", "INTEGER")
        await _ensure_column(db, "words", "next_due_ts", "INTEGER")
        await _ensure_column(
            db,
            "words",
            "mistake_count",
            "INTEGER NOT NULL DEFAULT 0",
            default_expr="0",
        )

        await db.execute(
            "CREATE INDEX IF NOT EXISTS idx_words_next_due ON words(next_due_ts);"
        )
        await db.execute(
            "CREATE INDEX IF NOT EXISTS idx_words_sheet_row ON words(sheet_row);"
        )
        await db.execute(
            "CREATE INDEX IF NOT EXISTS idx_mistakes_user_created "
            "ON mistakes(user_id, created_at);"
        )
        await db.execute(
            "CREATE INDEX IF NOT EXISTS idx_mistakes_word ON mistakes(word_id);"
        )

        await db.commit()


async def _ensure_column(
    db: aiosqlite.Connection,
    table: str,
    column: str,
    col_def: str,
    default_expr: Optional[str] = None,
) -> None:
    cur = await db.execute(f"PRAGMA table_info({table})")
    cols = [row[1] for row in await cur.fetchall()]
    await cur.close()
    if column not in cols:
        await db.execute(f"ALTER TABLE {table} ADD COLUMN {column} {col_def};")
        if default_expr is not None:
            await db.execute(
                f"UPDATE {table} SET {column} = {default_expr} WHERE {column} IS NULL;"
            )
        await db.commit()


# ---------- Core helpers ----------

async def replace_all_words(words: List[Word]) -> None:
    """
    Completely replace the content of the 'words' table with the given list.
    """
    now = int(time.time())
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM words;")

        for w in words:
            if w.last_success_ts is not None:
                next_due_ts = w.last_success_ts + _interval_seconds(w.progress)
            else:
                # never successfully answered -> make it due immediately
                next_due_ts = 0

            await db.execute(
                """
                INSERT INTO words (
                    sheet_row, progress, question, answer, example,
                    last_success_ts, next_due_ts, mistake_count
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?);
                """,
                (
                    w.sheet_row,
                    w.progress,
                    w.question,
                    w.answer,
                    w.example,
                    w.last_success_ts,
                    next_due_ts,
                    w.mistake_count,
                ),
            )

        await db.commit()


async def get_next_word() -> Optional[aiosqlite.Row]:
    """
    1) Try to pick a random word that is already due (next_due_ts <= now)
    2) If none are due, pick a random word from the 50 closest upcoming ones.
    """
    now = int(time.time())
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row

        # First: due words
        cur = await db.execute(
            """
            SELECT *
            FROM words
            WHERE next_due_ts IS NULL OR next_due_ts <= ?
            ORDER BY RANDOM()
            LIMIT 1;
            """,
            (now,),
        )
        row = await cur.fetchone()
        await cur.close()
        if row:
            return row

        # Second: upcoming words (take closest 50, then random among them)
        cur = await db.execute(
            """
            SELECT *
            FROM words
            ORDER BY next_due_ts ASC
            LIMIT 50;
            """
        )
        rows = await cur.fetchall()
        await cur.close()
        if not rows:
            return None

        return random.choice(rows)


async def get_due_count() -> int:
    now = int(time.time())
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            """
            SELECT COUNT(*)
            FROM words
            WHERE next_due_ts IS NULL OR next_due_ts <= ?;
            """,
            (now,),
        )
        (count,) = await cur.fetchone()
        await cur.close()
        return int(count)


async def get_word_by_id(word_id: int) -> Optional[aiosqlite.Row]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT * FROM words WHERE id = ?;", (word_id,))
        row = await cur.fetchone()
        await cur.close()
        return row


async def increment_progress_and_update_due(word_id: int) -> int:
    """
    Increase progress by 1, set last_success_ts=now, next_due_ts according to schedule.
    Returns new progress.
    """
    now = int(time.time())
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row

        cur = await db.execute("SELECT progress FROM words WHERE id = ?;", (word_id,))
        row = await cur.fetchone()
        await cur.close()
        if not row:
            return 0

        old_progress = row["progress"]
        new_progress = old_progress + 1
        next_due_ts = now + _interval_seconds(new_progress)

        await db.execute(
            """
            UPDATE words
            SET progress = ?, last_success_ts = ?, next_due_ts = ?
            WHERE id = ?;
            """,
            (new_progress, now, next_due_ts, word_id),
        )
        await db.commit()
        return new_progress


async def decrement_progress(word_id: int) -> int:
    """
    Decrease progress by 1 (not below 0) and make the word immediately due again.
    Returns new progress.
    """
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row

        cur = await db.execute("SELECT progress FROM words WHERE id = ?;", (word_id,))
        row = await cur.fetchone()
        await cur.close()
        if not row:
            return 0

        old_progress = row["progress"]
        new_progress = max(0, old_progress - 1)

        # make due immediately
        await db.execute(
            """
            UPDATE words
            SET progress = ?, last_success_ts = NULL, next_due_ts = 0
            WHERE id = ?;
            """,
            (new_progress, word_id),
        )
        await db.commit()
        return new_progress


async def get_all_progress() -> List[Dict[str, Any]]:
    """
    For Google Sheets sync: return sheet_row, progress, last_success_ts, mistake_count.
    """
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            """
            SELECT sheet_row, progress, last_success_ts, mistake_count
            FROM words
            ORDER BY sheet_row ASC;
            """
        )
        rows = await cur.fetchall()
        await cur.close()
        return [dict(r) for r in rows]


# ---------- Mistakes ----------

async def log_mistake(user_id: int, word_id: int) -> None:
    now = int(time.time())
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """
            INSERT INTO mistakes (user_id, word_id, created_at)
            VALUES (?, ?, ?);
            """,
            (user_id, word_id, now),
        )
        await db.execute(
            "UPDATE words SET mistake_count = mistake_count + 1 WHERE id = ?;",
            (word_id,),
        )
        await db.commit()


async def get_last_mistakes(user_id: int, limit: int = 50) -> List[Dict[str, Any]]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            """
            SELECT w.question, w.answer
            FROM mistakes m
            JOIN words w ON w.id = m.word_id
            WHERE m.user_id = ?
            ORDER BY m.id DESC
            LIMIT ?;
            """,
            (user_id, limit),
        )
        rows = await cur.fetchall()
        await cur.close()
        return [dict(r) for r in rows]


async def get_users_with_mistakes() -> List[int]:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT DISTINCT user_id FROM mistakes;")
        rows = await cur.fetchall()
        await cur.close()
        return [int(r[0]) for r in rows]


async def get_mistakes_for_sheet() -> List[Dict[str, Any]]:
    """
    Full mistakes log for exporting into Google Sheets (Log2 sheet).
    """
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            """
            SELECT
                w.sheet_row,
                w.question,
                w.answer,
                m.created_at
            FROM mistakes m
            JOIN words w ON w.id = m.word_id
            ORDER BY m.id ASC;
            """
        )
        rows = await cur.fetchall()
        await cur.close()
        return [dict(r) for r in rows]


# ---------- Stats ----------

async def get_stats(user_id: int) -> Dict[str, int]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row

        cur = await db.execute("SELECT COUNT(*) FROM words;")
        (total_words,) = await cur.fetchone()
        await cur.close()

        now = int(time.time())
        cur = await db.execute(
            """
            SELECT COUNT(*)
            FROM words
            WHERE next_due_ts IS NULL OR next_due_ts <= ?;
            """,
            (now,),
        )
        (due_now,) = await cur.fetchone()
        await cur.close()

        cur = await db.execute(
            "SELECT COUNT(*) FROM words WHERE progress >= 5;"
        )
        (well_known,) = await cur.fetchone()
        await cur.close()

        cur = await db.execute(
            "SELECT COUNT(*) FROM mistakes WHERE user_id = ?;",
            (user_id,),
        )
        (mistakes_total,) = await cur.fetchone()
        await cur.close()

        return {
            "total_words": int(total_words),
            "due_now": int(due_now),
            "well_known": int(well_known),
            "mistakes_total": int(mistakes_total),
        }
