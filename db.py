# db.py
import time
from dataclasses import dataclass
from typing import List, Optional, Tuple

import aiosqlite

from config import DB_PATH

# ---- Dataclass for importing from Google Sheets ----

@dataclass
class Word:
    sheet_row: int
    progress: int
    question: str
    answer: str
    example: Optional[str] = None
    last_success_ts: Optional[int] = None
    mistakes_count: int = 0


# ---- Spaced-repetition intervals (in minutes) ----
# level: minutes  (based on your sheet: 1, 30, 240, 1440, ...)
INTERVALS_MIN = {
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
    12: 213120,  # 12+
}

MAX_LEVEL = 12


def _calc_next_due_ts(progress: int, base_ts: int) -> int:
    """
    Calculate next_due_ts in seconds from given base_ts (usually "now").
    """
    if progress <= 0:
        delay_min = INTERVALS_MIN[0]
    elif progress in INTERVALS_MIN:
        delay_min = INTERVALS_MIN[progress]
    else:
        delay_min = INTERVALS_MIN[MAX_LEVEL]
    return base_ts + delay_min * 60


# ---- DB init ----

async def init_db() -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row

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
                mistakes_count  INTEGER NOT NULL DEFAULT 0
            )
            """
        )

        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS mistakes_log (
                id        INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id   INTEGER NOT NULL,
                word_id   INTEGER NOT NULL,
                ts        INTEGER NOT NULL,
                FOREIGN KEY (word_id) REFERENCES words(id)
            )
            """
        )

        await db.execute("CREATE INDEX IF NOT EXISTS idx_words_sheet_row ON words(sheet_row)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_words_next_due ON words(next_due_ts)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_mistakes_user_ts ON mistakes_log(user_id, ts)")
        await db.commit()


# ---- Core helpers ----

async def get_next_word():
    """
    Get one word that is due now (or never scheduled).
    """
    now = int(time.time())
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            """
            SELECT *
            FROM words
            WHERE next_due_ts IS NULL OR next_due_ts <= ?
            ORDER BY
                (next_due_ts IS NULL) DESC,   -- words never scheduled first
                next_due_ts ASC
            LIMIT 1
            """,
            (now,),
        )
        row = await cur.fetchone()
        await cur.close()
    return row


async def get_word_by_id(word_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT * FROM words WHERE id = ?", (word_id,))
        row = await cur.fetchone()
        await cur.close()
    return row


async def increment_progress_and_update_due(word_id: int) -> int:
    """
    Correct answer:
    - progress +1 (capped at MAX_LEVEL)
    - last_success_ts = now
    - next_due_ts based on spaced-repetition interval.
    Returns new progress.
    """
    now = int(time.time())
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row

        cur = await db.execute("SELECT progress FROM words WHERE id = ?", (word_id,))
        row = await cur.fetchone()
        await cur.close()
        if not row:
            return 0

        old_progress = row["progress"] or 0
        new_progress = min(old_progress + 1, MAX_LEVEL)

        next_due = _calc_next_due_ts(new_progress, now)

        await db.execute(
            """
            UPDATE words
            SET progress = ?, last_success_ts = ?, next_due_ts = ?
            WHERE id = ?
            """,
            (new_progress, now, next_due, word_id),
        )
        await db.commit()
    return new_progress


async def decrement_progress(word_id: int) -> None:
    """
    Wrong answer:
    - if progress > 6 -> progress -2
      else -> progress -1
    - last_success_ts = NULL
    - next_due_ts = NULL  (word becomes due immediately like "new")
    """
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row

        cur = await db.execute("SELECT progress FROM words WHERE id = ?", (word_id,))
        row = await cur.fetchone()
        await cur.close()
        if not row:
            return

        old_progress = row["progress"] or 0
        if old_progress > 6:
            new_progress = max(0, old_progress - 2)
        else:
            new_progress = max(0, old_progress - 1)

        await db.execute(
            """
            UPDATE words
            SET progress = ?, last_success_ts = NULL, next_due_ts = NULL
            WHERE id = ?
            """,
            (new_progress, word_id),
        )
        await db.commit()


async def get_due_count() -> int:
    """
    Number of words currently due.
    """
    now = int(time.time())
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT COUNT(*) AS c FROM words WHERE next_due_ts IS NULL OR next_due_ts <= ?",
            (now,),
        )
        row = await cur.fetchone()
        await cur.close()
    return row["c"] if row else 0


# ---- Full replace from Google Sheets ----

async def replace_all_words(words: List[Word]) -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM words")

        for w in words:
            await db.execute(
                """
                INSERT INTO words (
                    sheet_row, progress, question, answer, example,
                    last_success_ts, next_due_ts, mistakes_count
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    w.sheet_row,
                    w.progress,
                    w.question,
                    w.answer,
                    w.example,
                    w.last_success_ts,
                    None,  # next_due_ts пересчитаем по мере ответов
                    w.mistakes_count,
                ),
            )

        await db.commit()


async def get_all_progress():
    """
    For sync back to Google Sheets.
    Returns list of dicts:
      sheet_row, progress, last_success_ts, mistakes_count
    """
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            "SELECT sheet_row, progress, last_success_ts, mistakes_count FROM words"
        )
        rows = await cur.fetchall()
        await cur.close()
    return rows


# ---- Mistakes ----

async def log_mistake(user_id: int, word_id: int) -> None:
    now = int(time.time())
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """
            UPDATE words
            SET mistakes_count = mistakes_count + 1
            WHERE id = ?
            """,
            (word_id,),
        )
        await db.execute(
            """
            INSERT INTO mistakes_log (user_id, word_id, ts)
            VALUES (?, ?, ?)
            """,
            (user_id, word_id, now),
        )
        await db.commit()


async def get_last_mistakes(user_id: int, limit: int = 60):
    """
    Get LAST <limit> mistakes of user, but ordered from OLDEST to NEWEST.
    """
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            """
            SELECT m.ts, m.user_id, w.question, w.answer
            FROM mistakes_log m
            JOIN words w ON w.id = m.word_id
            WHERE m.user_id = ?
            ORDER BY m.ts DESC
            LIMIT ?
            """,
            (user_id, limit),
        )
        rows = await cur.fetchall()
        await cur.close()

    # rows сейчас от новых к старым, разворачиваем список
    rows_list = [dict(r) for r in rows]
    rows_list.reverse()
    return rows_list


async def get_users_with_mistakes():
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT DISTINCT user_id FROM mistakes_log"
        )
        rows = await cur.fetchall()
        await cur.close()
    return [r["user_id"] for r in rows]


async def get_all_mistakes_for_sync():
    """
    For syncing Log2 sheet: all mistake entries with sheet_row, question, answer.
    """
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            """
            SELECT
                m.user_id,
                w.sheet_row,
                m.ts,
                w.question,
                w.answer
            FROM mistakes_log m
            JOIN words w ON w.id = m.word_id
            ORDER BY m.ts ASC
            """
        )
        rows = await cur.fetchall()
        await cur.close()
    return rows


async def replace_all_mistakes(entries: List[Tuple[int, int, int]]) -> None:
    """
    Replace entire mistakes_log with given list of tuples:
    (user_id, sheet_row, ts_sec)
    """
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM mistakes_log")
        # восстановим также mistakes_count
        await db.execute("UPDATE words SET mistakes_count = 0")

        for user_id, sheet_row, ts_sec in entries:
            cur = await db.execute(
                "SELECT id FROM words WHERE sheet_row = ?", (sheet_row,)
            )
            row = await cur.fetchone()
            await cur.close()
            if not row:
                continue
            word_id = row["id"]

            await db.execute(
                """
                INSERT INTO mistakes_log (user_id, word_id, ts)
                VALUES (?, ?, ?)
                """,
                (user_id, word_id, ts_sec),
            )
            await db.execute(
                """
                UPDATE words
                SET mistakes_count = mistakes_count + 1
                WHERE id = ?
                """,
                (word_id,),
            )

        await db.commit()


# ---- Stats ----

async def get_stats(user_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row

        cur = await db.execute("SELECT COUNT(*) AS c FROM words")
        total_words = (await cur.fetchone())["c"]
        await cur.close()

        now = int(time.time())
        cur = await db.execute(
            "SELECT COUNT(*) AS c FROM words WHERE next_due_ts IS NULL OR next_due_ts <= ?",
            (now,),
        )
        due_now = (await cur.fetchone())["c"]
        await cur.close()

        cur = await db.execute(
            "SELECT COUNT(*) AS c FROM words WHERE progress >= 5"
        )
        well_known = (await cur.fetchone())["c"]
        await cur.close()

        cur = await db.execute(
            "SELECT COUNT(*) AS c FROM mistakes_log WHERE user_id = ?",
            (user_id,),
        )
        mistakes_total = (await cur.fetchone())["c"]
        await cur.close()

    return {
        "total_words": total_words,
        "due_now": due_now,
        "well_known": well_known,
        "mistakes_total": mistakes_total,
    }
