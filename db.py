import sqlite3
import time
import random
import json
import os
from dataclasses import dataclass
from typing import Optional, List, Tuple, Dict

from config import DB_PATH, INTERVALS_PATH

# ---------- intervals logic ----------

# Default intervals (fallback)
DEFAULT_INTERVALS = {
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

def load_intervals() -> Dict[int, int]:
    if not os.path.exists(INTERVALS_PATH):
        return DEFAULT_INTERVALS

    try:
        with open(INTERVALS_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)

        # normalize
        out = {}
        for level_str, minutes in data.items():
            out[int(level_str)] = int(minutes)

        # ensure 0 exists
        out[0] = 1
        return out
    except:
        return DEFAULT_INTERVALS


CURRENT_INTERVALS = load_intervals()


def interval_minutes(level: int) -> int:
    if level <= 0:
        return CURRENT_INTERVALS.get(0, 1)
    if level >= 12:
        return CURRENT_INTERVALS.get(12, DEFAULT_INTERVALS[12])
    return CURRENT_INTERVALS.get(level, DEFAULT_INTERVALS[level])


def compute_next_due_ts(last_success_ts: Optional[int], progress: int) -> int:
    base = last_success_ts if last_success_ts is not None else int(time.time())
    minutes = interval_minutes(progress)
    return base + minutes * 60


# ---------- dataclass ----------

@dataclass
class Word:
    sheet_row: int
    progress: int
    question: str
    answer: str
    example: Optional[str] = None
    last_success_ts: Optional[int] = None
    mistakes_count: int = 0


# ---------- DB core ----------

def get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


async def init_db() -> None:
    conn = get_connection()
    cur = conn.cursor()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS words (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            sheet_row       INTEGER NOT NULL UNIQUE,
            progress        INTEGER NOT NULL DEFAULT 0,
            question        TEXT NOT NULL,
            answer          TEXT NOT NULL,
            example         TEXT,
            last_success_ts INTEGER,
            next_due_ts     INTEGER,
            mistakes_count  INTEGER NOT NULL DEFAULT 0
        );
        """
    )

    cur.execute(
        """CREATE INDEX IF NOT EXISTS idx_words_next_due ON words(next_due_ts);"""
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS mistakes (
            id       INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id  INTEGER NOT NULL,
            question TEXT NOT NULL,
            answer   TEXT NOT NULL,
            ts       INTEGER NOT NULL
        );
        """
    )

    cur.execute(
        """CREATE INDEX IF NOT EXISTS idx_mistakes_user_ts ON mistakes(user_id, ts);"""
    )

    conn.commit()
    conn.close()


# ---------- core spaced repetition ----------

async def get_next_word():
    conn = get_connection()
    cur = conn.cursor()
    now = int(time.time())

    cur.execute(
        """
        SELECT * FROM words
        WHERE next_due_ts IS NULL OR next_due_ts <= ?
        ORDER BY RANDOM()
        LIMIT 1
        """,
        (now,),
    )
    row = cur.fetchone()
    if row:
        conn.close()
        return row

    cur.execute(
        """
        SELECT * FROM words
        WHERE next_due_ts IS NOT NULL
        ORDER BY next_due_ts ASC
        LIMIT 100
        """
    )
    rows = cur.fetchall()
    if rows:
        row = random.choice(rows)
        conn.close()
        return row

    cur.execute("SELECT * FROM words ORDER BY RANDOM() LIMIT 1")
    row = cur.fetchone()
    conn.close()
    return row


async def increment_progress_and_update_due(word_id: int) -> int:
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("SELECT progress FROM words WHERE id = ?", (word_id,))
    row = cur.fetchone()
    if not row:
        conn.close()
        return 0

    progress = int(row["progress"]) + 1
    now = int(time.time())
    next_due = compute_next_due_ts(now, progress)

    cur.execute(
        """
        UPDATE words
        SET progress = ?, last_success_ts = ?, next_due_ts = ?
        WHERE id = ?
        """,
        (progress, now, next_due, word_id),
    )

    conn.commit()
    conn.close()
    return progress


async def decrement_progress(word_id: int) -> int:
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("SELECT progress FROM words WHERE id = ?", (word_id,))
    row = cur.fetchone()
    if not row:
        conn.close()
        return 0

    current = int(row["progress"])
    step = 2 if current > 6 else 1
    new_progress = max(0, current - step)

    now = int(time.time())

    if current > 6:
        # schedule for tomorrow
        minutes = interval_minutes(new_progress)
        next_due = now + 24 * 3600  # tomorrow
        last_ts = next_due - minutes * 60
    else:
        next_due = now
        last_ts = None

    cur.execute(
        """
        UPDATE words
        SET progress = ?, last_success_ts = ?, next_due_ts = ?
        WHERE id = ?
        """,
        (new_progress, last_ts, next_due, word_id),
    )

    conn.commit()
    conn.close()
    return new_progress


async def get_word_by_id(word_id: int):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT * FROM words WHERE id = ?", (word_id,))
    row = cur.fetchone()
    conn.close()
    return row


async def get_due_count() -> int:
    conn = get_connection()
    cur = conn.cursor()
    now = int(time.time())
    cur.execute(
        "SELECT COUNT(*) AS cnt FROM words WHERE next_due_ts IS NULL OR next_due_ts <= ?",
        (now,),
    )
    row = cur.fetchone()
    conn.close()
    return int(row["cnt"] if row else 0)


# ---------- mistakes ----------

async def log_mistake(user_id: int, word_id: int) -> None:
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("SELECT question, answer FROM words WHERE id = ?", (word_id,))
    row = cur.fetchone()
    if not row:
        conn.close()
        return

    ts = int(time.time())

    cur.execute(
        """INSERT INTO mistakes (user_id, question, answer, ts) VALUES (?, ?, ?, ?)""",
        (user_id, row["question"], row["answer"], ts),
    )

    cur.execute(
        "UPDATE words SET mistakes_count = mistakes_count + 1 WHERE id = ?",
        (word_id,),
    )

    conn.commit()
    conn.close()


async def get_last_mistakes(user_id: int, limit: int = 80):
    conn = get_connection()
    cur = conn.cursor()

    cur.execute(
        """
        SELECT question, answer, ts
        FROM mistakes
        WHERE user_id = ?
        ORDER BY ts DESC
        LIMIT ?
        """,
        (user_id, limit),
    )
    rows = cur.fetchall()
    conn.close()

    rowlist = list(rows)
    rowlist.reverse()  # oldest → newest
    return rowlist


async def get_all_mistakes_for_sync():
    conn = get_connection()
    cur = conn.cursor()

    cur.execute(
        """
        SELECT user_id, question, answer, ts
        FROM mistakes
        ORDER BY ts ASC, id ASC
        """
    )
    rows = cur.fetchall()
    conn.close()
    return rows


async def replace_all_mistakes(entries: List[Tuple[int, str, str, int]]) -> None:
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("DELETE FROM mistakes")

    if entries:
        cur.executemany(
            """INSERT INTO mistakes (user_id, question, answer, ts) VALUES (?, ?, ?, ?)""",
            entries,
        )

    conn.commit()
    conn.close()


# ---------- sync ----------

async def replace_all_words(words: List[Word]) -> None:
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("DELETE FROM words")

    now = int(time.time())

    for w in words:
        if w.last_success_ts is not None:
            next_due = compute_next_due_ts(w.last_success_ts, w.progress)
        else:
            next_due = now

        cur.execute(
            """
            INSERT INTO words (
                sheet_row, progress, question, answer, example,
                last_success_ts, next_due_ts, mistakes_count
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                int(w.sheet_row),
                int(w.progress),
                w.question,
                w.answer,
                w.example,
                w.last_success_ts,
                next_due,
                int(w.mistakes_count or 0),
            ),
        )

    conn.commit()
    conn.close()


async def get_all_progress():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT sheet_row, progress, last_success_ts, mistakes_count
        FROM words
        ORDER BY sheet_row ASC
        """
    )
    rows = cur.fetchall()
    conn.close()
    return rows
