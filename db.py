# db.py
import sqlite3
import time
import random
from dataclasses import dataclass
from typing import Optional, List, Tuple, Dict

from config import DB_PATH

# ---------- helpers ----------


def get_connection() -> sqlite3.Connection:
    """
    Open SQLite connection with row_factory=Row
    so we can use row["column_name"] everywhere.
    """
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


# интервалы в МИНУТАХ для соответствующих уровней прогресса
# Irina's table: 1, 30, 240, 1440, 2880, 5760, 11520, 23040, 46080, 92160, 138240, 213120
LEVEL_TO_MINUTES: Dict[int, int] = {
    0: 1,       # для 0 берём как для 1
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


def progress_to_minutes(progress: int) -> int:
    if progress <= 0:
        return LEVEL_TO_MINUTES[0]
    if progress >= 12:
        return LEVEL_TO_MINUTES[12]
    return LEVEL_TO_MINUTES.get(progress, LEVEL_TO_MINUTES[12])


def compute_next_due_ts(last_success_ts: Optional[int], progress: int) -> int:
    """
    last_success_ts – unix time (sec).
    Если None – считаем от текущего момента.
    """
    base = last_success_ts if last_success_ts is not None else int(time.time())
    minutes = progress_to_minutes(progress)
    return base + minutes * 60


# ---------- dataclass for import ----------

@dataclass
class Word:
    sheet_row: int
    progress: int
    question: str
    answer: str
    example: Optional[str] = None
    last_success_ts: Optional[int] = None
    mistakes_count: int = 0


# ---------- schema & init ----------

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
        """
        CREATE INDEX IF NOT EXISTS idx_words_next_due
        ON words(next_due_ts);
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS mistakes (
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id   INTEGER NOT NULL,
            word_id   INTEGER NOT NULL,
            sheet_row INTEGER NOT NULL,
            ts        INTEGER NOT NULL,
            FOREIGN KEY(word_id) REFERENCES words(id)
        );
        """
    )

    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_mistakes_user_ts
        ON mistakes(user_id, ts);
        """
    )
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_mistakes_sheet_row
        ON mistakes(sheet_row);
        """
    )

    conn.commit()
    conn.close()


# ---------- core spaced repetition logic ----------

async def get_next_word():
    """
    Возвращает одну строку из words в виде sqlite3.Row.
    Логика:
      1) сначала слова, которые уже "должники" (next_due_ts <= now или NULL),
         случайное одно из них;
      2) если должников нет – берём до 100 ближайших по времени и среди них
         случайное;
      3) если и их нет (пустая БД) – None.
    """
    conn = get_connection()
    cur = conn.cursor()
    now = int(time.time())

    # сначала должники
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

    # ближайшие по времени (top 100)
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

    # fallback – любое слово
    cur.execute("SELECT * FROM words ORDER BY RANDOM() LIMIT 1")
    row = cur.fetchone()
    conn.close()
    return row


async def increment_progress_and_update_due(word_id: int) -> int:
    """
    Увеличиваем прогресс на 1 и обновляем last_success_ts / next_due_ts.
    Возвращаем новый прогресс.
    """
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
    """
    Уменьшаем прогресс:
      - если progress > 6 → минус 2
      - иначе → минус 1
    last_success_ts сбрасываем в NULL, next_due_ts ставим в now,
    чтобы слово стало "должником".
    """
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

    cur.execute(
        """
        UPDATE words
        SET progress = ?, last_success_ts = NULL, next_due_ts = ?
        WHERE id = ?
        """,
        (new_progress, now, word_id),
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
        """
        SELECT COUNT(*) AS cnt
        FROM words
        WHERE next_due_ts IS NULL OR next_due_ts <= ?
        """,
        (now,),
    )
    row = cur.fetchone()
    conn.close()
    return int(row["cnt"] if row else 0)


# ---------- mistakes ----------

async def log_mistake(user_id: int, word_id: int) -> None:
    """
    Записываем ошибку в mistakes и увеличиваем mistakes_count у слова.
    """
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("SELECT sheet_row FROM words WHERE id = ?", (word_id,))
    row = cur.fetchone()
    if not row:
        conn.close()
        return

    sheet_row = int(row["sheet_row"])
    ts = int(time.time())

    cur.execute(
        """
        INSERT INTO mistakes (user_id, word_id, sheet_row, ts)
        VALUES (?, ?, ?, ?)
        """,
        (user_id, word_id, sheet_row, ts),
    )
    cur.execute(
        """
        UPDATE words
        SET mistakes_count = mistakes_count + 1
        WHERE id = ?
        """,
        (word_id,),
    )

    conn.commit()
    conn.close()


async def get_last_mistakes(user_id: int, limit: int = 60):
    """
    Возвращает ошибки пользователя,
    строго в порядке от старых к новым.
    """
    conn = get_connection()
    cur = conn.cursor()

    cur.execute(
        """
        SELECT w.question, w.answer, m.ts
        FROM mistakes m
        JOIN words w ON w.sheet_row = m.sheet_row
        WHERE m.user_id = ?
        ORDER BY m.ts ASC
        LIMIT ?
        """,
        (user_id, limit),
    )

    rows = cur.fetchall()
    conn.close()

    return rows




async def get_users_with_mistakes() -> List[int]:
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT user_id FROM mistakes")
    rows = cur.fetchall()
    conn.close()
    return [int(r["user_id"]) for r in rows]


async def get_all_mistakes_for_sync():
    """
    Для экспорта в Google Sheets (Log2).
    Возвращаем список строк с полями:
      user_id, sheet_row, ts, question, answer
    """
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT m.user_id, m.sheet_row, m.ts, w.question, w.answer
        FROM mistakes m
        JOIN words w ON w.sheet_row = m.sheet_row
        ORDER BY m.ts ASC, m.id ASC
        """
    )
    rows = cur.fetchall()
    conn.close()
    return rows


async def replace_all_mistakes(entries: List[Tuple[int, int, int]]) -> None:
    """
    Полностью пересобираем таблицу mistakes.
    entries: список кортежей (user_id, sheet_row, ts_sec).
    После вставки пересчитываем mistakes_count в words.
    """
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("DELETE FROM mistakes")

    if entries:
        # нужно сопоставить sheet_row с id слова
        # сначала создаём map sheet_row -> id
        cur.execute("SELECT id, sheet_row FROM words")
        mapping = {int(r["sheet_row"]): int(r["id"]) for r in cur.fetchall()}

        insert_rows = []
        for user_id, sheet_row, ts_sec in entries:
            word_id = mapping.get(sheet_row)
            if word_id is None:
                continue
            insert_rows.append((user_id, word_id, sheet_row, int(ts_sec)))

        cur.executemany(
            """
            INSERT INTO mistakes (user_id, word_id, sheet_row, ts)
            VALUES (?, ?, ?, ?)
            """,
            insert_rows,
        )

    # пересчёт mistakes_count
    cur.execute("UPDATE words SET mistakes_count = 0")
    cur.execute(
        """
        UPDATE words
        SET mistakes_count = (
            SELECT COUNT(*)
            FROM mistakes m
            WHERE m.sheet_row = words.sheet_row
        )
        """
    )

    conn.commit()
    conn.close()


# ---------- sync with Google Sheets ----------

async def replace_all_words(words: List[Word]) -> None:
    """
    Полностью пересобираем таблицу words.
    next_due_ts пересчитываем на основании last_success_ts и progress.
    Если last_success_ts нет – слово считается уже "должником".
    """
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("DELETE FROM words")

    now = int(time.time())

    for w in words:
        if w.last_success_ts is not None:
            next_due = compute_next_due_ts(w.last_success_ts, w.progress)
        else:
            # если нет успешных ответов – сделать слово должником
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
    """
    Для экспорта в Google Sheets.
    Возвращаем список строк с полями:
      sheet_row, progress, last_success_ts, mistakes_count
    """
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


# ---------- stats ----------

async def get_stats(user_id: int):
    conn = get_connection()
    cur = conn.cursor()
    now = int(time.time())

    cur.execute("SELECT COUNT(*) AS cnt FROM words")
    total_words = int(cur.fetchone()["cnt"])

    cur.execute(
        """
        SELECT COUNT(*) AS cnt
        FROM words
        WHERE next_due_ts IS NULL OR next_due_ts <= ?
        """,
        (now,),
    )
    due_now = int(cur.fetchone()["cnt"])

    cur.execute(
        """
        SELECT COUNT(*) AS cnt
        FROM words
        WHERE progress >= 5
        """
    )
    well_known = int(cur.fetchone()["cnt"])

    cur.execute(
        """
        SELECT COUNT(*) AS cnt
        FROM mistakes
        WHERE user_id = ?
        """,
        (user_id,),
    )
    mistakes_total = int(cur.fetchone()["cnt"])

    conn.close()

    return {
        "total_words": total_words,
        "due_now": due_now,
        "well_known": well_known,
        "mistakes_total": mistakes_total,
    }
