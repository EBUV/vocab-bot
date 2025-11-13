# db.py

import aiosqlite
from dataclasses import dataclass
from typing import Optional, List

from config import DB_PATH


@dataclass
class Word:
    sheet_row: int
    progress: int
    question: str
    answer: str
    example: Optional[str] = None


async def init_db():
    """Создаём таблицу, если её ещё нет."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS words (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sheet_row INTEGER NOT NULL,
                progress INTEGER NOT NULL DEFAULT 0,
                question TEXT NOT NULL,
                answer TEXT NOT NULL,
                example TEXT
            )
            """
        )
        await db.commit()


async def add_dummy_words_if_empty():
    """Если база пустая – добавляем пару тестовых слов (на случай, если забыли экспорт)."""
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT COUNT(*) FROM words")
        (count,) = await cursor.fetchone()
        await cursor.close()

        if count == 0:
            sample_words = [
                (2, 0, "laufen — бегать/ходить", "laufen", "Ich laufe jeden Morgen im Park."),
                (3, 0, "sprechen — говорить", "sprechen", "Wir sprechen Deutsch."),
            ]
            await db.executemany(
                "INSERT INTO words (sheet_row, progress, question, answer, example) "
                "VALUES (?, ?, ?, ?, ?)",
                sample_words,
            )
            await db.commit()


async def replace_all_words(words: List[Word]):
    """Полностью заменяет содержимое таблицы words списком из Google Sheets."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM words")
        await db.executemany(
            """
            INSERT INTO words (sheet_row, progress, question, answer, example)
            VALUES (?, ?, ?, ?, ?)
            """,
            [
                (w.sheet_row, w.progress, w.question, w.answer, w.example)
                for w in words
            ],
        )
        await db.commit()


async def get_next_word():
    """Берём слово с минимальным прогрессом, при равенстве – случайно."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            """
            SELECT id, sheet_row, progress, question, answer, example
            FROM words
            ORDER BY progress ASC, RANDOM()
            LIMIT 1
            """
        )
        row = await cursor.fetchone()
        await cursor.close()
    return row


async def increment_progress(word_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE words SET progress = progress + 1 WHERE id = ?",
            (word_id,),
        )
        await db.commit()


async def decrement_progress(word_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """
            UPDATE words
            SET progress = CASE WHEN progress > 0 THEN progress - 1 ELSE 0 END
            WHERE id = ?
            """,
            (word_id,),
        )
        await db.commit()


async def get_word_by_id(word_id: int):
    """Возвращает одну строку words по id или None."""
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
    """Для экспорта в Google Sheets: sheet_row + progress для всех слов."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT sheet_row, progress FROM words ORDER BY sheet_row ASC"
        )
        rows = await cursor.fetchall()
        await cursor.close()

    return [
        {"sheet_row": row["sheet_row"], "progress": row["progress"]}
        for row in rows
    ]
