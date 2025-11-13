# db.py

import aiosqlite
from dataclasses import dataclass

DB_PATH = "vocab.db"


@dataclass
class Word:
    sheet_row: int
    progress: int
    question: str
    answer: str
    example: str | None = None


async def init_db():
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
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT COUNT(*) FROM words")
        count = (await cursor.fetchone())[0]
        await cursor.close()

        if count == 0:
            await db.execute(
                """
                INSERT INTO words (sheet_row, progress, question, answer, example)
                VALUES
                (1, 0, 'laufen — бегать/ходить', 'laufen', 'Ich laufe jeden Morgen im Park.'),
                (2, 0, 'sprechen — говорить', 'sprechen', 'Wir sprechen Deutsch.')
                """
            )
            await db.commit()


async def replace_all_words(word_list: list[Word]):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM words")
        for w in word_list:
            await db.execute(
                """
                INSERT INTO words (sheet_row, progress, question, answer, example)
                VALUES (?, ?, ?, ?, ?)
                """,
                (w.sheet_row, w.progress, w.question, w.answer, w.example),
            )
        await db.commit()


async def get_next_word():
    """Берёт слово с минимальным прогрессом случайным образом."""
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
            """
            UPDATE words
            SET progress = progress + 1
            WHERE id = ?
            """,
            (word_id,),
        )
        await db.commit()


async def decrement_progress(word_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """
            UPDATE words
            SET progress = CASE 
                WHEN progress > 0 THEN progress - 1
                ELSE 0
            END
            WHERE id = ?
            """,
            (word_id,),
        )
        await db.commit()


async def get_word_by_id(word_id: int):
    """Возвращает одну строку words по id."""
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
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "SELECT sheet_row, progress FROM words ORDER BY sheet_row"
        )
        rows = await cursor.fetchall()
        await cursor.close()

    return [{"sheet_row": r[0], "progress": r[1]} for r in rows]
