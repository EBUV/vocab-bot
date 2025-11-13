# db.py

import aiosqlite
from config import DB_PATH

CREATE_WORDS_TABLE = """
CREATE TABLE IF NOT EXISTS words (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    progress INTEGER NOT NULL DEFAULT 0,
    question TEXT NOT NULL,
    answer TEXT NOT NULL,
    example TEXT
);
"""

async def init_db():
    """Создаём таблицу, если её ещё нет."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(CREATE_WORDS_TABLE)
        await db.commit()

async def add_dummy_words_if_empty():
    """Для теста: добавляем несколько слов, если таблица пустая."""
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT COUNT(*) FROM words")
        (count,) = await cursor.fetchone()
        await cursor.close()

        if count == 0:
            sample_words = [
                (0, "дом (по-немецки?)", "das Haus", "Das Haus ist groß."),
                (0, "бежать/ходить (по-немецки?)", "laufen", "Ich laufe jeden Morgen im Park."),
                (0, "говорить (по-немецки?)", "sprechen", "Wir sprechen Deutsch."),
            ]
            await db.executemany(
                "INSERT INTO words (progress, question, answer, example) VALUES (?, ?, ?, ?)",
                sample_words,
            )
            await db.commit()

async def get_next_word():
    """
    Возвращает 'следующее' слово:
    сначала с минимальным прогрессом, при равенстве — случайное.
    """
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            """
            SELECT id, progress, question, answer, example
            FROM words
            ORDER BY progress ASC, RANDOM()
            LIMIT 1
            """
        )
        row = await cursor.fetchone()
        await cursor.close()
    return row

async def increment_progress(word_id: int):
    """Увеличиваем progress на 1 для данного слова."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE words SET progress = progress + 1 WHERE id = ?",
            (word_id,),
        )
        await db.commit()

async def decrement_progress(word_id: int):
    """Уменьшаем progress на 1, но не ниже 0 (на будущее)."""
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
