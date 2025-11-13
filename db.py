import aiosqlite
from dataclasses import dataclass
from typing import List, Optional

from config import DB_PATH


# ---------- Модель слова (для удобства в коде) ----------

@dataclass
class Word:
    sheet_row: int        # номер строки в Google Sheets
    progress: int         # прогресс (как в колонке A)
    question: str         # вопрос / формулировка
    answer: str           # правильный ответ
    example: Optional[str] = None  # пример (может быть None)


# ---------- Создание таблицы ----------

CREATE_WORDS_TABLE = """
CREATE TABLE IF NOT EXISTS words (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    sheet_row INTEGER NOT NULL,
    progress INTEGER NOT NULL DEFAULT 0,
    question TEXT NOT NULL,
    answer TEXT NOT NULL,
    example TEXT,
    UNIQUE(sheet_row) ON CONFLICT REPLACE
);
"""


async def init_db():
    """Создаём таблицу, если её ещё нет."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(CREATE_WORDS_TABLE)
        await db.commit()


# ---------- Тестовые данные (чтоб бот не был пустым) ----------

async def add_dummy_words_if_empty():
    """Для теста: добавляем несколько слов, если таблица пустая."""
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT COUNT(*) FROM words")
        (count,) = await cursor.fetchone()
        await cursor.close()

        if count == 0:
            sample_words = [
                # sheet_row, progress, question, answer, example
                (2, 0, "дом (по-немецки?)", "das Haus", "Das Haus ist groß."),
                (3, 0, "бежать/ходить (по-немецки?)", "laufen", "Ich laufe jeden Morgen im Park."),
                (4, 0, "говорить (по-немецки?)", "sprechen", "Wir sprechen Deutsch."),
            ]
            await db.executemany(
                "INSERT INTO words (sheet_row, progress, question, answer, example) "
                "VALUES (?, ?, ?, ?, ?)",
                sample_words,
            )
            await db.commit()


# ---------- Логика для бота ----------

async def get_next_word():
    """
    Возвращает 'следующее' слово:
    сначала с минимальным прогрессом, при равенстве — случайное.
    """
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
    """Увеличиваем progress на 1 для данного слова."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE words SET progress = progress + 1 WHERE id = ?",
            (word_id,),
        )
        await db.commit()


async def decrement_progress(word_id: int):
    """Уменьшаем progress на 1, но не ниже 0 (на будущее, когда добавим проверку ответа)."""
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


# ---------- Функции для синхронизации с Google Sheets ----------

async def replace_all_words(words: List[Word]):
    """
    Полностью заменяет содержимое таблицы words списком слов из Google Sheets.

    Используем, когда в таблице нажимаем "Экспорт в бота":
    мы считаем, что Google Sheets = источник правды.
    """
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


async def get_all_progress():
    """
    Возвращает список {sheet_row, progress} для экспорта в Google Sheets.
    """
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
