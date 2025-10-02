"""
Сервис для асинхронного анализа структуры БД с помощью LLM
Архитектура: FastAPI + Kafka + PostgreSQL
"""

# main.py
import json
import logging
import sys
import uuid
from contextlib import asynccontextmanager
from typing import Any, Optional

from aiokafka import AIOKafkaProducer
from fastapi import FastAPI, HTTPException
from psycopg2.extras import Json, RealDictCursor
from psycopg2.pool import ThreadedConnectionPool
from pydantic import BaseModel

from src.config import Config


def setup_logging():
    """Настройка логирования для приложения"""
    # Создаем форматтер
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Консольный хендлер
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)

    # Файловый хендлер
    file_handler = logging.FileHandler("app.log", encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)

    # Настраиваем root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)
    root_logger.addHandler(console_handler)
    root_logger.addHandler(file_handler)

    # Уменьшаем уровень логирования для сторонних библиотек
    logging.getLogger("aiokafka").setLevel(logging.WARNING)
    logging.getLogger("kafka").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)

    return logging.getLogger(__name__)


logger = setup_logging()


# === Модели данных ===


class AnalysisRequest(BaseModel):
    """Запрос на анализ структуры БД"""

    url: str
    ddl: list[Any]
    queries: list[Any]
    pass


class TaskResponse(BaseModel):
    """Ответ с ID задачи"""

    taskid: str


class StatusResponse(BaseModel):
    """Статус выполнения задачи"""

    taskid: str
    status: str  # pending, processing, completed, failed
    progress: Optional[int] = None  # процент выполнения (0-100)


class ResultResponse(BaseModel):
    """Результат анализа"""

    taskid: str
    status: str
    result: Optional[dict] = None
    error: Optional[str] = None


# === База данных ===


def get_db_pool():
    """Создание пула соединений с PostgreSQL"""
    logger.info("Создание пула соединений с PostgreSQL")
    try:
        pool = ThreadedConnectionPool(
            Config.DB_POOL_MIN,
            Config.DB_POOL_MAX,
            host=Config.POSTGRES_HOST,
            port=Config.POSTGRES_PORT,
            database=Config.POSTGRES_DB,
            user=Config.POSTGRES_USER,
            password=Config.POSTGRES_PASSWORD,
        )
        logger.info(
            f"Пул соединений создан успешно (min={Config.DB_POOL_MIN}, max={Config.DB_POOL_MAX})"
        )
        return pool
    except Exception as e:
        logger.error(f"Ошибка при создании пула соединений: {e}")
        raise


def init_db_schema(pool):
    """Создание таблиц в БД"""
    logger.info("Инициализация схемы базы данных")
    conn = pool.getconn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS tasks (
                    task_id UUID PRIMARY KEY,
                    status VARCHAR(50) NOT NULL,
                    created_at TIMESTAMP DEFAULT NOW(),
                    updated_at TIMESTAMP DEFAULT NOW(),
                    request_data JSONB,
                    result_data JSONB,
                    error_message TEXT,
                    progress INTEGER DEFAULT 0
                )
            """)
            conn.commit()
        logger.info("Схема базы данных инициализирована успешно")
    except Exception as e:
        logger.error(f"Ошибка при инициализации схемы БД: {e}")
        raise
    finally:
        pool.putconn(conn)


# === Lifespan управление ===


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Управление жизненным циклом приложения"""
    # Startup
    logger.info("Запуск приложения...")

    try:
        app.state.db_pool = get_db_pool()
        init_db_schema(app.state.db_pool)

        # Инициализация Kafka producer
        logger.info("Инициализация Kafka Producer")
        app.state.kafka_producer = AIOKafkaProducer(
            bootstrap_servers=Config.KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
        await app.state.kafka_producer.start()
        logger.info("Kafka Producer запущен успешно")

        logger.info("Приложение успешно запущено")
    except Exception as e:
        logger.error(f"Ошибка при запуске приложения: {e}")
        raise

    yield

    # Shutdown
    logger.info("Остановка приложения...")
    try:
        await app.state.kafka_producer.stop()
        logger.info("Kafka Producer остановлен")
        app.state.db_pool.closeall()
        logger.info("Пул соединений закрыт")
        logger.info("Приложение остановлено успешно")
    except Exception as e:
        logger.error(f"Ошибка при остановке приложения: {e}")


# === FastAPI приложение ===

app = FastAPI(title="LLM DB Analysis Service", lifespan=lifespan)


@app.post("/new", response_model=TaskResponse)
async def create_task(request: AnalysisRequest):
    """
    Создание новой задачи на анализ БД
    """
    task_id = str(uuid.uuid4())
    logger.info(f"Создание новой задачи {task_id}")

    # Сохраняем задачу в БД со статусом pending
    conn = app.state.db_pool.getconn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO tasks (task_id, status, request_data)
                VALUES (%s, %s, %s)
            """,
                (task_id, "pending", Json(request.model_dump())),
            )
            conn.commit()
        logger.info(f"Задача {task_id} сохранена в БД со статусом 'pending'")
    except Exception as e:
        logger.error(f"Ошибка при сохранении задачи {task_id} в БД: {e}")
        raise HTTPException(status_code=500, detail="Failed to create task in database")
    finally:
        app.state.db_pool.putconn(conn)

    # Отправляем задачу в Kafka
    message = {
        "task_id": task_id,
        "body": request.dict(),
    }

    try:
        await app.state.kafka_producer.send(Config.KAFKA_TOPIC, message)
        logger.info(f"Задача {task_id} отправлена в Kafka топик '{Config.KAFKA_TOPIC}'")
    except Exception as e:
        logger.error(f"Ошибка при отправке задачи {task_id} в Kafka: {e}")
        # Если не удалось отправить в Kafka, помечаем задачу как failed
        conn = app.state.db_pool.getconn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE tasks 
                    SET status = %s, error_message = %s, updated_at = NOW()
                    WHERE task_id = %s
                """,
                    ("failed", f"Kafka error: {str(e)}", task_id),
                )
                conn.commit()
            logger.warning(f"Задача {task_id} помечена как 'failed' из-за ошибки Kafka")
        finally:
            app.state.db_pool.putconn(conn)
        raise HTTPException(status_code=500, detail="Failed to queue task")

    return TaskResponse(taskid=task_id)


@app.get("/status", response_model=StatusResponse)
async def get_status(taskid: str):
    """
    Получение статуса задачи
    """
    logger.info(f"Запрос статуса задачи {taskid}")

    conn = app.state.db_pool.getconn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT task_id, status, progress
                FROM tasks
                WHERE task_id = %s
            """,
                (taskid,),
            )
            row = cur.fetchone()
    finally:
        app.state.db_pool.putconn(conn)

    if not row:
        logger.warning(f"Задача {taskid} не найдена")
        raise HTTPException(status_code=404, detail="Task not found")

    logger.info(
        f"Статус задачи {taskid}: {row['status']} (прогресс: {row['progress']}%)"
    )

    return StatusResponse(
        taskid=str(row["task_id"]), status=row["status"], progress=row["progress"]
    )


@app.get("/getresult", response_model=ResultResponse)
async def get_result(taskid: str):
    """
    Получение результата выполнения задачи
    """
    logger.info(f"Запрос результата задачи {taskid}")

    conn = app.state.db_pool.getconn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT task_id, status, result_data, error_message
                FROM tasks
                WHERE task_id = %s
            """,
                (taskid,),
            )
            row = cur.fetchone()
    finally:
        app.state.db_pool.putconn(conn)

    if not row:
        logger.warning(f"Задача {taskid} не найдена")
        raise HTTPException(status_code=404, detail="Task not found")

    if row["status"] not in ["completed", "failed"]:
        logger.info(f"Задача {taskid} еще не готова (статус: {row['status']})")
        raise HTTPException(
            status_code=400,
            detail=f"Task is not ready. Current status: {row['status']}",
        )

    if row["status"] == "completed":
        logger.info(f"Результат задачи {taskid} успешно получен")
    else:
        logger.info(f"Задача {taskid} завершена с ошибкой: {row['error_message']}")

    return ResultResponse(
        taskid=str(row["task_id"]),
        status=row["status"],
        result=row["result_data"],
        error=row["error_message"],
    )
