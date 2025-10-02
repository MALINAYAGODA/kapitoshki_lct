# === Kafka Consumer (отдельный процесс) ===
# worker.py

import asyncio
import json
import logging
import re
import sys
from typing import Any, Optional

from aiokafka import AIOKafkaConsumer
from psycopg2.pool import ThreadedConnectionPool
from pydantic import BaseModel

from src.config import Config
from src.get_counts import get_counts
from src.models import DDLGenerationOutput, MigrationOutput, RewrittenQuery
from src.prompts import (
    DDL_PROMPT,
    INPUT_DDL_PROMPT,
    INPUT_MIGRATION_PROMPT,
    INPUT_SQL_PROMPT,
    MIGRATION_PROMPT,
    SQL_PROMPT,
)

# === Настройка логирования для воркера ===


def setup_worker_logging():
    """Настройка логирования для воркера"""
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - [WORKER] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Консольный хендлер
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)

    # Файловый хендлер для воркера
    file_handler = logging.FileHandler("worker.log", encoding="utf-8")
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
    logging.getLogger("openai").setLevel(logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.WARNING)

    return logging.getLogger(__name__)


worker_logger = setup_worker_logging()


class LLMProcessor:
    """Обработчик для работы с LLM через AsyncOpenAI"""

    model_name = "gpt-4.1"

    def __init__(self, api_key: str = None):
        from openai import AsyncOpenAI

        if api_key is None:
            api_key = "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"

        self.client = AsyncOpenAI(api_key=api_key)
        self.logger = logging.getLogger(self.__class__.__name__)

    def parse_catalog_and_schema_from_ddl(
        self,
        ddl_statements: list[dict[str, Any]],
    ) -> tuple[Optional[str], Optional[str]]:
        if not ddl_statements:
            return None, None

        # Паттерн для поиска CREATE TABLE catalog.schema.table_name
        pattern = r"CREATE\s+TABLE\s+([^.\s]+)\.([^.\s]+)\.([^.\s\(]+)"

        for ddl_item in ddl_statements:
            statement = ddl_item.get("statement", "")
            match = re.search(pattern, statement, re.IGNORECASE)
            if match:
                catalog = match.group(1)
                schema = match.group(2)
                self.logger.info(f"Найден каталог: '{catalog}', схема: '{schema}'")
                return catalog, schema

        self.logger.info("Не удалось найти каталог и схему в DDL statements")
        return None, None

    def get_database_config_from_data(
        self, data: dict[str, Any], new_schema: str = "optimized"
    ) -> dict[str, str]:
        ddl_statements = data.get("ddl", [])
        catalog, source_schema = self.parse_catalog_and_schema_from_ddl(ddl_statements)

        return {
            "catalog": catalog or "unknown",
            "source_schema": source_schema or "public",
            "new_schema": new_schema,
        }

    def count_tokens(self, text: str) -> int:
        """
        Подсчет токенов в тексте
        Для точного подсчета используйте tiktoken, здесь простая оценка
        """
        # Простая оценка: ~4 символа на токен для английского текста
        # Для точности установите tiktoken: pip install tiktoken
        try:
            import tiktoken

            encoding = tiktoken.encoding_for_model("gpt-4")
            return len(encoding.encode(text))
        except ImportError:
            # Грубая оценка если tiktoken не установлен
            return len(text) // 4

    def calculate_tokens(
        self, system_prompt: str, user_prompt: str, output_data: Any
    ) -> tuple[int, int]:
        """Подсчет токенов для входных и выходных данных"""
        input_tokens = self.count_tokens(system_prompt) + self.count_tokens(user_prompt)

        # Преобразуем выходные данные в JSON строку
        if hasattr(output_data, "model_dump"):
            output_text = json.dumps(output_data.model_dump(), ensure_ascii=False)
        else:
            output_text = json.dumps(output_data, ensure_ascii=False)

        output_tokens = self.count_tokens(output_text)

        return input_tokens, output_tokens

    async def make_openai_request(
        self,
        # model_name: str,
        system_prompt: str,
        user_prompt: str,
        response_schema: BaseModel,
        schema_name: str,
        temperature: float = 0,
        seed: int = 42,
    ) -> Any:
        """
        Асинхронная функция для запросов в OpenAI API
        """
        self.logger.info(
            f"Выполнение запроса к OpenAI API (model={self.model_name}, temperature={temperature})"
        )
        try:
            response = await self.client.chat.completions.create(
                model=self.model_name,
                temperature=temperature,
                seed=seed,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
                response_format={
                    "type": "json_schema",
                    "json_schema": {
                        "name": schema_name,
                        "schema": response_schema.model_json_schema(),
                    },
                },
            )

            result = response_schema.model_validate_json(
                response.choices[0].message.content
            )

            # Подсчет токенов
            input_tokens, output_tokens = self.calculate_tokens(
                system_prompt, user_prompt, result
            )
            self.logger.info(
                f"Запрос к OpenAI API выполнен успешно. "
                f"Токены: вход={input_tokens:,}, выход={output_tokens:,}"
            )

            return result

        except Exception as e:
            self.logger.error(f"Ошибка при выполнении запроса к OpenAI API: {e}")
            raise Exception(f"Ошибка при выполнении запроса к OpenAI API: {e}")

    async def process_queries_batch(
        self,
        system_prompt: str,
        queries: list[dict[str, Any]],
        user_prompt_template: str,
        response_schema: BaseModel,
        schema_name: str,
        additional_data: Optional[dict[str, Any]] = None,
    ) -> tuple[list[dict[str, Any]], int, int]:
        """
        Асинхронная пакетная обработка запросов
        """
        processed_queries = []
        total_input_tokens = 0
        total_output_tokens = 0

        self.logger.info(f"Обрабатываем {len(queries)} запросов...")

        for i, query_data in enumerate(queries, 1):
            self.logger.info(
                f"Обработка запроса {i}/{len(queries)}: {query_data['queryid']}"
            )

            try:
                # Подготавливаем данные для шаблона
                template_data = {
                    "query_json": json.dumps(query_data, ensure_ascii=False)
                }
                if additional_data:
                    template_data.update(additional_data)

                # Формируем пользовательский промпт
                user_prompt = user_prompt_template.format(**template_data)

                # Делаем запрос к API
                result = await self.make_openai_request(
                    system_prompt=system_prompt,
                    user_prompt=user_prompt,
                    response_schema=response_schema,
                    schema_name=schema_name,
                )

                # Сохраняем результат с queryid из исходного запроса
                processed_result = {
                    "queryid": query_data["queryid"],
                    "query": result.query if hasattr(result, "query") else result,
                }
                processed_queries.append(processed_result)

                # Считаем токены
                input_tokens, output_tokens = self.calculate_tokens(
                    system_prompt, user_prompt, result
                )
                total_input_tokens += input_tokens
                total_output_tokens += output_tokens

                self.logger.info(
                    f"Запрос {query_data['queryid']} успешно обработан. "
                    f"Токены: вход={input_tokens:,}, выход={output_tokens:,}"
                )

            except Exception as e:
                self.logger.error(
                    f"ОШИБКА при обработке запроса {query_data['queryid']}: {e}",
                    exc_info=True,
                )
                # Добавляем запрос с ошибкой для отладки
                processed_queries.append(
                    {
                        "queryid": query_data["queryid"],
                        "query": f"-- ОШИБКА ОБРАБОТКИ: {e}\n{query_data.get('query', '')}",
                        "error": str(e),
                    }
                )

        self.logger.info(
            f"Пакетная обработка завершена. "
            f"Всего токенов: вход={total_input_tokens:,}, выход={total_output_tokens:,}"
        )

        return processed_queries, total_input_tokens, total_output_tokens

    async def create_new_ddl(self, body: dict[Any], db_config) -> DDLGenerationOutput:
        input_ddl_json = json.dumps(body.get("ddl", []), ensure_ascii=False)
        queries_json = json.dumps(body.get("queries", []), ensure_ascii=False)

        tables = []
        for ddl in body.get("ddl"):
            tables.append(ddl["statement"].split()[2])

        counts = get_counts(body.get("url"), tables)
        self.logger.info("Got counts from all tables")

        system_prompt = DDL_PROMPT
        user_prompt = INPUT_DDL_PROMPT.format(
            catalog=db_config["catalog"],
            source_schema=db_config["source_schema"],
            new_schema=db_config["new_schema"],
            input_ddl_json=input_ddl_json,
            queries_json=queries_json,
            stats=counts,
        )

        # Выполняем запрос к OpenAI API
        ddl_out = await self.make_openai_request(
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            response_schema=DDLGenerationOutput,
            schema_name="ddl_generation_output",
        )

        return ddl_out

    async def create_migrations(
        self, ddl_output: DDLGenerationOutput, body: dict[Any], db_config
    ) -> MigrationOutput:
        """
        Создание миграций на основе нового DDL
        """
        input_ddl_json = json.dumps(body.get("ddl", []), ensure_ascii=False)
        new_ddl_json = json.dumps(ddl_output.ddl, ensure_ascii=False)

        system_prompt = MIGRATION_PROMPT
        user_prompt = INPUT_MIGRATION_PROMPT.format(
            catalog=db_config["catalog"],
            source_schema=db_config["source_schema"],
            new_schema=db_config["new_schema"],
            input_ddl_json=input_ddl_json,
            new_ddl_json=new_ddl_json,
        )

        # Выполняем запрос к OpenAI API
        migration_out = await self.make_openai_request(
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            response_schema=MigrationOutput,
            schema_name="migration_output",
        )

        return migration_out

    async def create_queries(
        self, ddl_output: DDLGenerationOutput, body: dict[Any], db_config
    ):
        queries = body.get("queries", [])
        new_ddl_json = json.dumps(ddl_output.ddl, ensure_ascii=False)

        system_prompt = SQL_PROMPT.format(
            catalog=db_config["catalog"], new_schema=db_config["new_schema"]
        )

        # Обрабатываем все запросы с помощью утилиты
        (
            rewritten_queries,
            total_input_tokens,
            total_output_tokens,
        ) = await self.process_queries_batch(
            system_prompt=system_prompt,
            queries=queries,
            user_prompt_template=INPUT_SQL_PROMPT,
            response_schema=RewrittenQuery,
            schema_name="rewritten_query",
            additional_data={
                "catalog": db_config["catalog"],
                "new_schema": db_config["new_schema"],
                "new_ddl_json": new_ddl_json,
            },
        )

        return rewritten_queries

    async def refactor_db_schema(self, body: dict[Any]):
        """
        Основная функция анализа БД с использованием LLM
        """

        db_config = self.get_database_config_from_data(body)

        self.logger.info("Генерируем новые DDL запросы")
        ddl = await self.create_new_ddl(body, db_config)

        self.logger.info("Генерируем запросы для миграции")
        migrations = await self.create_migrations(ddl, body, db_config)

        self.logger.info("Генерируем запросы для новой схемы данных")
        queries = await self.create_queries(ddl, body, db_config)

        return {
            "ddl": ddl.model_dump(),
            "migrations": migrations.model_dump(),
            "queries": queries,
        }


# async def main():
#     processor = LLMProcessor()
#     with open("./data/flights.json", "r", encoding="utf-8") as f:
#         task_data = json.load(f)
#     result = await processor.refactor_db_schema(task_data)
#     result_str = json.dumps(result, ensure_ascii=False)
#     print(result_str)


# if __name__ == "__main__":
#     asyncio.run(main())


async def process_task(task_data: dict, db_pool: ThreadedConnectionPool):
    """
    Обработка одной задачи
    """
    task_id = task_data["task_id"]

    try:
        # Обновляем статус на processing
        conn = db_pool.getconn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE tasks 
                    SET status = %s, updated_at = NOW()
                    WHERE task_id = %s
                """,
                    ("processing", task_id),
                )
                conn.commit()
        finally:
            db_pool.putconn(conn)

        # Обработка через LLM
        processor = LLMProcessor()
        result = await processor.refactor_db_schema(task_data["body"])

        # Сохраняем результат
        conn = db_pool.getconn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE tasks 
                    SET status = %s, result_data = %s, progress = %s, updated_at = NOW()
                    WHERE task_id = %s
                """,
                    ("completed", json.dumps(result, ensure_ascii=False), 100, task_id),
                )
                conn.commit()
        finally:
            db_pool.putconn(conn)

        worker_logger.info(f"Task {task_id} completed successfully")

    except Exception as e:
        # Сохраняем ошибку
        conn = db_pool.getconn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE tasks 
                    SET status = %s, error_message = %s, updated_at = NOW()
                    WHERE task_id = %s
                """,
                    ("failed", str(e), task_id),
                )
                conn.commit()
        finally:
            db_pool.putconn(conn)

        worker_logger.info(f"Task {task_id} failed: {str(e)}")


async def kafka_worker():
    """
    Основной воркер для обработки сообщений из Kafka
    """
    worker_logger.info("Инициализация воркера...")

    # Kafka consumer (асинхронный) с исправленными настройками
    consumer = AIOKafkaConsumer(
        Config.KAFKA_TOPIC,
        bootstrap_servers=Config.KAFKA_BOOTSTRAP_SERVERS,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        group_id="llm_workers",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        consumer_timeout_ms=2000,
    )

    worker_logger.info(
        f"Подключение к Kafka (bootstrap_servers={Config.KAFKA_BOOTSTRAP_SERVERS})..."
    )
    worker_logger.info(f"Подписка на топик: {Config.KAFKA_TOPIC}, группа: llm_workers")

    max_retries = 10
    retry_delay = 10

    for attempt in range(1, max_retries + 1):
        try:
            await consumer.start()
            worker_logger.info("✅ Kafka Consumer успешно подключен и запущен")

            # Проверяем назначенные партиции
            await asyncio.sleep(2)  # Даем время на присоединение к группе
            partitions = consumer.assignment()
            worker_logger.info(f"Назначенные партиции: {partitions}")

            break
        except Exception as e:
            worker_logger.error(
                f"❌ Попытка {attempt}/{max_retries} подключения к Kafka не удалась: {e}"
            )
            if attempt < max_retries:
                worker_logger.info(f"Повторная попытка через {retry_delay} секунд...")
                await asyncio.sleep(retry_delay)
            else:
                worker_logger.error(
                    "Не удалось подключиться к Kafka после всех попыток"
                )
                raise

    worker_logger.info(
        f"🚀 Воркер запущен, ожидание задач из топика '{Config.KAFKA_TOPIC}'..."
    )

    # Подключение к БД
    try:
        db_pool = ThreadedConnectionPool(
            Config.DB_POOL_MIN,
            Config.DB_POOL_MAX,
            host=Config.POSTGRES_HOST,
            port=Config.POSTGRES_PORT,
            database=Config.POSTGRES_DB,
            user=Config.POSTGRES_USER,
            password=Config.POSTGRES_PASSWORD,
        )
        worker_logger.info("Пул соединений с БД создан успешно")
    except Exception as e:
        worker_logger.error(f"Ошибка при создании пула соединений с БД: {e}")
        raise

    try:
        message_count = 0
        async for message in consumer:
            message_count += 1
            task_data = message.value
            worker_logger.info(
                f"📥 Получена задача #{message_count}: {task_data['task_id']} "
                f"(partition={message.partition}, offset={message.offset})"
            )
            await process_task(task_data, db_pool)
    except KeyboardInterrupt:
        worker_logger.info("Получен сигнал остановки (Ctrl+C)")
    except Exception as e:
        worker_logger.error(f"Критическая ошибка в воркере: {e}", exc_info=True)
    finally:
        worker_logger.info("Остановка воркера...")
        await consumer.stop()
        db_pool.closeall()
        worker_logger.info("Воркер остановлен")


if __name__ == "__main__":
    asyncio.run(kafka_worker())
