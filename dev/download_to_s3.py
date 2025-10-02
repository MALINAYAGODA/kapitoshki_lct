import os
import time
from io import BytesIO

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from trino.auth import BasicAuthentication

import trino

os.environ["NO_PROXY"] = "127.0.0.1,localhost"

# ============ КОНФИГУРАЦИЯ ============
# Удаленный Trino
REMOTE_HOST = "trino.czxqx2r9.data.bizmrg.com"
REMOTE_PORT = 443
REMOTE_USER = "hackuser"
REMOTE_PASSWORD = "dovq(ozaq8ngt)oS"
REMOTE_SCHEMA = "public"
# TABLE_NAME = "flights"

# Локальный MinIO (S3)
S3_ENDPOINT = "http://localhost:9000"
S3_ACCESS_KEY = "minioadmin"
S3_SECRET_KEY = "minioadmin"
S3_BUCKET = "warehouse"
# S3_PREFIX = "imported_data"  # Папка в бакете

# Параметры выгрузки
SAMPLE_PERCENT = 0.1  # Процент строк для выгрузки (1-100)
BATCH_SIZE = 1000000  # Размер батча для чтения
# ======================================


def download_table_to_s3(table_name: str, catalog: str):
    """Скачивает данные из Trino и сохраняет в S3 в формате Parquet"""

    # Подключение к удаленному Trino
    print(f"Connecting to Trino at {REMOTE_HOST}...")
    conn = trino.dbapi.connect(
        host=REMOTE_HOST,
        port=REMOTE_PORT,
        user=REMOTE_USER,
        http_scheme="https",
        auth=BasicAuthentication(REMOTE_USER, REMOTE_PASSWORD),
        catalog=catalog,
        schema=REMOTE_SCHEMA,
    )

    # Подключение к MinIO
    s3_client = boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
    )

    # Запрос с семплированием
    if SAMPLE_PERCENT < 100:
        query = f"SELECT * FROM {table_name} TABLESAMPLE BERNOULLI ({SAMPLE_PERCENT})"
    else:
        query = f"SELECT * FROM {table_name}"

    print(f"Executing query: {query}")
    cursor = conn.cursor()
    cursor.execute(query)

    # Получаем схему
    columns = [desc[0] for desc in cursor.description]
    print(f"Columns: {', '.join(columns)}")

    # Читаем и записываем батчами
    batch_num = 0
    total_rows = 0
    start_time = time.time()

    print(f"\nStarting download (batch size: {BATCH_SIZE:,})...")
    print("-" * 60)

    while True:
        # Читаем батч
        rows = cursor.fetchmany(BATCH_SIZE)

        if not rows:
            break

        # Конвертируем в PyArrow Table
        data_dict = {col: [row[i] for row in rows] for i, col in enumerate(columns)}
        table = pa.table(data_dict)

        # Записываем в Parquet
        buffer = BytesIO()
        pq.write_table(table, buffer, compression="snappy")
        buffer.seek(0)

        # Загружаем в S3
        s3_key = f"hive/{table_name}/part-{batch_num:05d}.parquet"
        s3_client.put_object(Bucket=S3_BUCKET, Key=s3_key, Body=buffer.getvalue())

        # Статистика
        total_rows += len(rows)
        batch_num += 1
        elapsed = time.time() - start_time
        rate = total_rows / elapsed if elapsed > 0 else 0

        print(
            f"Batch {batch_num}: {len(rows):,} rows | Total: {total_rows:,} | Rate: {rate:.0f} rows/sec"
        )

    # Итоговая статистика
    elapsed = time.time() - start_time
    avg_rate = total_rows / elapsed if elapsed > 0 else 0

    print("-" * 60)
    print("\n✅ Download completed!")
    print(f"Total rows: {total_rows:,}")
    print(f"Total batches: {batch_num}")
    print(f"Time: {elapsed:.1f} seconds")
    print(f"Average rate: {avg_rate:.0f} rows/sec")
    print(f"\nData location: s3://{S3_BUCKET}/hive/{table_name}/")

    conn.close()


if __name__ == "__main__":
    download_table_to_s3("flights", "flights")
