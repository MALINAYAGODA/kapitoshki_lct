#!/usr/bin/env python3
"""
Простой скрипт для копирования 10% данных из удаленного Trino в локальный
"""

import os
import time

from trino.auth import BasicAuthentication

import trino

os.environ["NO_PROXY"] = "127.0.0.1,localhost"


def wait_for_trino(conn, name, max_retries=10):
    """Ждём готовности Trino"""
    print(f"   Checking {name} availability...")
    cursor = conn.cursor()

    for attempt in range(max_retries):
        try:
            cursor.execute("SELECT 1")
            cursor.fetchall()
            print(f"   ✓ {name} is ready")
            cursor.close()
            return True
        except Exception as e:
            print(f"   Got an error: {e}")
            if attempt < max_retries - 1:
                print(
                    f"   ⏳ Waiting for {name}... (attempt {attempt + 1}/{max_retries})"
                )
                time.sleep(3)
            else:
                print(f"   ❌ {name} not available: {e}")
                cursor.close()
                return False
    return False


# ============= КОНФИГУРАЦИЯ =============
# Удаленный Trino (источник)
REMOTE_HOST = "trino.czxqx2r9.data.bizmrg.com"
REMOTE_PORT = 443
REMOTE_USER = "hackuser"
REMOTE_PASSWORD = "dovq(ozaq8ngt)oS"
SOURCE_CATALOG = "flights"
SOURCE_SCHEMA = "public"  # ИЗМЕНИТЕ НА ВАШУ СХЕМУ

# Локальный Trino (назначение)
LOCAL_HOST = "localhost"
LOCAL_PORT = 8080
TARGET_CATALOG = "iceberg"
TARGET_SCHEMA = "lct"  # ИЗМЕНИТЕ НА ВАШУ СХЕМУ

# Название таблицы (одинаковое в обоих кластерах)
TABLE_NAME = "flights"

# Процент для копирования
SAMPLE_PERCENT = 0.01
# ========================================


def main():
    print("=" * 60)
    print(f"Trino Sample Copy Tool ({SAMPLE_PERCENT}%)")
    print("=" * 60)

    # Подключение к удаленному Trino
    print("\n📡 Connecting to remote Trino...")
    print(f"   Host: {REMOTE_HOST}:{REMOTE_PORT}")
    remote_conn = trino.dbapi.connect(
        host=REMOTE_HOST,
        port=REMOTE_PORT,
        user=REMOTE_USER,
        http_scheme="https",
        auth=BasicAuthentication(REMOTE_USER, REMOTE_PASSWORD),
        catalog=SOURCE_CATALOG,
        schema=SOURCE_SCHEMA,
    )
    print("   ✓ Connected")

    if not wait_for_trino(remote_conn, "Remote Trino"):
        return

    # Подключение к локальному Trino
    print("\n📡 Connecting to local Trino...")
    print(f"   Host: {LOCAL_HOST}:{LOCAL_PORT}")
    local_conn = trino.dbapi.connect(
        host=LOCAL_HOST,
        port=LOCAL_PORT,
        user="trino",
        catalog=TARGET_CATALOG,
        schema=TARGET_SCHEMA,
        verify=False,
    )
    print("   ✓ Connected")

    if not wait_for_trino(local_conn, "Local Trino"):
        return

    remote_cur = remote_conn.cursor()
    local_cur = local_conn.cursor()

    try:
        # Получаем информацию о таблице
        print(f"\n📊 Analyzing table: {SOURCE_CATALOG}.{SOURCE_SCHEMA}.{TABLE_NAME}")

        remote_cur.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}")
        total_rows = remote_cur.fetchone()[0]

        remote_cur.execute(f"DESCRIBE {TABLE_NAME}")
        columns = [row[0] for row in remote_cur.fetchall()]

        rows_to_copy = int(total_rows * SAMPLE_PERCENT / 100)

        print(f"\n📋 Table Info:")
        print(f"   Total rows: {total_rows:,}")
        print(
            f"   Columns: {len(columns)} ({', '.join(columns[:5])}{'...' if len(columns) > 5 else ''})"
        )
        print(f"   Sample size: {SAMPLE_PERCENT}% = {rows_to_copy:,} rows")

        # Копирование
        print(f"\n🚀 Starting copy...")
        print(f"   Source: {SOURCE_CATALOG}.{SOURCE_SCHEMA}.{TABLE_NAME}")
        print(f"   Target: {TARGET_CATALOG}.{TARGET_SCHEMA}.{TABLE_NAME}")
        print()

        start_time = time.time()

        # Читаем данные с TABLESAMPLE
        query = f"""
            SELECT {", ".join(columns)}
            FROM {TABLE_NAME}
            TABLESAMPLE BERNOULLI ({SAMPLE_PERCENT})
        """

        print("   📥 Fetching data from remote...")
        fetch_start = time.time()
        remote_cur.execute(query)
        rows = remote_cur.fetchall()
        fetch_time = time.time() - fetch_start

        actual_rows = len(rows)
        print(f"   ✓ Fetched {actual_rows:,} rows in {fetch_time:.1f}s")

        # Записываем данные
        print("   📤 Inserting to local...")
        insert_start = time.time()

        placeholders = ", ".join(["?" for _ in columns])
        insert_query = f"INSERT INTO {TABLE_NAME} VALUES ({placeholders})"

        for i, row in enumerate(rows, 1):
            local_cur.execute(insert_query, row)
            if i % 100 == 0:
                print(
                    f"      Progress: {i:,}/{actual_rows:,} ({i / actual_rows * 100:.1f}%)"
                )

        insert_time = time.time() - insert_start
        total_time = time.time() - start_time

        print(f"   ✓ Inserted {actual_rows:,} rows in {insert_time:.1f}s")

        # Итоговая статистика
        print(f"\n✅ COMPLETED")
        print("=" * 60)
        print(f"📊 Statistics:")
        print(
            f"   Rows copied: {actual_rows:,} / {total_rows:,} ({actual_rows / total_rows * 100:.1f}%)"
        )
        print(
            f"   Fetch time: {fetch_time:.1f}s ({actual_rows / fetch_time:.0f} rows/s)"
        )
        print(
            f"   Insert time: {insert_time:.1f}s ({actual_rows / insert_time:.0f} rows/s)"
        )
        print(f"   Total time: {total_time:.1f}s")
        print(f"   Average speed: {actual_rows / total_time:.0f} rows/s")

        # Проверка
        print(f"\n🔍 Verifying target table...")
        local_cur.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}")
        target_count = local_cur.fetchone()[0]
        print(f"   Target table now has: {target_count:,} rows")

        print("=" * 60)

    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback

        traceback.print_exc()
    finally:
        remote_cur.close()
        local_cur.close()
        remote_conn.close()
        local_conn.close()


if __name__ == "__main__":
    main()
