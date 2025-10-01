# ============ ТАСКА ДЛЯ ЗАПУСКА СКРИПТОВ МИГРАЦИИ ============

import os
import trino
import json
import re

os.environ["NO_PROXY"] = "127.0.0.1,localhost"

# ============ КОНФИГ ============
LOCAL_HOST = "localhost"
LOCAL_PORT = 8080
TARGET_CATALOG = "iceberg"
TARGET_SCHEMA = "lct"
DEFAULT_SCHEMA = "default"
JSON_PATH = "response/migration_output.json"
HIVE_CATALOG = "hive"
ICEBERG_CATALOG = "iceberg"
# =================================

def run_migrations_from_json():
    # Подключение к Trino
    conn = trino.dbapi.connect(
        host=LOCAL_HOST,
        port=LOCAL_PORT,
        user="trino",
        catalog=ICEBERG_CATALOG,
        schema=TARGET_SCHEMA,
        http_scheme="http"
    )
    cursor = conn.cursor()
    print("✓ Connected to Trino")

    # Загружаем JSON
    with open(JSON_PATH, "r", encoding="utf-8") as f:
        migration_json = json.load(f)
        print('got json!')

    migrations = migration_json.get("migrations", [])

    for i, sql in enumerate(migrations, start=1):
        sql_strip = sql.strip().rstrip(";")

        # --- Обработка INSERT INTO ---
        match_insert = re.search(r"INSERT INTO\s+([^\s(]+)", sql_strip, re.IGNORECASE)
        if not match_insert:
            print(f"Could not parse target table in migration {i}")
            continue

        orig_target = match_insert.group(1)  # flights.optimized.flights
        base_table = orig_target.split(".")[-1]  # flights
        new_target = f"{ICEBERG_CATALOG}.{DEFAULT_SCHEMA}.{base_table}_v2"

        sql_strip = re.sub(r"INSERT INTO\s+[^\s(]+",
                           f"INSERT INTO {new_target}",
                           sql_strip,
                           flags=re.IGNORECASE)

        # --- Обработка FROM ---
        match_from = re.search(r"FROM\s+([^\s;]+)", sql_strip, re.IGNORECASE)
        if not match_from:
            print(f"Could not parse source table in migration {i}")
            continue

        orig_source = match_from.group(1) 
        base_source = orig_source.split(".")[-1] 
        new_source = f"{ICEBERG_CATALOG}.{DEFAULT_SCHEMA}.{base_source}"
        print(new_source)
        sql_strip = re.sub(r"FROM\s+[^\s;]+",
                           f"FROM {new_source}",
                           sql_strip,
                           flags=re.IGNORECASE)
        
        print(sql_strip)

        # --- Лог ---
        print(f"\nMigration {i}:")
        print(f"   Source: {orig_source} → {new_source}")
        print(f"   Target: {orig_target} → {new_target}")

        # --- Выполнение ---
        try:
            cursor.execute(sql_strip)
            print("Migration executed")
        except Exception as e:
            print(f"Migration {i} failed: {e}")

    conn.close()
    print("\nAll migrations processed successfully.")

if __name__ == "__main__":
    run_migrations_from_json()