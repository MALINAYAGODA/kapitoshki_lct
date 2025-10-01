# ============ ТАСКА СОЗДАНИЯ ТАБЛИЦЫ HIVE & ICEBERG / КОПИИ ИЗ TRINO-VK ============

import os
import trino
import json

os.environ["NO_PROXY"] = "127.0.0.1,localhost"

# ============ КОНФИГ ============
LOCAL_HOST = "localhost"
LOCAL_PORT = 8080
TARGET_CATALOG = "iceberg"
TARGET_SCHEMA = "lct"
DEFAULT_SCHEMA = "default"
JSON_PATH = "data/flights.json"
HIVE_CATALOG = "hive"
ICEBERG_CATALOG = "iceberg"
# =================================

def run_ddl_from_json():
    # Подключение к локальному Trino
    conn = trino.dbapi.connect(
        host=LOCAL_HOST,
        port=LOCAL_PORT,
        user="trino",
        catalog=TARGET_CATALOG,
        schema=TARGET_SCHEMA,
        verify=False,
    )
    print("✓ Connected")
    cursor = conn.cursor()

    # Чтение JSON
    with open(JSON_PATH, "r", encoding="utf-8") as f:
        ddl_json = json.load(f)

    ddl_statements = ddl_json.get("ddl", [])

    for i, ddl_obj in enumerate(ddl_statements, start=1):
        sql = ddl_obj["statement"]
        sql_strip = sql.strip().rstrip().rstrip(';')

        if sql_strip.lower().startswith("create schema"):
            print(f"Skipping schema creation (statement {i})")
            continue

        # ---  Создаём Hive таблицу ---
        sql_hive = sql_strip.replace(", format_version = 2", "").replace(", format_version=2", "")
        sql_hive = sql_hive.replace("flights.public", f"{HIVE_CATALOG}.{DEFAULT_SCHEMA}")
        sql_hive = sql_hive.replace("CREATE TABLE", "CREATE TABLE IF NOT EXISTS")
        if "WITH (" in sql_hive.upper():
            sql_hive = sql_hive.rstrip(")").rstrip()
            if sql_hive.endswith(")"):  # защита от двойного удаления
                sql_hive = sql_hive[:-1]
            sql_hive += ", external_location = 's3a://warehouse/imported_data/' )"
        else:
            sql_hive += "\nWITH ( external_location = 's3a://warehouse/imported_data/' )"
        print(f"\nExecuting Hive table DDL (statement {i})...")
        print('-------- start --------')
        print(sql_hive)
        cursor.execute(sql_hive)
        print('-------- end --------')
        print("Hive table created")

        # ---  Создаём Iceberg таблицу ---
        table_name = sql_strip.split(" ")[2].split('.')[-1]
        iceberg_table_name = f"{table_name}_iceberg"

        sql_iceberg = f"""
        CREATE TABLE IF NOT EXISTS {ICEBERG_CATALOG}.{DEFAULT_SCHEMA}.{iceberg_table_name}
        WITH (
            format = 'PARQUET',
            location = 's3a://warehouse/{iceberg_table_name}/'
        )
        AS SELECT * FROM {HIVE_CATALOG}.{DEFAULT_SCHEMA}.{table_name}
        """
        print(sql_iceberg)
        print(f"Executing Iceberg table DDL for {iceberg_table_name}...")
        cursor.execute(sql_iceberg)
        print("Iceberg table created")

    conn.close()
    print("\nAll tables created successfully.")


if __name__ == "__main__":
    run_ddl_from_json()
