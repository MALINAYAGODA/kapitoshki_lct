# ============ ТАСКА СОЗДАНИЯ НОВЫХ ICEBERG ТАБЛИЦ ПО ВЫХОДНОМУ JSON ============

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
JSON_PATH = "response/ddl_output.json"
HIVE_CATALOG = "hive"
ICEBERG_CATALOG = "iceberg"
S3_LOCATION = "s3a://warehouse/flights_iceberg/"
# =================================

def transform_table_structure_with_new_ddl():
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

    for i, sql_strip in enumerate(ddl_statements, start=1):
        sql_strip = sql_strip.strip().rstrip().rstrip(';')

        if sql_strip.lower().startswith("create schema"):
            print(f"➡️ Skipping schema creation (statement {i})")
            continue

        match = re.search(r"CREATE TABLE\s+([^\s(]+)", sql_strip, re.IGNORECASE)
        if not match:
            print(f"Could not parse table name in statement {i}")
            continue

        original_table = match.group(1)  # напр. flights.optimized.flights
        base_table = original_table.split(".")[-1]  # flightscl
        iceberg_table = f"{ICEBERG_CATALOG}.{DEFAULT_SCHEMA}.{base_table}_v2"

        sql_fixed = re.sub(r"CREATE TABLE\s+[^\s(]+",
                           f"CREATE TABLE IF NOT EXISTS {iceberg_table}",
                           sql_strip,
                           flags=re.IGNORECASE)
        print(sql_fixed)
        if "WITH (" in sql_fixed.upper():
                sql_fixed = re.sub(r"WITH\s*\((.*?)\)",
                                   rf"WITH (\1, location = '{S3_LOCATION}')",
                                   sql_fixed,
                                   flags=re.IGNORECASE | re.DOTALL)

        print(f"\nExecuting Iceberg table DDL for {iceberg_table}...")
        try:
            cursor.execute(sql_fixed)
            print("Iceberg table created")
        except Exception as e:
            print(f"Failed to create {iceberg_table}: {e}")

    conn.close()
    print("\nAll Iceberg v2 tables processed successfully.")

if __name__ == "__main__":
    transform_table_structure_with_new_ddl()
