# ============ ТАСКА СОЗДАНИЯ ТАБЛИЦЫ HIVE & ICEBERG / КОПИИ ИЗ TRINO-VK ============

import json
import os
import re

import trino

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


def run_ddl_from_json(ddl: str):
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

    sql_strip = re.split(r"\bwith\b", ddl.strip(), flags=re.IGNORECASE)[0]
    table_name = (
        re.search(
            r"\bcreate\s+table\s+(if\s+not\s+exists\s+)?([a-zA-Z0-9_\.]+)",
            ddl,
            flags=re.IGNORECASE,
        )
        .group(2)
        .split(".")[-1]
    )

    if sql_strip.lower().startswith("create schema"):
        print(f"Skipping schema creation (statement {ddl})")
        return

    # ---  Создаём Hive таблицу ---
    sql_hive = (
        sql_strip
        + f"""
    WITH (
        format = 'PARQUET',
        external_location = 's3a://warehouse/hive/{table_name}'
    )
    """
    )
    sql_hive = sql_hive.replace("flights.public", f"{HIVE_CATALOG}.{DEFAULT_SCHEMA}")
    sql_hive = sql_hive.replace("CREATE TABLE", "CREATE TABLE IF NOT EXISTS")
    print(f"\nExecuting Hive table DDL (table {table_name})...")
    print("-------- start --------")
    print(sql_hive)
    cursor.execute(sql_hive)
    print("-------- end --------")
    print("Hive table created\n")

    # ---  Создаём Iceberg таблицу ---
    table_name = sql_strip.split(" ")[2].split(".")[-1]
    iceberg_table_name = f"{table_name}_iceberg"

    sql_iceberg = f"""
    CREATE TABLE IF NOT EXISTS {ICEBERG_CATALOG}.{DEFAULT_SCHEMA}.{iceberg_table_name}
    WITH (
        format = 'PARQUET',
        location = 's3a://warehouse/iceberg/{iceberg_table_name}/'
    )
    AS SELECT * FROM {HIVE_CATALOG}.{DEFAULT_SCHEMA}.{table_name}
    """
    print(f"Executing Iceberg table DDL for {iceberg_table_name}...")
    print("-------- start --------")
    print(sql_iceberg)
    cursor.execute(sql_iceberg)
    print("-------- end --------")
    print("Iceberg table created")

    conn.close()
    print("\nAll tables created successfully.")


if __name__ == "__main__":
    ddl = "CREATE TABLE flights.public.flights ( flightdate date, airline varchar, origin varchar, dest varchar, cancelled boolean, diverted boolean, crsdeptime integer, deptime double, depdelayminutes double, depdelay double, arrtime double, arrdelayminutes double, airtime double, crselapsedtime double, actualelapsedtime double, distance double, year integer, quarter integer, month integer, dayofmonth integer, dayofweek integer, marketing_airline_network varchar, operated_or_branded_code_share_partners varchar, dot_id_marketing_airline integer, iata_code_marketing_airline varchar, flight_number_marketing_airline integer, operating_airline varchar, dot_id_operating_airline integer, iata_code_operating_airline varchar, tail_number varchar, flight_number_operating_airline integer, originairportid integer, originairportseqid integer, origincitymarketid integer, origincityname varchar, originstate varchar, originstatefips integer, originstatename varchar, originwac integer, destairportid integer, destairportseqid integer, destcitymarketid integer, destcityname varchar, deststate varchar, deststatefips integer, deststatename varchar, destwac integer, depdel15 double, departuredelaygroups double, deptimeblk varchar, taxiout double, wheelsoff double, wheelson double, taxiin double, crsarrtime integer, arrdelay double, arrdel15 double, arrivaldelaygroups double, arrtimeblk varchar, distancegroup integer, divairportlandings double ) WITH ( format = 'PARQUET', format_version = 2 );"
    run_ddl_from_json(ddl)
