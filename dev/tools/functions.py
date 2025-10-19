import json
import sys
import os
from typing import Dict, Any, List, Tuple, Optional

# Добавляем родительскую директорию в путь для импортов
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.models import DDLGenerationOutput, MigrationOutput, RewrittenQuery
from src.prompts import DDL_PROMPT, INPUT_DDL_PROMPT, MIGRATION_PROMPT, INPUT_MIGRATION_PROMPT, SQL_PROMPT, INPUT_SQL_PROMPT
from src.utils import get_database_config_from_data, make_llm_request, calculate_and_print_tokens, process_queries_batch

# Опциональный импорт get_counts (может отсутствовать trino)
try:
    from src.get_counts import get_counts
except ImportError:
    def get_counts(url, tables):
        return "Статистика недоступна (trino не установлен)"


def generate_ddl(input_data: Dict[str, Any], api_url: str = "http://213.181.111.2:57715/v1/chat/completions", new_schema: str = "optimized") -> Tuple[DDLGenerationOutput, Dict[str, str], Tuple[int, int]]:
    db_config = get_database_config_from_data(input_data, new_schema)
    input_ddl_json = json.dumps(input_data.get("ddl", []), ensure_ascii=False)
    queries_json = json.dumps(input_data.get("queries", []), ensure_ascii=False)
    
    stats = ""
    if input_data.get("url"):
        try:
            tables = [ddl.get("statement", "").split()[2].split('(')[0].strip().split('.')[-1] 
                     for ddl in input_data.get("ddl", []) if len(ddl.get("statement", "").split()) > 2]
            stats = get_counts(input_data.get("url"), tables)
        except:
            stats = "Статистика недоступна"
    
    user_prompt = INPUT_DDL_PROMPT.format(
        catalog=db_config["catalog"], source_schema=db_config["source_schema"], 
        new_schema=db_config["new_schema"], input_ddl_json=input_ddl_json, 
        queries_json=queries_json, stats=stats
    )
    
    result = make_llm_request(DDL_PROMPT, user_prompt, DDLGenerationOutput, api_url)
    tokens = calculate_and_print_tokens(DDL_PROMPT, user_prompt, result)
    return result, db_config, tokens


def generate_migration(input_data: Dict[str, Any], ddl_output: DDLGenerationOutput, 
                      api_url: str = "http://213.181.111.2:57715/v1/chat/completions", 
                      db_config: Optional[Dict[str, str]] = None) -> Tuple[MigrationOutput, Tuple[int, int]]:
    if not db_config:
        db_config = get_database_config_from_data(input_data)
    
    input_ddl_json = json.dumps(input_data.get("ddl", []), ensure_ascii=False)
    new_ddl_json = json.dumps(ddl_output.model_dump(), ensure_ascii=False)
    
    user_prompt = INPUT_MIGRATION_PROMPT.format(
        catalog=db_config["catalog"], source_schema=db_config["source_schema"],
        new_schema=db_config["new_schema"], input_ddl_json=input_ddl_json, new_ddl_json=new_ddl_json
    )
    
    result = make_llm_request(MIGRATION_PROMPT, user_prompt, MigrationOutput, api_url)
    tokens = calculate_and_print_tokens(MIGRATION_PROMPT, user_prompt, result)
    return result, tokens


def generate_sql_queries(queries: List[Dict[str, Any]], ddl_output: DDLGenerationOutput, 
                        db_config: Dict[str, str], api_url: str = "http://213.181.111.2:57715/v1/chat/completions") -> Tuple[List[Dict[str, Any]], int, int]:
    new_ddl_json = json.dumps(ddl_output.model_dump(), ensure_ascii=False)
    system_prompt = SQL_PROMPT.format(catalog=db_config["catalog"], new_schema=db_config["new_schema"])
    
    return process_queries_batch(
        system_prompt, queries, INPUT_SQL_PROMPT, RewrittenQuery,
        {"catalog": db_config["catalog"], "new_schema": db_config["new_schema"], "new_ddl_json": new_ddl_json},
        api_url
    )


def optimize_database_complete(input_data: Dict[str, Any], api_url: str = "http://213.181.111.2:57715/v1/chat/completions", 
                              new_schema: str = "optimized") -> Dict[str, Any]:
    ddl_result, db_config, ddl_tokens = generate_ddl(input_data, api_url, new_schema)
    migration_result, migration_tokens = generate_migration(input_data, ddl_result, api_url, db_config)
    
    queries = input_data.get("queries", [])
    if queries:
        rewritten_queries, sql_input_tokens, sql_output_tokens = generate_sql_queries(queries, ddl_result, db_config, api_url)
    else:
        rewritten_queries, sql_input_tokens, sql_output_tokens = [], 0, 0
    
    return {
        "ddl_result": ddl_result.model_dump(),
        "migration_result": migration_result.model_dump(), 
        "rewritten_queries": rewritten_queries,
        "db_config": db_config,
        "token_usage": {
            "total_input_tokens": ddl_tokens[0] + migration_tokens[0] + sql_input_tokens,
            "total_output_tokens": ddl_tokens[1] + migration_tokens[1] + sql_output_tokens
        }
    }


# Примеры использования функций с данными flights.json
if __name__ == "__main__":
    from src.utils import load_json_file, save_json_file
    import time

    # Загрузка данных
    input_data = load_json_file("data/flights.json")
    api_url = "http://213.181.111.2:57715/v1/chat/completions"
    
    print("=== Пример 1: Генерация DDL ===")
    start = time.time()
    ddl_result, db_config, ddl_tokens = generate_ddl(
        input_data=input_data,
        api_url=api_url,
        new_schema="flights_optimized"
    )
    save_json_file(ddl_result.model_dump(), "response/flights_ddl_output.json")
    print(f"DDL сгенерирована. Токены: {ddl_tokens}")
    print(time.time() - start)
    
    print("\n=== Пример 2: Генерация миграций ===")
    start = time.time()
    migration_result, migration_tokens = generate_migration(
        input_data=input_data,
        ddl_output=ddl_result,
        api_url=api_url,
        db_config=db_config
    )
    save_json_file(migration_result.model_dump(), "response/flights_migration_output.json")
    print(f"Миграции сгенерированы. Токены: {migration_tokens}")
    print(time.time() - start)
    
    print("\n=== Пример 3: Переписывание SQL запросов ===")
    start = time.time()
    queries = input_data.get("queries", [])
    rewritten_queries, sql_input_tokens, sql_output_tokens = generate_sql_queries(
        queries=queries,
        ddl_output=ddl_result,
        db_config=db_config,
        api_url=api_url
    )
    save_json_file({"queries": rewritten_queries}, "response/flights_sql_rewritten_output.json")
    print(f"SQL запросы переписаны. Токены: вход={sql_input_tokens}, выход={sql_output_tokens}")
    print(time.time() - start)
    
    # print("\n=== Пример 4: Полная оптимизация ===")
    # complete_result = optimize_database_complete(
    #     input_data=input_data,
    #     api_url=api_url,
    #     new_schema="flights_complete_optimized"
    # )
    # save_json_file(complete_result, "response/flights_complete_optimization.json")
    # print("Полная оптимизация завершена!")
    # print(f"Общие токены: вход={complete_result['token_usage']['total_input_tokens']}, "
    #       f"выход={complete_result['token_usage']['total_output_tokens']}")
    # print(f"Переписано запросов: {len(complete_result['rewritten_queries'])}")
