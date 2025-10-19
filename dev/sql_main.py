from src.prompts import SQL_PROMPT, INPUT_SQL_PROMPT
from src.models import RewrittenQuery
from src.utils import (
    load_json_file,
    save_json_file,
    process_queries_batch,
    print_processing_summary,
    get_database_config_from_data
)
import json
import time

api_url = "http://213.181.111.2:57715/v1/chat/completions"
name_table = "data/flights.json"

start_time = time.time()
print("=== Переписывание SQL запросов ===")

input_data = load_json_file(name_table)
ddl_output = load_json_file("response/ddl_output.json")

# Автоматически извлекаем конфигурацию базы данных
db_config = get_database_config_from_data(input_data)

queries = input_data.get("queries", [])
new_ddl_json = json.dumps(ddl_output, ensure_ascii=False)

system_prompt = SQL_PROMPT.format(catalog=db_config["catalog"], new_schema=db_config["new_schema"])

# Обрабатываем все запросы с помощью утилиты
rewritten_queries, total_input_tokens, total_output_tokens = process_queries_batch(
    system_prompt=system_prompt,
    queries=queries,
    user_prompt_template=INPUT_SQL_PROMPT,
    response_schema=RewrittenQuery,
    additional_data={
        "catalog": db_config["catalog"],
        "new_schema": db_config["new_schema"],
        "new_ddl_json": new_ddl_json
    },
    api_url=api_url
)

output_data = {
    "queries": rewritten_queries
}

save_json_file(output_data, "response/sql_rewritten_output.json")

print_processing_summary(
    total_queries=len(rewritten_queries),
    total_input_tokens=total_input_tokens,
    total_output_tokens=total_output_tokens,
    output_filename="sql_rewritten_output.json"
)

# Выводим время выполнения
elapsed_time = time.time() - start_time
print(f"\n⏱️  Время выполнения: {elapsed_time:.2f} секунд")
