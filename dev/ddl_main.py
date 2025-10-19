import json
import time

from src.get_counts import get_counts
from src.prompts import DDL_PROMPT, INPUT_DDL_PROMPT
from src.utils import (
    calculate_and_print_tokens,
    get_database_config_from_data,
    load_json_file,
    make_llm_request,
    save_json_file,
)
from src.models import DDLGenerationOutput

api_url = "http://213.181.111.2:57715/v1/chat/completions"
name_table = "data/flights.json"

start_time = time.time()

input_data = load_json_file(name_table)

# Автоматически извлекаем конфигурацию базы данных
db_config = get_database_config_from_data(input_data)

input_ddl_json = json.dumps(input_data.get("ddl", []), ensure_ascii=False)
queries_json = json.dumps(input_data.get("queries", []), ensure_ascii=False)

tables = []
for ddl in input_data.get("ddl"):
    tables.append(ddl["statement"].split()[2])

counts = get_counts(input_data.get("url"), tables)
print("Got counts from all tables")

system_prompt = DDL_PROMPT
user_prompt = INPUT_DDL_PROMPT.format(
    catalog=db_config["catalog"],
    source_schema=db_config["source_schema"],
    new_schema=db_config["new_schema"],
    input_ddl_json=input_ddl_json,
    queries_json=queries_json,
    stats=counts,
)

ddl_out = make_llm_request(
    system_prompt=system_prompt,
    user_prompt=user_prompt,
    response_schema=DDLGenerationOutput,
    api_url=api_url,
)

# Сохраняем результат
save_json_file(ddl_out.model_dump(), "response/ddl_output.json")

# Считаем и выводим токены
calculate_and_print_tokens(system_prompt, user_prompt, ddl_out)

# Выводим время выполнения
elapsed_time = time.time() - start_time
print(f"\n⏱️  Время выполнения: {elapsed_time:.2f} секунд")
