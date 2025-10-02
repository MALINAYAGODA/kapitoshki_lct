import json

from src.get_counts import get_counts
from src.prompts import DDL_PROMPT, INPUT_DDL_PROMPT
from src.utils import (
    calculate_and_print_tokens,
    get_database_config_from_data,
    get_openai_client,
    load_json_file,
    make_openai_request,
    save_json_file,
)
from src.models import DDLGenerationOutput

client = get_openai_client()
model_name = "gpt-4.1"
name_table = "data/questsH.json"

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

# Выполняем запрос к OpenAI API
ddl_out = make_openai_request(
    client=client,
    model_name=model_name,
    system_prompt=system_prompt,
    user_prompt=user_prompt,
    response_schema=DDLGenerationOutput,
    schema_name="ddl_generation_output",
)

# Сохраняем результат
save_json_file(ddl_out.model_dump(), "response/ddl_output.json")

# Считаем и выводим токены
calculate_and_print_tokens(system_prompt, user_prompt, ddl_out)
