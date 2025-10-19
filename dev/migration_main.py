from src.prompts import MIGRATION_PROMPT, INPUT_MIGRATION_PROMPT
from src.models import MigrationOutput
from src.utils import (
    load_json_file,
    save_json_file,
    make_llm_request,
    calculate_and_print_tokens,
    get_database_config_from_data
)
import json
import time

api_url = "http://213.181.111.2:57715/v1/chat/completions"
name_table = "data/flights.json"

start_time = time.time()

input_data = load_json_file(name_table)
ddl_output = load_json_file("response/ddl_output.json")

db_config = get_database_config_from_data(input_data)

input_ddl_json = json.dumps(input_data.get("ddl", []), ensure_ascii=False)
new_ddl_json = json.dumps(ddl_output.get("ddl", []), ensure_ascii=False)

system_prompt = MIGRATION_PROMPT
user_prompt = INPUT_MIGRATION_PROMPT.format(
    catalog=db_config["catalog"], 
    source_schema=db_config["source_schema"], 
    new_schema=db_config["new_schema"],
    input_ddl_json=input_ddl_json,
    new_ddl_json=new_ddl_json
)

migration_out = make_llm_request(
    system_prompt=system_prompt,
    user_prompt=user_prompt,
    response_schema=MigrationOutput,
    api_url=api_url
)

save_json_file(migration_out.model_dump(), "response/migration_output.json")

calculate_and_print_tokens(system_prompt, user_prompt, migration_out)

# Выводим время выполнения
elapsed_time = time.time() - start_time
print(f"\n⏱️  Время выполнения: {elapsed_time:.2f} секунд")
