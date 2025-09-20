from tools.prompts import MIGRATION_PROMPT, INPUT_MIGRATION_PROMPT
from tools.models import MigrationOutput
from utils import (
    get_openai_client,
    load_json_file,
    save_json_file,
    make_openai_request,
    calculate_and_print_tokens,
    get_database_config_from_data
)
import json

client = get_openai_client()
model_name = "gpt-4.1"
name_table = "data/questsH.json"

input_data = load_json_file(name_table)
ddl_output = load_json_file("response/ddl_output.json")

# Автоматически извлекаем конфигурацию базы данных
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

# Выполняем запрос к OpenAI API
migration_out = make_openai_request(
    client=client,
    model_name=model_name,
    system_prompt=system_prompt,
    user_prompt=user_prompt,
    response_schema=MigrationOutput,
    schema_name="migration_output"
)

save_json_file(migration_out.model_dump(), "response/migration_output.json")

calculate_and_print_tokens(system_prompt, user_prompt, migration_out)