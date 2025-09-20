from tools.prompts import DDL_PROMPT, INPUT_DDL_PROMPT
from tools.models import DDLGenerationOutput
from utils import (
    get_openai_client,
    load_json_file,
    save_json_file,
    make_openai_request,
    calculate_and_print_tokens
)
import json

client = get_openai_client()
model_name = "gpt-4.1"

flights_data = load_json_file("data/flights.json")


input_ddl_json = json.dumps(flights_data.get("ddl", []), ensure_ascii=False)
queries_json = json.dumps(flights_data.get("queries", []), ensure_ascii=False)

system_prompt = DDL_PROMPT
user_prompt = INPUT_DDL_PROMPT.format(
    catalog="flights", 
    source_schema="public", 
    new_schema="optimized",
    input_ddl_json=input_ddl_json,
    queries_json=queries_json
)

# Выполняем запрос к OpenAI API
ddl_out = make_openai_request(
    client=client,
    model_name=model_name,
    system_prompt=system_prompt,
    user_prompt=user_prompt,
    response_schema=DDLGenerationOutput,
    schema_name="ddl_generation_output"
)

# Сохраняем результат
save_json_file(ddl_out.model_dump(), "response/ddl_output.json")

# Считаем и выводим токены
calculate_and_print_tokens(system_prompt, user_prompt, ddl_out)