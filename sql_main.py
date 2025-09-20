from tools.prompts import SQL_PROMPT, INPUT_SQL_PROMPT
from tools.models import RewrittenQuery
from utils import (
    get_openai_client,
    load_json_file,
    save_json_file,
    process_queries_batch,
    print_processing_summary
)
import json

client = get_openai_client()
model_name = "gpt-4.1"

flights_data = load_json_file("data/flights.json")
ddl_output = load_json_file("response/ddl_output.json")

queries = flights_data.get("queries", [])
new_ddl_json = json.dumps(ddl_output, ensure_ascii=False)

system_prompt = SQL_PROMPT.format(catalog="flights", new_schema="optimized")

# Обрабатываем все запросы с помощью утилиты
rewritten_queries, total_input_tokens, total_output_tokens = process_queries_batch(
    client=client,
    model_name=model_name,
    system_prompt=system_prompt,
    queries=queries,
    user_prompt_template=INPUT_SQL_PROMPT,
    response_schema=RewrittenQuery,
    schema_name="rewritten_query",
    additional_data={
        "catalog": "flights",
        "new_schema": "optimized",
        "new_ddl_json": new_ddl_json
    }
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