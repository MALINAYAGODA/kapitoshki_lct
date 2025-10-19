from dev.tools.count_tokens import count_tokens
import json
import re
import requests
from typing import Dict, Any, Optional, List, Tuple
from pydantic import BaseModel


def load_json_file(filename: str) -> Dict[str, Any]:
    with open(filename, "r", encoding="utf-8") as f:
        return json.load(f)


def save_json_file(data: Dict[str, Any], filename: str) -> None:
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def make_llm_request(
    system_prompt: str,
    user_prompt: str,
    response_schema: BaseModel,
    api_url: str = "http://213.181.111.2:57715/v1/chat/completions",
    temperature: float = 0
) -> Any:
    """Запрос к LLM API с парсингом JSON из текста."""
    try:
        enhanced_system_prompt = f"""{system_prompt}

ВАЖНО: Ответ должен быть в формате JSON, соответствующем следующей схеме:
{json.dumps(response_schema.model_json_schema(), ensure_ascii=False, indent=2)}

Верни только валидный JSON без дополнительного текста."""

        response = requests.post(api_url, json={
            "messages": [
                {"role": "system", "content": enhanced_system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            "temperature": temperature
        })
        
        if response.status_code != 200:
            raise Exception(f"HTTP ошибка {response.status_code}: {response.text}")
        
        response_data = response.json()
        content = response_data['choices'][0]['message']['content']
        
        json_content = extract_json_from_text(content)
        
        if 'properties' in json_content and isinstance(json_content['properties'], dict):
            json_content = json_content['properties']
        
        return response_schema.model_validate(json_content)
    
    except Exception as e:
        raise Exception(f"Ошибка при выполнении запроса к LLM API: {e}")


def calculate_and_print_tokens(
    system_prompt: str,
    user_prompt: str,
    output_data: Any,
    prefix: str = ""
) -> Tuple[int, int]:
    input_tokens = count_tokens(system_prompt) + count_tokens(user_prompt)
    
    # Преобразуем выходные данные в JSON строку
    if hasattr(output_data, 'model_dump'):
        output_text = json.dumps(output_data.model_dump(), ensure_ascii=False)
    else:
        output_text = json.dumps(output_data, ensure_ascii=False)
    
    output_tokens = count_tokens(output_text)
    
    prefix_str = f"{prefix}: " if prefix else ""
    print(f"{prefix_str}Токены в входных данных: {input_tokens:,}")
    print(f"{prefix_str}Токены в выходных данных: {output_tokens:,}")
    
    return input_tokens, output_tokens


def process_queries_batch(
    system_prompt: str,
    queries: List[Dict[str, Any]],
    user_prompt_template: str,
    response_schema: BaseModel,
    additional_data: Optional[Dict[str, Any]] = None,
    api_url: str = "http://213.181.111.2:57715/v1/chat/completions"
) -> Tuple[List[Dict[str, Any]], int, int]:
    """Обработка запросов через LLM API."""
    processed_queries = []
    total_input_tokens = 0
    total_output_tokens = 0
    
    print(f"Обрабатываем {len(queries)} запросов через LLM API...")
    
    for i, query_data in enumerate(queries, 1):
        print(f"\nОбработка запроса {i}/{len(queries)}: {query_data['queryid']}")
        
        try:
            template_data = {
                "query_json": json.dumps(query_data, ensure_ascii=False)
            }
            if additional_data:
                template_data.update(additional_data)
            
            user_prompt = user_prompt_template.format(**template_data)
            
            result = make_llm_request(
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                response_schema=response_schema,
                api_url=api_url
            )
            
            processed_result = {
                "queryid": query_data['queryid'],
                "query": result.query
            }
            processed_queries.append(processed_result)
            
            input_tokens = count_tokens(system_prompt) + count_tokens(user_prompt)
            output_tokens = count_tokens(result.query)
            total_input_tokens += input_tokens
            total_output_tokens += output_tokens
            
            print(f"  Успешно обработан. Токены: вход={input_tokens:,}, выход={output_tokens:,}")
            
        except Exception as e:
            print(f"  ОШИБКА при обработке запроса {query_data['queryid']}: {e}")
            processed_queries.append({
                "queryid": query_data['queryid'],
                "query": f"-- ОШИБКА ОБРАБОТКИ: {e}\n{query_data['query']}",
                "error": str(e)
            })
    
    return processed_queries, total_input_tokens, total_output_tokens


def print_processing_summary(
    total_queries: int,
    total_input_tokens: int,
    total_output_tokens: int,
    output_filename: str
) -> None:
    print(f"\n=== ИТОГИ ===")
    print(f"Всего запросов обработано: {total_queries}")
    print(f"Общие токены в входных данных: {total_input_tokens:,}")
    print(f"Общие токены в выходных данных: {total_output_tokens:,}")
    print(f"Результаты сохранены в: {output_filename}")


def parse_catalog_and_schema_from_ddl(ddl_statements: List[Dict[str, Any]]) -> Tuple[Optional[str], Optional[str]]:
    if not ddl_statements:
        return None, None
    
    # Паттерн для поиска CREATE TABLE catalog.schema.table_name
    pattern = r'CREATE\s+TABLE\s+([^.\s]+)\.([^.\s]+)\.([^.\s\(]+)'
    
    for ddl_item in ddl_statements:
        statement = ddl_item.get("statement", "")
        match = re.search(pattern, statement, re.IGNORECASE)
        if match:
            catalog = match.group(1)
            schema = match.group(2)
            print(f"Найден каталог: '{catalog}', схема: '{schema}'")
            return catalog, schema
    
    print("Не удалось найти каталог и схему в DDL statements")
    return None, None


def get_database_config_from_data(data: Dict[str, Any], new_schema: str = "optimized") -> Dict[str, str]:
    ddl_statements = data.get("ddl", [])
    catalog, source_schema = parse_catalog_and_schema_from_ddl(ddl_statements)
    
    return {
        "catalog": catalog or "unknown",
        "source_schema": source_schema or "public", 
        "new_schema": new_schema
    }




def extract_json_from_text(text: str) -> Dict[str, Any]:
    """Извлекает JSON из текста, который может содержать дополнительный текст."""
    text = re.sub(r'```json\s*', '', text)
    text = re.sub(r'```\s*$', '', text)
    
    json_pattern = r'\{.*\}'
    match = re.search(json_pattern, text, re.DOTALL)
    
    if match:
        json_str = match.group(0)
        try:
            return json.loads(json_str)
        except json.JSONDecodeError:
            pass
    
    try:
        return json.loads(text.strip())
    except json.JSONDecodeError as e:
        raise Exception(f"Не удалось извлечь валидный JSON из ответа: {text[:200]}... Ошибка: {e}")




