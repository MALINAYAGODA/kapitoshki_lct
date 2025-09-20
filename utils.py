from openai import OpenAI
from tools.count_tokens import count_tokens
import json
from typing import Dict, Any, Optional, List, Tuple
from pydantic import BaseModel


def get_openai_client() -> OpenAI:
    api_key = "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"
    return OpenAI(api_key=api_key)


def load_json_file(filename: str) -> Dict[str, Any]:
    with open(filename, "r", encoding="utf-8") as f:
        return json.load(f)


def save_json_file(data: Dict[str, Any], filename: str) -> None:
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def make_openai_request(
    client: OpenAI,
    model_name: str,
    system_prompt: str,
    user_prompt: str,
    response_schema: BaseModel,
    schema_name: str,
    temperature: float = 0
) -> Any:
    try:
        response = client.chat.completions.create(
            model=model_name,
            temperature=temperature,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            response_format={
                "type": "json_schema",
                "json_schema": {
                    "name": schema_name,
                    "schema": response_schema.model_json_schema()
                }
            }
        )
        
        return response_schema.model_validate_json(response.choices[0].message.content)
    
    except Exception as e:
        raise Exception(f"Ошибка при выполнении запроса к OpenAI API: {e}")


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
    client: OpenAI,
    model_name: str,
    system_prompt: str,
    queries: List[Dict[str, Any]],
    user_prompt_template: str,
    response_schema: BaseModel,
    schema_name: str,
    additional_data: Optional[Dict[str, Any]] = None
) -> Tuple[List[Dict[str, Any]], int, int]:
    processed_queries = []
    total_input_tokens = 0
    total_output_tokens = 0
    
    print(f"Обрабатываем {len(queries)} запросов...")
    
    for i, query_data in enumerate(queries, 1):
        print(f"\nОбработка запроса {i}/{len(queries)}: {query_data['queryid']}")
        
        try:
            # Подготавливаем данные для шаблона
            template_data = {
                "query_json": json.dumps(query_data, ensure_ascii=False)
            }
            if additional_data:
                template_data.update(additional_data)
            
            # Формируем пользовательский промпт
            user_prompt = user_prompt_template.format(**template_data)
            
            # Делаем запрос к API
            result = make_openai_request(
                client=client,
                model_name=model_name,
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                response_schema=response_schema,
                schema_name=schema_name
            )
            
            # Сохраняем результат с queryid из исходного запроса
            processed_result = {
                "queryid": query_data['queryid'],
                "query": result.query
            }
            processed_queries.append(processed_result)
            
            # Считаем токены
            input_tokens = count_tokens(system_prompt) + count_tokens(user_prompt)
            output_tokens = count_tokens(result.query)
            total_input_tokens += input_tokens
            total_output_tokens += output_tokens
            
            print(f"  Успешно обработан. Токены: вход={input_tokens:,}, выход={output_tokens:,}")
            
        except Exception as e:
            print(f"  ОШИБКА при обработке запроса {query_data['queryid']}: {e}")
            # Добавляем запрос с ошибкой для отладки
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
