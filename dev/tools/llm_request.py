import requests
from pydantic import BaseModel
from typing import List, Optional
import json
import re

# URL API для LLM
api_url = "http://213.181.111.2:57715/v1/chat/completions"

# Определяем модель для структурированного ответа
class AnalysisResult(BaseModel):
    """Модель для структурированного ответа анализа"""
    summary: str  # Краткое резюме
    key_findings: List[str]  # Ключевые выводы
    recommendations: Optional[List[str]] = None  # Рекомендации (опционально)
    confidence_score: Optional[float] = None  # Уровень уверенности (0-1)


def extract_json_from_text(text: str) -> dict:
    """Извлекает JSON из текста"""
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
        raise Exception(f"Не удалось извлечь валидный JSON: {e}")


system_prompt = """Ты аналитик данных. Анализируй предоставленную информацию и давай структурированный ответ.

ВАЖНО: Ответ должен быть в формате JSON, соответствующем следующей схеме:
{
  "summary": "string",
  "key_findings": ["string"],
  "recommendations": ["string"],
  "confidence_score": 0.0
}

Верни только валидный JSON без дополнительного текста."""

user_input = """Проанализируй данные о полетах и дай рекомендации по оптимизации."""

response = requests.post(api_url, json={
    "messages": [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_input}
    ],
    "temperature": 0
})

if response.status_code == 200:
    response_data = response.json()
    content = response_data['choices'][0]['message']['content']
    
    # Извлекаем JSON из ответа
    json_content = extract_json_from_text(content)
    
    # Парсим структурированный ответ
    result = AnalysisResult.model_validate(json_content)
    
    # Выводим структурированный результат
    print("=== СТРУКТУРИРОВАННЫЙ АНАЛИЗ ===")
    print(f"Резюме: {result.summary}")
    print(f"\nКлючевые выводы:")
    for i, finding in enumerate(result.key_findings, 1):
        print(f"  {i}. {finding}")
    
    if result.recommendations:
        print(f"\nРекомендации:")
        for i, rec in enumerate(result.recommendations, 1):
            print(f"  {i}. {rec}")
    
    if result.confidence_score is not None:
        print(f"\nУровень уверенности: {result.confidence_score:.2f}")
    
    print(f"\n=== RAW JSON ===")
    print(json.dumps(result.model_dump(), ensure_ascii=False, indent=2))
else:
    print(f"Ошибка: {response.status_code}")
    print(response.text)

