from openai import OpenAI
from pydantic import BaseModel
from typing import List, Optional
import json

api_key = "sk-svcacct-N_SwqUhS6U-HhniuGbwpFvpAZe5QZwKCyrzsnUWW9rIi7cnFIKGWi3whweIfQsT3BlbkFJ89GChN-RtBu9Ume-ebmvtZH7HESoZWvPxaGJ2ShrpFpo6_pYV7DtXDZjlA_IkA"

client = OpenAI(
    # This is the default and can be omitted
    api_key=api_key,
)

# Определяем модель для структурированного ответа
class AnalysisResult(BaseModel):
    """Модель для структурированного ответа анализа"""
    summary: str  # Краткое резюме
    key_findings: List[str]  # Ключевые выводы
    recommendations: Optional[List[str]] = None  # Рекомендации (опционально)
    confidence_score: Optional[float] = None  # Уровень уверенности (0-1)

system_prompt = """Ты аналитик данных. Анализируй предоставленную информацию и давай структурированный ответ."""
user_input = """Проанализируй данные о полетах и дай рекомендации по оптимизации."""
chat_completion = client.chat.completions.create(
    messages=[
        {
            "role": "system",
            "content": system_prompt,
        },
        {
            "role": "user",
            "content": user_input,
        }
    ],
    model="gpt-4o-mini",
    response_format={"type": "json_schema", "json_schema": {"name": "analysis_result", "schema": AnalysisResult.model_json_schema()}}
)

# Парсим структурированный ответ
response_content = chat_completion.choices[0].message.content
result = AnalysisResult.model_validate_json(response_content)

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