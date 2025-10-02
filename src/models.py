from pydantic import BaseModel, Field, constr, validator
from typing import List, Optional


SqlStatement = constr(strip_whitespace=True, min_length=1)

class DDLGenerationOutput(BaseModel):
    """Результат шага генерации новой схемы с промежуточными рассуждениями."""
    reasoning: str = Field(
        ...,
        description="Подробные рассуждения и анализ: изучение паттернов запросов, выявление узких мест, обоснование архитектурных решений."
    )
    ddl: List[SqlStatement] = Field(
        ...,
        description="Упорядоченный список SQL. Первый элемент обязательно CREATE SCHEMA <catalog>.<new_schema>."
    )
    design_notes: Optional[str] = Field(
        None,
        description="Краткое резюме принятых решений: денормализация, партиционирование, материализация и т.п."
    )

    @validator("ddl")
    def first_is_create_schema(cls, v: List[str]):
        if not v:
            raise ValueError("ddl не должен быть пустым")
        head = v[0].strip().upper().replace("\n", " ")
        if not head.startswith("CREATE SCHEMA "):
            raise ValueError("Первая команда должна быть CREATE SCHEMA <catalog>.<new_schema>")
        return v


class MigrationOutput(BaseModel):
    """Результат шага генерации запросов миграции данных."""
    reasoning: str = Field(
        ...,
        description="Подробные рассуждения и анализ миграции: порядок выполнения, зависимости, обработка типов данных."
    )
    migrations: List[SqlStatement] = Field(
        ...,
        description="Упорядоченный список INSERT INTO <catalog>.<new_schema>.<table> SELECT ... FROM <catalog>.<source_schema>.<table> ..."
    )
    migration_notes: Optional[List[str]] = Field(
        None,
        description="Ключевые комментарии по миграциям (агрегации, дедупликация, объединения, исправления типов)."
    )


class RewrittenQuery(BaseModel):
    """Переписанный запрос с сохранением идентификатора."""
    query: SqlStatement


class RewrittenQueriesOutput(BaseModel):
    """Результат шага переписывания исходных SQL на новую схему."""
    queries: List[RewrittenQuery]
    global_notes: Optional[str] = Field(
        None,
        description="Общие рекомендации к использованию новой схемы/материализаций."
    )


class StructureOutput(BaseModel):
    """
    Универсальная обёртка, если хочешь возвращать все этапы вместе
    (или по частям — пустые поля допускаются).
    """
    new_ddl: Optional[DDLGenerationOutput] = None
    migrations: Optional[MigrationOutput] = None
    rewritten_queries: Optional[RewrittenQueriesOutput] = None
