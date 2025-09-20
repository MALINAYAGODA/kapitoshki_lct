import tiktoken

def count_tokens(text: str, model: str = "gpt-4") -> int:
    """Подсчитывает количество токенов для заданного текста и модели"""
    try:
        encoding = tiktoken.encoding_for_model(model)
    except KeyError:
        # Fallback для неизвестных моделей
        encoding = tiktoken.get_encoding("cl100k_base")
    
    return len(encoding.encode(text))