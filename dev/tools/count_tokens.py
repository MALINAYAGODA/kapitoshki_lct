import tiktoken

def count_tokens(text: str, model: str = "cl100k_base") -> int:
    """Подсчитывает количество токенов для заданного текста"""
    try:
        encoding = tiktoken.get_encoding(model)
    except KeyError:
        # Fallback для неизвестных кодировок
        encoding = tiktoken.get_encoding("cl100k_base")
    
    return len(encoding.encode(text))