from urllib.parse import parse_qs, urlparse

import trino
from trino.auth import BasicAuthentication

# Пример JDBC URL
jdbc_url = "jdbc:trino://trino.czxqx2r9.data.bizmrg.com:443?user=hackuser&password=dovq(ozaq8ngt)oS"


# Функция для парсинга JDBC URL
def parse_trino_jdbc(jdbc_url):
    if not jdbc_url.startswith("jdbc:trino://"):
        raise ValueError("Некорректный JDBC URL")

    # Убираем префикс jdbc:
    url = jdbc_url[len("jdbc:") :]
    parsed = urlparse(url)

    host = parsed.hostname
    port = parsed.port or 8080  # порт по умолчанию
    path_parts = parsed.path.lstrip("/").split("/")
    catalog = path_parts[0] if len(path_parts) > 0 else None
    schema = path_parts[1] if len(path_parts) > 1 else None

    # Параметры из query string
    params = parse_qs(parsed.query)
    user = params.get("user", [None])[0]
    password = params.get("password", [None])[0]

    return host, port, catalog, schema, user, password


def get_counts(jdbc_url, tables):
    # Парсим JDBC URL
    host, port, catalog, schema, user, password = parse_trino_jdbc(jdbc_url)

    # Подключение к Trino
    auth = BasicAuthentication(user, password) if user and password else None

    # Подключение к Trino
    conn = trino.dbapi.connect(
        host=host, port=port, user=user, catalog=catalog, schema=schema, auth=auth
    )

    cursor = conn.cursor()
    # counts = {}

    final_list = ""
    for table in tables:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        final_list += f"{table}: {cursor.fetchone()[0]}\n"

    cursor.close()
    conn.close()

    return final_list
