class Config:
    KAFKA_BOOTSTRAP_SERVERS = "kafka:29092"
    KAFKA_TOPIC = "db_analysis_tasks"
    POSTGRES_HOST = "postgres"
    POSTGRES_PORT = 5432
    POSTGRES_DB = "llm_service"
    POSTGRES_USER = "user"
    POSTGRES_PASSWORD = "password"
    DB_POOL_MIN = 2
    DB_POOL_MAX = 10
