import psycopg2

# Centralized database configuration
DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "client_dashboard_local",
    "user": "postgres",
    "password": "postgres"
}

def get_connection():
    """
    Returns a new connection to the PostgreSQL database using the configuration above.
    Usage:
        from db_connection import get_connection
        conn = get_connection()
    """
    return psycopg2.connect(**DB_CONFIG)
