import psycopg2
from .config_postgresql import load_config
from .logger import logger

def connect(config):
    """Connect to the PostgreSQL database server and return an open connection."""
    try:
        conn = psycopg2.connect(**config)
        logger.info('Connected to the PostgreSQL server.')
        return conn
    except (psycopg2.DatabaseError, Exception) as error:
        logger.error(f"Connection error: {error}")
        return None


if __name__ == '__main__':
    config = load_config()
    connect(config)