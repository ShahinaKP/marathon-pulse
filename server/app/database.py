"""Database connection management module.

Provides a reusable context manager for acquiring and safely closing
PostgreSQL database connections using psycopg2.
"""

from contextlib import contextmanager
import psycopg2

from app.config import settings


@contextmanager
def get_db_connection():
    """Manage PostgreSQL database connection lifecycle.

    Yields:
        psycopg2.extensions.connection: An active PostgreSQL connection.

    Ensures:
        The connection is closed when exiting the context block,
        even if an unhandled exception is raised.
    """
    conn = psycopg2.connect(settings.DATABASE_URL)
    try:
        yield conn
    finally:
        conn.close()