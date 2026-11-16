"""Database connection management module.

Uses a small connection pool. The database lives in the cloud, so opening a fresh
TLS connection for every request (as the first version did) costs several network
round trips each time, which is noticeable when the UI polls job status every second.
Pooled connections are re-used; a stale one (the server closes idle connections) is
detected with a cheap `SELECT 1` and replaced.
"""

import threading
from contextlib import contextmanager

from psycopg2 import pool as pg_pool

from app.config import settings

_pool = None
_pool_lock = threading.Lock()


def _get_pool():
    """Create the pool on first use (so importing this module never touches the network)."""
    global _pool
    if _pool is None:
        with _pool_lock:
            if _pool is None:
                _pool = pg_pool.ThreadedConnectionPool(
                    1,
                    settings.DB_POOL_MAX,
                    settings.DATABASE_URL,
                    connect_timeout=10,
                    keepalives=1,
                    keepalives_idle=30,
                    keepalives_interval=10,
                    keepalives_count=3,
                )
    return _pool


def _acquire():
    """Get a working connection, discarding any that went stale while idle."""
    db_pool = _get_pool()
    last_error = None
    for _ in range(3):
        conn = db_pool.getconn()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
            conn.rollback()
            return conn
        except Exception as exc:  # noqa: BLE001 - stale or broken connection
            last_error = exc
            db_pool.putconn(conn, close=True)
    raise last_error


@contextmanager
def get_db_connection():
    """Borrow a PostgreSQL connection from the pool.

    Yields:
        psycopg2.extensions.connection: An active connection. Call `commit()` yourself;
        anything not committed is rolled back when the block ends.

    Ensures:
        The connection is returned to the pool even if an exception is raised.
    """
    conn = _acquire()
    try:
        yield conn
    finally:
        try:
            if not conn.closed:
                conn.rollback()
        except Exception:  # noqa: BLE001
            pass
        _get_pool().putconn(conn)
