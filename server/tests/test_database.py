"""Connection pool behaviour, with a fake pool (no database needed)."""
import pytest

from app import database


class FakeConn:
    def __init__(self, healthy=True):
        self.healthy = healthy
        self.closed = False
        self.rollbacks = 0

    def cursor(self):
        return self

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql):
        if not self.healthy:
            raise RuntimeError("server closed the connection unexpectedly")

    def rollback(self):
        self.rollbacks += 1


class FakePool:
    def __init__(self, conns):
        self.conns = list(conns)
        self.returned = []
        self.discarded = []

    def getconn(self):
        return self.conns.pop(0)

    def putconn(self, conn, close=False):
        (self.discarded if close else self.returned).append(conn)


@pytest.fixture
def fake_pool(monkeypatch):
    def install(*conns):
        p = FakePool(conns)
        monkeypatch.setattr(database, "_get_pool", lambda: p)
        return p

    return install


def test_connection_is_returned_to_the_pool(fake_pool):
    conn = FakeConn()
    pool = fake_pool(conn)
    with database.get_db_connection() as c:
        assert c is conn
    assert pool.returned == [conn] and conn.rollbacks >= 1  # uncommitted work is discarded


def test_connection_is_returned_even_when_the_block_raises(fake_pool):
    conn = FakeConn()
    pool = fake_pool(conn)
    with pytest.raises(ValueError):
        with database.get_db_connection():
            raise ValueError("boom")
    assert pool.returned == [conn]


def test_stale_connection_is_replaced(fake_pool):
    stale, good = FakeConn(healthy=False), FakeConn()
    pool = fake_pool(stale, good)
    with database.get_db_connection() as c:
        assert c is good
    assert pool.discarded == [stale] and pool.returned == [good]


def test_gives_up_after_three_broken_connections(fake_pool):
    pool = fake_pool(FakeConn(False), FakeConn(False), FakeConn(False))
    with pytest.raises(RuntimeError):
        with database.get_db_connection():
            pass
    assert len(pool.discarded) == 3


def test_pool_is_created_lazily_and_only_once(monkeypatch):
    created = []

    class FakeThreadedPool:
        def __init__(self, *args, **kwargs):
            created.append((args, kwargs))

    monkeypatch.setattr(database.pg_pool, "ThreadedConnectionPool", FakeThreadedPool)
    monkeypatch.setattr(database, "_pool", None)

    assert created == []                       # importing / patching never connects
    first, second = database._get_pool(), database._get_pool()
    assert first is second and len(created) == 1
    assert created[0][0][:2] == (1, database.settings.DB_POOL_MAX)
    assert created[0][1]["keepalives"] == 1    # idle cloud connections are kept alive
