from contextlib import contextmanager
from typing import Generator

from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

from database.config import settings


class DatabasePool:
    def __init__(self) -> None:
        self.pool = ConnectionPool(
            conninfo=settings.conninfo,
            min_size=1,
            max_size=10,
            kwargs={"connect_timeout": 5, "autocommit": False},
            open=False,
        )
        self.pool.open()

    @contextmanager
    def get_connection(self):
        with self.pool.connection() as conn:
            yield conn

    @contextmanager
    def get_cursor(self, dict_cursor: bool = False) -> Generator:
        with self.get_connection() as conn:
            try:
                if dict_cursor:
                    with conn.cursor(row_factory=dict_row) as cur:
                        yield conn, cur
                else:
                    with conn.cursor() as cur:
                        yield conn, cur
                conn.commit()
            except Exception:
                conn.rollback()
                raise

    def close_all(self) -> None:
        self.pool.close()


_db_pool: DatabasePool | None = None


def get_db_pool() -> DatabasePool:
    global _db_pool
    if _db_pool is None:
        _db_pool = DatabasePool()
    return _db_pool