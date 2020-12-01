from contextlib import contextmanager
from typing import NamedTuple

import psycopg2
from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

from action_log_model import ActionLogParams


class PostgresConfig(object):
    dbname = "postgres"
    user = "hong"
    host = "localhost"
    password = "w14"


class DB(object):
    """Mixin for Postgres connection"""
    dbname = PostgresConfig.dbname
    user = PostgresConfig.user
    host = PostgresConfig.host
    password = PostgresConfig.password

    @contextmanager
    def connection(self):
        con = psycopg2.connect(dbname=self.dbname,
                               user=self.user,
                               host=self.host,
                               password=self.password)

        con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

        try:
            yield con
        finally:
            con.close()

    @contextmanager
    def cursor(self, cursor_factory=None):
        with self.connection() as connection:
            cursor = connection.cursor(cursor_factory=cursor_factory)
            try:
                yield cursor
            finally:
                cursor.close()

_postgres = None


def init_connection():
    global _postgres
    _postgres = DB()
    return _postgres


def get_postgres():
    return _postgres


def create_db():
    with get_postgres().cursor() as cur:
        cur.execute(sql.SQL("CREATE DATABASE {}").format(
            sql.Identifier("amundsen"))
        )


def create_table():
    with get_postgres().cursor() as cur:
        cur.execute(sql.SQL("SELECT version()"))
        for r in cur.fetchall():
            print(r)


# def insert(record: ActionLogParams):
#     sql = """
#
#     """


if __name__ == '__main__':
    init_connection()
    create_table()
