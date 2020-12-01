from contextlib import contextmanager
import json
from typing import NamedTuple

import psycopg2
from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from psycopg2.extras import Json

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
        cur.execute(sql.SQL("""
        CREATE TABLE action_log (
            command text,
            start_epoch_ms bigint,
            end_epoch_ms bigint,
            "user" varchar(1024),
            host_name varchar(512),
            pos_args_json json,
            keyword_args_json json
        )
        """))


def insert_record(record: ActionLogParams):
    with get_postgres().cursor() as cur:
        cur.execute("""
        INSERT INTO action_log (
        command,
        start_epoch_ms,
        end_epoch_ms,
        "user",
        host_name,
        pos_args_json,
        keyword_args_json
        ) VALUES(%s, %s, %s, %s, %s, %s, %s)""",
                    (
                        record.command,
                        record.start_epoch_ms,
                        record.end_epoch_ms,
                        record.user,
                        record.host_name,
                        Json(record.pos_args_json),
                        Json(record.keyword_args_json)
                    ))

        cur.execute(sql.SQL("select * from action_log;"))
        for r in cur.fetchall():
            print(r)


if __name__ == '__main__':
    init_connection()
    metrics = {
        "command": "test",
        "start_epoch_ms": 1,
        "end_epoch_ms": None,
        "user": "hong",
        "host_name": "local",
        "pos_args_json": {"a": 100},
        "keyword_args_json": {"key": "value"}
    }
    a = ActionLogParams(**metrics)
    insert_record(a)
