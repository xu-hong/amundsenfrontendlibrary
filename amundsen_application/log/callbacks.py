from contextlib import contextmanager
import os
import urllib.parse

import psycopg2
from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from psycopg2.extras import Json

from amundsen_application.log.action_log_model import ActionLogParams

DATABASE_URL = os.getenv("DATABASE_URL", "postgres://hong:w14@localhost:5432/postgres")
db = urllib.parse.urlparse(DATABASE_URL)


class PostgresConfig(object):
    dbname = db.path[1:]
    user = db.username
    host = db.hostname
    password = db.password
    port = db.port


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


_postgres = DB()


def get_postgres():
    return _postgres


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
