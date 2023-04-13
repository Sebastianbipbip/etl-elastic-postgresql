import logging
from functools import wraps

import psycopg2
from psycopg2 import sql


def postgres_connect_if_not(func):
    """Вызвать подключение, если оно не установлено"""

    @wraps(func)
    def wrapped(self, *args, **kwargs):
        if self.connection is None:
            self.connect()
        elif self.connection.closed:
            self.connect()
        return func(self, *args, **kwargs)

    return wrapped


class Postgres:
    def __init__(self, url, table, mapping, logger):
        self.logger: logging = logger

        self.url = url

        self.connection = None

        self.table = table
        self.mapping = mapping

    def connect(self):
        self.connection = psycopg2.connect(self.url)

    def save(self):
        if not self.connection.closed:
            self.connection.commit()
            self.logger.info("Commit")

    def close(self):
        if not self.connection.closed:
            self.save()
            self.connection.close()

            self.logger.info("Соединение закрыто")

    @postgres_connect_if_not
    def create_table(self):
        with self.connection.cursor() as cursor:
            query = sql.SQL(self.mapping).format(table=sql.Identifier(self.table))

            cursor.execute(query)

        self.save()
        self.logger.info(f"Используется таблица {self.table}")

    @postgres_connect_if_not
    def insert_data(self, keys: tuple, values: list):
        with self.connection.cursor() as cursor:
            try:
                keys = tuple(key.lower() for key in keys)

                args = ','.join(
                    cursor.mogrify(f"({','.join('%s' for _ in range(len(keys)))})", row).decode('utf-8')
                    for row in values
                )

                query = sql.SQL("""
                                INSERT INTO {table}({keys})
                                VALUES{values}
                                ON CONFLICT (uid)
                                DO NOTHING
                            """).format(
                    table=sql.Identifier(self.table),
                    keys=sql.SQL(', ').join(map(sql.Identifier, keys)),
                    values=sql.SQL(args))

                cursor.execute(query)

            except psycopg2.errors.StringDataRightTruncation as error:
                self.logger.error(error)

    @postgres_connect_if_not
    def get_last_date(self):
        query = sql.SQL("""
                SELECT timestamp
                FROM {table}
                ORDER BY id DESC
                LIMIT 1
            """).format(table=sql.Identifier(self.table))

        with self.connection.cursor() as cursor:
            cursor.execute(query)
            return cursor.fetchone()[0]
