from chuckwalla2.schema import Schema
from contextlib import contextmanager
from textwrap import dedent
from typing import List

import logging
import pyathena
import pyathena.connection
import pyarrow
import re

S3_BUCKET = "chuckwalla2-storage"
AWS_REGION = "us-east-1"
ATHENA_WORK_GROUP = "primary"


def get_folder(database : str, table_name : str, partition_date : str = None):
    assert "__" not in database
    database_as_folders = database.replace("_", "/")
    if partition_date:
        return f"{S3_BUCKET}/{database_as_folders}/{table_name}/partition_date={partition_date}"
    else:
        return f"{S3_BUCKET}/{database_as_folders}/{table_name}"


class ConnectionManager:
    connection: pyathena.connection.Connection

    def __init__(self, connection : pyathena.connection.Connection):
        self.connection = connection

    @contextmanager
    def get_cursor(self):
        cursor = self.connection.cursor()
        try:
            yield cursor
        finally:
            cursor.close()

    def execute(self, sql_input : str):
        logging.info(f"Executing sql")
        sql = dedent(sql_input)
        logging.info(sql)

        with self.get_cursor() as cursor:
            cursor.execute(sql)
            return cursor.fetchall()

    def create_database(self, database : str):
        sql = f"create database if not exists {database}"
        self.execute(sql)

    def create_table(self, database : str, table_name : str, schema: Schema):
        fields_ddl = schema.to_athena_ddl()
        folder = get_folder(database, table_name)

        sql = f"""
            create external table if not exists {database}.{table_name} (
                {fields_ddl}
            )
            partitioned by (partition_date string)
            row format serde 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
            location 's3://{folder}/'
        """

        logging.info("Executing sql:")
        logging.info(sql)

        with self.get_cursor() as cursor:
            cursor.execute(sql)

    def describe_table(self, database : str, table_name : str) -> List[str]:
        sql = f"describe {database}.{table_name}"
        raw_description = self.execute(sql)
        assert all(len(x) == 1 for x in raw_description)
        description = [x[0].strip() for x in raw_description]
        return description

    def msck_table(self, database : str, table_name : str):
        sql = f"msck repair table {database}.{table_name}"
        self.execute(sql)

    def update_table(self, database : str, table_name : str, schema: Schema):
        self.create_database(database)
        self.create_table(database, table_name, schema)
        description = self.describe_table(database, table_name)

        # Check if any columns changed names/type
        existing_schema = Schema.from_athena_description(description)
        schema.assert_equivalent(existing_schema, excludes = ("partition_date",))

        self.msck_table(database, table_name)


@contextmanager
def get_connection() -> pyathena.connection.Connection:
    connection = pyathena.connect(region = AWS_REGION, work_group = ATHENA_WORK_GROUP)
    try:
        yield connection
    finally:
        connection.close()


@contextmanager
def get_connection_manager() -> ConnectionManager:
    with get_connection() as connection:
        yield ConnectionManager(connection)


