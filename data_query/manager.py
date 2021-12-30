from typing import Union

from typer import Abort
from pyathena import connect
import pandas as pd

from data_query.settings import (
    ATHENA_USERNAME,
    ATHENA_PASSWORD,
    AWS_REGION,
    S3_LOCATION,
    ROOT_PATH,
)

from data_query.echo import err_echo
from data_query.query_block import QueryBlock


class QManager:
    QUERY_FILE = "sql.yaml"

    def __init__(self):
        self._conn = self._connect()

    def add_query_block(self, query_block: QueryBlock) -> Union[str, None]:
        with open(f"{ROOT_PATH}/{self.QUERY_FILE}", "a") as f:
            serialized_query_block = query_block.serialize(f)

        return serialized_query_block

    def get_query_result(self, query: str, row: str):
        df = pd.read_sql_query(query, self._conn)

        return [field.strip() for field in df.to_dict(orient="list").get(row)]

    @staticmethod
    def _connect():
        try:
            return connect(
                s3_staging_dir=S3_LOCATION,
                region_name=AWS_REGION,
                aws_access_key_id=ATHENA_USERNAME,
                aws_secret_access_key=ATHENA_PASSWORD,
            )
        except Exception as e:
            err_echo(str(e))
            raise Abort()

    @staticmethod
    def _make_select_query(fields: list, table: str, schema: str) -> str:
        columns: str = ",".join(fields)

        return f'SELECT f{columns} FROM "{schema}".{table}'

    @staticmethod
    def _make_where_statement(**conditions) -> str:
        return "where " + " and ".join(
            [f"{key}='{value}'" for key, value in conditions.items()]
        )
