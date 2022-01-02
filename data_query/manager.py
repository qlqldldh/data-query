from typing import Union

from typer import Abort
from pyathena import connect
import pandas as pd

from data_query.settings import ROOT_PATH

from data_query.client import AthenaClient
from data_query.echo import err_echo
from data_query.query_block import QueryBlock
from data_query.utils.queries import make_select_query, make_where_statement
from data_query.utils.data import data_sql_str_to_py_type


class QManager:
    QUERY_FILE = "sql.yaml"

    def __init__(self):
        try:
            self._db_client = AthenaClient()
        except Exception as e:
            err_echo(str(e))
            raise Abort()

    @property
    def db_client(self):  # TODO: remove
        return self._db_client

    def add_query_block(self, query_block: QueryBlock) -> Union[str, None]:
        with open(f"{ROOT_PATH}/{self.QUERY_FILE}", "a") as f:
            serialized_query_block = query_block.serialize(f)

        return serialized_query_block

    def get_table_columns(self, tab: str, schema: str, cols: tuple[str]) -> dict:
        fields = ["column_name", "data_type"]
        table = "information_schema.columns"
        conditions = [
            f"table_name='{tab}'",
            f"table_schema='{schema}'",
            f"column_name in {cols}",
        ]

        query = " ".join(
            [make_select_query(fields, table), make_where_statement(conditions)]
        )
        result = self._db_client.get_query_result(query, len(cols))

        return result.set_index("column_name").T.to_dict()

    def convert_data_to_pytype(self, tab: str, schema: str, data: dict):
        try:
            columns_info = self.get_table_columns(tab, schema, tuple(data.keys()))

            for field, type_obj in columns_info.items():
                data[field] = data_sql_str_to_py_type(
                    data.get(field), type_obj["data_type"]
                )
        except ValueError as e:
            err_echo(str(e))
            raise Abort()
