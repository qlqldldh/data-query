from typer import Abort

from data_query.settings import ROOT_PATH

from data_query.client import AthenaClient
from data_query.echo import err_echo
from data_query.query_block import QueryBlock
from data_query.utils.queries import make_query_str
from data_query.utils.type_casting import data_sql_str_to_py_type


class QManager:
    QUERY_FILE_PATH = f"{ROOT_PATH}/sql.yaml"

    def __init__(self):
        try:
            self._db_client = AthenaClient()
        except Exception as e:
            err_echo(str(e))
            raise Abort()

    def add_query_block(
        self,
        name: str,
        table: str,
        columns: list[str],
        schema: str,
        conditions: dict,
    ):
        query_block = QueryBlock(
            name=name,
            table=table,
            columns=columns,
            schema=schema,
            conditions=self.cast_data_to_pytype(table, schema, conditions),
        )
        try:
            with open(self.QUERY_FILE_PATH, "a") as f:
                serialized_query_block = query_block.serialize(f)
        except Exception as e:
            err_echo(str(e))
            raise Abort()

        return serialized_query_block
    
    def get_query_block(self, name: str) -> QueryBlock:
        try:
            return QueryBlock.from_file(self.QUERY_FILE_PATH, name)
        except Exception as e:
            err_echo(str(e))
            raise Abort()

    def cast_data_to_pytype(self, tab: str, schema: str, data: dict) -> dict:
        converted_data = dict()
        try:
            columns_info = self.get_table_columns(tab, schema, tuple(data.keys()))

            for field, type_obj in columns_info.items():
                converted_data[field] = data_sql_str_to_py_type(
                    data.get(field), type_obj["data_type"]
                )

            return converted_data
        except ValueError as e:
            err_echo(str(e))
            raise Abort()

    def get_table_columns(self, tab: str, schema: str, cols: tuple[str]) -> dict:
        fields = ["column_name", "data_type"]
        table = "information_schema.columns"
        conditions = [
            f"table_name='{tab}'",
            f"table_schema='{schema}'",
            f"column_name in {cols}",
        ]

        query = make_query_str(fields, table, conditions)
        result = self._db_client.get_query_result(query, len(cols))

        return result.set_index("column_name").T.to_dict()
