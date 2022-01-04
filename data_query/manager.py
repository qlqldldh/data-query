from pandas.core.frame import DataFrame

from data_query.settings import ROOT_PATH
from data_query.client import AthenaClient
from data_query.query_block import QueryBlock
from data_query.utils.queries import make_query_str
from data_query.utils.type_casting import cast_sql_to_py_type


class QManager:
    QUERY_FILE_PATH = f"{ROOT_PATH}/sql.yaml"

    def __init__(self):
        self._db_client = AthenaClient()

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
        with open(self.QUERY_FILE_PATH, "a") as f:
            query_block.serialize(f)

    def get_query_result(self, name: str, n: int, to_csv: bool = False) -> DataFrame:
        query_block = self.get_query_block(name)
        result = self._db_client.get_query_result(query=query_block.to_query(), n=n)

        if to_csv:
            result.to_csv(f"{ROOT_PATH}/{name}.csv")

        return result

    def get_query_block(self, name: str) -> QueryBlock:
        try:
            return QueryBlock.from_file(self.QUERY_FILE_PATH, name)
        except FileNotFoundError:
            raise Exception("'sql.yaml' file does not exist.")

    def cast_data_to_pytype(self, tab: str, schema: str, data: dict) -> dict:
        converted_data = dict()
        columns_info = self.get_table_columns(tab, schema, tuple(data.keys()))

        for field, type_obj in columns_info.items():
            converted_data[field] = cast_sql_to_py_type(
                data.get(field), type_obj["data_type"]
            )

        return converted_data

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
