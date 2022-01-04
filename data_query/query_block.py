from typing import Union

from dataclasses import dataclass
import yaml

from data_query.utils.queries import make_query_str
from data_query.utils.type_casting import cast_py_to_sql_str


@dataclass
class QueryBlock:
    name: str
    table: str
    columns: list[str]
    conditions: dict[str, Union[str, int, float]]
    schema: str

    def serialize(self, f=None) -> Union[str, None]:
        return yaml.dump(self.to_dict(), f)

    @classmethod
    def from_file(cls, file: str, name: str):
        with open(file) as f:
            query_block: dict = yaml.load(f, Loader=yaml.Loader).get(name)
            if not query_block:
                raise ValueError("Not existed query block in file.")

            return cls(name=name, **query_block)

    def to_dict(self) -> dict:
        return {
            self.name: {
                "schema": self.schema,
                "table": self.table,
                "columns": self.columns,
                "conditions": self.conditions,
            }
        }

    def to_query(self) -> str:
        conditions = []
        for column, value in self.conditions.items():
            conditions.append(f"{column}={cast_py_to_sql_str(value)}")

        return make_query_str(self.columns, f'"{self.schema}".{self.table}', conditions)
