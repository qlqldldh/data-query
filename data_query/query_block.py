from typing import Union

from dataclasses import dataclass
import yaml


@dataclass
class QueryBlock:
    name: str
    table: str
    columns: list[str]
    conditions: dict[str, Union[str, int, float]]
    schema: str

    def to_dict(self) -> dict:
        return {
            self.name: {
                "schema": self.schema,
                "table": self.table,
                "columns": self.columns,
                "conditions": self.conditions,
            }
        }

    def serialize(self, f=None) -> Union[str, None]:
        return yaml.dump(self.to_dict(), f)

    @classmethod
    def from_file(cls, file: str, name: str):
        with open(file) as f:
            query_block = yaml.load(f, Loader=yaml.Loader).get(name)
            if not query_block:
                raise ValueError("Not existed query block in file.")
            
            return cls(name=name, **query_block)
