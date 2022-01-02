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

    def to_dict(self):
        return {
            self.name: {
                "schema": self.schema,
                "table": self.table,
                "columns": self.columns,
                "conditions": self.conditions,
            }
        }

    def serialize(self, f=None) -> str:
        return yaml.dump(self.to_dict(), f)
