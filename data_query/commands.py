from typing import List, Optional

from typer import Option, echo, prompt

from data_query import app, q_manager
from data_query.echo import success_echo


@app.command(help="Create SQL Query")
def make_query(
    name: str,
    tab: str,
    has_condition: bool = Option("", prompt=True),
    col: Optional[List[str]] = Option(None),
    schema: str = Option("default", show_default=False),
):
    conditions = dict()
    if has_condition:
        echo("Format: Field=value")
        echo("If you want to finish to set condition, type 'quit'.")
        condition_seq = 1
        while True:
            condition = prompt(f"[Condition {condition_seq}]: ")
            if condition.lower() == "quit":
                break

            field, value = condition.split("=")
            conditions.update({field: value})
            condition_seq += 1

    q_manager.add_query_block(name, tab, col, schema, conditions)
    success_echo("Success to make query")
