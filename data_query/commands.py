from typing import List, Optional

from typer import Option, Abort, echo, prompt

from data_query import app, q_manager
from data_query.echo import success_echo, err_echo
from data_query.query_block import QueryBlock


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

    q_manager.convert_data_to_pytype(tab, schema, conditions)

    query_block = QueryBlock(
        name=name, table=tab, columns=col, schema=schema, conditions=conditions
    )

    try:
        result = q_manager.add_query_block(query_block)
    except Exception as e:
        err_echo(str(e))
        raise Abort()

    echo(result)
    success_echo("Success to make query")
