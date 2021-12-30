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
        echo("Format: 'Field'='value'")
        echo("If you want to finish to set condition, type 'quit'.")
        seq = 1
        while True:
            cond = prompt(f"[Condition {seq}]: ")
            if cond.lower() == "quit":
                break

            field, value = cond.split("=")
            conditions.update({field: value})
            seq += 1

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
