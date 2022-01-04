from typing import List, Optional

from typer import Option, echo, prompt, Abort

from data_query import q_manager, app
from data_query.echo import err_echo, success_echo


@app.command()
def make_query(
    name: str,
    tab: str,
    has_condition: bool = Option("", prompt=True),
    col: Optional[List[str]] = Option(None),
    schema: str = Option("default", show_default=False),
):
    if not col:
        col = ("*")

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

    try:
        q_manager.add_query_block(name, tab, col, schema, conditions)
    except Exception as e:
        err_echo(str(e))
        raise Abort()

    success_echo("Success to make query")


@app.command()
def send_query(name: str, row_num: int = Option(100), to_csv: bool = Option(False)):
    try:
        result = q_manager.get_query_result(name=name, n=row_num, to_csv=to_csv)
    except Exception as e:
        err_echo(str(e))
        raise Abort()

    echo(result)
