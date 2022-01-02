from typing import Union


def make_query_str(
    columns: list[str],
    sel_from: str,
    conditions: Union[list[str], None],
) -> str:
    query = make_select_query_str(columns, sel_from)
    if conditions:
        query = " ".join([query, make_where_statement_str(conditions)])

    return query


def make_select_query_str(columns: list[str], sel_from: str) -> str:
    columns_: str = ",".join(columns)

    return f"SELECT {columns_} FROM {sel_from}"


def make_where_statement_str(conditions: list[str]) -> str:
    return "WHERE " + " AND ".join(conditions)
