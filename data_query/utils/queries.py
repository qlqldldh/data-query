def make_select_query(columns: list[str], sel_from: str) -> str:
    columns_: str = ",".join(columns)

    return f"SELECT {columns_} FROM {sel_from}"


def make_where_statement(conditions: list[str]) -> str:
    return "WHERE " + " AND ".join(conditions)
