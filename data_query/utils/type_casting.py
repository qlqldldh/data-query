from typing import Any


def cast_sql_to_py_type(data: str, sql_data_type: str):
    if sql_data_type in ["varchar", "char"]:
        return str(data)
    if sql_data_type in ["float", "double"]:
        return float(data)
    if sql_data_type in ["int", "integer"]:
        return int(data)
    if sql_data_type == "boolean" and data in ["false", "true"]:
        return True if data == "true" else False
    else:
        raise ValueError("Not supported sql type yet.")


def cast_py_to_sql_str(data: Any):
    if isinstance(data, str):
        return f"'{data}'"
    if isinstance(data, bool):
        return "true" if data else "false"

    return str(data)
