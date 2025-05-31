from typing import Any


def _load(data: dict[str, Any]) -> int | str:
    for k, v in data.items():
        match (k, v):
            case ("N", int() | str()):
                return int(v)
            case ("S", _):
                return str(v)
            case _:
                raise ValueError(f"Invalid data type: {data}")

    raise ValueError(f"Invalid data type: {data}")


def load(data: dict[str, Any]) -> dict[str, Any]:
    """
    Loads a DynamoDB item into a Python dictionary

    >>> load({"a": {"S": "b"}})
    {"a": "b"}

    >>> load({"a": {"N": "1"}})
    {"a": 1}

    >>> load({"a": {"S": "b"}, "c": {"N": "1"}})
    {"a": "b", "c": 1}
    """
    return {k: _load(v) for k, v in data.items() if v is not None}


def _dump(data: dict[str, Any] | str | int) -> dict[str, dict[str, Any] | str | int]:
    if isinstance(data, int):
        return {"N": str(data)}
    if isinstance(data, str):
        return {"S": data}
    if isinstance(data, dict):
        return {k: _dump(v) for k, v in data.items() if v is not None}

    raise ValueError(f"Invalid data type: {type(data)}")


def dump(data: dict[str, Any]) -> dict[str, dict[str, Any] | str | int]:
    """
    Dumps a Python dictionary into a DynamoDB item

    >>> dump({"a": "b"})
    {"a": {"S": "b"}}

    >>> dump({"a": 1})
    {"a": {"N": "1"}}
    """
    return {k: _dump(v) for k, v in data.items() if v is not None}
