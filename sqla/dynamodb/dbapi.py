import logging
from dataclasses import dataclass, field
from typing import Any, NamedTuple, TypedDict

import sqlalchemy as sa
from pydantic import TypeAdapter, ValidationError
from types_boto3_dynamodb import DynamoDBClient
from types_boto3_dynamodb.type_defs import (
    CreateTableInputTypeDef,
    DeleteTableInputTypeDef,
)

from sqla.dynamodb import utils

logger = logging.getLogger(__name__)

# Basic error classes
Error = Exception
InterfaceError = Exception

apilevel = "2.0"
threadsafety = 1
# for some reason, setting it to qmark, the default format for partiql, the
# parameters argument is always passed empty. while setting to numeric makes
# it pass a tuple of parameters
# TODO: figure out how to make it work with qmark
paramstyle = "numeric"


TYPES_DYNAMODB_TO_PY: dict[str, type[Any]] = {
    "S": str,
    "N": int,
}


def connect(client: DynamoDBClient):
    """
    Args:
        endpoint_url: endpoint url to connect to

    This function is called with the results of
    `DynamoDialect.create_connect_args()`.
    """
    logger.info("connect() called")
    return Connection(client)


@dataclass
class Connection(sa.Connection):
    """Mock DBAPI Connection."""

    client: DynamoDBClient

    def __init__(self, client: DynamoDBClient):
        self.client = client

    def commit(self):
        logger.info("Connection.commit() called")

    def cursor(self):
        logger.info("Connection.cursor() called")
        return Cursor(self)

    def rollback(self):
        logger.info("Connection.rollback() called")


class Description(NamedTuple):
    """
    A named tuple with same following the dbapi2.0 cursor.description spec

    Created for type hinting and tooling
    """

    name: str
    type_code: type[str | int]
    display_size: int | None
    internal_size: int | None
    precision: int | None
    scale: int | None
    null_ok: bool | None

    @staticmethod
    def from_dynamodb(name: str, typ: str, null_ok: bool = False):
        return Description(
            name=name,
            type_code=TYPES_DYNAMODB_TO_PY[typ],
            display_size=None,
            internal_size=None,
            precision=None,
            scale=None,
            null_ok=null_ok,
        )


def _process_response(
    response: dict[str, Any],
) -> tuple[tuple[sa.Row, ...], tuple[Description]]:
    """
    Iterate over the results of a DynamoDB query.

    Returns a tuple of the results rows, ordered by the field names. And the
    field names, in the order of the rows.

    >>> _iter_result({"Items": [{"a": {"S": "BANANA"}, "b": {"N": 123}}]})
    ((("BANANA", 123),), ["a", "b"])

    >>> _iter_result({"Items": [{"a": {"S": "BANANA"}, "b": {"N": 123}, "c": {"S": "CHERRY"}}]})
    ((("BANANA", 123, "CHERRY"),), ["a", "b", "c"])

    >>> _iter_result({"Items": [{"a": {"N": 123}, "b": {"S": "BANANA"}}]})
    ([123, "BANANA"], ["a", "b"])

    >>> _iter_result({"Items": [{"a": {"N": 123}}, {}"a": {"S": "BANANA"}}]})
    ([123, "BANANA", ["a"])
    """

    items: list[dict[str, dict[str, Any]]] = response.get("Items", [])

    # gets all fields names and types (last one wins)
    fields: dict[str, str] = {}
    for item in items:
        for fname, value in item.items():
            for dtype in value:
                fields[fname] = dtype

    # sort them so we can guarantee that return rows have same order as
    # description
    names = sorted(fields)
    factory = sa.result_tuple(names)

    results: list[sa.Row] = []
    for item in items:
        loaded = utils.load(item)
        # result will be in same order as names
        tupled = tuple(loaded.get(n, None) for n in names)
        row = factory(tupled)
        results.append(row)

    # and description will have the same order as names
    description = tuple(Description.from_dynamodb(n, fields[n]) for n in names)

    return tuple(results), description


class _ExecuteParams(TypedDict):
    """Because pydantic TypeAdapter does not support ExecuteStatementInputTypeDef"""

    Statement: str


_ExecuteAdapter = TypeAdapter(_ExecuteParams)
_CreateAdapter = TypeAdapter(CreateTableInputTypeDef)
_DropAdapter = TypeAdapter(DeleteTableInputTypeDef)


@dataclass
class Cursor:
    connection: Connection

    rowcount: int = field(default=0, init=False)

    _description: tuple[Description, ...] | None = field(default=None, init=False)
    _results: tuple[sa.Row, ...] | None = field(default=None, init=False)
    _index: int = field(default=0, init=False)
    _closed: bool = field(default=False, init=False)

    def execute(
        self,
        sql: str,
        parameters: tuple[Any, ...] | None = None,
    ) -> tuple[sa.Row, ...] | None:
        logger.info("Cursor.execute() called")
        logger.info(f"cursor sql: {sql}")
        logger.info(f"cursor params: {parameters}")

        TYPES = {
            str: "S",
            int: "N",
            bool: "BOOL",
        }

        params: list[dict[str, Any]] = []
        for v in parameters or []:
            key = TYPES[type(v)]
            params.append({key: str(v)})

        try:
            logger.info(f"cursor params modified: {params}")
            kw = _ExecuteAdapter.validate_json(sql)
            if len(params) > 0:
                kw["Parameters"] = params  # type: ignore
            response = self.connection.client.execute_statement(**kw)
            self._update_cursor(response)  # type: ignore
            return self._results
        except ValidationError:
            logger.info("Not a execute statement")

        try:
            in_create = _CreateAdapter.validate_json(sql)
            self.connection.client.create_table(**in_create)
            return self._results
        except ValidationError:
            logger.info("Not a create table statement")

        try:
            in_drop = _DropAdapter.validate_json(sql)
            self.connection.client.delete_table(**in_drop)
            return self._results
        except ValidationError:
            logger.info("Not a drop table statement")

        raise ValueError(f"Unknown statement: {sql}")

    @property
    def description(self) -> tuple[Description, ...] | None:
        return self._description

    def _update_cursor(self, response: dict[str, Any]):
        results, description = _process_response(response)
        self._description = description
        self._results = results
        self._index = 0
        self.rowcount = len(self._results)

    def fetchone(self):
        logger.info("Cursor.fetchone() called")

        if self._results is None:
            return None
        if self._index >= len(self._results):
            return None

        row = self._results[self._index]
        self._index += 1
        return row

    def fetchall(self):
        logger.info("Cursor.fetchall() called")
        if self._results is None:
            return None
        if self._index >= len(self._results):
            return []
        rows = self._results[self._index :]
        self._index += len(rows)
        return rows

    def close(self):
        logger.info("Cursor.close() called")
        self._results = None
        self._index = 0
        self._description = None
        self.rowcount = -1
        self._closed = True
