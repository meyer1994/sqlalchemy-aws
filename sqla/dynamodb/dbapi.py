import logging
from dataclasses import dataclass, field
from typing import Any, NamedTuple

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
paramstyle = "pyformat"


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

    def close(self):
        logger.info("Connection.close() called")
        self.client.close()

    def commit(self):
        logger.info("Connection.commit() called")

    def cursor(self):
        logger.info("Connection.cursor() called")
        return Cursor(self)

    def rollback(self):
        logger.info("Connection.rollback() called")


class Description(NamedTuple):
    name: str
    type_code: type[str | int]
    display_size: int | None
    internal_size: int | None
    precision: int | None
    scale: int | None
    null_ok: bool

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
        row = utils.load(item)
        # result will be in same order as names
        row = tuple(row.get(n, None) for n in names)
        row = factory(row)
        results.append(row)

    # and description will have the same order as names
    description = tuple(Description.from_dynamodb(n, fields[n]) for n in names)

    return tuple(results), description


_CreateAdapter = TypeAdapter(CreateTableInputTypeDef)
_DropAdapter = TypeAdapter(DeleteTableInputTypeDef)


@dataclass
class Cursor:
    connection: Connection

    description: tuple[Description, ...] = field(default_factory=tuple, init=False)
    rowcount: int = field(default=0, init=False)

    _results: tuple[sa.Row, ...] = field(default_factory=tuple, init=False)
    _index: int = field(default=0, init=False)
    _closed: bool = field(default=False, init=False)

    def execute(
        self,
        sql: str,
        parameters: dict[str, Any] | None = None,
    ) -> tuple[sa.Row, ...]:
        logger.info("Cursor.execute() called")
        # print(sql)
        # print(parameters)
        # print("=" * 80)

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

        TYPES = {
            str: "S",
            int: "N",
            bool: "BOOL",
        }

        params: list[dict[str, Any]] = []
        for _, v in parameters.items():
            key = TYPES[type(v)]
            params.append({key: str(v)})

        if len(params) == 0:
            response = self.connection.client.execute_statement(
                Statement=sql,
            )
        else:
            response = self.connection.client.execute_statement(
                Statement=sql,
                Parameters=params,
            )

        self._update_cursor(response)  # type: ignore
        return self._results

    def _update_cursor(self, response: dict[str, Any]):
        results, description = _process_response(response)
        self.description = description
        self._results = results
        self._index = 0
        self.rowcount = len(self._results)

    def fetchone(self):
        logger.info("Cursor.fetchone() called")

        if self._results == tuple():
            return None
        if self._index >= len(self._results):
            return None

        row = self._results[self._index]
        self._index += 1
        return row

    def fetchall(self):
        logger.info("Cursor.fetchall() called")
        if self._results == tuple():
            return []

        if self._index >= len(self._results):
            return []

        rows = self._results[self._index :]
        self._index = len(self._results)
        return rows

    def close(self):
        logger.info("Cursor.close() called")
        self._results = tuple()
        self._index = 0
        self.description = tuple()
        self.rowcount = -1
        self._closed = True
