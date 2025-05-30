import logging
from dataclasses import dataclass, field
from typing import Any, NamedTuple, TypedDict

import boto3
import sqlalchemy as sa
from pydantic import TypeAdapter, ValidationError
from types_boto3_dynamodb.type_defs import CreateTableInputTypeDef

logger = logging.getLogger(__name__)

dynamodb = boto3.resource("dynamodb", endpoint_url="http://localhost:4566")
dynamodbc = boto3.client("dynamodb", endpoint_url="http://localhost:4566")
table = dynamodb.Table("TEST_TABLE")

# Basic error classes
Error = Exception
InterfaceError = Exception


def unwrap(value: Any):
    res: dict[str, Any] = {}
    for key, value in value.items():
        if key == "S":
            return str(value)
        elif key == "N":
            return int(value)
        elif key == "BOOL":
            return bool(value)
        elif key == "NULL":
            return None
        elif key == "L":
            return [unwrap(item) for item in value]
        elif key == "M":
            return {k: unwrap(v) for k, v in value.items()}
        else:
            res[key] = unwrap(value)

    return res


def connect():
    logger.info("connect() called")
    return Connection()


@dataclass
class Connection:
    """Mock DBAPI Connection."""

    def close(self):
        logger.info("Connection.close() called")

    def commit(self):
        logger.info("Connection.commit() called")

    def cursor(self):
        logger.info("Connection.cursor() called")
        return Cursor(self)

    def rollback(self):
        logger.info("Connection.rollback() called")


class _Execute(TypedDict):
    Statement: str


class _Insert(TypedDict):
    TableName: str
    Item: dict[str, Any]


_CreateAdapter = TypeAdapter(CreateTableInputTypeDef)
_ExecuteAdapter = TypeAdapter(_Execute)
_InsertAdapter = TypeAdapter(_Insert)


class Description(NamedTuple):
    name: str
    type_code: type[str | int | bool] | None
    display_size: int | None
    internal_size: int | None
    precision: int | None
    scale: int | None
    null_ok: bool

    @staticmethod
    def from_dynamodb(name: str, typ: str, null_ok: bool = False):
        TYPES = {
            "S": str,
            "N": int,
            "BOOL": bool,
            "NULL": None,
        }

        return Description(
            name=name,
            type_code=TYPES[typ],
            display_size=None,
            internal_size=None,
            precision=None,
            scale=None,
            null_ok=null_ok,
        )


@dataclass
class Cursor:
    connection: Connection
    description: list[Description] = field(default_factory=list, init=False)

    _results: list[sa.Row] = field(default_factory=list)
    _index: int = 0
    _rowcount: int = -1
    _closed: bool = False

    def _to_row(self, data: dict[str, Any]):
        fields = [i.name for i in self.description]
        factory = sa.result_tuple(fields)
        return factory(data[k] for k in fields if k in data)

    def _unwrap(self, value: Any):
        data = unwrap(value)
        return self._to_row(data)  # type: ignore

    def execute(self, sql, parameters=None, **kwargs):
        logger.info("Cursor.execute() called")
        logger.debug("sql=%s", sql)
        logger.debug("kwargs=%s", kwargs)

        try:
            res_create = _CreateAdapter.validate_json(sql)
            # do not return anything nor alter the state of the cursor
            dynamodbc.create_table(**res_create)
            return self._results
        except ValidationError:
            logger.info("Not a create table statement")
            pass

        try:
            in_insert = _InsertAdapter.validate_json(sql)
            out_insert = dynamodbc.put_item(**in_insert)
            self._update_description(out_insert)  # type: ignore
            self._update_cursor(out_insert)  # type: ignore
            return self._results
        except ValidationError:
            logger.info("Not an insert statement")
            pass

        try:
            in_execute = _ExecuteAdapter.validate_json(sql)
            out_execute = dynamodbc.execute_statement(**in_execute)
            self._update_description(out_execute)  # type: ignore
            self._update_cursor(out_execute)  # type: ignore
            return self._results
        except ValidationError:
            logger.info("Not an execute statement")
            pass

        raise Error(f"Invalid SQL: {sql}")

    def _update_cursor(self, response: dict[str, Any]):
        items = response.get("Items", [])

        self._results = [self._unwrap(i) for i in items]
        self._index = 0
        self._rowcount = len(self._results)

    def _update_description(self, response: dict[str, Any]):
        items = response.get("Items", [])

        descs = []

        for item in items:  # items
            for column, value in item.items():  # {"id": {"S": "1"}, ...}
                for typ in value:
                    desc = Description.from_dynamodb(column, typ)
                    descs.append(desc)
                    break  # just the first item is enough
            break  # just the first item is enough

        print(descs)
        print("=" * 5)
        self.description = descs

    def fetchone(self):
        logger.debug(f"{self._index=}")
        logger.info("Cursor.fetchone() called")

        if self._results is None:
            raise Error("No results available. Did you call execute()?")

        if self._index >= len(self._results):
            return None

        row = self._results[self._index]
        self._index += 1
        return row

    def fetchall(self):
        logger.debug(f"{self._index=}")
        logger.info("Cursor.fetchall() called")
        if self._results is None:
            raise Error("No results available. Did you call execute()?")

        if self._index >= len(self._results):
            return []

        rows = self._results[self._index :]
        self._index = len(self._results)
        logger.debug(f"{len(rows)=}")

        return rows

    @property
    def rowcount(self):
        logger.debug(f"{self._rowcount=}")
        logger.info("Cursor.rowcount property accessed")
        return self._rowcount

    def close(self):
        logger.info("Cursor.close() called")
        self._results = None
        self._index = 0
        self.description = None
        self._rowcount = -1
        self._closed = True


apilevel = "2.0"
threadsafety = 1
paramstyle = "pyformat"
