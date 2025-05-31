import json
import logging
from typing import Any

import boto3
import sqlalchemy as sa
import sqlalchemy.sql as sql
from sqlalchemy.engine import default
from types_boto3_dynamodb.client import DynamoDBClient
from types_boto3_dynamodb.literals import ScalarAttributeTypeType
from types_boto3_dynamodb.type_defs import (
    AttributeDefinitionTypeDef,
    CreateTableInputTypeDef,
    DeleteTableInputTypeDef,
    KeySchemaElementTypeDef,
    TableDescriptionTypeDef,
)

from sqla.dynamodb import dbapi

logger = logging.getLogger(__name__)


TYPES_SA_TO_DYNAMODB: dict[type[sa.types.TypeEngine], ScalarAttributeTypeType] = {
    # numbers
    sa.Integer: "N",
    sa.BigInteger: "N",
    sa.SmallInteger: "N",
    sa.Numeric: "N",
    sa.Float: "N",
    # strings
    sa.String: "S",
    sa.Text: "S",
    sa.Enum: "S",
    sa.UUID: "S",
    sa.DateTime: "S",
    # booleans
    # sa.Boolean: "BOOL",
}


class DynamoSqlCompiler(sql.compiler.SQLCompiler):
    def visit_insert(
        self,
        insert_stmt: sa.Insert,
        visited_bindparam: Any | None = None,
        visiting_cte: Any | None = None,
        **kwargs,
    ) -> str:
        """
        The result of this method is going to be passed, as is, to the
        `cursor.execute()` method.
        """
        logger.info("visit_insert() called")
        super().visit_insert(insert_stmt, visited_bindparam, visiting_cte, **kwargs)

        params: list[str] = []
        for name in self.params:
            name = self.escape_literal_column(name)
            params.append(name)

        table: str = self.escape_literal_column(insert_stmt.table.name)

        # INSERT INTO "TEST_TABLE" VALUE { 'id': ?, 'name': ?, ... }
        query = f'INSERT INTO "{table}" VALUE {{'
        query += ", ".join(f"'{name}': ?" for name in params)
        query += " }"

        return query

    def visit_column(
        self,
        column: sa.ColumnClause[Any],
        add_to_result_map: Any | None = None,
        include_table: bool = True,
        result_map_targets: tuple[Any, ...] = (),
        ambiguous_table_name_map: Any | None = None,
        **kwargs,
    ) -> str:
        logger.info("visit_column() called")
        # this removes the table name from the column name
        # EG: "TEST_TABLE.id" -> "id"
        return super().visit_column(
            column,
            add_to_result_map,
            False,
            result_map_targets,
            ambiguous_table_name_map,
            **kwargs,
        )

    def visit_select(
        self,
        select_stmt: sa.Select,
        asfrom: bool = False,
        insert_into: bool = False,
        fromhints: Any | None = None,
        compound_index: Any | None = None,
        select_wraps_for: Any | None = None,
        lateral: bool = False,
        from_linter: Any | None = None,
        **kwargs,
    ) -> str:
        logger.info("visit_select() called")
        return super().visit_select(
            select_stmt,
            asfrom,
            insert_into,
            fromhints,
            compound_index,
            select_wraps_for,
            lateral,
            from_linter,
            **kwargs,
        )

    def visit_bindparam(
        self,
        bindparam: sa.BindParameter,
        within_columns_clause: bool = False,
        literal_binds: bool = False,
        skip_bind_expression: bool = False,
        literal_execute: bool = False,
        render_postcompile: bool = False,
        **kwargs,
    ) -> str:
        logger.info("visit_bindparam() called")
        super().visit_bindparam(
            bindparam,
            within_columns_clause,
            literal_binds,
            skip_bind_expression,
            literal_execute,
            render_postcompile,
            **kwargs,
        )
        return "?"

    def visit_update(
        self,
        update_stmt: sa.Update,
        visiting_cte: Any | None = None,
        **kwargs,
    ) -> str:
        logger.info("visit_update() called")
        return super().visit_update(update_stmt, visiting_cte, **kwargs)

    def visit_delete(
        self,
        delete_stmt: sa.Delete,
        visiting_cte: Any | None = None,
        **kwargs,
    ) -> str:
        logger.info("visit_delete() called")
        return super().visit_delete(delete_stmt, visiting_cte, **kwargs)


class DynamoDDLCompiler(sql.compiler.DDLCompiler):
    def visit_create_table(self, create: sa.schema.CreateTable, **kwargs) -> str:
        logger.info("visit_create_table() called")

        # Extract columns and PKs
        assert isinstance(create.target, sa.Table), "DynamoDB requires a table"
        columns: list[sa.Column] = list(create.target.columns)
        pk_columns = [col for col in columns if col.primary_key]
        assert len(pk_columns) > 0, "DynamoDB requires at least one primary key"
        assert len(pk_columns) < 3, "DynamoDB only supports up to 2 primary keys"

        key_schema: list[KeySchemaElementTypeDef] = []
        attr_schema: list[AttributeDefinitionTypeDef] = []

        if len(pk_columns) == 1:
            hk = pk_columns[0]
            hkt = TYPES_SA_TO_DYNAMODB[type(hk.type)]
            key_schema.append({"AttributeName": hk.name, "KeyType": "HASH"})
            attr_schema.append({"AttributeName": hk.name, "AttributeType": hkt})

        if len(pk_columns) == 2:
            hk, rk = pk_columns
            hkt = TYPES_SA_TO_DYNAMODB[type(hk.type)]
            rkt = TYPES_SA_TO_DYNAMODB[type(rk.type)]
            key_schema.append({"AttributeName": hk.name, "KeyType": "HASH"})
            key_schema.append({"AttributeName": rk.name, "KeyType": "RANGE"})
            attr_schema.append({"AttributeName": hk.name, "AttributeType": hkt})
            attr_schema.append({"AttributeName": rk.name, "AttributeType": rkt})

        data: CreateTableInputTypeDef = {
            "TableName": create.target.name,
            "KeySchema": key_schema,
            "AttributeDefinitions": attr_schema,
            "ProvisionedThroughput": {"ReadCapacityUnits": 1, "WriteCapacityUnits": 1},
        }

        return json.dumps(data)

    def visit_drop_table(self, drop: sa.schema.DropTable, **kw):
        logger.info("visit_drop_table() called")
        assert isinstance(drop.target, sa.Table), "DynamoDB requires a table"
        data: DeleteTableInputTypeDef = {"TableName": drop.target.name}
        return json.dumps(data)


class DynamoTypeCompiler(sql.compiler.GenericTypeCompiler):
    """Leaving this here for reference"""

    pass


class DynamoDialect(default.DefaultDialect):
    name = "dynamodb"
    driver = "dynamodriver"

    ddl_compiler = DynamoDDLCompiler
    statement_compiler = DynamoSqlCompiler

    supports_statement_cache = False
    supports_schemas = False

    @classmethod
    def import_dbapi(cls) -> Any:
        logger.info("import_dbapi() called")
        return dbapi

    def create_connect_args(self, url: sa.URL) -> tuple[tuple[DynamoDBClient], dict]:
        logger.info("create_connect_args() called")
        region = url.query.get("region_name", "us-east-1")
        endpoint = url.query.get("endpoint_url", "http://localhost:4566")
        client = boto3.client("dynamodb", endpoint_url=endpoint, region_name=region)
        return (client,), {}

    def _describe_table(
        self,
        connection: dbapi.Connection,
        name: str,
    ) -> TableDescriptionTypeDef:
        response = connection.connection.client.describe_table(TableName=name)
        return response["Table"]

    def has_table(
        self,
        connection: dbapi.Connection,
        table_name: str,
        schema: str | None = None,
        **kw,
    ) -> bool:
        logger.info("has_table() called")
        return table_name in self.get_table_names(connection, schema, **kw)

    def get_columns(
        self,
        connection: dbapi.Connection,
        table_name: str,
        schema: str | None = None,
        **kw,
    ) -> list[sa.engine.interfaces.ReflectedColumn]:
        logger.info("get_columns() called")

        type_map: dict[ScalarAttributeTypeType, type[sa.types.TypeEngine[Any]]] = {
            "S": sa.String,
            "N": sa.Float,
            "B": sa.LargeBinary,
        }

        table = self._describe_table(connection, table_name)
        columns: list[sa.engine.interfaces.ReflectedColumn] = []

        for attr in table["AttributeDefinitions"]:
            attr_type = type_map.get(attr["AttributeType"], sa.String)

            columns.append(
                {
                    "name": attr["AttributeName"],
                    "type": attr_type(),
                    "nullable": False,
                    "default": None,
                    "autoincrement": False,
                }
            )

        return columns

    def get_table_names(
        self,
        connection: dbapi.Connection,
        schema: str | None = None,
        **kw,
    ) -> list[str]:
        logger.info("get_table_names() called")
        tables = connection.connection.client.list_tables()
        return tables["TableNames"]

    def get_pk_constraint(
        self,
        connection: dbapi.Connection,
        table_name: str,
        schema: str | None = None,
        **kw,
    ) -> sa.engine.interfaces.ReflectedPrimaryKeyConstraint:
        logger.info("get_pk_constraint() called")

        table = self._describe_table(connection, table_name)

        if len(table["KeySchema"]) == 1:
            return {
                "constrained_columns": [table["KeySchema"][0]["AttributeName"]],
                "name": "HASH",  # DynamoDB does not name PK constraints
            }

        if len(table["KeySchema"]) == 2:
            return {
                "constrained_columns": [
                    table["KeySchema"][0]["AttributeName"],
                    table["KeySchema"][1]["AttributeName"],
                ],
                "name": "HASH_RANGE",  # DynamoDB does not name PK constraints
            }

        raise ValueError("DynamoDB only supports up to 2 primary keys")

    def get_foreign_keys(
        self,
        connection: dbapi.Connection,
        table_name: str,
        schema: str | None = None,
        **kw,
    ) -> list[sa.engine.interfaces.ReflectedForeignKeyConstraint]:
        logger.info("get_foreign_keys() called")
        return []

    def get_indexes(
        self,
        connection: dbapi.Connection,
        table_name: str,
        schema: str | None = None,
        **kw,
    ) -> list[sa.engine.interfaces.ReflectedIndex]:
        logger.info("get_indexes() called")

        table = self._describe_table(connection, table_name)
        indexes = []

        # Global Secondary Indexes
        for idx in table["GlobalSecondaryIndexes"] or []:
            indexes.append(
                {
                    "name": idx["IndexName"],
                    "column_names": [k["AttributeName"] for k in idx["KeySchema"]],
                    "unique": True,
                }
            )

        # Local Secondary Indexes
        for idx in table["LocalSecondaryIndexes"] or []:
            indexes.append(
                {
                    "name": idx["IndexName"],
                    "column_names": [k["AttributeName"] for k in idx["KeySchema"]],
                    "unique": True,
                }
            )

        return indexes
