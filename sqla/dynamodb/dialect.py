import json
import logging

import boto3
import sqlalchemy as sa
import sqlalchemy.sql as sql
from sqlalchemy.engine import default
from types_boto3_dynamodb.type_defs import (
    CreateTableInputTypeDef,
    DeleteTableInputTypeDef,
)

from . import dbapi

logger = logging.getLogger(__name__)

dynamodb = boto3.resource("dynamodb", endpoint_url="http://localhost:4566")
table = dynamodb.Table("TEST_TABLE")


class DynamoSqlCompiler(sql.compiler.SQLCompiler):
    def visit_insert(self, insert: sa.Insert, **kwargs) -> str:
        """
        The result of this method is going to be passed, as is, to the
        `cursor.execute()` method.
        """
        logger.info("visit_insert() called")
        super().visit_insert(insert, **kwargs)

        params = []
        for name in self.params:
            name = self.escape_literal_column(name)
            params.append(name)

        # INSERT INTO "TEST_TABLE" VALUE { 'id': ?, 'name': ?, ... }
        query = f'INSERT INTO "{insert.table.name}" VALUE {{ '
        query += ", ".join(f"'{name}': ?" for name in params)
        query += " }"

        return query

    def visit_column(self, column: sa.Column, **kwargs) -> str:
        logger.info("visit_column() called")
        # this removes the table name from the column name
        # EG: "TEST_TABLE.id" -> "id"
        kwargs["include_table"] = False
        return super().visit_column(column, **kwargs)

    def visit_select(self, select: sa.Select, **kwargs) -> str:
        logger.info("visit_select() called")
        return super().visit_select(select, **kwargs)

    def visit_bindparam(self, bindparam: sa.BindParameter, **kwargs) -> str:
        logger.info("visit_bindparam() called")
        super().visit_bindparam(bindparam, **kwargs)
        return "?"


class DynamoDDLCompiler(sql.compiler.DDLCompiler):
    def visit_create_table(self, create: sa.schema.CreateTable, **kwargs) -> str:
        logger.info("visit_create_table() called")

        # Extract columns and PKs
        columns: list[sa.Column] = list(create.target.columns)
        pk_columns = [col for col in columns if col.primary_key]
        assert len(pk_columns) > 0, "DynamoDB requires at least one primary key"
        assert len(pk_columns) < 3, "DynamoDB only supports up to 2 primary keys"

        TYPES = {
            sa.String: "S",
        }

        if len(pk_columns) == 1:
            pk = pk_columns[0]
            pkt = TYPES[type(pk.type)]
            key_schema = [{"AttributeName": pk.name, "KeyType": "HASH"}]
            attr_schema = [{"AttributeName": pk.name, "AttributeType": pkt}]

        if len(pk_columns) == 2:
            hk, rk = pk_columns
            key_schema = [
                {"AttributeName": hk.name, "KeyType": "HASH"},
                {"AttributeName": rk.name, "KeyType": "RANGE"},
            ]
            attr_schema = [
                {"AttributeName": hk.name, "AttributeType": TYPES[type(hk.type)]},
                {"AttributeName": rk.name, "AttributeType": TYPES[type(rk.type)]},
            ]

        data: CreateTableInputTypeDef = {
            "TableName": create.target.name,
            "KeySchema": key_schema,
            "AttributeDefinitions": attr_schema,
            "ProvisionedThroughput": {
                "ReadCapacityUnits": 1,
                "WriteCapacityUnits": 1,
            },
        }

        return json.dumps(data)

    def visit_drop_table(self, drop: sa.schema.DropTable, **kw):
        logger.info("visit_drop_table() called")

        data: DeleteTableInputTypeDef = {
            "TableName": drop.target.name,
        }

        return json.dumps(data)

    def get_column_specification(self, column, **kwargs):
        logger.info("get_column_specification() called")
        return super().get_column_specification(column, **kwargs)


class DynamoDialect(default.DefaultDialect):
    name = "dynamodb"
    driver = "dynamodriver"
    dbapi_class = dbapi

    ddl_compiler = DynamoDDLCompiler
    statement_compiler = DynamoSqlCompiler

    supports_statement_cache = False
    supports_schemas = False

    @classmethod
    def import_dbapi(cls):
        logger.info("import_dbapi() called")
        return cls.dbapi_class

    def create_connect_args(self, url):
        logger.info("create_connect_args() called")
        return [], {}

    def has_table(self, connection, table_name, schema=None):
        logger.info("has_table() called")
        return table_name in self.get_table_names(connection)

    def get_columns(self, connection, table_name, schema=None, **kw):
        logger.info("get_columns() called")

        attribute_definitions = table.attribute_definitions
        key_attributes = {k["AttributeName"] for k in table.key_schema}

        type_map = {
            "S": sa.String,
            "N": sa.Float,
            "B": sa.LargeBinary,
        }

        columns = []
        for attr in attribute_definitions:
            attr_name = attr["AttributeName"]
            attr_type = type_map.get(attr["AttributeType"], sa.String)

            columns.append(
                {
                    "name": attr_name,
                    "type": attr_type,
                    "nullable": attr_name not in key_attributes,
                    "default": None,
                    "autoincrement": False,
                }
            )

        return columns

    def get_table_names(self, connection, schema=None, **kw):
        logger.info("get_table_names() called")
        response = dynamodb.tables.all()
        return [i.name for i in response]

    def get_pk_constraint(self, connection, table_name, schema=None, **kw):
        logger.info("get_pk_constraint() called")

        return {
            "constrained_columns": [k["AttributeName"] for k in table.key_schema],
            "name": "pk",  # DynamoDB does not name PK constraints
        }

    def get_foreign_keys(self, connection, table_name, schema=None, **kw):
        logger.info("get_foreign_keys() called")
        return []

    def get_indexes(self, connection, table_name, schema=None, **kw):
        logger.info("get_indexes() called")

        indexes = []

        # Global Secondary Indexes
        for idx in table.global_secondary_indexes or []:
            indexes.append(
                {
                    "name": idx["IndexName"],
                    "column_names": [k["AttributeName"] for k in idx["KeySchema"]],
                    "unique": True,  # DynamoDB indexes are not unique by default
                }
            )

        # Local Secondary Indexes
        for idx in table.local_secondary_indexes or []:
            indexes.append(
                {
                    "name": idx["IndexName"],
                    "column_names": [k["AttributeName"] for k in idx["KeySchema"]],
                    "unique": True,
                }
            )

        return indexes
