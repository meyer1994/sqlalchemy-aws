import json
import logging

import boto3
import sqlalchemy as sa
from sqlalchemy.engine import default
from sqlalchemy.sql import compiler
from types_boto3_dynamodb.type_defs import (
    CreateTableInputTypeDef,
    ExecuteStatementInputTypeDef,
    PutItemInputTypeDef,
)

from . import dbapi

logger = logging.getLogger(__name__)

dynamodb = boto3.resource("dynamodb", endpoint_url="http://localhost:4566")
table = dynamodb.Table("TEST_TABLE")


class DynamoCompiler(compiler.SQLCompiler):
    def visit_insert(self, insert: sa.Insert, **kwargs) -> str:
        """
        The result of this method is going to be passed, as is, to the
        `cursor.execute()` method.
        """
        logger.info("visit_insert() called")

        item = {}
        for k, v in insert._values.items():
            v: sa.BindParameter = v
            item[k] = {"S": str(v.value)}

        data: PutItemInputTypeDef = {
            "TableName": insert.table.name,
            "Item": item,
        }

        return json.dumps(data)

    def visit_column(self, column: sa.Column, **kwargs) -> str:
        logger.info("visit_column() called")
        # this removes the table name from the column name
        # EG: "TEST_TABLE.id" -> "id"
        kwargs["include_table"] = False
        return super().visit_column(column, **kwargs)

    def visit_bindparam(self, bindparam: sa.BindParameter, **kwargs) -> str:
        logger.info("visit_bindparam() called")
        # very bad implementation, but it works for now
        # EG: ":id" -> "1"
        return repr(bindparam.value)

    def visit_select(self, select: sa.Select, **kwargs) -> str:
        logger.info("visit_select() called")
        select = super().visit_select(select, **kwargs)

        data: ExecuteStatementInputTypeDef = {
            "Statement": str(select),
        }

        return json.dumps(data)


class DynamoDDLCompiler(compiler.DDLCompiler):
    def visit_create_table(self, create: sa.schema.CreateTable, **kwargs) -> str:
        logger.info("visit_create_table() called")

        # Map SQLAlchemy types to DynamoDB types
        def _type(coltype):
            if isinstance(coltype, sa.String):
                return "S"
            elif isinstance(coltype, (sa.Integer, sa.Float, sa.Numeric)):
                return "N"
            elif isinstance(coltype, sa.LargeBinary):
                return "B"
            else:
                return "S"  # Default fallback

        # Extract columns and PKs
        columns: list[sa.Column] = list(create.target.columns)
        pk_columns = [col for col in columns if col.primary_key]
        logger.debug("pk_columns=%s", pk_columns)
        assert len(pk_columns) > 0, "DynamoDB requires at least one primary key"
        assert len(pk_columns) < 3, "DynamoDB only supports up to 2 primary keys"

        pk = pk_columns[0]
        rg = pk_columns[1] if len(pk_columns) > 1 else None

        pkt = _type(pk.type)
        rgt = _type(rg.type) if rg else None

        key_schema = [{"AttributeName": pk.name, "KeyType": "HASH"}]
        attr_schema = [{"AttributeName": pk.name, "AttributeType": pkt}]
        if rg:
            key_schema.append({"AttributeName": rg.name, "KeyType": "RANGE"})
            attr_schema.append({"AttributeName": rg.name, "AttributeType": rgt})

        table: sa.Table = create.target

        data: CreateTableInputTypeDef = {
            "TableName": table.name,
            "KeySchema": key_schema,
            "AttributeDefinitions": attr_schema,
            "ProvisionedThroughput": {
                "ReadCapacityUnits": 1,
                "WriteCapacityUnits": 1,
            },
        }

        return json.dumps(data)

    def get_column_specification(self, column, **kwargs):
        logger.info("get_column_specification() called")
        return super().get_column_specification(column, **kwargs)


class DynamoDialect(default.DefaultDialect):
    name = "dynamo"
    driver = "dynamodriver"
    dbapi_class = dbapi

    ddl_compiler = DynamoDDLCompiler
    statement_compiler = DynamoCompiler

    supports_statement_cache = False
    supports_schemas = False
    supports_alter = True

    @classmethod
    def import_dbapi(cls):
        return cls.dbapi_class

    def create_connect_args(self, url):
        return [], {}

    def has_table(self, connection, table_name, schema=None):
        return table_name in self.get_table_names(connection)

    def get_columns(self, connection, table_name, schema=None, **kw):
        logger.info("get_columns() called")
        logger.debug("connection=%s", connection)
        logger.debug("table_name=%s", table_name)
        logger.debug("schema=%s", schema)
        logger.debug("kw=%s", kw)

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
        logger.debug("connection=%s", connection)
        logger.debug("schema=%s", schema)
        logger.debug("kw=%s", kw)

        response = dynamodb.tables.all()
        logger.debug("response=%s", response)
        names = [i.name for i in response]
        logger.debug("names=%s", names)
        return names

    def get_pk_constraint(self, connection, table_name, schema=None, **kw):
        logger.info("get_pk_constraint() called")
        logger.debug("connection=%s", connection)
        logger.debug("table_name=%s", table_name)
        logger.debug("schema=%s", schema)
        logger.debug("kw=%s", kw)

        constrained_columns = [k["AttributeName"] for k in table.key_schema]
        return {
            "constrained_columns": constrained_columns,
            "name": "pk",  # DynamoDB does not name PK constraints
        }

    def get_foreign_keys(self, connection, table_name, schema=None, **kw):
        return []

    def get_indexes(self, connection, table_name, schema=None, **kw):
        logger.info("get_indexes() called")
        logger.debug("connection=%s", connection)
        logger.debug("table_name=%s", table_name)
        logger.debug("schema=%s", schema)
        logger.debug("kw=%s", kw)

        indexes = []

        # Global Secondary Indexes
        for idx in table.global_secondary_indexes or []:
            indexes.append(
                {
                    "name": idx["IndexName"],
                    "column_names": [k["AttributeName"] for k in idx["KeySchema"]],
                    "unique": False,  # DynamoDB indexes are not unique by default
                }
            )

        # Local Secondary Indexes
        for idx in table.local_secondary_indexes or []:
            indexes.append(
                {
                    "name": idx["IndexName"],
                    "column_names": [k["AttributeName"] for k in idx["KeySchema"]],
                    "unique": False,
                }
            )

        return indexes
