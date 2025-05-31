import datetime as dt
import os
import unittest

import boto3
import sqlalchemy as sa
import sqlalchemy.orm as orm
from botocore.exceptions import ClientError
from sqlalchemy.dialects import registry
from types_boto3_dynamodb import DynamoDBClient
from types_boto3_dynamodb.service_resource import DynamoDBServiceResource, Table

import sqla.dynamodb
import sqla.dynamodb.dialect

registry.register("dynamodb", "sqla.dynamodb.dialect", "DynamoDialect")


def _now():
    """
    Returns the current UTC time in ISO format.

    >>> _now()
    '2025-05-30T12:00:00.000000'
    """
    return dt.datetime.now(dt.UTC).isoformat()


def _name():
    """
    Generates a unique name for a test table

    It uses the current UTC time and replaces colons and plus signs with dashes.

    >>> _now()
    '2025-05-30T12:00:00.000000+00:00'
    >>> _name()
    'TEST_TABLE-2025-05-30T12-00-00-000000-00-00'
    """
    now = _now()
    now = now.replace(":", "-")
    now = now.replace("+", "-")
    return f"TEST_TABLE-{now}"


class Mixin(unittest.TestCase):
    client: DynamoDBClient
    resource: DynamoDBServiceResource
    engine: sa.Engine

    def setUp(self):
        super().setUp()
        region = os.environ.get("AWS_REGION", "us-east-1")
        endpoint = os.environ.get("AWS_ENDPOINT_URL", "http://localhost:4566")

        self.client = boto3.client(
            "dynamodb",
            region_name=region,
            endpoint_url=endpoint,
        )
        self.resource = boto3.resource(
            "dynamodb",
            region_name=region,
            endpoint_url=endpoint,
        )
        self.engine = sa.create_engine(
            f"dynamodb://?endpoint_url={endpoint}&region_name={region}"
        )


class TestDynamoSimple(Mixin, unittest.TestCase):
    """Tests the simple case of a table with a single primary key"""

    stable: sa.Table
    dtable: Table

    def setUp(self):
        super().setUp()

        name = f"TEST_TABLE-{_now()}"
        name = name.replace(":", "-")
        name = name.replace("+", "-")

        self.client.create_table(
            TableName=name,
            KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}],
            ProvisionedThroughput={"ReadCapacityUnits": 1, "WriteCapacityUnits": 1},
        )

        self.dtable = self.resource.Table(name)

        self.stable = sa.Table(
            name,
            sa.MetaData(),
            sa.Column("id", sa.String, primary_key=True),
        )

        self.client.get_waiter("table_exists").wait(TableName=name)

    def tearDown(self):
        self.dtable.delete()

    def test_insert_one(self):
        with self.engine.connect() as conn:
            q = sa.insert(self.stable).values(id="1")
            conn.execute(q)

        items = self.dtable.scan()["Items"]
        self.assertListEqual(items, [{"id": "1"}])

    def test_insert_many(self):
        with self.engine.connect() as conn:
            q = sa.insert(self.stable).values(id="1")
            conn.execute(q)
            q = sa.insert(self.stable).values(id="2")
            conn.execute(q)
            q = sa.insert(self.stable).values(id="3")
            conn.execute(q)

        items = self.dtable.scan()["Items"]
        items.sort(key=lambda x: x["id"])  # type: ignore
        self.assertListEqual(items, [{"id": "1"}, {"id": "2"}, {"id": "3"}])


class TestDynamoHashWithAttributes(Mixin, unittest.TestCase):
    """Tests the case of a table with a primary key and other attributes"""

    stable: sa.Table
    dtable: Table

    def setUp(self):
        super().setUp()

        name = f"TEST_TABLE-{_now()}"
        name = name.replace(":", "-")
        name = name.replace("+", "-")

        self.client.create_table(
            TableName=name,
            KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}],
            ProvisionedThroughput={"ReadCapacityUnits": 1, "WriteCapacityUnits": 1},
        )

        self.dtable = self.resource.Table(name)
        self.dtable.wait_until_exists()

        self.stable = sa.Table(
            name,
            sa.MetaData(),
            sa.Column("id", sa.String, primary_key=True),
            sa.Column("name", sa.String),
        )

    def tearDown(self):
        self.dtable.delete()
        self.dtable.wait_until_not_exists()

    def test_insert_one(self):
        with self.engine.connect() as conn:
            q = sa.insert(self.stable).values(id="1", name="John")
            conn.execute(q)

        items = self.dtable.scan()["Items"]
        self.assertListEqual(items, [{"id": "1", "name": "John"}])

    def test_insert_many(self):
        with self.engine.connect() as conn:
            q = sa.insert(self.stable).values(id="1", name="John")
            conn.execute(q)
            q = sa.insert(self.stable).values(id="2", name="Jane")
            conn.execute(q)
            q = sa.insert(self.stable).values(id="3", name="Jim")
            conn.execute(q)

        items = self.dtable.scan()["Items"]
        items.sort(key=lambda x: x["id"])  # type: ignore
        self.assertListEqual(
            items,
            [
                {"id": "1", "name": "John"},
                {"id": "2", "name": "Jane"},
                {"id": "3", "name": "Jim"},
            ],
        )


class TestDynamoHashRange(Mixin, unittest.TestCase):
    """Tests the case of a table with a hash and range key"""

    stable: sa.Table
    dtable: Table

    def setUp(self):
        super().setUp()

        name = f"TEST_TABLE-{_now()}"
        name = name.replace(":", "-")
        name = name.replace("+", "-")

        self.client.create_table(
            TableName=name,
            KeySchema=[
                {"AttributeName": "id", "KeyType": "HASH"},
                {"AttributeName": "ts", "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "id", "AttributeType": "S"},
                {"AttributeName": "ts", "AttributeType": "S"},
            ],
            ProvisionedThroughput={
                "ReadCapacityUnits": 1,
                "WriteCapacityUnits": 1,
            },
        )

        self.dtable = self.resource.Table(name)
        self.dtable.wait_until_exists()

        self.stable = sa.Table(
            name,
            sa.MetaData(),
            sa.Column("id", sa.String, primary_key=True),
            sa.Column("ts", sa.String, primary_key=True),
        )

    def tearDown(self):
        self.dtable.delete()
        self.dtable.wait_until_not_exists()

    def test_insert_one(self):
        with self.engine.connect() as conn:
            ts = _now()
            q = sa.insert(self.stable).values(id="1", ts=ts)
            conn.execute(q)

        items = self.dtable.scan()["Items"]
        self.assertListEqual(items, [{"id": "1", "ts": ts}])

    def test_insert_many(self):
        with self.engine.connect() as conn:
            ts = _now()
            q = sa.insert(self.stable).values(id="1", ts=ts)
            conn.execute(q)
            q = sa.insert(self.stable).values(id="2", ts=ts)
            conn.execute(q)
            q = sa.insert(self.stable).values(id="3", ts=ts)
            conn.execute(q)

        items = self.dtable.scan()["Items"]
        items.sort(key=lambda x: x["id"])  # type: ignore
        self.assertListEqual(
            items,
            [
                {"id": "1", "ts": ts},
                {"id": "2", "ts": ts},
                {"id": "3", "ts": ts},
            ],
        )


class TestDynamoHashRangeWithAttributes(Mixin, unittest.TestCase):
    """Tests the case of a table with a hash and range key and attributes"""

    stable: sa.Table
    dtable: Table

    def setUp(self):
        super().setUp()

        name = f"TEST_TABLE-{_now()}"
        name = name.replace(":", "-")
        name = name.replace("+", "-")

        self.client.create_table(
            TableName=name,
            KeySchema=[
                {"AttributeName": "id", "KeyType": "HASH"},
                {"AttributeName": "ts", "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "id", "AttributeType": "S"},
                {"AttributeName": "ts", "AttributeType": "S"},
            ],
            ProvisionedThroughput={
                "ReadCapacityUnits": 1,
                "WriteCapacityUnits": 1,
            },
        )

        self.dtable = self.resource.Table(name)
        self.dtable.wait_until_exists()

        self.stable = sa.Table(
            name,
            sa.MetaData(),
            sa.Column("id", sa.String, primary_key=True),
            sa.Column("ts", sa.String, primary_key=True),
            sa.Column("name", sa.String),
        )

    def tearDown(self):
        self.dtable.delete()
        self.dtable.wait_until_not_exists()

    def test_insert_one(self):
        with self.engine.connect() as conn:
            ts = _now()
            q = sa.insert(self.stable).values(id="1", ts=ts, name="John")
            conn.execute(q)

        items = self.dtable.scan()["Items"]
        self.assertListEqual(items, [{"id": "1", "ts": ts, "name": "John"}])

    def test_insert_many(self):
        with self.engine.connect() as conn:
            ts = _now()
            q = sa.insert(self.stable).values(id="1", ts=ts, name="John")
            conn.execute(q)
            q = sa.insert(self.stable).values(id="2", ts=ts, name="Jane")
            conn.execute(q)
            q = sa.insert(self.stable).values(id="3", ts=ts, name="Jim")
            conn.execute(q)

        items = self.dtable.scan()["Items"]
        items.sort(key=lambda x: x["id"])  # type: ignore
        self.assertListEqual(
            items,
            [
                {"id": "1", "ts": ts, "name": "John"},
                {"id": "2", "ts": ts, "name": "Jane"},
                {"id": "3", "ts": ts, "name": "Jim"},
            ],
        )


class TestMeta(Mixin, unittest.TestCase):
    """Tests the case of a table with a hash and range key and attributes"""

    def test_create(self):
        name = f"TEST_TABLE-{_now()}"
        name = name.replace(":", "-")
        name = name.replace("+", "-")

        table = sa.Table(
            name,
            sa.MetaData(),
            sa.Column("id", sa.String, primary_key=True),
        )

        table.create(self.engine)

        table = self.resource.Table(name)
        table.wait_until_exists()
        self.addCleanup(table.wait_until_not_exists)
        self.addCleanup(table.delete)

        self.assertListEqual(
            table.key_schema,
            [{"AttributeName": "id", "KeyType": "HASH"}],
        )

        self.assertListEqual(
            table.attribute_definitions,
            [{"AttributeName": "id", "AttributeType": "S"}],
        )

    def test_delete(self):
        name = f"TEST_TABLE-{_now()}"
        name = name.replace(":", "-")
        name = name.replace("+", "-")

        table = sa.Table(
            name,
            sa.MetaData(),
            sa.Column("id", sa.String, primary_key=True),
        )
        table.create(self.engine)
        table.drop(self.engine)

        with self.assertRaises(ClientError) as e:
            self.resource.Table(name).load()

        self.assertIn("ResourceNotFoundException", str(e.exception))
        self.assertIn("Cannot do operations on a non-existent table", str(e.exception))

    def test_create_hash_table(self):
        name = f"TEST_TABLE-{_now()}"
        name = name.replace(":", "-")
        name = name.replace("+", "-")

        meta = sa.MetaData()
        sa.Table(name, meta, sa.Column("id", sa.String, primary_key=True))
        meta.create_all(self.engine)

        table = self.resource.Table(name)
        table.wait_until_exists()
        self.addCleanup(table.wait_until_not_exists)
        self.addCleanup(table.delete)

        self.assertListEqual(
            table.key_schema,
            [{"AttributeName": "id", "KeyType": "HASH"}],
        )

        self.assertListEqual(
            table.attribute_definitions,
            [{"AttributeName": "id", "AttributeType": "S"}],
        )

    def test_create_hash_range_table(self):
        name = f"TEST_TABLE-{_now()}"
        name = name.replace(":", "-")
        name = name.replace("+", "-")

        meta = sa.MetaData()
        sa.Table(
            name,
            meta,
            # order matters!
            sa.Column("ts", sa.String, primary_key=True),
            sa.Column("id", sa.String, primary_key=True),
        )
        meta.create_all(self.engine)

        table = self.resource.Table(name)
        table.wait_until_exists()
        self.addCleanup(table.wait_until_not_exists)
        self.addCleanup(table.delete)

        self.assertListEqual(
            table.key_schema,
            [
                {"AttributeName": "ts", "KeyType": "HASH"},
                {"AttributeName": "id", "KeyType": "RANGE"},
            ],
        )

        self.assertListEqual(
            table.attribute_definitions,
            [
                {"AttributeName": "ts", "AttributeType": "S"},
                {"AttributeName": "id", "AttributeType": "S"},
            ],
        )

    def test_create_table_hash_with_attributes(self):
        name = f"TEST_TABLE-{_now()}"
        name = name.replace(":", "-")
        name = name.replace("+", "-")

        meta = sa.MetaData()
        sa.Table(
            name,
            meta,
            sa.Column("id", sa.String, primary_key=True),
            sa.Column("name", sa.String),
        )
        meta.create_all(self.engine)

        table = self.resource.Table(name)
        table.wait_until_exists()
        self.addCleanup(table.wait_until_not_exists)
        self.addCleanup(table.delete)

        self.assertListEqual(
            table.key_schema,
            [{"AttributeName": "id", "KeyType": "HASH"}],
        )

        self.assertListEqual(
            table.attribute_definitions,
            [{"AttributeName": "id", "AttributeType": "S"}],
        )

    def test_create_table_hash_range_with_attributes(self):
        name = f"TEST_TABLE-{_now()}"
        name = name.replace(":", "-")
        name = name.replace("+", "-")

        meta = sa.MetaData()
        sa.Table(
            name,
            meta,
            sa.Column("id", sa.String, primary_key=True),
            sa.Column("ts", sa.String, primary_key=True),
            sa.Column("name1", sa.String),
            sa.Column("name2", sa.String),
            sa.Column("name3", sa.String),
            sa.Column("name4", sa.String),
            sa.Column("name5", sa.String),
        )
        meta.create_all(self.engine)

        table = self.resource.Table(name)
        table.wait_until_exists()
        self.addCleanup(table.wait_until_not_exists)
        self.addCleanup(table.delete)

        self.assertListEqual(
            table.key_schema,
            [
                {"AttributeName": "id", "KeyType": "HASH"},
                {"AttributeName": "ts", "KeyType": "RANGE"},
            ],
        )

        self.assertListEqual(
            table.attribute_definitions,
            [
                {"AttributeName": "id", "AttributeType": "S"},
                {"AttributeName": "ts", "AttributeType": "S"},
            ],
        )

    def test_create_table_typed_hash_key(self):
        for sa_type, dy_type in sqla.dynamodb.dialect.TYPES_SA_TO_DYNAMODB.items():
            with self.subTest(sa_type=sa_type, dy_type=dy_type):
                self._test_create_table_typed_hash_key(sa_type, dy_type)

    def _test_create_table_typed_hash_key(
        self,
        sa_type: type[sa.types.TypeEngine],
        dy_type: str,
    ):
        name = f"TEST_TABLE-{_now()}"
        name = name.replace(":", "-")
        name = name.replace("+", "-")

        meta = sa.MetaData()
        sa.Table(name, meta, sa.Column("id", sa_type, primary_key=True))
        meta.create_all(self.engine)

        table = self.resource.Table(name)
        table.wait_until_exists()
        self.addCleanup(table.wait_until_not_exists)
        self.addCleanup(table.delete)

        self.assertListEqual(
            table.key_schema,
            [{"AttributeName": "id", "KeyType": "HASH"}],
        )

        self.assertListEqual(
            table.attribute_definitions,
            [{"AttributeName": "id", "AttributeType": dy_type}],
        )

    def test_create_table_typed_range_key(self):
        for sa_type, dy_type in sqla.dynamodb.dialect.TYPES_SA_TO_DYNAMODB.items():
            with self.subTest(sa_type=sa_type, dy_type=dy_type):
                self._test_create_table_typed_range_key(sa_type, dy_type)

    def _test_create_table_typed_range_key(
        self,
        sa_type: type[sa.types.TypeEngine],
        dy_type: str,
    ):
        name = f"TEST_TABLE-{_now()}"
        name = name.replace(":", "-")
        name = name.replace("+", "-")

        meta = sa.MetaData()
        sa.Table(
            name,
            meta,
            sa.Column("id", sa.String, primary_key=True),
            sa.Column("ts", sa_type, primary_key=True),
        )
        meta.create_all(self.engine)

        table = self.resource.Table(name)
        table.wait_until_exists()
        self.addCleanup(table.wait_until_not_exists)
        self.addCleanup(table.delete)

        self.assertListEqual(
            table.key_schema,
            [
                {"AttributeName": "id", "KeyType": "HASH"},
                {"AttributeName": "ts", "KeyType": "RANGE"},
            ],
        )

        self.assertListEqual(
            table.attribute_definitions,
            [
                {"AttributeName": "id", "AttributeType": "S"},
                {"AttributeName": "ts", "AttributeType": dy_type},
            ],
        )

    def test_create_create_table_with_attributes(self):
        name = f"TEST_TABLE-{_now()}"
        name = name.replace(":", "-")
        name = name.replace("+", "-")

        columns: list[sa.Column] = []
        for k, v in sqla.dynamodb.dialect.TYPES_SA_TO_DYNAMODB.items():
            columns.append(sa.Column(str(k), k))

        meta = sa.MetaData()
        sa.Table(name, meta, sa.Column("id", sa.String, primary_key=True), *columns)
        meta.create_all(self.engine)


class TestOrm(Mixin, unittest.TestCase):
    """Tests the case of a table with a hash and range key and attributes"""

    def test_orm(self):
        class Base(orm.DeclarativeBase):
            pass

        class Test(Base):
            __tablename__ = _name()
            id = orm.mapped_column(sa.Integer, primary_key=True)
            name = orm.mapped_column(sa.String)

        Base.metadata.create_all(self.engine)

        table = self.resource.Table(Test.__tablename__)
        table.wait_until_exists()
        self.addCleanup(table.wait_until_not_exists)
        self.addCleanup(table.delete)

        self.assertListEqual(
            table.key_schema,
            [{"AttributeName": "id", "KeyType": "HASH"}],
        )

        self.assertListEqual(
            table.attribute_definitions,
            [{"AttributeName": "id", "AttributeType": "N"}],
        )


class TestSelect(Mixin, unittest.TestCase):
    """Tests the case of a table with a hash and range key and attributes"""

    stable: sa.Table
    dtable: Table

    def setUp(self):
        super().setUp()

        name = f"TEST_TABLE-{_now()}"
        name = name.replace(":", "-")
        name = name.replace("+", "-")

        self.client.create_table(
            TableName=name,
            KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}],
            ProvisionedThroughput={"ReadCapacityUnits": 1, "WriteCapacityUnits": 1},
        )

        self.dtable = self.resource.Table(name)
        self.dtable.wait_until_exists()

        self.stable = sa.Table(
            name,
            sa.MetaData(),
            sa.Column("id", sa.String, primary_key=True),
        )

    def tearDown(self):
        self.dtable.delete()
        self.dtable.wait_until_not_exists()

    def test_select_all(self):
        self.dtable.put_item(Item={"id": "1"})
        self.dtable.put_item(Item={"id": "2"})
        self.dtable.put_item(Item={"id": "3"})

        with self.engine.connect() as conn:
            q = sa.select(self.stable)
            result = conn.execute(q)
            result = sorted(result)

        items = [i._asdict() for i in result]
        self.assertListEqual(items, [{"id": "1"}, {"id": "2"}, {"id": "3"}])

    def test_select_one(self):
        self.dtable.put_item(Item={"id": "1"})
        self.dtable.put_item(Item={"id": "2"})
        self.dtable.put_item(Item={"id": "3"})

        with self.engine.connect() as conn:
            q = sa.select(self.stable).where(self.stable.c.id == "1")
            result = conn.execute(q)
            result = sorted(result)

        items = [i._asdict() for i in result]
        self.assertListEqual(items, [{"id": "1"}])
