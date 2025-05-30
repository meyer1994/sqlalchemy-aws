import datetime as dt
import unittest

import boto3
import sqlalchemy as sa
from sqlalchemy.dialects import registry
from types_boto3_dynamodb import DynamoDBClient
from types_boto3_dynamodb.service_resource import DynamoDBServiceResource, Table

registry.register("dynamodb", "sqla.dynamodb.dialect", "DynamoDialect")


class Mixin(unittest.TestCase):
    dy_table: Table
    dy_client: DynamoDBClient
    dy_resource: DynamoDBServiceResource

    def setUp(self):
        self.dy_client = boto3.client(
            "dynamodb",
            endpoint_url="http://localhost:4566",
        )

        self.dy_resource = boto3.resource(
            "dynamodb",
            endpoint_url="http://localhost:4566",
        )

        time = dt.datetime.now(dt.UTC).isoformat()
        time = time.replace(":", "-")
        time = time.replace("+", "-")
        self.dy_table = self.dy_resource.create_table(
            TableName=f"test-{time}",
            KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}],
            ProvisionedThroughput={"ReadCapacityUnits": 1, "WriteCapacityUnits": 1},
        )
        self.dy_table.wait_until_exists()

    def dy_get(self, id: str):
        return self.dy_table.get_item(Key={"id": id})["Item"]

    def dy_scan(self):
        return self.dy_table.scan()["Items"]

    def dy_count(self):
        return len(self.dy_scan())

    def dy_insert(self, id: str):
        self.dy_table.put_item(Item={"id": id})


class TestDynamo(Mixin, unittest.TestCase):
    sa_engine: sa.Engine
    sa_table: sa.Table
    sa_metadata: sa.MetaData

    def setUp(self):
        super().setUp()

        self.sa_engine = sa.create_engine("dynamodb://")
        self.sa_metadata = sa.MetaData()
        self.sa_table = sa.Table(
            self.dy_table.name,
            self.sa_metadata,
            sa.Column("id", sa.String, primary_key=True),
        )

    def test_insert_one(self):
        dy_count = self.dy_count()

        with self.sa_engine.connect() as conn:
            q = sa.insert(self.sa_table).values(id="1")
            conn.execute(q)

        dy_count_after = self.dy_count()
        self.assertEqual(dy_count_after, dy_count + 1)

        dy_item = self.dy_get("1")
        self.assertDictEqual(dy_item, {"id": "1"})

    def test_insert_many(self):
        dy_count = self.dy_count()

        with self.sa_engine.connect() as conn:
            for i in range(10):
                q = sa.insert(self.sa_table).values(id=str(i))
                conn.execute(q)

        dy_count_after = self.dy_count()
        self.assertEqual(dy_count_after, dy_count + 10)

        dy_items = self.dy_scan()
        dy_items = sorted(dy_items, key=lambda x: x["id"])
        dy_expected = [{"id": str(i)} for i in range(10)]
        self.assertListEqual(dy_items, dy_expected)

    def test_select_one(self):
        with self.sa_engine.connect() as conn:
            q1 = sa.insert(self.sa_table).values(id="1")
            conn.execute(q1)

            q2 = sa.select(self.sa_table)
            result = conn.execute(q2)
            sa_items = result.fetchall()
            self.assertListEqual(list(sa_items), [("1",)])

        dy_item = self.dy_table.get_item(Key={"id": "1"})
        dy_item = dy_item["Item"]
        self.assertEqual(dy_item["id"], "1")

    def test_select_many(self):
        with self.sa_engine.connect() as conn:
            for i in range(10):
                q1 = sa.insert(self.sa_table).values(id=str(i))
                conn.execute(q1)

            q2 = sa.select(self.sa_table)
            result = conn.execute(q2)
            sa_items = result.fetchall()

            sa_items = sorted(sa_items, key=lambda x: x[0])
            sa_expected = [(str(i),) for i in range(10)]
            self.assertListEqual(sa_items, sa_expected)

        dy_items = self.dy_table.scan()
        dy_items = dy_items["Items"]
        dy_items = sorted(dy_items, key=lambda x: str(x["id"]))
        dy_expected = [{"id": str(i)} for i in range(10)]
        self.assertListEqual(dy_items, dy_expected)

    def test_select_where_by_id(self):
        with self.sa_engine.connect() as conn:
            for i in range(10):
                q1 = sa.insert(self.sa_table).values(id=str(i))
                conn.execute(q1)

            for i in range(10):
                q2 = sa.select(self.sa_table).where(self.sa_table.c.id == str(i))
                result = conn.execute(q2)
                sa_items = result.fetchall()
                self.assertListEqual(list(sa_items), [(str(i),)])

        dy_items = self.dy_scan()
        dy_items = sorted(dy_items, key=lambda x: str(x["id"]))
        dy_expected = [{"id": str(i)} for i in range(10)]
        self.assertListEqual(dy_items, dy_expected)
