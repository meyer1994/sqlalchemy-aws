import random
from pprint import pp

import sqlalchemy as sa
import sqlalchemy.orm as orm
from sqlalchemy.dialects import registry

from sqla import log

if True:
    log.init()


class Base(orm.DeclarativeBase):
    pass


class Test(Base):
    __tablename__ = f"TEST_TABLE_{random.randint(0, 1000000)}"

    id = sa.Column(sa.Integer, primary_key=True)
    name = sa.Column(sa.String)
    cool = sa.Column(sa.Integer)


registry.register("dynamodb", "sqla.dynamodb.dialect", "DynamoDialect")
url = "dynamodb://?endpoint_url=http://localhost:4566&region_name=us-east-1"
engine = sa.create_engine(url)
Base.metadata.create_all(engine)

with engine.connect() as conn:
    q1 = sa.insert(Test).values(id=1, name="test", cool=123)
    result = conn.execute(q1)

    q2 = sa.select(Test)
    q2 = q2.where(Test.id == 1)
    q2 = q2.where(Test.name == "test")
    result = conn.execute(q2)
    row = result.fetchone()

    q3 = sa.update(Test)
    q3 = q3.where(Test.id == 1)
    q3 = q3.values(name="test2")
    result = conn.execute(q3)

    q4 = sa.select(Test)
    q4 = q4.where(Test.id == 1)
    result = conn.execute(q4)
    row = result.fetchone()
    pp(row._asdict())  # type: ignore

    q5 = sa.delete(Test)
    q5 = q5.where(Test.id == 1)
    result = conn.execute(q5)

    q4 = sa.select(Test)
    q4 = q4.where(Test.id == 1)
    result = conn.execute(q4)
    row = result.fetchone()
    print(row)


raise SystemExit()

with engine.connect() as conn:
    q2 = sa.select(Test).where(Test.id == 1)
    result = conn.execute(q2)
    row = result.fetchone()
    pp(row._asdict())  # type: ignore

    q2 = sa.select(Test).where(Test.id == 2)
    result = conn.execute(q2)
    row = result.fetchone()
    print(row)

with engine.connect() as conn:
    for i in range(5, 15):
        q = sa.insert(Test).values(id=i, name=f"test {i}")
        conn.execute(q)

    for i in range(5, 15):
        q2 = sa.select(Test).where(Test.id == i)
        result = conn.execute(q2)
        row = result.fetchone()
        pp(row._asdict())  # type: ignore
