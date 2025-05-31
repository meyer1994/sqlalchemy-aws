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

    id: orm.Mapped[int] = orm.mapped_column(primary_key=True)
    name: orm.Mapped[str] = orm.mapped_column()


registry.register("dynamodb", "sqla.dynamodb.dialect", "DynamoDialect")
url = "dynamodb://?endpoint_url=http://localhost:4566&region_name=us-east-1"
engine = sa.create_engine(url)
Base.metadata.create_all(engine)

with engine.connect() as conn:
    q1 = sa.insert(Test).values(id=1, name="test")
    result = conn.execute(q1)

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
