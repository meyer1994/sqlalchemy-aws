import logging.config
import random
import uuid

import sqlalchemy as sa
import sqlalchemy.orm as orm
from sqlalchemy.dialects import registry

logging.config.dictConfig(
    {
        "version": 1,
        "formatters": {
            "detailed": {
                "format": "[%(asctime)s] [%(levelname)-8s] [%(name)s:%(lineno)d] - %(message)s",
                "datefmt": "%Y-%m-%d %H:%M:%S",
            },
        },
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "level": "DEBUG",
                "formatter": "detailed",
            },
        },
        "loggers": {
            "sqla": {"level": "DEBUG", "handlers": ["console"]},
            # "sqlalchemy": {"level": "DEBUG", "handlers": ["console"]},
        },
    }
)


class Base(orm.DeclarativeBase):
    pass


class Test(Base):
    __tablename__ = f"TEST_TABLE_{random.randint(0, 1000000)}"

    id = sa.Column(sa.String, primary_key=True)
    name = sa.Column(sa.String)


registry.register("dynamo", "sqla.dyanmo.dialect", "DynamoDialect")
engine = sa.create_engine("dynamo://")

Base.metadata.create_all(engine)


table = sa.Table("TEST_TABLE", Base.metadata, autoload_with=engine)
for column in table.columns:
    print(column.name, column.type)
    print(column.name, column.type)
    print(column.name, column.type)
    print(column.name, column.type)

select = sa.select(table)
with engine.connect() as conn:
    result = conn.execute(select)
    for row in result.fetchall():
        print(row._asdict())

select = sa.select(table)
select = select.where(table.c.id == "test")
with engine.connect() as conn:
    result = conn.execute(select)
    res = result.fetchone()
    print(res._asdict())

insert = sa.insert(table).values(id="%s" % uuid.uuid4())
with engine.connect() as conn:
    result = conn.execute(insert)
    for row in result:
        print(row._asdict())

insert = sa.insert(table).values(id="%s" % uuid.uuid4(), name="test")
with engine.connect() as conn:
    result = conn.execute(insert)
    for row in result:
        print(row._asdict())
raise SystemExit


# update = sa.update(table).where(table.c.id == "test").values(id="%s" % uuid.uuid4())
# with engine.connect() as conn:
#     result = conn.execute(update)
#     for row in result:
#         print(row)
