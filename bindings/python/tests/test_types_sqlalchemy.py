import pytest
import sqlalchemy
from sqlalchemy import Column, Integer, String, Date, DateTime, Time, Numeric, Uuid, Enum, create_engine, select
from sqlalchemy.orm import Session, declarative_base
import datetime
import decimal
import uuid
import enum

Base = declarative_base()

class MyEnum(enum.Enum):
    A = "a"
    B = "b"

class AllTypes(Base):
    __tablename__ = "all_types"
    id = Column(Integer, primary_key=True)
    d = Column(Date)
    dt = Column(DateTime)
    t = Column(Time)
    n = Column(Numeric(10, 2))
    u = Column(Uuid)
    e = Column(Enum(MyEnum))

def test_sqlalchemy_types(tmp_path):
    db_path = str(tmp_path / "sa_types.db")
    url = f"decentdb:///{db_path}"
    engine = create_engine(url)
    
    Base.metadata.create_all(engine)
    
    today = datetime.date.today()
    now = datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0) # Round to ms or sec? DB stores ms.
    # Python datetime has us.
    # Our impl: (value.timestamp() * 1000).
    # This keeps ms precision.
    now = now.replace(microsecond=(now.microsecond // 1000) * 1000)
    
    t = datetime.time(12, 34, 56, 123000) # 123ms
    dec = decimal.Decimal("123.45")
    u = uuid.uuid4()
    
    obj = AllTypes(id=1, d=today, dt=now, t=t, n=dec, u=u, e=MyEnum.A)
    
    with Session(engine) as session:
        session.add(obj)
        session.commit()
    
    with Session(engine) as session:
        o = session.execute(select(AllTypes)).scalar_one()
        assert o.d == today
        # DateTime comparison: ensure timezone awareness matches or compare timestamps
        # Our result processor returns naive UTC or aware?
        # My impl: if self.timezone: return dt else: return dt.replace(tzinfo=None)
        # Default DateTime timezone=False. So it returns naive.
        # But `now` was aware UTC.
        # `o.dt` should be naive UTC (equivalent).
        assert o.dt == now.replace(tzinfo=None)
        assert o.t == t
        assert o.n == dec
        assert o.u == u
        assert o.e == MyEnum.A

def test_sqlalchemy_core_types(tmp_path):
    db_path = str(tmp_path / "sa_core.db")
    url = f"decentdb:///{db_path}"
    engine = create_engine(url)
    
    metadata = sqlalchemy.MetaData()
    t = sqlalchemy.Table("core_types", metadata,
        Column("id", Integer, primary_key=True),
        Column("val", Numeric)
    )
    metadata.create_all(engine)
    
    with engine.begin() as conn:
        conn.execute(t.insert(), {"id": 1, "val": decimal.Decimal("99.99")})
        
    with engine.connect() as conn:
        row = conn.execute(t.select()).fetchone()
        assert row.val == decimal.Decimal("99.99")
