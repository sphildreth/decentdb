import pytest
from sqlalchemy import create_engine, text, select, inspect, MetaData, Table, Column, Integer, String
from sqlalchemy.orm import Session, declarative_base, Mapped, mapped_column

Base = declarative_base()

class User(Base):
    __tablename__ = "users"
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str]

def test_core_basic(db_path):
    engine = create_engine(f"decentdb+pysql:///{db_path}")
    
    with engine.connect() as conn:
        conn.execute(text("CREATE TABLE users (id INT64, name TEXT)"))
        conn.execute(text("INSERT INTO users VALUES (:id, :name)"), {"id": 1, "name": "Alice"})
        conn.commit()
        
        result = conn.execute(text("SELECT * FROM users"))
        row = result.fetchone()
        assert row[0] == 1
        assert row[1] == "Alice"

def test_orm_crud(db_path):
    # Setup schema
    engine = create_engine(f"decentdb+pysql:///{db_path}")
    with engine.connect() as conn:
        conn.execute(text("CREATE TABLE users (id INT64, name TEXT)"))
        conn.commit()
    
    # Session
    with Session(engine) as session:
        u1 = User(id=1, name="Bob")
        session.add(u1)
        session.commit()
        
    with Session(engine) as session:
        u = session.get(User, 1)
        assert u is not None
        assert u.name == "Bob"
        
        u.name = "Bobby"
        session.commit()
        
    with Session(engine) as session:
        u = session.get(User, 1)
        assert u.name == "Bobby"
        
        session.delete(u)
        session.commit()
        
    with Session(engine) as session:
        u = session.get(User, 1)
        assert u is None

def test_metadata_create_all(db_path):
    # This requires DDL compilation support in the dialect
    # DecentDB dialect supports basic types.
    
    engine = create_engine(f"decentdb+pysql:///{db_path}")
    metadata = MetaData()
    t = Table("items", metadata,
        Column("id", Integer, primary_key=True),
        Column("val", String)
    )
    
    # create_all should work if CREATE TABLE is supported
    metadata.create_all(engine)
    
    with engine.connect() as conn:
        conn.execute(t.insert().values(id=1, val="test"))
        conn.commit()
        
        res = conn.execute(t.select())
        row = res.fetchone()
        assert row.id == 1
        assert row.val == "test"

def test_limit_offset(db_path):
    engine = create_engine(f"decentdb+pysql:///{db_path}")
    with engine.connect() as conn:
        conn.execute(text("CREATE TABLE items (id INT64)"))
        for i in range(10):
            conn.execute(text(f"INSERT INTO items VALUES ({i})"))
        conn.commit()
        
    with Session(engine) as session:
        # 10 items: 0..9
        # Limit 3 Offset 2 -> 2, 3, 4
        stmt = select(User).select_from(text("items")).limit(3).offset(2)
        # Note: we are selecting User but from items table, columns might mismatch if we actually fetch User objects
        # Better use core select
        
    with engine.connect() as conn:
        stmt = select(text("id")).select_from(text("items")).order_by(text("id")).limit(3).offset(2)
        rows = conn.execute(stmt).fetchall()
        assert len(rows) == 3
        assert rows[0][0] == 2
        assert rows[2][0] == 4
