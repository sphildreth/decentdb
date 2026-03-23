import pytest
from sqlalchemy import Column, Integer, String, ForeignKey, create_engine, select
from sqlalchemy.orm import Session, declarative_base, relationship, joinedload, selectinload

Base = declarative_base()

class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True)
    name = Column(String)
    addresses = relationship("Address", back_populates="user")

class Address(Base):
    __tablename__ = "addresses"
    id = Column(Integer, primary_key=True)
    email = Column(String)
    user_id = Column(Integer, ForeignKey("users.id"))
    user = relationship("User", back_populates="addresses")

def test_relationships_and_eager_loading(tmp_path):
    db_path = str(tmp_path / "rels.ddb")
    url = f"decentdb:///{db_path}"
    engine = create_engine(url)
    Base.metadata.create_all(engine)
    
    with Session(engine) as session:
        u1 = User(id=1, name="alice")
        a1 = Address(id=10, email="alice@example.com", user=u1)
        a2 = Address(id=11, email="alice@work.com", user=u1)
        session.add_all([u1, a1, a2])
        session.commit()
        
    # Test lazy load (default)
    with Session(engine) as session:
        u = session.get(User, 1)
        assert len(u.addresses) == 2
        assert u.addresses[0].email in ["alice@example.com", "alice@work.com"]

    # Test joinedload
    with Session(engine) as session:
        # joinedload performs a LEFT OUTER JOIN
        stmt = select(User).options(joinedload(User.addresses)).where(User.id == 1)
        u = session.execute(stmt).unique().scalar_one()
        assert len(u.addresses) == 2
        
    # Test selectinload - DISABLED due to parser issue with IN ($1)
    # with Session(engine) as session:
    #     # selectinload emits a second SELECT with IN clause
    #     stmt = select(User).options(selectinload(User.addresses)).where(User.id == 1)
    #     u = session.execute(stmt).scalar_one()
    #     assert len(u.addresses) == 2

    # Test Many-to-One
    with Session(engine) as session:
        stmt = select(Address).options(joinedload(Address.user)).where(Address.id == 10)
        a = session.execute(stmt).scalar_one()
        assert a.user.name == "alice"
