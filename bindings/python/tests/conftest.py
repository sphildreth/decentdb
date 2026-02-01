import pytest
import os
import decentdb
from sqlalchemy.dialects import registry

registry.register("decentdb.pysql", "decentdb_sqlalchemy.dialect", "DecentDbDialect")
registry.register("decentdb", "decentdb_sqlalchemy.dialect", "DecentDbDialect")

@pytest.fixture
def db_path(tmp_path):
    return str(tmp_path / "test.ddb")
