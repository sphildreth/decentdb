# DecentDB Python Bindings

This package provides:
1. `decentdb`: A DB-API 2.0 compliant driver for DecentDB.
2. `decentdb_sqlalchemy`: A SQLAlchemy 2.x dialect.

## Usage

```python
import sqlalchemy
from sqlalchemy import create_engine

# Use the decentdb dialect
engine = create_engine("decentdb+pysql:////path/to/database.db")

with engine.connect() as conn:
    conn.execute(sqlalchemy.text("CREATE TABLE IF NOT EXISTS users (id INT, name TEXT)"))
    conn.execute(sqlalchemy.text("INSERT INTO users VALUES (1, 'Alice')"))
    conn.commit()

    result = conn.execute(sqlalchemy.text("SELECT * FROM users"))
    for row in result:
        print(row)
```
