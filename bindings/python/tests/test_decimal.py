import pytest
import decimal
import decentdb
import os

@pytest.fixture
def db_path(tmp_path):
    return str(tmp_path / "test_decimal.db")

def test_decimal_roundtrip(db_path):
    conn = decentdb.connect(db_path)
    cur = conn.cursor()
    cur.execute("CREATE TABLE t_decimal_rt (d DECIMAL(18, 9))")
    
    # Test values
    vals = [
        decimal.Decimal("0.0"),
        decimal.Decimal("1.0"),
        decimal.Decimal("-1.0"),
        decimal.Decimal("123.456789012"),
        decimal.Decimal("-0.000000001"),
        decimal.Decimal("999999999.999999999"),
        decimal.Decimal("-999999999.999999999"),
    ]
    
    for v in vals:
        cur.execute("INSERT INTO t_decimal_rt VALUES (?)", (v,))
    
    cur.execute("SELECT d FROM t_decimal_rt")
    rows = cur.fetchall()
    
    assert len(rows) == len(vals)
    for i, row in enumerate(rows):
        # We expect 9 decimal places because column is DECIMAL(18,9)
        expected = vals[i].quantize(decimal.Decimal("1.000000000"))
        assert row[0] == expected
        assert isinstance(row[0], decimal.Decimal)
    
    conn.close()

def test_decimal_overflow(db_path):
    conn = decentdb.connect(db_path)
    cur = conn.cursor()
    cur.execute("CREATE TABLE t_overflow (d DECIMAL(18, 0))")
    
    # Too large for 64-bit unscaled
    v = decimal.Decimal("10000000000000000000") # 1e19 > 2^63
    
    with pytest.raises(decentdb.DataError):
        cur.execute("INSERT INTO t_overflow VALUES (?)", (v,))
    
    conn.close()
