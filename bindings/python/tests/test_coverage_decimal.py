import pytest
import datetime
import decimal
import decentdb
import ctypes
from decentdb import _format_params_for_error, _format_value_for_error, ProgrammingError, DataError

def test_decimal_coverage(db_path):
    conn = decentdb.connect(db_path)
    cur = conn.cursor()
    cur.execute("CREATE TABLE dt_cov_real (d DECIMAL(10,4))")
    
    with pytest.raises(DataError):
        cur.execute("INSERT INTO dt_cov_real VALUES (?)", (decimal.Decimal("NaN"),))
        
    with pytest.raises(DataError):
        cur.execute("INSERT INTO dt_cov_real VALUES (?)", (decimal.Decimal("Infinity"),))
        
    # Exponent < 0 implies positive scale (-exponent > 0 doesn't trigger scale < 0)
    # Exponent > 0 implies scale < 0
    cur.execute("INSERT INTO dt_cov_real VALUES (?)", (decimal.Decimal("1.23E+5"),))
    
    # Scale > 18
    long_dec = decimal.Decimal("0.12345678901234567890123456789")
    cur.execute("INSERT INTO dt_cov_real VALUES (?)", (long_dec,))
