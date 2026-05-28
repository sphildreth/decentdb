#!/usr/bin/env python3
import ctypes
import json
import os
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
DEFAULT_LIB = ROOT / "target" / "debug" / "libdecentdb.so"
LIB_PATH = Path(os.environ.get("DECENTDB_NATIVE_LIB", DEFAULT_LIB))


class DdbValue(ctypes.Structure):
    _fields_ = [
        ("tag", ctypes.c_uint32),
        ("bool_value", ctypes.c_uint8),
        ("reserved0", ctypes.c_uint8 * 7),
        ("int64_value", ctypes.c_int64),
        ("float64_value", ctypes.c_double),
        ("decimal_scaled", ctypes.c_int64),
        ("decimal_scale", ctypes.c_uint8),
        ("reserved1", ctypes.c_uint8 * 7),
        ("data", ctypes.POINTER(ctypes.c_uint8)),
        ("len", ctypes.c_size_t),
        ("uuid_bytes", ctypes.c_uint8 * 16),
        ("timestamp_micros", ctypes.c_int64),
        ("enum_type_id", ctypes.c_uint64),
        ("enum_label_id", ctypes.c_uint64),
        ("ip_family", ctypes.c_uint8),
        ("cidr_prefix_len", ctypes.c_uint8),
        ("reserved2", ctypes.c_uint8 * 6),
        ("ip_cidr_addr_bytes", ctypes.c_uint8 * 16),
        ("date_days", ctypes.c_int32),
        ("time_micros", ctypes.c_int64),
        ("timestamptz_micros", ctypes.c_int64),
        ("interval_months", ctypes.c_int32),
        ("interval_days", ctypes.c_int32),
        ("interval_micros", ctypes.c_int64),
    ]


class DdbWriteQueueMetrics(ctypes.Structure):
    _fields_ = [
        ("capacity", ctypes.c_size_t),
        ("current_depth", ctypes.c_size_t),
        ("admitted", ctypes.c_uint64),
        ("rejected", ctypes.c_uint64),
        ("timed_out", ctypes.c_uint64),
        ("canceled", ctypes.c_uint64),
        ("executed", ctypes.c_uint64),
        ("committed", ctypes.c_uint64),
        ("failed", ctypes.c_uint64),
        ("group_commit_batches", ctypes.c_uint64),
        ("group_commit_syncs", ctypes.c_uint64),
        ("group_commit_max_batch", ctypes.c_uint64),
        ("group_commit_commits_covered", ctypes.c_uint64),
        ("physical_syncs_saved", ctypes.c_uint64),
        ("total_queue_wait_ns", ctypes.c_uint64),
    ]


DDB_OK = 0
DDB_ERR_SQL = 5
DDB_ERR_BUSY = 9
DDB_ERR_TIMEOUT = 10
DDB_ERR_CANCELED = 11
DDB_ERR_QUEUE_FULL = 12
DDB_ERR_QUEUE_CLOSED = 13
DDB_WRITE_QUEUE_TIMEOUT_DEFAULT = ctypes.c_uint64(-1).value
DDB_VALUE_INT64 = 1
DDB_VALUE_TEXT = 4


def load_library():
    if not LIB_PATH.exists():
        raise SystemExit(f"native library not found: {LIB_PATH}")

    lib = ctypes.CDLL(str(LIB_PATH))
    lib.ddb_last_error_message.restype = ctypes.c_char_p
    lib.ddb_last_error_json.argtypes = [ctypes.POINTER(ctypes.c_char_p)]
    lib.ddb_last_error_json.restype = ctypes.c_uint32
    lib.ddb_string_free.argtypes = [ctypes.POINTER(ctypes.c_char_p)]
    lib.ddb_string_free.restype = ctypes.c_uint32

    lib.ddb_db_open_or_create.argtypes = [ctypes.c_char_p, ctypes.POINTER(ctypes.c_void_p)]
    lib.ddb_db_open_or_create.restype = ctypes.c_uint32
    lib.ddb_db_free.argtypes = [ctypes.POINTER(ctypes.c_void_p)]
    lib.ddb_db_free.restype = ctypes.c_uint32
    lib.ddb_db_execute.argtypes = [
        ctypes.c_void_p,
        ctypes.c_char_p,
        ctypes.POINTER(DdbValue),
        ctypes.c_size_t,
        ctypes.POINTER(ctypes.c_void_p),
    ]
    lib.ddb_db_execute.restype = ctypes.c_uint32
    lib.ddb_db_execute_queued.argtypes = [
        ctypes.c_void_p,
        ctypes.c_char_p,
        ctypes.POINTER(DdbValue),
        ctypes.c_size_t,
        ctypes.c_uint64,
        ctypes.POINTER(ctypes.c_void_p),
    ]
    lib.ddb_db_execute_queued.restype = ctypes.c_uint32
    lib.ddb_db_write_queue_metrics.argtypes = [
        ctypes.c_void_p,
        ctypes.POINTER(DdbWriteQueueMetrics),
    ]
    lib.ddb_db_write_queue_metrics.restype = ctypes.c_uint32
    lib.ddb_db_watch_query_json.argtypes = [
        ctypes.c_void_p,
        ctypes.c_char_p,
        ctypes.POINTER(ctypes.c_void_p),
    ]
    lib.ddb_db_watch_query_json.restype = ctypes.c_uint32
    lib.ddb_watch_next_json.argtypes = [
        ctypes.c_void_p,
        ctypes.c_uint32,
        ctypes.POINTER(ctypes.c_char_p),
    ]
    lib.ddb_watch_next_json.restype = ctypes.c_uint32
    lib.ddb_watch_close.argtypes = [ctypes.POINTER(ctypes.c_void_p)]
    lib.ddb_watch_close.restype = ctypes.c_uint32
    lib.ddb_db_begin_transaction.argtypes = [ctypes.c_void_p]
    lib.ddb_db_begin_transaction.restype = ctypes.c_uint32
    lib.ddb_db_commit_transaction.argtypes = [ctypes.c_void_p, ctypes.POINTER(ctypes.c_uint64)]
    lib.ddb_db_commit_transaction.restype = ctypes.c_uint32
    lib.ddb_db_rollback_transaction.argtypes = [ctypes.c_void_p]
    lib.ddb_db_rollback_transaction.restype = ctypes.c_uint32
    lib.ddb_db_in_transaction.argtypes = [ctypes.c_void_p, ctypes.POINTER(ctypes.c_uint8)]
    lib.ddb_db_in_transaction.restype = ctypes.c_uint32
    lib.ddb_db_save_as.argtypes = [ctypes.c_void_p, ctypes.c_char_p]
    lib.ddb_db_save_as.restype = ctypes.c_uint32
    lib.ddb_result_free.argtypes = [ctypes.POINTER(ctypes.c_void_p)]
    lib.ddb_result_free.restype = ctypes.c_uint32
    lib.ddb_result_row_count.argtypes = [ctypes.c_void_p, ctypes.POINTER(ctypes.c_size_t)]
    lib.ddb_result_row_count.restype = ctypes.c_uint32
    lib.ddb_result_column_count.argtypes = [ctypes.c_void_p, ctypes.POINTER(ctypes.c_size_t)]
    lib.ddb_result_column_count.restype = ctypes.c_uint32
    lib.ddb_result_value_copy.argtypes = [
        ctypes.c_void_p,
        ctypes.c_size_t,
        ctypes.c_size_t,
        ctypes.POINTER(DdbValue),
    ]
    lib.ddb_result_value_copy.restype = ctypes.c_uint32
    lib.ddb_value_dispose.argtypes = [ctypes.POINTER(DdbValue)]
    lib.ddb_value_dispose.restype = ctypes.c_uint32
    return lib


def last_error(lib) -> str:
    message = lib.ddb_last_error_message()
    return "" if not message else message.decode("utf-8")


def last_error_diagnostic(lib):
    out = ctypes.c_char_p()
    status = lib.ddb_last_error_json(ctypes.byref(out))
    if status != DDB_OK:
        raise AssertionError(f"last_error_json failed with status {status}")
    if not out.value:
        return None
    try:
        return json.loads(ctypes.string_at(out).decode("utf-8"))
    finally:
        check(lib, lib.ddb_string_free(ctypes.byref(out)), "free diagnostic json")


def check(lib, status: int, context: str) -> None:
    if status != DDB_OK:
        raise AssertionError(f"{context} failed with status {status}: {last_error(lib)}")


def make_int64(value: int) -> DdbValue:
    return DdbValue(tag=DDB_VALUE_INT64, int64_value=value)


def make_text(value: str):
    encoded = value.encode("utf-8")
    backing = ctypes.create_string_buffer(encoded)
    ffi_value = DdbValue(
        tag=DDB_VALUE_TEXT,
        data=ctypes.cast(backing, ctypes.POINTER(ctypes.c_uint8)),
        len=len(encoded),
    )
    return ffi_value, backing


def copied_text(ffi_value: DdbValue) -> str:
    buffer = ctypes.string_at(ffi_value.data, ffi_value.len)
    return buffer.decode("utf-8")


def watch_event(lib, watch, timeout_ms=1000):
    out = ctypes.c_char_p()
    status = lib.ddb_watch_next_json(watch, timeout_ms, ctypes.byref(out))
    if status == DDB_ERR_TIMEOUT:
        return None
    check(lib, status, "watch next")
    try:
        return json.loads(out.value.decode("utf-8"))
    finally:
        check(lib, lib.ddb_string_free(ctypes.byref(out)), "free watch event")


def run() -> None:
    lib = load_library()
    db = ctypes.c_void_p()

    assert DDB_ERR_BUSY == 9
    assert DDB_ERR_TIMEOUT == 10
    assert DDB_ERR_CANCELED == 11
    assert DDB_ERR_QUEUE_FULL == 12
    assert DDB_ERR_QUEUE_CLOSED == 13
    assert DDB_WRITE_QUEUE_TIMEOUT_DEFAULT == ctypes.c_uint64(-1).value

    check(lib, lib.ddb_db_open_or_create(b":memory:", ctypes.byref(db)), "open_or_create")

    result = ctypes.c_void_p()
    check(
        lib,
        lib.ddb_db_execute(
            db,
            b"CREATE TABLE items (id INT64 PRIMARY KEY, name TEXT NOT NULL)",
            None,
            0,
            ctypes.byref(result),
        ),
        "create table",
    )
    check(lib, lib.ddb_result_free(ctypes.byref(result)), "free create result")

    check(lib, lib.ddb_db_begin_transaction(db), "begin transaction")
    in_tx = ctypes.c_uint8()
    check(lib, lib.ddb_db_in_transaction(db, ctypes.byref(in_tx)), "in_transaction")
    assert in_tx.value == 1

    text_param, text_backing = make_text("Ada")
    params = (DdbValue * 2)(make_int64(1), text_param)
    check(
        lib,
        lib.ddb_db_execute(
            db,
            b"INSERT INTO items (id, name) VALUES ($1, $2)",
            params,
            2,
            ctypes.byref(result),
        ),
        "insert row",
    )
    check(lib, lib.ddb_result_free(ctypes.byref(result)), "free insert result")
    del text_backing

    check(lib, lib.ddb_db_rollback_transaction(db), "rollback transaction")

    check(
        lib,
        lib.ddb_db_execute(db, b"SELECT id, name FROM items", None, 0, ctypes.byref(result)),
        "select after rollback",
    )
    row_count = ctypes.c_size_t()
    check(lib, lib.ddb_result_row_count(result, ctypes.byref(row_count)), "row count after rollback")
    assert row_count.value == 0
    check(lib, lib.ddb_result_free(ctypes.byref(result)), "free rollback select result")

    check(lib, lib.ddb_db_begin_transaction(db), "begin second transaction")
    text_param, text_backing = make_text("Grace")
    params = (DdbValue * 2)(make_int64(2), text_param)
    check(
        lib,
        lib.ddb_db_execute(
            db,
            b"INSERT INTO items (id, name) VALUES ($1, $2)",
            params,
            2,
            ctypes.byref(result),
        ),
        "insert committed row",
    )
    check(lib, lib.ddb_result_free(ctypes.byref(result)), "free committed insert result")
    del text_backing

    committed_lsn = ctypes.c_uint64()
    check(lib, lib.ddb_db_commit_transaction(db, ctypes.byref(committed_lsn)), "commit transaction")
    assert committed_lsn.value > 0

    text_param, text_backing = make_text("Queue")
    params = (DdbValue * 2)(make_int64(3), text_param)
    check(
        lib,
        lib.ddb_db_execute_queued(
            db,
            b"INSERT INTO items (id, name) VALUES ($1, $2)",
            params,
            2,
            DDB_WRITE_QUEUE_TIMEOUT_DEFAULT,
            ctypes.byref(result),
        ),
        "queued insert",
    )
    check(lib, lib.ddb_result_free(ctypes.byref(result)), "free queued insert result")
    del text_backing

    metrics = DdbWriteQueueMetrics()
    check(lib, lib.ddb_db_write_queue_metrics(db, ctypes.byref(metrics)), "queue metrics")
    assert metrics.admitted == 1
    assert metrics.executed == 1
    assert metrics.committed == 1
    assert metrics.failed == 0

    watch = ctypes.c_void_p()
    request = json.dumps({"sql": "SELECT id, name FROM items ORDER BY id"}).encode("utf-8")
    check(
        lib,
        lib.ddb_db_watch_query_json(db, request, ctypes.byref(watch)),
        "watch query",
    )
    initial = watch_event(lib, watch)
    assert initial["type"] == "initial"
    assert initial["rows"] == [[2, "Grace"], [3, "Queue"]]

    text_param, text_backing = make_text("Watch")
    params = (DdbValue * 2)(make_int64(4), text_param)
    check(
        lib,
        lib.ddb_db_execute(
            db,
            b"INSERT INTO items (id, name) VALUES ($1, $2)",
            params,
            2,
            ctypes.byref(result),
        ),
        "watch insert",
    )
    check(lib, lib.ddb_result_free(ctypes.byref(result)), "free watch insert result")
    del text_backing

    invalidate = watch_event(lib, watch)
    assert invalidate["type"] == "invalidate"
    assert invalidate["tables"] == ["items"]
    assert invalidate["row_changes"][0]["operation"] == "insert"
    assert watch_event(lib, watch, timeout_ms=1) is None
    check(lib, lib.ddb_watch_close(ctypes.byref(watch)), "watch close")
    assert not watch.value

    check(
        lib,
        lib.ddb_db_execute(
            db,
            b"SELECT id, name FROM items ORDER BY id",
            None,
            0,
            ctypes.byref(result),
        ),
        "select committed rows",
    )
    row_count = ctypes.c_size_t()
    column_count = ctypes.c_size_t()
    check(lib, lib.ddb_result_row_count(result, ctypes.byref(row_count)), "row count")
    check(lib, lib.ddb_result_column_count(result, ctypes.byref(column_count)), "column count")
    assert row_count.value == 3
    assert column_count.value == 2

    value = DdbValue()
    check(lib, lib.ddb_result_value_copy(result, 0, 1, ctypes.byref(value)), "copy text value")
    assert copied_text(value) == "Grace"
    check(lib, lib.ddb_value_dispose(ctypes.byref(value)), "dispose copied value")
    check(lib, lib.ddb_result_free(ctypes.byref(result)), "free select result")

    with tempfile.TemporaryDirectory() as tmp:
        dest = Path(tmp) / "snapshot.ddb"
        check(lib, lib.ddb_db_save_as(db, str(dest).encode("utf-8")), "save_as")
        assert dest.exists()

    status = lib.ddb_db_execute(
        db,
        b"SELECT * FROM missing_table",
        None,
        0,
        ctypes.byref(result),
    )
    assert status == DDB_ERR_SQL
    assert "missing_table" in last_error(lib)
    diagnostic = last_error_diagnostic(lib)
    assert diagnostic is not None
    assert diagnostic["code_name"] == "ERR_SQL"
    assert diagnostic["subcode"] == "sql.relation_not_found"
    assert diagnostic["relation"] == "missing_table"

    check(lib, lib.ddb_db_free(ctypes.byref(db)), "free db")
    check(lib, lib.ddb_db_free(ctypes.byref(db)), "double free db")


if __name__ == "__main__":
    run()
