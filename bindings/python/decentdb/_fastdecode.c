#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <stdint.h>
#include <string.h>
#include "decentdb.h"

static PyObject *decode_i64_text_f64_values(
    int64_t id_value,
    const uint8_t *text_data,
    size_t text_len,
    double float_value);
static PyObject *decode_i64_text_text_values(
    int64_t id_value,
    const uint8_t *text1_data,
    size_t text1_len,
    const uint8_t *text2_data,
    size_t text2_len);
static PyObject *decode_i64_f64_text_values(
    int64_t id_value,
    double float_value,
    const uint8_t *text_data,
    size_t text_len);
static PyObject *decode_text_i64_f64_values(
    const uint8_t *text_data,
    size_t text_len,
    int64_t id_value,
    double float_value);
static PyObject *raise_decentdb_error(ddb_status_t code, const char *context);

static PyObject *decode_utf8_text_value(const uint8_t *text_data, size_t text_len) {
    if (text_data == NULL || text_len == 0) {
        return PyUnicode_New(0, 127);
    }
    return PyUnicode_FromStringAndSize((const char *)text_data, (Py_ssize_t)text_len);
}

static PyObject *decode_i64_text_f64_row(const ddb_value_view_t *row) {
    if (row[0].tag != DDB_VALUE_INT64 || row[1].tag != DDB_VALUE_TEXT ||
        row[2].tag != DDB_VALUE_FLOAT64) {
        PyErr_SetString(PyExc_ValueError, "row tags are not INT64/TEXT/FLOAT64");
        return NULL;
    }
    return decode_i64_text_f64_values(
        row[0].int64_value,
        row[1].data,
        row[1].len,
        row[2].float64_value);
}

static PyObject *decode_i64_text_f64_values(
    int64_t id_value,
    const uint8_t *text_data,
    size_t text_len,
    double float_value) {
    PyObject *tuple = PyTuple_New(3);
    if (tuple == NULL) {
        return NULL;
    }

    PyObject *id_obj = PyLong_FromLongLong(id_value);
    if (id_obj == NULL) {
        Py_DECREF(tuple);
        return NULL;
    }
    PyTuple_SET_ITEM(tuple, 0, id_obj);

    PyObject *text_obj = decode_utf8_text_value(text_data, text_len);
    if (text_obj == NULL) {
        Py_DECREF(tuple);
        return NULL;
    }
    PyTuple_SET_ITEM(tuple, 1, text_obj);

    PyObject *float_obj = PyFloat_FromDouble(float_value);
    if (float_obj == NULL) {
        Py_DECREF(tuple);
        return NULL;
    }
    PyTuple_SET_ITEM(tuple, 2, float_obj);
    return tuple;
}

static PyObject *decode_i64_text_text_values(
    int64_t id_value,
    const uint8_t *text1_data,
    size_t text1_len,
    const uint8_t *text2_data,
    size_t text2_len) {
    PyObject *tuple = PyTuple_New(3);
    if (tuple == NULL) {
        return NULL;
    }

    PyObject *id_obj = PyLong_FromLongLong(id_value);
    if (id_obj == NULL) {
        Py_DECREF(tuple);
        return NULL;
    }
    PyTuple_SET_ITEM(tuple, 0, id_obj);

    PyObject *text1_obj = decode_utf8_text_value(text1_data, text1_len);
    if (text1_obj == NULL) {
        Py_DECREF(tuple);
        return NULL;
    }
    PyTuple_SET_ITEM(tuple, 1, text1_obj);

    PyObject *text2_obj = decode_utf8_text_value(text2_data, text2_len);
    if (text2_obj == NULL) {
        Py_DECREF(tuple);
        return NULL;
    }
    PyTuple_SET_ITEM(tuple, 2, text2_obj);
    return tuple;
}

static PyObject *decode_i64_f64_text_values(
    int64_t id_value,
    double float_value,
    const uint8_t *text_data,
    size_t text_len) {
    PyObject *tuple = PyTuple_New(3);
    if (tuple == NULL) {
        return NULL;
    }

    PyObject *id_obj = PyLong_FromLongLong(id_value);
    if (id_obj == NULL) {
        Py_DECREF(tuple);
        return NULL;
    }
    PyTuple_SET_ITEM(tuple, 0, id_obj);

    PyObject *float_obj = PyFloat_FromDouble(float_value);
    if (float_obj == NULL) {
        Py_DECREF(tuple);
        return NULL;
    }
    PyTuple_SET_ITEM(tuple, 1, float_obj);

    PyObject *text_obj = decode_utf8_text_value(text_data, text_len);
    if (text_obj == NULL) {
        Py_DECREF(tuple);
        return NULL;
    }
    PyTuple_SET_ITEM(tuple, 2, text_obj);
    return tuple;
}

static PyObject *decode_text_i64_f64_values(
    const uint8_t *text_data,
    size_t text_len,
    int64_t id_value,
    double float_value) {
    PyObject *tuple = PyTuple_New(3);
    if (tuple == NULL) {
        return NULL;
    }

    PyObject *text_obj = decode_utf8_text_value(text_data, text_len);
    if (text_obj == NULL) {
        Py_DECREF(tuple);
        return NULL;
    }
    PyTuple_SET_ITEM(tuple, 0, text_obj);

    PyObject *id_obj = PyLong_FromLongLong(id_value);
    if (id_obj == NULL) {
        Py_DECREF(tuple);
        return NULL;
    }
    PyTuple_SET_ITEM(tuple, 1, id_obj);

    PyObject *float_obj = PyFloat_FromDouble(float_value);
    if (float_obj == NULL) {
        Py_DECREF(tuple);
        return NULL;
    }
    PyTuple_SET_ITEM(tuple, 2, float_obj);
    return tuple;
}

static PyObject *decode_i64_text_text_row(const ddb_value_view_t *row) {
    if (row[0].tag != DDB_VALUE_INT64 || row[1].tag != DDB_VALUE_TEXT ||
        row[2].tag != DDB_VALUE_TEXT) {
        PyErr_SetString(PyExc_ValueError, "row tags are not INT64/TEXT/TEXT");
        return NULL;
    }
    return decode_i64_text_text_values(
        row[0].int64_value,
        row[1].data,
        row[1].len,
        row[2].data,
        row[2].len);
}

static PyObject *decode_i64_f64_text_row(const ddb_value_view_t *row) {
    if (row[0].tag != DDB_VALUE_INT64 || row[1].tag != DDB_VALUE_FLOAT64 ||
        row[2].tag != DDB_VALUE_TEXT) {
        PyErr_SetString(PyExc_ValueError, "row tags are not INT64/FLOAT64/TEXT");
        return NULL;
    }
    return decode_i64_f64_text_values(
        row[0].int64_value,
        row[1].float64_value,
        row[2].data,
        row[2].len);
}

static PyObject *decode_text_i64_f64_row(const ddb_value_view_t *row) {
    if (row[0].tag != DDB_VALUE_TEXT || row[1].tag != DDB_VALUE_INT64 ||
        row[2].tag != DDB_VALUE_FLOAT64) {
        PyErr_SetString(PyExc_ValueError, "row tags are not TEXT/INT64/FLOAT64");
        return NULL;
    }
    return decode_text_i64_f64_values(
        row[0].data,
        row[0].len,
        row[1].int64_value,
        row[2].float64_value);
}

static PyObject *decode_i64_row(const ddb_value_view_t *row) {
    if (row[0].tag != DDB_VALUE_INT64) {
        PyErr_SetString(PyExc_ValueError, "row tag is not INT64");
        return NULL;
    }
    PyObject *tuple = PyTuple_New(1);
    if (tuple == NULL) {
        return NULL;
    }
    PyObject *value = PyLong_FromLongLong(row[0].int64_value);
    if (value == NULL) {
        Py_DECREF(tuple);
        return NULL;
    }
    PyTuple_SET_ITEM(tuple, 0, value);
    return tuple;
}

static int parse_row_i64_text_f64(
    PyObject *row,
    int64_t *out_id,
    const char **out_text_ptr,
    size_t *out_text_len,
    double *out_float) {
    PyObject *id_obj = NULL;
    PyObject *text_obj = NULL;
    PyObject *float_obj = NULL;
    PyObject *row_fast = NULL;

    if (PyTuple_CheckExact(row)) {
        if (PyTuple_GET_SIZE(row) != 3) {
            PyErr_SetString(PyExc_ValueError, "each row must contain 3 values");
            return -1;
        }
        id_obj = PyTuple_GET_ITEM(row, 0);
        text_obj = PyTuple_GET_ITEM(row, 1);
        float_obj = PyTuple_GET_ITEM(row, 2);
    } else if (PyList_CheckExact(row)) {
        if (PyList_GET_SIZE(row) != 3) {
            PyErr_SetString(PyExc_ValueError, "each row must contain 3 values");
            return -1;
        }
        id_obj = PyList_GET_ITEM(row, 0);
        text_obj = PyList_GET_ITEM(row, 1);
        float_obj = PyList_GET_ITEM(row, 2);
    } else {
        row_fast = PySequence_Fast(row, "each row must be a sequence");
        if (row_fast == NULL) {
            return -1;
        }
        if (PySequence_Fast_GET_SIZE(row_fast) != 3) {
            Py_DECREF(row_fast);
            PyErr_SetString(PyExc_ValueError, "each row must contain 3 values");
            return -1;
        }
        id_obj = PySequence_Fast_GET_ITEM(row_fast, 0);
        text_obj = PySequence_Fast_GET_ITEM(row_fast, 1);
        float_obj = PySequence_Fast_GET_ITEM(row_fast, 2);
    }

    *out_id = PyLong_AsLongLong(id_obj);
    if (PyErr_Occurred()) {
        Py_XDECREF(row_fast);
        return -1;
    }

    Py_ssize_t text_len = 0;
    const char *text_ptr = PyUnicode_AsUTF8AndSize(text_obj, &text_len);
    if (text_ptr == NULL) {
        Py_XDECREF(row_fast);
        return -1;
    }
    *out_text_ptr = text_ptr;
    *out_text_len = (size_t)text_len;

    *out_float = PyFloat_AsDouble(float_obj);
    if (PyErr_Occurred()) {
        Py_XDECREF(row_fast);
        return -1;
    }

    Py_XDECREF(row_fast);
    return 0;
}

static int parse_row_i64(PyObject *row, int64_t *out_id) {
    PyObject *id_obj = NULL;
    PyObject *row_fast = NULL;

    if (PyTuple_CheckExact(row)) {
        if (PyTuple_GET_SIZE(row) != 1) {
            PyErr_SetString(PyExc_ValueError, "each row must contain 1 value");
            return -1;
        }
        id_obj = PyTuple_GET_ITEM(row, 0);
    } else if (PyList_CheckExact(row)) {
        if (PyList_GET_SIZE(row) != 1) {
            PyErr_SetString(PyExc_ValueError, "each row must contain 1 value");
            return -1;
        }
        id_obj = PyList_GET_ITEM(row, 0);
    } else {
        row_fast = PySequence_Fast(row, "each row must be a sequence");
        if (row_fast == NULL) {
            return -1;
        }
        if (PySequence_Fast_GET_SIZE(row_fast) != 1) {
            Py_DECREF(row_fast);
            PyErr_SetString(PyExc_ValueError, "each row must contain 1 value");
            return -1;
        }
        id_obj = PySequence_Fast_GET_ITEM(row_fast, 0);
    }

    *out_id = PyLong_AsLongLong(id_obj);
    Py_XDECREF(row_fast);
    if (PyErr_Occurred()) {
        return -1;
    }
    return 0;
}

static int execute_typed_row(
    ddb_stmt_t *stmt,
    PyObject *row,
    const char *signature,
    Py_ssize_t signature_len,
    uint64_t *out_affected) {
    ddb_status_t code = ddb_stmt_reset(stmt);
    if (code != DDB_OK) {
        raise_decentdb_error(code, "ddb_stmt_reset");
        return -1;
    }

    PyObject *row_fast = PySequence_Fast(row, "each row must be a sequence");
    if (row_fast == NULL) {
        return -1;
    }
    if (PySequence_Fast_GET_SIZE(row_fast) != signature_len) {
        Py_DECREF(row_fast);
        PyErr_Format(
            PyExc_ValueError,
            "each row must contain %zd values",
            signature_len);
        return -1;
    }

    for (Py_ssize_t i = 0; i < signature_len; i++) {
        PyObject *value = PySequence_Fast_GET_ITEM(row_fast, i);
        switch (signature[i]) {
            case 'i': {
                int64_t int_value = (int64_t)PyLong_AsLongLong(value);
                if (PyErr_Occurred()) {
                    Py_DECREF(row_fast);
                    return -1;
                }
                code = ddb_stmt_bind_int64(stmt, (size_t)(i + 1), int_value);
                if (code != DDB_OK) {
                    Py_DECREF(row_fast);
                    raise_decentdb_error(code, "ddb_stmt_bind_int64");
                    return -1;
                }
                break;
            }
            case 'f': {
                double float_value = PyFloat_AsDouble(value);
                if (PyErr_Occurred()) {
                    Py_DECREF(row_fast);
                    return -1;
                }
                code = ddb_stmt_bind_float64(stmt, (size_t)(i + 1), float_value);
                if (code != DDB_OK) {
                    Py_DECREF(row_fast);
                    raise_decentdb_error(code, "ddb_stmt_bind_float64");
                    return -1;
                }
                break;
            }
            case 't': {
                Py_ssize_t text_len = 0;
                const char *text_ptr = PyUnicode_AsUTF8AndSize(value, &text_len);
                if (text_ptr == NULL) {
                    Py_DECREF(row_fast);
                    return -1;
                }
                code = ddb_stmt_bind_text(stmt, (size_t)(i + 1), text_ptr, (size_t)text_len);
                if (code != DDB_OK) {
                    Py_DECREF(row_fast);
                    raise_decentdb_error(code, "ddb_stmt_bind_text");
                    return -1;
                }
                break;
            }
            default:
                Py_DECREF(row_fast);
                PyErr_Format(
                    PyExc_ValueError,
                    "unsupported signature character '%c'",
                    signature[i]);
                return -1;
        }
    }

    uint8_t has_row = 0;
    code = ddb_stmt_step(stmt, &has_row);
    if (code != DDB_OK) {
        Py_DECREF(row_fast);
        raise_decentdb_error(code, "ddb_stmt_step");
        return -1;
    }
    code = ddb_stmt_affected_rows(stmt, out_affected);
    if (code != DDB_OK) {
        Py_DECREF(row_fast);
        raise_decentdb_error(code, "ddb_stmt_affected_rows");
        return -1;
    }
    Py_DECREF(row_fast);
    return 0;
}

static PyObject *raise_decentdb_error(ddb_status_t code, const char *context) {
    const char *msg = ddb_last_error_message();
    if (msg != NULL && msg[0] != '\0') {
        PyErr_Format(
            PyExc_RuntimeError,
            "DecentDB error %u in %s: %s",
            (unsigned int)code,
            context,
            msg);
    } else {
        PyErr_Format(
            PyExc_RuntimeError,
            "DecentDB error %u in %s",
            (unsigned int)code,
            context);
    }
    return NULL;
}

static PyObject *decode_row_i64_text_f64(PyObject *self, PyObject *args) {
    unsigned long long addr = 0;
    if (!PyArg_ParseTuple(args, "K", &addr)) {
        return NULL;
    }
    if (addr == 0) {
        PyErr_SetString(PyExc_ValueError, "row pointer is null");
        return NULL;
    }
    const ddb_value_view_t *row = (const ddb_value_view_t *)(uintptr_t)addr;
    return decode_i64_text_f64_row(row);
}

static PyObject *decode_matrix_i64_text_f64(PyObject *self, PyObject *args) {
    unsigned long long addr = 0;
    Py_ssize_t row_count = 0;
    if (!PyArg_ParseTuple(args, "Kn", &addr, &row_count)) {
        return NULL;
    }
    if (row_count < 0) {
        PyErr_SetString(PyExc_ValueError, "row_count must be non-negative");
        return NULL;
    }
    if (row_count == 0) {
        return PyList_New(0);
    }
    if (addr == 0) {
        PyErr_SetString(PyExc_ValueError, "matrix pointer is null");
        return NULL;
    }

    const ddb_value_view_t *values = (const ddb_value_view_t *)(uintptr_t)addr;
    PyObject *rows = PyList_New(row_count);
    if (rows == NULL) {
        return NULL;
    }

    for (Py_ssize_t i = 0; i < row_count; i++) {
        const ddb_value_view_t *row = values + (i * 3);
        PyObject *tuple = decode_i64_text_f64_row(row);
        if (tuple == NULL) {
            Py_DECREF(rows);
            return NULL;
        }
        PyList_SET_ITEM(rows, i, tuple);
    }
    return rows;
}

static PyObject *decode_row_i64_text_text(PyObject *self, PyObject *args) {
    unsigned long long addr = 0;
    if (!PyArg_ParseTuple(args, "K", &addr)) {
        return NULL;
    }
    if (addr == 0) {
        PyErr_SetString(PyExc_ValueError, "row pointer is null");
        return NULL;
    }
    const ddb_value_view_t *row = (const ddb_value_view_t *)(uintptr_t)addr;
    return decode_i64_text_text_row(row);
}

static PyObject *decode_matrix_i64_text_text(PyObject *self, PyObject *args) {
    unsigned long long addr = 0;
    Py_ssize_t row_count = 0;
    if (!PyArg_ParseTuple(args, "Kn", &addr, &row_count)) {
        return NULL;
    }
    if (row_count < 0) {
        PyErr_SetString(PyExc_ValueError, "row_count must be non-negative");
        return NULL;
    }
    if (row_count == 0) {
        return PyList_New(0);
    }
    if (addr == 0) {
        PyErr_SetString(PyExc_ValueError, "matrix pointer is null");
        return NULL;
    }

    const ddb_value_view_t *values = (const ddb_value_view_t *)(uintptr_t)addr;
    PyObject *rows = PyList_New(row_count);
    if (rows == NULL) {
        return NULL;
    }

    for (Py_ssize_t i = 0; i < row_count; i++) {
        const ddb_value_view_t *row = values + (i * 3);
        PyObject *tuple = decode_i64_text_text_row(row);
        if (tuple == NULL) {
            Py_DECREF(rows);
            return NULL;
        }
        PyList_SET_ITEM(rows, i, tuple);
    }
    return rows;
}

static PyObject *decode_row_i64_f64_text(PyObject *self, PyObject *args) {
    unsigned long long addr = 0;
    if (!PyArg_ParseTuple(args, "K", &addr)) {
        return NULL;
    }
    if (addr == 0) {
        PyErr_SetString(PyExc_ValueError, "row pointer is null");
        return NULL;
    }
    const ddb_value_view_t *row = (const ddb_value_view_t *)(uintptr_t)addr;
    return decode_i64_f64_text_row(row);
}

static PyObject *decode_matrix_i64_f64_text(PyObject *self, PyObject *args) {
    unsigned long long addr = 0;
    Py_ssize_t row_count = 0;
    if (!PyArg_ParseTuple(args, "Kn", &addr, &row_count)) {
        return NULL;
    }
    if (row_count < 0) {
        PyErr_SetString(PyExc_ValueError, "row_count must be non-negative");
        return NULL;
    }
    if (row_count == 0) {
        return PyList_New(0);
    }
    if (addr == 0) {
        PyErr_SetString(PyExc_ValueError, "matrix pointer is null");
        return NULL;
    }

    const ddb_value_view_t *values = (const ddb_value_view_t *)(uintptr_t)addr;
    PyObject *rows = PyList_New(row_count);
    if (rows == NULL) {
        return NULL;
    }

    for (Py_ssize_t i = 0; i < row_count; i++) {
        const ddb_value_view_t *row = values + (i * 3);
        PyObject *tuple = decode_i64_f64_text_row(row);
        if (tuple == NULL) {
            Py_DECREF(rows);
            return NULL;
        }
        PyList_SET_ITEM(rows, i, tuple);
    }
    return rows;
}

static PyObject *decode_row_text_i64_f64(PyObject *self, PyObject *args) {
    unsigned long long addr = 0;
    if (!PyArg_ParseTuple(args, "K", &addr)) {
        return NULL;
    }
    if (addr == 0) {
        PyErr_SetString(PyExc_ValueError, "row pointer is null");
        return NULL;
    }
    const ddb_value_view_t *row = (const ddb_value_view_t *)(uintptr_t)addr;
    return decode_text_i64_f64_row(row);
}

static PyObject *decode_matrix_text_i64_f64(PyObject *self, PyObject *args) {
    unsigned long long addr = 0;
    Py_ssize_t row_count = 0;
    if (!PyArg_ParseTuple(args, "Kn", &addr, &row_count)) {
        return NULL;
    }
    if (row_count < 0) {
        PyErr_SetString(PyExc_ValueError, "row_count must be non-negative");
        return NULL;
    }
    if (row_count == 0) {
        return PyList_New(0);
    }
    if (addr == 0) {
        PyErr_SetString(PyExc_ValueError, "matrix pointer is null");
        return NULL;
    }

    const ddb_value_view_t *values = (const ddb_value_view_t *)(uintptr_t)addr;
    PyObject *rows = PyList_New(row_count);
    if (rows == NULL) {
        return NULL;
    }

    for (Py_ssize_t i = 0; i < row_count; i++) {
        const ddb_value_view_t *row = values + (i * 3);
        PyObject *tuple = decode_text_i64_f64_row(row);
        if (tuple == NULL) {
            Py_DECREF(rows);
            return NULL;
        }
        PyList_SET_ITEM(rows, i, tuple);
    }
    return rows;
}

static PyObject *decode_row_i64(PyObject *self, PyObject *args) {
    unsigned long long addr = 0;
    if (!PyArg_ParseTuple(args, "K", &addr)) {
        return NULL;
    }
    if (addr == 0) {
        PyErr_SetString(PyExc_ValueError, "row pointer is null");
        return NULL;
    }
    const ddb_value_view_t *row = (const ddb_value_view_t *)(uintptr_t)addr;
    return decode_i64_row(row);
}

static PyObject *decode_matrix_i64(PyObject *self, PyObject *args) {
    unsigned long long addr = 0;
    Py_ssize_t row_count = 0;
    if (!PyArg_ParseTuple(args, "Kn", &addr, &row_count)) {
        return NULL;
    }
    if (row_count < 0) {
        PyErr_SetString(PyExc_ValueError, "row_count must be non-negative");
        return NULL;
    }
    if (row_count == 0) {
        return PyList_New(0);
    }
    if (addr == 0) {
        PyErr_SetString(PyExc_ValueError, "matrix pointer is null");
        return NULL;
    }

    const ddb_value_view_t *values = (const ddb_value_view_t *)(uintptr_t)addr;
    PyObject *rows = PyList_New(row_count);
    if (rows == NULL) {
        return NULL;
    }
    for (Py_ssize_t i = 0; i < row_count; i++) {
        const ddb_value_view_t *row = values + i;
        PyObject *tuple = decode_i64_row(row);
        if (tuple == NULL) {
            Py_DECREF(rows);
            return NULL;
        }
        PyList_SET_ITEM(rows, i, tuple);
    }
    return rows;
}

static PyObject *execute_batch_i64_text_f64(PyObject *self, PyObject *args) {
    unsigned long long stmt_addr = 0;
    PyObject *rows_obj = NULL;
    if (!PyArg_ParseTuple(args, "KO", &stmt_addr, &rows_obj)) {
        return NULL;
    }
    if (stmt_addr == 0) {
        PyErr_SetString(PyExc_ValueError, "statement pointer is null");
        return NULL;
    }

    PyObject *rows = PySequence_Fast(rows_obj, "rows must be a sequence");
    if (rows == NULL) {
        return NULL;
    }
    Py_ssize_t row_count = PySequence_Fast_GET_SIZE(rows);

    int64_t *ids = NULL;
    const char **text_ptrs = NULL;
    size_t *text_lens = NULL;
    double *floats = NULL;
    if (row_count > 0) {
        ids = PyMem_Malloc((size_t)row_count * sizeof(int64_t));
        text_ptrs = PyMem_Malloc((size_t)row_count * sizeof(const char *));
        text_lens = PyMem_Malloc((size_t)row_count * sizeof(size_t));
        floats = PyMem_Malloc((size_t)row_count * sizeof(double));
        if (ids == NULL || text_ptrs == NULL || text_lens == NULL || floats == NULL) {
            Py_DECREF(rows);
            PyMem_Free(ids);
            PyMem_Free(text_ptrs);
            PyMem_Free(text_lens);
            PyMem_Free(floats);
            return PyErr_NoMemory();
        }
    }

    for (Py_ssize_t i = 0; i < row_count; i++) {
        PyObject *row = PySequence_Fast_GET_ITEM(rows, i);
        if (parse_row_i64_text_f64(
                row,
                &ids[i],
                &text_ptrs[i],
                &text_lens[i],
                &floats[i]) != 0) {
            Py_DECREF(rows);
            PyMem_Free(ids);
            PyMem_Free(text_ptrs);
            PyMem_Free(text_lens);
            PyMem_Free(floats);
            return NULL;
        }
    }

    uint64_t affected = 0;
    ddb_status_t code = ddb_stmt_execute_batch_i64_text_f64(
        (ddb_stmt_t *)(uintptr_t)stmt_addr,
        (size_t)row_count,
        ids,
        text_ptrs,
        text_lens,
        floats,
        &affected);

    Py_DECREF(rows);
    PyMem_Free(ids);
    PyMem_Free(text_ptrs);
    PyMem_Free(text_lens);
    PyMem_Free(floats);

    if (code != DDB_OK) {
        return raise_decentdb_error(code, "ddb_stmt_execute_batch_i64_text_f64");
    }
    return PyLong_FromUnsignedLongLong(affected);
}

static PyObject *execute_batch_i64(PyObject *self, PyObject *args) {
    unsigned long long stmt_addr = 0;
    PyObject *rows_obj = NULL;
    if (!PyArg_ParseTuple(args, "KO", &stmt_addr, &rows_obj)) {
        return NULL;
    }
    if (stmt_addr == 0) {
        PyErr_SetString(PyExc_ValueError, "statement pointer is null");
        return NULL;
    }

    PyObject *rows = PySequence_Fast(rows_obj, "rows must be a sequence");
    if (rows == NULL) {
        return NULL;
    }
    Py_ssize_t row_count = PySequence_Fast_GET_SIZE(rows);

    int64_t *ids = NULL;
    if (row_count > 0) {
        ids = PyMem_Malloc((size_t)row_count * sizeof(int64_t));
        if (ids == NULL) {
            Py_DECREF(rows);
            return PyErr_NoMemory();
        }
    }

    for (Py_ssize_t i = 0; i < row_count; i++) {
        PyObject *row = PySequence_Fast_GET_ITEM(rows, i);
        if (parse_row_i64(row, &ids[i]) != 0) {
            Py_DECREF(rows);
            PyMem_Free(ids);
            return NULL;
        }
    }

    uint64_t affected = 0;
    ddb_status_t code = ddb_stmt_execute_batch_i64(
        (ddb_stmt_t *)(uintptr_t)stmt_addr,
        (size_t)row_count,
        ids,
        &affected);

    Py_DECREF(rows);
    PyMem_Free(ids);

    if (code != DDB_OK) {
        return raise_decentdb_error(code, "ddb_stmt_execute_batch_i64");
    }
    return PyLong_FromUnsignedLongLong(affected);
}

static PyObject *execute_batch_i64_text_f64_iter(PyObject *self, PyObject *args) {
    unsigned long long stmt_addr = 0;
    PyObject *first_row = NULL;
    PyObject *rows_iterable = NULL;
    Py_ssize_t batch_size = 8192;
    if (!PyArg_ParseTuple(args, "KOO|n", &stmt_addr, &first_row, &rows_iterable, &batch_size)) {
        return NULL;
    }
    if (stmt_addr == 0) {
        PyErr_SetString(PyExc_ValueError, "statement pointer is null");
        return NULL;
    }
    if (batch_size <= 0) {
        batch_size = 8192;
    }

    PyObject *iterator = PyObject_GetIter(rows_iterable);
    if (iterator == NULL) {
        return NULL;
    }

    const size_t cap = (size_t)batch_size;
    int64_t *ids = PyMem_Malloc(cap * sizeof(int64_t));
    const char **text_ptrs = PyMem_Malloc(cap * sizeof(const char *));
    size_t *text_lens = PyMem_Malloc(cap * sizeof(size_t));
    double *floats = PyMem_Malloc(cap * sizeof(double));
    PyObject **keepalive = PyMem_Malloc(cap * sizeof(PyObject *));
    if (ids == NULL || text_ptrs == NULL || text_lens == NULL || floats == NULL ||
        keepalive == NULL) {
        Py_DECREF(iterator);
        PyMem_Free(ids);
        PyMem_Free(text_ptrs);
        PyMem_Free(text_lens);
        PyMem_Free(floats);
        PyMem_Free(keepalive);
        return PyErr_NoMemory();
    }

    size_t in_batch = 0;
    uint64_t total_affected = 0;

    if (parse_row_i64_text_f64(first_row, &ids[0], &text_ptrs[0], &text_lens[0], &floats[0]) !=
        0) {
        Py_DECREF(iterator);
        PyMem_Free(ids);
        PyMem_Free(text_ptrs);
        PyMem_Free(text_lens);
        PyMem_Free(floats);
        PyMem_Free(keepalive);
        return NULL;
    }
    Py_INCREF(first_row);
    keepalive[0] = first_row;
    in_batch = 1;

    PyObject *row = NULL;
    while ((row = PyIter_Next(iterator)) != NULL) {
        if (parse_row_i64_text_f64(
                row,
                &ids[in_batch],
                &text_ptrs[in_batch],
                &text_lens[in_batch],
                &floats[in_batch]) != 0) {
            Py_DECREF(row);
            goto execute_batch_i64_text_f64_iter_error;
        }

        keepalive[in_batch] = row;
        in_batch += 1;
        if (in_batch == cap) {
            uint64_t affected = 0;
            ddb_status_t code = ddb_stmt_execute_batch_i64_text_f64(
                (ddb_stmt_t *)(uintptr_t)stmt_addr,
                in_batch,
                ids,
                text_ptrs,
                text_lens,
                floats,
                &affected);
            if (code != DDB_OK) {
                raise_decentdb_error(code, "ddb_stmt_execute_batch_i64_text_f64");
                goto execute_batch_i64_text_f64_iter_error;
            }
            total_affected += affected;
            for (size_t i = 0; i < in_batch; i++) {
                Py_DECREF(keepalive[i]);
            }
            in_batch = 0;
        }
    }

    if (PyErr_Occurred()) {
        goto execute_batch_i64_text_f64_iter_error;
    }

    if (in_batch > 0) {
        uint64_t affected = 0;
        ddb_status_t code = ddb_stmt_execute_batch_i64_text_f64(
            (ddb_stmt_t *)(uintptr_t)stmt_addr,
            in_batch,
            ids,
            text_ptrs,
            text_lens,
            floats,
            &affected);
        if (code != DDB_OK) {
            raise_decentdb_error(code, "ddb_stmt_execute_batch_i64_text_f64");
            goto execute_batch_i64_text_f64_iter_error;
        }
        total_affected += affected;
        for (size_t i = 0; i < in_batch; i++) {
            Py_DECREF(keepalive[i]);
        }
    }

    Py_DECREF(iterator);
    PyMem_Free(ids);
    PyMem_Free(text_ptrs);
    PyMem_Free(text_lens);
    PyMem_Free(floats);
    PyMem_Free(keepalive);
    return PyLong_FromUnsignedLongLong(total_affected);

execute_batch_i64_text_f64_iter_error:
    for (size_t i = 0; i < in_batch; i++) {
        Py_DECREF(keepalive[i]);
    }
    Py_DECREF(iterator);
    PyMem_Free(ids);
    PyMem_Free(text_ptrs);
    PyMem_Free(text_lens);
    PyMem_Free(floats);
    PyMem_Free(keepalive);
    return NULL;
}

static PyObject *execute_batch_i64_iter(PyObject *self, PyObject *args) {
    unsigned long long stmt_addr = 0;
    PyObject *first_row = NULL;
    PyObject *rows_iterable = NULL;
    Py_ssize_t batch_size = 8192;
    if (!PyArg_ParseTuple(args, "KOO|n", &stmt_addr, &first_row, &rows_iterable, &batch_size)) {
        return NULL;
    }
    if (stmt_addr == 0) {
        PyErr_SetString(PyExc_ValueError, "statement pointer is null");
        return NULL;
    }
    if (batch_size <= 0) {
        batch_size = 8192;
    }

    PyObject *iterator = PyObject_GetIter(rows_iterable);
    if (iterator == NULL) {
        return NULL;
    }

    const size_t cap = (size_t)batch_size;
    int64_t *ids = PyMem_Malloc(cap * sizeof(int64_t));
    PyObject **keepalive = PyMem_Malloc(cap * sizeof(PyObject *));
    if (ids == NULL || keepalive == NULL) {
        Py_DECREF(iterator);
        PyMem_Free(ids);
        PyMem_Free(keepalive);
        return PyErr_NoMemory();
    }

    size_t in_batch = 0;
    uint64_t total_affected = 0;

    if (parse_row_i64(first_row, &ids[0]) != 0) {
        Py_DECREF(iterator);
        PyMem_Free(ids);
        PyMem_Free(keepalive);
        return NULL;
    }
    Py_INCREF(first_row);
    keepalive[0] = first_row;
    in_batch = 1;

    PyObject *row = NULL;
    while ((row = PyIter_Next(iterator)) != NULL) {
        if (parse_row_i64(row, &ids[in_batch]) != 0) {
            Py_DECREF(row);
            goto execute_batch_i64_iter_error;
        }

        keepalive[in_batch] = row;
        in_batch += 1;
        if (in_batch == cap) {
            uint64_t affected = 0;
            ddb_status_t code = ddb_stmt_execute_batch_i64(
                (ddb_stmt_t *)(uintptr_t)stmt_addr,
                in_batch,
                ids,
                &affected);
            if (code != DDB_OK) {
                raise_decentdb_error(code, "ddb_stmt_execute_batch_i64");
                goto execute_batch_i64_iter_error;
            }
            total_affected += affected;
            for (size_t i = 0; i < in_batch; i++) {
                Py_DECREF(keepalive[i]);
            }
            in_batch = 0;
        }
    }

    if (PyErr_Occurred()) {
        goto execute_batch_i64_iter_error;
    }

    if (in_batch > 0) {
        uint64_t affected = 0;
        ddb_status_t code =
            ddb_stmt_execute_batch_i64((ddb_stmt_t *)(uintptr_t)stmt_addr, in_batch, ids, &affected);
        if (code != DDB_OK) {
            raise_decentdb_error(code, "ddb_stmt_execute_batch_i64");
            goto execute_batch_i64_iter_error;
        }
        total_affected += affected;
        for (size_t i = 0; i < in_batch; i++) {
            Py_DECREF(keepalive[i]);
        }
    }

    Py_DECREF(iterator);
    PyMem_Free(ids);
    PyMem_Free(keepalive);
    return PyLong_FromUnsignedLongLong(total_affected);

execute_batch_i64_iter_error:
    for (size_t i = 0; i < in_batch; i++) {
        Py_DECREF(keepalive[i]);
    }
    Py_DECREF(iterator);
    PyMem_Free(ids);
    PyMem_Free(keepalive);
    return NULL;
}

static PyObject *execute_batch_typed_iter(PyObject *self, PyObject *args) {
    unsigned long long stmt_addr = 0;
    PyObject *first_row = NULL;
    PyObject *rows_iterable = NULL;
    const char *signature = NULL;
    if (!PyArg_ParseTuple(args, "KOOs", &stmt_addr, &first_row, &rows_iterable, &signature)) {
        return NULL;
    }
    if (stmt_addr == 0) {
        PyErr_SetString(PyExc_ValueError, "statement pointer is null");
        return NULL;
    }
    if (signature == NULL || signature[0] == '\0') {
        PyErr_SetString(PyExc_ValueError, "signature must not be empty");
        return NULL;
    }

    PyObject *iterator = PyObject_GetIter(rows_iterable);
    if (iterator == NULL) {
        return NULL;
    }

    ddb_stmt_t *stmt = (ddb_stmt_t *)(uintptr_t)stmt_addr;
    const Py_ssize_t signature_len = (Py_ssize_t)strlen(signature);
    uint64_t total_affected = 0;
    uint64_t affected = 0;
    if (execute_typed_row(stmt, first_row, signature, signature_len, &affected) != 0) {
        Py_DECREF(iterator);
        return NULL;
    }
    total_affected += affected;

    PyObject *row = NULL;
    while ((row = PyIter_Next(iterator)) != NULL) {
        if (execute_typed_row(stmt, row, signature, signature_len, &affected) != 0) {
            Py_DECREF(row);
            Py_DECREF(iterator);
            return NULL;
        }
        total_affected += affected;
        Py_DECREF(row);
    }
    if (PyErr_Occurred()) {
        Py_DECREF(iterator);
        return NULL;
    }

    Py_DECREF(iterator);
    return PyLong_FromUnsignedLongLong(total_affected);
}

static PyObject *fetch_rows_i64_text_f64(PyObject *self, PyObject *args) {
    unsigned long long stmt_addr = 0;
    unsigned int include_current_row = 0;
    unsigned long long max_rows = 0;
    if (!PyArg_ParseTuple(args, "KIK", &stmt_addr, &include_current_row, &max_rows)) {
        return NULL;
    }
    if (stmt_addr == 0) {
        PyErr_SetString(PyExc_ValueError, "statement pointer is null");
        return NULL;
    }

    const ddb_row_i64_text_f64_view_t *rows_ptr = NULL;
    size_t row_count = 0;
    ddb_status_t code = ddb_stmt_fetch_rows_i64_text_f64(
        (ddb_stmt_t *)(uintptr_t)stmt_addr,
        (uint8_t)(include_current_row ? 1 : 0),
        (size_t)max_rows,
        &rows_ptr,
        &row_count);
    if (code != DDB_OK) {
        return raise_decentdb_error(code, "ddb_stmt_fetch_rows_i64_text_f64");
    }

    PyObject *rows = PyList_New((Py_ssize_t)row_count);
    if (rows == NULL) {
        return NULL;
    }
    for (size_t i = 0; i < row_count; i++) {
        PyObject *tuple = decode_i64_text_f64_values(
            rows_ptr[i].int64_value,
            rows_ptr[i].text_data,
            rows_ptr[i].text_len,
            rows_ptr[i].float64_value);
        if (tuple == NULL) {
            Py_DECREF(rows);
            return NULL;
        }
        PyList_SET_ITEM(rows, (Py_ssize_t)i, tuple);
    }
    return rows;
}

static PyObject *bind_int64_step_i64_text_f64(PyObject *self, PyObject *args) {
    unsigned long long stmt_addr = 0;
    long long value = 0;
    if (!PyArg_ParseTuple(args, "KL", &stmt_addr, &value)) {
        return NULL;
    }
    if (stmt_addr == 0) {
        PyErr_SetString(PyExc_ValueError, "statement pointer is null");
        return NULL;
    }

    int64_t out_int64 = 0;
    const uint8_t *out_text_data = NULL;
    size_t out_text_len = 0;
    double out_float64 = 0.0;
    uint8_t has_row = 0;
    ddb_status_t code = ddb_stmt_bind_int64_step_i64_text_f64(
        (ddb_stmt_t *)(uintptr_t)stmt_addr,
        1,
        (int64_t)value,
        &out_int64,
        &out_text_data,
        &out_text_len,
        &out_float64,
        &has_row);
    if (code != DDB_OK) {
        return raise_decentdb_error(code, "ddb_stmt_bind_int64_step_i64_text_f64");
    }
    if (has_row == 0) {
        Py_RETURN_NONE;
    }
    return decode_i64_text_f64_values(out_int64, out_text_data, out_text_len, out_float64);
}

static PyObject *bind_int64_step_row_view(PyObject *self, PyObject *args) {
    unsigned long long stmt_addr = 0;
    long long value = 0;
    if (!PyArg_ParseTuple(args, "KL", &stmt_addr, &value)) {
        return NULL;
    }
    if (stmt_addr == 0) {
        PyErr_SetString(PyExc_ValueError, "statement pointer is null");
        return NULL;
    }

    const ddb_value_view_t *values = NULL;
    size_t columns = 0;
    uint8_t has_row = 0;
    ddb_status_t code = ddb_stmt_bind_int64_step_row_view(
        (ddb_stmt_t *)(uintptr_t)stmt_addr,
        1,
        (int64_t)value,
        &values,
        &columns,
        &has_row);
    if (code != DDB_OK) {
        return raise_decentdb_error(code, "ddb_stmt_bind_int64_step_row_view");
    }
    if (has_row == 0) {
        Py_RETURN_NONE;
    }
    if (values == NULL) {
        PyErr_SetString(PyExc_RuntimeError, "row view pointer is null");
        return NULL;
    }
    if (columns == 3) {
        return decode_i64_text_f64_row(values);
    }
    if (columns == 1) {
        return decode_i64_row(values);
    }
    PyErr_SetString(PyExc_ValueError, "unsupported row shape for fast point-read decoder");
    return NULL;
}

static PyObject *bind_i64_text_step(PyObject *self, PyObject *args) {
    unsigned long long stmt_addr = 0;
    long long id_value = 0;
    const char *text_ptr = NULL;
    Py_ssize_t text_len = 0;
    if (!PyArg_ParseTuple(args, "KLs#", &stmt_addr, &id_value, &text_ptr, &text_len)) {
        return NULL;
    }
    if (stmt_addr == 0) {
        PyErr_SetString(PyExc_ValueError, "statement pointer is null");
        return NULL;
    }

    ddb_stmt_t *stmt = (ddb_stmt_t *)(uintptr_t)stmt_addr;
    ddb_status_t code = ddb_stmt_bind_int64(stmt, 1, (int64_t)id_value);
    if (code != DDB_OK) {
        return raise_decentdb_error(code, "ddb_stmt_bind_int64");
    }
    code = ddb_stmt_bind_text(stmt, 2, text_ptr, (size_t)text_len);
    if (code != DDB_OK) {
        return raise_decentdb_error(code, "ddb_stmt_bind_text");
    }
    uint8_t has_row = 0;
    code = ddb_stmt_step(stmt, &has_row);
    if (code != DDB_OK) {
        return raise_decentdb_error(code, "ddb_stmt_step");
    }
    return PyBool_FromLong((long)(has_row != 0));
}

static PyObject *bind_text_i64_step(PyObject *self, PyObject *args) {
    unsigned long long stmt_addr = 0;
    const char *text_ptr = NULL;
    Py_ssize_t text_len = 0;
    long long id_value = 0;
    if (!PyArg_ParseTuple(args, "Ks#L", &stmt_addr, &text_ptr, &text_len, &id_value)) {
        return NULL;
    }
    if (stmt_addr == 0) {
        PyErr_SetString(PyExc_ValueError, "statement pointer is null");
        return NULL;
    }

    ddb_stmt_t *stmt = (ddb_stmt_t *)(uintptr_t)stmt_addr;
    ddb_status_t code = ddb_stmt_bind_text(stmt, 1, text_ptr, (size_t)text_len);
    if (code != DDB_OK) {
        return raise_decentdb_error(code, "ddb_stmt_bind_text");
    }
    code = ddb_stmt_bind_int64(stmt, 2, (int64_t)id_value);
    if (code != DDB_OK) {
        return raise_decentdb_error(code, "ddb_stmt_bind_int64");
    }
    uint8_t has_row = 0;
    code = ddb_stmt_step(stmt, &has_row);
    if (code != DDB_OK) {
        return raise_decentdb_error(code, "ddb_stmt_step");
    }
    return PyBool_FromLong((long)(has_row != 0));
}

static PyMethodDef methods[] = {
    {"decode_row_i64_text_f64", decode_row_i64_text_f64, METH_VARARGS,
     "Decode one INT64/TEXT/FLOAT64 row from a ddb_value_view_t pointer."},
    {"decode_matrix_i64_text_f64", decode_matrix_i64_text_f64, METH_VARARGS,
     "Decode row_count INT64/TEXT/FLOAT64 rows from a ddb_value_view_t pointer."},
    {"decode_row_i64_text_text", decode_row_i64_text_text, METH_VARARGS,
     "Decode one INT64/TEXT/TEXT row from a ddb_value_view_t pointer."},
    {"decode_matrix_i64_text_text", decode_matrix_i64_text_text, METH_VARARGS,
     "Decode row_count INT64/TEXT/TEXT rows from a ddb_value_view_t pointer."},
    {"decode_row_i64_f64_text", decode_row_i64_f64_text, METH_VARARGS,
     "Decode one INT64/FLOAT64/TEXT row from a ddb_value_view_t pointer."},
    {"decode_matrix_i64_f64_text", decode_matrix_i64_f64_text, METH_VARARGS,
     "Decode row_count INT64/FLOAT64/TEXT rows from a ddb_value_view_t pointer."},
    {"decode_row_text_i64_f64", decode_row_text_i64_f64, METH_VARARGS,
     "Decode one TEXT/INT64/FLOAT64 row from a ddb_value_view_t pointer."},
    {"decode_matrix_text_i64_f64", decode_matrix_text_i64_f64, METH_VARARGS,
     "Decode row_count TEXT/INT64/FLOAT64 rows from a ddb_value_view_t pointer."},
    {"decode_row_i64", decode_row_i64, METH_VARARGS,
     "Decode one INT64 row from a ddb_value_view_t pointer."},
    {"decode_matrix_i64", decode_matrix_i64, METH_VARARGS,
     "Decode row_count INT64 rows from a ddb_value_view_t pointer."},
    {"execute_batch_i64_text_f64", execute_batch_i64_text_f64, METH_VARARGS,
     "Execute ddb_stmt_execute_batch_i64_text_f64 from Python rows."},
    {"execute_batch_i64", execute_batch_i64, METH_VARARGS,
     "Execute ddb_stmt_execute_batch_i64 from Python rows."},
    {"execute_batch_i64_text_f64_iter", execute_batch_i64_text_f64_iter, METH_VARARGS,
     "Execute ddb_stmt_execute_batch_i64_text_f64 from a first row + iterable."},
    {"execute_batch_i64_iter", execute_batch_i64_iter, METH_VARARGS,
     "Execute ddb_stmt_execute_batch_i64 from a first row + iterable."},
    {"execute_batch_typed_iter", execute_batch_typed_iter, METH_VARARGS,
     "Execute typed row-by-row bind/step from first row + iterable."},
    {"fetch_rows_i64_text_f64", fetch_rows_i64_text_f64, METH_VARARGS,
     "Fetch rows via ddb_stmt_fetch_rows_i64_text_f64."},
    {"bind_int64_step_i64_text_f64", bind_int64_step_i64_text_f64, METH_VARARGS,
     "Bind INT64 parameter and fetch INT64/TEXT/FLOAT64 row in a single call."},
    {"bind_int64_step_row_view", bind_int64_step_row_view, METH_VARARGS,
     "Bind INT64 parameter and fetch one row view in a single call."},
    {"bind_i64_text_step", bind_i64_text_step, METH_VARARGS,
     "Bind (INT64, TEXT) and step statement once."},
    {"bind_text_i64_step", bind_text_i64_step, METH_VARARGS,
     "Bind (TEXT, INT64) and step statement once."},
    {NULL, NULL, 0, NULL},
};

static struct PyModuleDef module = {
    PyModuleDef_HEAD_INIT,
    "_fastdecode",
    "Optional native row decode accelerators for DecentDB Python bindings.",
    -1,
    methods,
};

PyMODINIT_FUNC PyInit__fastdecode(void) { return PyModule_Create(&module); }
