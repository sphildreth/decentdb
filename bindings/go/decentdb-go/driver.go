package decentdb

/*
#cgo LDFLAGS: -L../../../build -lc_api -Wl,-rpath,$ORIGIN/../../../build
#include "decentdb.h"
#include <stdlib.h>
*/
import "C"
import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"net/url"
	"time"
	"unsafe"
)

func init() {
	sql.Register("decentdb", &Driver{})
}

type Driver struct{}

func (d *Driver) Open(dsn string) (driver.Conn, error) {
	connector, err := d.OpenConnector(dsn)
	if err != nil {
		return nil, err
	}
	return connector.Connect(context.Background())
}

func (d *Driver) OpenConnector(dsn string) (driver.Connector, error) {
	return &connector{dsn: dsn}, nil
}

type connector struct {
	dsn string
}

func (c *connector) Connect(ctx context.Context) (driver.Conn, error) {
	// Parse DSN: file:/path/to.db?opt=val
	u, err := url.Parse(c.dsn)
	if err != nil {
		return nil, err
	}
	path := u.Path
	if u.Scheme == "file" {
		// Handle file:/// path
	} else if u.Scheme == "" && path == "" {
		path = c.dsn
	}

	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))

	// Options string (simpler for MVP)
	cOpts := C.CString(u.RawQuery)
	defer C.free(unsafe.Pointer(cOpts))

	db := C.decentdb_open(cPath, cOpts)
	if db == nil {
		return nil, errors.New("failed to open database")
	}

	code := int(C.decentdb_last_error_code(db))
	if code != 0 {
		msg := C.GoString(C.decentdb_last_error_message(db))
		C.decentdb_close(db)
		return nil, &DecentDBError{Code: code, Message: msg}
	}

	return &conn{db: db}, nil
}

func (c *connector) Driver() driver.Driver {
	return &Driver{}
}

type DecentDBError struct {
	Code    int
	Message string
	SQL     string
}

func (e *DecentDBError) Error() string {
	return fmt.Sprintf("decentdb error %d: %s", e.Code, e.Message)
}

type conn struct {
	db *C.decentdb_db
}

func (c *conn) Prepare(query string) (driver.Stmt, error) {
	return c.PrepareContext(context.Background(), query)
}

func (c *conn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	cQuery := C.CString(query)
	defer C.free(unsafe.Pointer(cQuery))

	var stmt *C.decentdb_stmt
	res := C.decentdb_prepare(c.db, cQuery, &stmt)
	if res != 0 {
		msg := C.GoString(C.decentdb_last_error_message(c.db))
		return nil, &DecentDBError{Code: int(res), Message: msg, SQL: query}
	}

	return &stmtStruct{c: c, query: query, stmt: stmt}, nil
}

func (c *conn) Close() error {
	if c.db != nil {
		C.decentdb_close(c.db)
		c.db = nil
	}
	return nil
}

func (c *conn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

func (c *conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	_, err := c.ExecContext(ctx, "BEGIN", nil)
	if err != nil {
		return nil, err
	}
	return &tx{c: c}, nil
}

func (c *conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	s, err := c.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer s.Close()
	return s.(driver.StmtExecContext).ExecContext(ctx, args)
}

func (c *conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	s, err := c.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}
	// We don't close the stmt here because Rows needs it.
	// But database/sql handles it if we return it as part of Rows or if we use Stmt directly.
	// Actually for QueryContext on Conn, we should probably follow what other drivers do.
	rows, err := s.(driver.StmtQueryContext).QueryContext(ctx, args)
	if err != nil {
		s.Close()
		return nil, err
	}
	return &rowsWithStmt{Rows: rows, stmt: s}, nil
}

type tx struct {
	c *conn
}

func (t *tx) Commit() error {
	_, err := t.c.ExecContext(context.Background(), "COMMIT", nil)
	return err
}

func (t *tx) Rollback() error {
	_, err := t.c.ExecContext(context.Background(), "ROLLBACK", nil)
	return err
}

type stmtStruct struct {
	c     *conn
	query string
	stmt  *C.decentdb_stmt
}

func (s *stmtStruct) Close() error {
	if s.stmt != nil {
		C.decentdb_finalize(s.stmt)
		s.stmt = nil
	}
	return nil
}

func (s *stmtStruct) NumInput() int {
	return -1 // Driver doesn't know until bind
}

func (s *stmtStruct) Exec(args []driver.Value) (driver.Result, error) {
	return nil, errors.New("Exec not implemented, use ExecContext")
}

func (s *stmtStruct) Query(args []driver.Value) (driver.Rows, error) {
	return nil, errors.New("Query not implemented, use QueryContext")
}

func (s *stmtStruct) bind(args []driver.NamedValue) error {
	for _, arg := range args {
		idx := C.int(arg.Ordinal - 1)
		var res C.int
		switch v := arg.Value.(type) {
		case nil:
			res = C.decentdb_bind_null(s.stmt, idx)
		case int64:
			res = C.decentdb_bind_int64(s.stmt, idx, C.int64_t(v))
		case float64:
			res = C.decentdb_bind_float64(s.stmt, idx, C.double(v))
		case bool:
			vi := int64(0)
			if v {
				vi = 1
			}
			res = C.decentdb_bind_int64(s.stmt, idx, C.int64_t(vi))
		case string:
			cs := C.CString(v)
			res = C.decentdb_bind_text(s.stmt, idx, cs, C.int(len(v)))
			C.free(unsafe.Pointer(cs))
		case []byte:
			if len(v) == 0 {
				res = C.decentdb_bind_blob(s.stmt, idx, nil, 0)
			} else {
				res = C.decentdb_bind_blob(s.stmt, idx, (*C.uint8_t)(unsafe.Pointer(&v[0])), C.int(len(v)))
			}
		case time.Time:
			// Epoch ms UTC
			ms := v.UnixNano() / 1e6
			res = C.decentdb_bind_int64(s.stmt, idx, C.int64_t(ms))
		default:
			return fmt.Errorf("unsupported type: %T", v)
		}
		if res != 0 {
			msg := C.GoString(C.decentdb_last_error_message(s.c.db))
			return &DecentDBError{Code: int(res), Message: msg, SQL: s.query}
		}
	}
	return nil
}

func (s *stmtStruct) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	if err := s.bind(args); err != nil {
		return nil, err
	}

	res := C.decentdb_step(s.stmt)
	if res < 0 {
		msg := C.GoString(C.decentdb_last_error_message(s.c.db))
		return nil, &DecentDBError{Code: int(res), Message: msg, SQL: s.query}
	}

	affected := int64(C.decentdb_rows_affected(s.stmt))
	return driver.RowsAffected(affected), nil
}

func (s *stmtStruct) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	if err := s.bind(args); err != nil {
		return nil, err
	}

	return &rows{s: s}, nil
}

type rows struct {
	s *stmtStruct
}

func (r *rows) Columns() []string {
	count := int(C.decentdb_column_count(r.s.stmt))
	cols := make([]string, count)
	for i := 0; i < count; i++ {
		cols[i] = C.GoString(C.decentdb_column_name(r.s.stmt, C.int(i)))
	}
	return cols
}

func (r *rows) Close() error {
	// We don't finalize the stmt here because it might be reused.
	// But we should probably reset it or something if we had a reset.
	// Database/sql will call stmt.Close eventually.
	return nil
}

func (r *rows) Next(dest []driver.Value) error {
	res := C.decentdb_step(r.s.stmt)
	if res == 0 {
		return io.EOF
	}
	if res < 0 {
		msg := C.GoString(C.decentdb_last_error_message(r.s.c.db))
		return &DecentDBError{Code: int(res), Message: msg, SQL: r.s.query}
	}

	count := int(C.decentdb_column_count(r.s.stmt))
	for i := 0; i < count; i++ {
		if C.decentdb_column_is_null(r.s.stmt, C.int(i)) != 0 {
			dest[i] = nil
			continue
		}
		typ := int(C.decentdb_column_type(r.s.stmt, C.int(i)))
		// Define mapping (vkNull=0, vkInt64=1, vkBool=2, vkFloat64=3, vkText=4, vkBlob=5)
		// Wait, I should check the enum values in record.nim
		switch typ {
		case 1: // vkInt64
			dest[i] = int64(C.decentdb_column_int64(r.s.stmt, C.int(i)))
		case 2: // vkBool
			dest[i] = C.decentdb_column_int64(r.s.stmt, C.int(i)) != 0
		case 3: // vkFloat64
			dest[i] = float64(C.decentdb_column_float64(r.s.stmt, C.int(i)))
		case 4: // vkText
			var length C.int
			ptr := C.decentdb_column_text(r.s.stmt, C.int(i), &length)
			dest[i] = C.GoStringN(ptr, length)
		case 5: // vkBlob
			var length C.int
			ptr := C.decentdb_column_blob(r.s.stmt, C.int(i), &length)
			dest[i] = C.GoBytes(unsafe.Pointer(ptr), length)
		default:
			dest[i] = nil
		}
	}

	return nil
}

type rowsWithStmt struct {
	driver.Rows
	stmt driver.Stmt
}

func (r *rowsWithStmt) Close() error {
	err := r.Rows.Close()
	r.stmt.Close()
	return err
}
