package decentdb

/*
#cgo LDFLAGS: -L${SRCDIR}/../../../build -lc_api -Wl,-rpath,${SRCDIR}/../../../build
#include "decentdb.h"
#include <stdlib.h>
#include <string.h>
*/
import "C"
import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
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
	// Parse DSN: file:/path/to.ddb?opt=val
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

type Decimal struct {
	Unscaled int64
	Scale    int
}

func (e *DecentDBError) Error() string {
	return fmt.Sprintf("decentdb error %d: %s", e.Code, e.Message)
}

type conn struct {
	db *C.decentdb_db
}

// DB provides direct access to DecentDB-specific operations beyond
// the standard database/sql interface.
type DB struct {
	c *conn
}

// OpenDirect opens a DecentDB database for direct (non-sql.DB) access,
// exposing checkpoint and schema introspection methods.
func OpenDirect(path string) (*DB, error) {
	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))
	cOpts := C.CString("")
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
	return &DB{c: &conn{db: db}}, nil
}

// Close closes the database.
func (d *DB) Close() error { return d.c.Close() }

// Checkpoint flushes the WAL to the main database file.
func (d *DB) Checkpoint() error { return d.c.Checkpoint() }

// ListTables returns the names of all tables.
func (d *DB) ListTables() ([]string, error) { return d.c.ListTables() }

// GetTableColumns returns column metadata for a given table.
func (d *DB) GetTableColumns(tableName string) ([]ColumnInfo, error) { return d.c.GetTableColumns(tableName) }

// ListIndexes returns metadata about all indexes.
func (d *DB) ListIndexes() ([]IndexInfo, error) { return d.c.ListIndexes() }

// Exec executes a SQL statement and returns the number of affected rows.
func (d *DB) Exec(sql string, args ...driver.Value) (int64, error) {
	namedArgs := make([]driver.NamedValue, len(args))
	for i, a := range args {
		namedArgs[i] = driver.NamedValue{Ordinal: i + 1, Value: a}
	}
	result, err := d.c.ExecContext(context.Background(), sql, namedArgs)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

func (c *conn) CheckNamedValue(nv *driver.NamedValue) error {
	switch nv.Value.(type) {
	case Decimal:
		return nil
	}
	return driver.ErrSkip
}

func hasUnsupportedParamStyle(sqlText string) bool {
	// sqlc-generated SQL should use $N. Reject common alternative styles to avoid
	// silent misbinding. Best-effort parsing: ignores tokens inside single quotes.
	inSingle := false
	for i := 0; i < len(sqlText); i++ {
		ch := sqlText[i]
		if ch == '\'' {
			if inSingle {
				// Handle doubled single-quote escape in SQL strings.
				if i+1 < len(sqlText) && sqlText[i+1] == '\'' {
					i++
					continue
				}
				inSingle = false
				continue
			}
			inSingle = true
			continue
		}
		if inSingle {
			continue
		}
		if ch == '?' {
			return true
		}
		if ch == '@' && i+1 < len(sqlText) {
			n := sqlText[i+1]
			if (n >= 'a' && n <= 'z') || (n >= 'A' && n <= 'Z') || n == '_' {
				return true
			}
		}
	}
	return false
}

func (c *conn) Prepare(query string) (driver.Stmt, error) {
	return c.PrepareContext(context.Background(), query)
}

func (c *conn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if hasUnsupportedParamStyle(query) {
		return nil, fmt.Errorf("unsupported parameter style: use $1..$N only")
	}
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

// Checkpoint flushes the WAL to the main database file.
func (c *conn) Checkpoint() error {
	if c.db == nil {
		return errors.New("connection is closed")
	}
	res := C.decentdb_checkpoint(c.db)
	if res != 0 {
		msg := C.GoString(C.decentdb_last_error_message(c.db))
		return &DecentDBError{Code: int(res), Message: msg}
	}
	return nil
}

// ListTables returns the names of all tables in the database.
func (c *conn) ListTables() ([]string, error) {
	if c.db == nil {
		return nil, errors.New("connection is closed")
	}
	var outLen C.int
	ptr := C.decentdb_list_tables_json(c.db, &outLen)
	if ptr == nil {
		msg := C.GoString(C.decentdb_last_error_message(c.db))
		return nil, &DecentDBError{Code: int(C.decentdb_last_error_code(c.db)), Message: msg}
	}
	defer C.decentdb_free(unsafe.Pointer(ptr))
	jsonStr := C.GoStringN(ptr, outLen)
	var tables []string
	if err := json.Unmarshal([]byte(jsonStr), &tables); err != nil {
		return nil, fmt.Errorf("failed to parse table list: %w", err)
	}
	return tables, nil
}

// ColumnInfo describes a column in a table.
type ColumnInfo struct {
	Name       string `json:"name"`
	Type       string `json:"type"`
	NotNull    bool   `json:"not_null"`
	Unique     bool   `json:"unique"`
	PrimaryKey bool   `json:"primary_key"`
	RefTable   string `json:"ref_table,omitempty"`
	RefColumn  string `json:"ref_column,omitempty"`
	RefOnDelete string `json:"ref_on_delete,omitempty"`
	RefOnUpdate string `json:"ref_on_update,omitempty"`
}

// GetTableColumns returns column metadata for a given table.
func (c *conn) GetTableColumns(tableName string) ([]ColumnInfo, error) {
	if c.db == nil {
		return nil, errors.New("connection is closed")
	}
	cName := C.CString(tableName)
	defer C.free(unsafe.Pointer(cName))
	var outLen C.int
	ptr := C.decentdb_get_table_columns_json(c.db, cName, &outLen)
	if ptr == nil {
		msg := C.GoString(C.decentdb_last_error_message(c.db))
		return nil, &DecentDBError{Code: int(C.decentdb_last_error_code(c.db)), Message: msg}
	}
	defer C.decentdb_free(unsafe.Pointer(ptr))
	jsonStr := C.GoStringN(ptr, outLen)
	var cols []ColumnInfo
	if err := json.Unmarshal([]byte(jsonStr), &cols); err != nil {
		return nil, fmt.Errorf("failed to parse column info: %w", err)
	}
	return cols, nil
}

// IndexInfo describes an index in the database.
type IndexInfo struct {
	Name    string   `json:"name"`
	Table   string   `json:"table"`
	Columns []string `json:"columns"`
	Unique  bool     `json:"unique"`
	Kind    string   `json:"kind"`
}

// ListIndexes returns metadata about all indexes in the database.
func (c *conn) ListIndexes() ([]IndexInfo, error) {
	if c.db == nil {
		return nil, errors.New("connection is closed")
	}
	var outLen C.int
	ptr := C.decentdb_list_indexes_json(c.db, &outLen)
	if ptr == nil {
		msg := C.GoString(C.decentdb_last_error_message(c.db))
		return nil, &DecentDBError{Code: int(C.decentdb_last_error_code(c.db)), Message: msg}
	}
	defer C.decentdb_free(unsafe.Pointer(ptr))
	jsonStr := C.GoStringN(ptr, outLen)
	var indexes []IndexInfo
	if err := json.Unmarshal([]byte(jsonStr), &indexes); err != nil {
		return nil, fmt.Errorf("failed to parse index info: %w", err)
	}
	return indexes, nil
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
	if s.stmt == nil {
		return errors.New("statement is closed")
	}
	// Ensure statement reuse is safe: clear previous execution state and bindings.
	C.decentdb_reset(s.stmt)
	C.decentdb_clear_bindings(s.stmt)

	for _, arg := range args {
		if arg.Ordinal <= 0 {
			return fmt.Errorf("invalid bind ordinal: %d", arg.Ordinal)
		}
		idx := C.int(arg.Ordinal) // 1-based
		var res C.int
		switch v := arg.Value.(type) {
		case nil:
			res = C.decentdb_bind_null(s.stmt, idx)
		case int:
			res = C.decentdb_bind_int64(s.stmt, idx, C.int64_t(int64(v)))
		case int64:
			res = C.decentdb_bind_int64(s.stmt, idx, C.int64_t(v))
		case float64:
			res = C.decentdb_bind_float64(s.stmt, idx, C.double(v))
		case bool:
			vi := 0
			if v {
				vi = 1
			}
			res = C.decentdb_bind_bool(s.stmt, idx, C.int(vi))
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
		case Decimal:
			res = C.decentdb_bind_decimal(s.stmt, idx, C.int64_t(v.Unscaled), C.int(v.Scale))
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
	if err := ctx.Err(); err != nil {
		return nil, err
	}
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
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := s.bind(args); err != nil {
		return nil, err
	}

	return &rows{s: s, ctx: ctx}, nil
}

type rows struct {
	s   *stmtStruct
	ctx context.Context
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
	// Make statement reusable (and release any held read snapshot).
	if r.s != nil && r.s.stmt != nil {
		C.decentdb_reset(r.s.stmt)
	}
	return nil
}

func (r *rows) Next(dest []driver.Value) error {
	if r.ctx != nil {
		select {
		case <-r.ctx.Done():
			return r.ctx.Err()
		default:
		}
	}
	res := C.decentdb_step(r.s.stmt)
	if res == 0 {
		return io.EOF
	}
	if res < 0 {
		msg := C.GoString(C.decentdb_last_error_message(r.s.c.db))
		return &DecentDBError{Code: int(res), Message: msg, SQL: r.s.query}
	}

	var views *C.decentdb_value_view
	var count C.int
	viewRes := C.decentdb_row_view(r.s.stmt, &views, &count)
	if viewRes != 0 {
		msg := C.GoString(C.decentdb_last_error_message(r.s.c.db))
		return &DecentDBError{Code: int(viewRes), Message: msg, SQL: r.s.query}
	}
	if count == 0 {
		return nil
	}
	goViews := unsafe.Slice((*C.decentdb_value_view)(unsafe.Pointer(views)), int(count))
	for i := 0; i < int(count) && i < len(dest); i++ {
		v := goViews[i]
		if v.is_null != 0 {
			dest[i] = nil
			continue
		}
		switch int(v.kind) {
		case 1: // vkInt64
			dest[i] = int64(v.int64_val)
		case 2: // vkBool
			dest[i] = v.int64_val != 0
		case 3: // vkFloat64
			dest[i] = float64(v.float64_val)
		case 4: // vkText
			if v.bytes_len == 0 || v.bytes == nil {
				dest[i] = ""
				continue
			}
			dest[i] = C.GoStringN((*C.char)(unsafe.Pointer(v.bytes)), v.bytes_len)
		case 5: // vkBlob
			if v.bytes_len == 0 || v.bytes == nil {
				dest[i] = []byte{}
				continue
			}
			dest[i] = C.GoBytes(unsafe.Pointer(v.bytes), v.bytes_len)
		case 12: // vkDecimal
			dest[i] = Decimal{
				Unscaled: int64(v.int64_val),
				Scale:    int(v.decimal_scale),
			}
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
