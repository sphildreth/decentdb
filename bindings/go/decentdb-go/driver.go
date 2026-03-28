package decentdb

/*
#cgo linux LDFLAGS: -L${SRCDIR}/../../../target/release -L${SRCDIR}/../../../target/debug -ldecentdb -Wl,-rpath,${SRCDIR}/../../../target/release -Wl,-rpath,${SRCDIR}/../../../target/debug
#cgo darwin LDFLAGS: -L${SRCDIR}/../../../target/release -L${SRCDIR}/../../../target/debug -ldecentdb -Wl,-rpath,${SRCDIR}/../../../target/release -Wl,-rpath,${SRCDIR}/../../../target/debug
#cgo windows LDFLAGS: -L${SRCDIR}/../../../target/debug -ldecentdb
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
	"runtime"
	"strings"
	"time"
	"unsafe"
)

func init() {
	sql.Register("decentdb", &Driver{})
}

// AbiVersion returns the DecentDB C ABI version.
func AbiVersion() int {
	return int(C.ddb_abi_version())
}

// EngineVersion returns the DecentDB engine version string.
func EngineVersion() string {
	return C.GoString(C.ddb_version())
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
	// Parse DSN: file:/path/to.ddb?opt=val or :memory:
	var path string
	var rawQuery string

	if c.dsn == ":memory:" {
		path = ":memory:"
	} else {
		u, err := url.Parse(c.dsn)
		if err != nil {
			return nil, err
		}
		path = u.Path
		if u.Scheme == "file" {
			// Handle file:/// path
		} else if u.Scheme == "" && path == "" {
			path = c.dsn
		}
		rawQuery = u.RawQuery
	}

	// Parse mode before any native call to avoid the open-then-recreate bug
	mode := ""
	if rawQuery != "" {
		if q, err := url.ParseQuery(rawQuery); err == nil {
			mode = q.Get("mode")
		}
	}

	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))

	var db *C.ddb_db_t
	var status C.ddb_status_t
	switch mode {
	case "create":
		status = C.ddb_db_create(cPath, &db)
	case "open":
		status = C.ddb_db_open(cPath, &db)
	default:
		status = C.ddb_db_open_or_create(cPath, &db)
	}
	if status != C.DDB_OK || db == nil {
		return nil, statusError(status, "")
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

func statusCode(status C.ddb_status_t) int {
	switch status {
	case C.DDB_OK:
		return 0
	case C.DDB_ERR_IO:
		return 1
	case C.DDB_ERR_CORRUPTION:
		return 2
	case C.DDB_ERR_CONSTRAINT:
		return 3
	case C.DDB_ERR_TRANSACTION:
		return 4
	case C.DDB_ERR_SQL:
		return 5
	case C.DDB_ERR_INTERNAL:
		return 6
	case C.DDB_ERR_PANIC:
		return 7
	default:
		return 6
	}
}

func statusError(status C.ddb_status_t, sql string) error {
	msg := C.GoString(C.ddb_last_error_message())
	return &DecentDBError{
		Code:    statusCode(status),
		Message: msg,
		SQL:     sql,
	}
}

func freeAPIString(ptr *C.char) {
	if ptr == nil {
		return
	}
	p := ptr
	C.ddb_string_free(&p)
}

type conn struct {
	db *C.ddb_db_t
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

	var db *C.ddb_db_t
	status := C.ddb_db_open_or_create(cPath, &db)
	if status != C.DDB_OK || db == nil {
		return nil, statusError(status, "")
	}
	wrapper := &DB{c: &conn{db: db}}
	runtime.SetFinalizer(wrapper, func(d *DB) { d.c.Close() })
	return wrapper, nil
}

// Close closes the database.
func (d *DB) Close() error { return d.c.Close() }

// Checkpoint flushes the WAL to the main database file.
func (d *DB) Checkpoint() error { return d.c.Checkpoint() }

// SaveAs exports the database to a new on-disk file at destPath.
func (d *DB) SaveAs(destPath string) error { return d.c.SaveAs(destPath) }

// ListTables returns the names of all tables.
func (d *DB) ListTables() ([]string, error) { return d.c.ListTables() }

// GetTableColumns returns column metadata for a given table.
func (d *DB) GetTableColumns(tableName string) ([]ColumnInfo, error) {
	return d.c.GetTableColumns(tableName)
}

// ListIndexes returns metadata about all indexes.
func (d *DB) ListIndexes() ([]IndexInfo, error) { return d.c.ListIndexes() }

// GetTableDdl returns the CREATE TABLE DDL for the given table.
func (d *DB) GetTableDdl(tableName string) (string, error) { return d.c.GetTableDdl(tableName) }

// ListViews returns metadata about all views as a JSON array.
func (d *DB) ListViews() (string, error) { return d.c.ListViews() }

// GetViewDdl returns the CREATE VIEW DDL for the given view.
func (d *DB) GetViewDdl(viewName string) (string, error) { return d.c.GetViewDdl(viewName) }

// ListTriggers returns metadata about all triggers as a JSON array.
func (d *DB) ListTriggers() (string, error) { return d.c.ListTriggers() }

// InTransaction returns true if the engine currently has an active transaction.
func (d *DB) InTransaction() bool { return d.c.InTransaction() }

// ExecImmediate executes a SQL statement without parameters, returning JSON result info.
func (d *DB) ExecImmediate(sqlText string) (string, error) { return d.c.ExecImmediate(sqlText) }

// EvictSharedWAL evicts the shared WAL file for the given database path.
func EvictSharedWAL(path string) error {
	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))
	status := C.ddb_evict_shared_wal(cPath)
	if status != C.DDB_OK {
		return statusError(status, "")
	}
	return nil
}

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

	var stmt *C.ddb_stmt_t
	status := C.ddb_db_prepare(c.db, cQuery, &stmt)
	if status != C.DDB_OK {
		return nil, statusError(status, query)
	}

	return &stmtStruct{c: c, query: query, stmt: stmt}, nil
}

func (c *conn) Close() error {
	if c.db != nil {
		dbp := c.db
		status := C.ddb_db_free(&dbp)
		if status != C.DDB_OK {
			return statusError(status, "")
		}
		c.db = nil
	}
	return nil
}

// Checkpoint flushes the WAL to the main database file.
func (c *conn) Checkpoint() error {
	if c.db == nil {
		return driver.ErrBadConn
	}
	status := C.ddb_db_checkpoint(c.db)
	if status != C.DDB_OK {
		return statusError(status, "")
	}
	return nil
}

// SaveAs exports the database to a new on-disk file at destPath.
func (c *conn) SaveAs(destPath string) error {
	if c.db == nil {
		return driver.ErrBadConn
	}
	cPath := C.CString(destPath)
	defer C.free(unsafe.Pointer(cPath))
	status := C.ddb_db_save_as(c.db, cPath)
	if status != C.DDB_OK {
		return statusError(status, "")
	}
	return nil
}

// ListTables returns the names of all tables in the database.
func (c *conn) ListTables() ([]string, error) {
	if c.db == nil {
		return nil, driver.ErrBadConn
	}
	var ptr *C.char
	status := C.ddb_db_list_tables_json(c.db, &ptr)
	if status != C.DDB_OK || ptr == nil {
		return nil, statusError(status, "")
	}
	defer freeAPIString(ptr)
	jsonStr := C.GoString(ptr)
	var tables []string
	if err := json.Unmarshal([]byte(jsonStr), &tables); err == nil {
		return tables, nil
	}
	var tableObjects []struct {
		Name string `json:"name"`
	}
	if err := json.Unmarshal([]byte(jsonStr), &tableObjects); err != nil {
		return nil, fmt.Errorf("failed to parse table list: %w", err)
	}
	tables = make([]string, 0, len(tableObjects))
	for _, entry := range tableObjects {
		if entry.Name != "" {
			tables = append(tables, entry.Name)
		}
	}
	return tables, nil
}

// ColumnInfo describes a column in a table.
type ColumnInfo struct {
	Name        string `json:"name"`
	Type        string `json:"type"`
	NotNull     bool   `json:"not_null"`
	Unique      bool   `json:"unique"`
	PrimaryKey  bool   `json:"primary_key"`
	RefTable    string `json:"ref_table,omitempty"`
	RefColumn   string `json:"ref_column,omitempty"`
	RefOnDelete string `json:"ref_on_delete,omitempty"`
	RefOnUpdate string `json:"ref_on_update,omitempty"`
}

// GetTableColumns returns column metadata for a given table.
func (c *conn) GetTableColumns(tableName string) ([]ColumnInfo, error) {
	if c.db == nil {
		return nil, driver.ErrBadConn
	}
	cName := C.CString(tableName)
	defer C.free(unsafe.Pointer(cName))
	var ptr *C.char
	status := C.ddb_db_describe_table_json(c.db, cName, &ptr)
	if status != C.DDB_OK || ptr == nil {
		return nil, statusError(status, "")
	}
	defer freeAPIString(ptr)
	jsonStr := C.GoString(ptr)
	var cols []ColumnInfo
	if err := json.Unmarshal([]byte(jsonStr), &cols); err == nil {
		return cols, nil
	}

	var describe struct {
		Columns []struct {
			Name       string `json:"name"`
			ColumnType string `json:"column_type"`
			Nullable   bool   `json:"nullable"`
			Unique     bool   `json:"unique"`
			PrimaryKey bool   `json:"primary_key"`
			ForeignKey *struct {
				Table    string `json:"table"`
				Column   string `json:"column"`
				OnDelete string `json:"on_delete"`
				OnUpdate string `json:"on_update"`
			} `json:"foreign_key"`
		} `json:"columns"`
	}
	if err := json.Unmarshal([]byte(jsonStr), &describe); err != nil {
		return nil, fmt.Errorf("failed to parse column info: %w", err)
	}
	cols = make([]ColumnInfo, 0, len(describe.Columns))
	for _, c := range describe.Columns {
		info := ColumnInfo{
			Name:       c.Name,
			Type:       c.ColumnType,
			NotNull:    !c.Nullable,
			Unique:     c.Unique,
			PrimaryKey: c.PrimaryKey,
		}
		if c.ForeignKey != nil {
			info.RefTable = c.ForeignKey.Table
			info.RefColumn = c.ForeignKey.Column
			info.RefOnDelete = c.ForeignKey.OnDelete
			info.RefOnUpdate = c.ForeignKey.OnUpdate
		}
		cols = append(cols, info)
	}
	return cols, nil
}

// IndexInfo describes an index in the database.
type IndexInfo struct {
	Name      string   `json:"name"`
	Table     string   `json:"table"`
	TableName string   `json:"table_name,omitempty"`
	Columns   []string `json:"columns"`
	Unique    bool     `json:"unique"`
	Kind      string   `json:"kind"`
}

// ListIndexes returns metadata about all indexes in the database.
func (c *conn) ListIndexes() ([]IndexInfo, error) {
	if c.db == nil {
		return nil, driver.ErrBadConn
	}
	var ptr *C.char
	status := C.ddb_db_list_indexes_json(c.db, &ptr)
	if status != C.DDB_OK || ptr == nil {
		return nil, statusError(status, "")
	}
	defer freeAPIString(ptr)
	jsonStr := C.GoString(ptr)
	var indexes []IndexInfo
	if err := json.Unmarshal([]byte(jsonStr), &indexes); err != nil {
		return nil, fmt.Errorf("failed to parse index info: %w", err)
	}
	for i := range indexes {
		if indexes[i].Table == "" {
			indexes[i].Table = indexes[i].TableName
		}
	}
	return indexes, nil
}

// GetTableDdl returns the CREATE TABLE DDL for the given table.
func (c *conn) GetTableDdl(tableName string) (string, error) {
	if c.db == nil {
		return "", driver.ErrBadConn
	}
	cName := C.CString(tableName)
	defer C.free(unsafe.Pointer(cName))
	var ptr *C.char
	status := C.ddb_db_get_table_ddl(c.db, cName, &ptr)
	if status != C.DDB_OK {
		return "", statusError(status, "")
	}
	defer freeAPIString(ptr)
	return C.GoString(ptr), nil
}

// ListViews returns metadata about all views as a JSON array.
func (c *conn) ListViews() (string, error) {
	if c.db == nil {
		return "", driver.ErrBadConn
	}
	var ptr *C.char
	status := C.ddb_db_list_views_json(c.db, &ptr)
	if status != C.DDB_OK {
		return "", statusError(status, "")
	}
	defer freeAPIString(ptr)
	return C.GoString(ptr), nil
}

// GetViewDdl returns the CREATE VIEW DDL for the given view.
func (c *conn) GetViewDdl(viewName string) (string, error) {
	if c.db == nil {
		return "", driver.ErrBadConn
	}
	cName := C.CString(viewName)
	defer C.free(unsafe.Pointer(cName))
	var ptr *C.char
	status := C.ddb_db_get_view_ddl(c.db, cName, &ptr)
	if status != C.DDB_OK {
		return "", statusError(status, "")
	}
	defer freeAPIString(ptr)
	return C.GoString(ptr), nil
}

// ListTriggers returns metadata about all triggers as a JSON array.
func (c *conn) ListTriggers() (string, error) {
	if c.db == nil {
		return "", driver.ErrBadConn
	}
	var ptr *C.char
	status := C.ddb_db_list_triggers_json(c.db, &ptr)
	if status != C.DDB_OK {
		return "", statusError(status, "")
	}
	defer freeAPIString(ptr)
	return C.GoString(ptr), nil
}

// InTransaction returns true if the engine currently has an active transaction.
func (c *conn) InTransaction() bool {
	if c.db == nil {
		return false
	}
	var flag C.uint8_t
	status := C.ddb_db_in_transaction(c.db, &flag)
	if status != C.DDB_OK {
		return false
	}
	return flag != 0
}

// ExecImmediate executes a SQL statement without parameters using ddb_db_execute,
// returning the JSON result or an error.
func (c *conn) ExecImmediate(sqlText string) (string, error) {
	if c.db == nil {
		return "", driver.ErrBadConn
	}
	cSQL := C.CString(sqlText)
	defer C.free(unsafe.Pointer(cSQL))

	var result *C.ddb_result_t
	status := C.ddb_db_execute(c.db, cSQL, nil, 0, &result)
	if status != C.DDB_OK {
		return "", statusError(status, sqlText)
	}
	defer C.ddb_result_free(&result)

	// Read result metadata using the full result set API
	var affected C.uint64_t
	C.ddb_result_affected_rows(result, &affected)

	var rowCnt C.size_t
	C.ddb_result_row_count(result, &rowCnt)

	var colCnt C.size_t
	C.ddb_result_column_count(result, &colCnt)

	// Build column names
	colNames := make([]string, int(colCnt))
	for i := 0; i < int(colCnt); i++ {
		var name *C.char
		C.ddb_result_column_name_copy(result, C.size_t(i), &name)
		if name != nil {
			colNames[i] = C.GoString(name)
			freeAPIString(name)
		}
	}

	// Read result values
	var rows []map[string]interface{}
	for r := 0; r < int(rowCnt); r++ {
		row := make(map[string]interface{})
		for c := 0; c < int(colCnt); c++ {
			var val C.ddb_value_t
			C.ddb_value_init(&val)
			rc := C.ddb_result_value_copy(result, C.size_t(r), C.size_t(c), &val)
			if rc == C.DDB_OK {
				colName := colNames[c]
				if colName == "" {
					colName = fmt.Sprintf("col%d", c)
				}
				row[colName] = valueToGo(val)
			}
			C.ddb_value_dispose(&val)
		}
		rows = append(rows, row)
	}

	out, _ := json.Marshal(map[string]interface{}{
		"affected": int64(affected),
		"rows":     rows,
	})
	return string(out), nil
}

// valueToGo converts a ddb_value_t to a Go value.
func valueToGo(val C.ddb_value_t) interface{} {
	switch val.tag {
	case C.DDB_VALUE_NULL:
		return nil
	case C.DDB_VALUE_INT64:
		return int64(val.int64_value)
	case C.DDB_VALUE_BOOL:
		return val.bool_value != 0
	case C.DDB_VALUE_FLOAT64:
		return float64(val.float64_value)
	case C.DDB_VALUE_TEXT:
		if val.data == nil || val.len == 0 {
			return ""
		}
		return C.GoStringN((*C.char)(unsafe.Pointer(val.data)), C.int(val.len))
	case C.DDB_VALUE_BLOB:
		if val.data == nil || val.len == 0 {
			return []byte{}
		}
		return C.GoBytes(unsafe.Pointer(val.data), C.int(val.len))
	case C.DDB_VALUE_UUID:
		return C.GoBytes(unsafe.Pointer(&val.uuid_bytes[0]), 16)
	case C.DDB_VALUE_DECIMAL:
		return Decimal{Unscaled: int64(val.decimal_scaled), Scale: int(val.decimal_scale)}
	case C.DDB_VALUE_TIMESTAMP_MICROS:
		micros := int64(val.timestamp_micros)
		return time.Unix(micros/1_000_000, (micros%1_000_000)*1_000).UTC()
	default:
		return nil
	}
}

func (c *conn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

func (c *conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	status := C.ddb_db_begin_transaction(c.db)
	if status != C.DDB_OK {
		_, err := c.ExecContext(ctx, "BEGIN", nil)
		if err != nil {
			return nil, err
		}
		return &tx{c: c}, nil
	}
	return &tx{c: c}, nil
}

func isTransactionControlQuery(query string, args []driver.NamedValue) string {
	if len(args) != 0 {
		return ""
	}
	trimmed := strings.TrimSpace(query)
	trimmed = strings.TrimRight(trimmed, ";")
	trimmed = strings.ToUpper(strings.Join(strings.Fields(trimmed), " "))
	switch trimmed {
	case "BEGIN", "BEGIN TRANSACTION", "START TRANSACTION":
		return "BEGIN"
	case "COMMIT", "END", "END TRANSACTION":
		return "COMMIT"
	case "ROLLBACK", "ROLLBACK TRANSACTION":
		return "ROLLBACK"
	default:
		return ""
	}
}

func (c *conn) executeTransactionControl(ctx context.Context, control string) (driver.Result, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var status C.ddb_status_t
	switch control {
	case "BEGIN":
		status = C.ddb_db_begin_transaction(c.db)
	case "COMMIT":
		var lsn C.uint64_t
		status = C.ddb_db_commit_transaction(c.db, &lsn)
	case "ROLLBACK":
		status = C.ddb_db_rollback_transaction(c.db)
	default:
		return nil, fmt.Errorf("unsupported transaction control: %s", control)
	}
	if status != C.DDB_OK {
		return nil, statusError(status, control)
	}
	return driver.RowsAffected(0), nil
}

func (c *conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if control := isTransactionControlQuery(query, args); control != "" {
		return c.executeTransactionControl(ctx, control)
	}
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
	stmt  *C.ddb_stmt_t
}

func (s *stmtStruct) Close() error {
	if s.stmt != nil {
		stmtp := s.stmt
		status := C.ddb_stmt_free(&stmtp)
		if status != C.DDB_OK {
			return statusError(status, s.query)
		}
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
	status := C.ddb_stmt_reset(s.stmt)
	if status != C.DDB_OK {
		return statusError(status, s.query)
	}
	status = C.ddb_stmt_clear_bindings(s.stmt)
	if status != C.DDB_OK {
		return statusError(status, s.query)
	}

	for _, arg := range args {
		if arg.Ordinal <= 0 {
			return fmt.Errorf("invalid bind ordinal: %d", arg.Ordinal)
		}
		idx := C.size_t(arg.Ordinal) // 1-based
		switch v := arg.Value.(type) {
		case nil:
			status = C.ddb_stmt_bind_null(s.stmt, idx)
		case int:
			status = C.ddb_stmt_bind_int64(s.stmt, idx, C.int64_t(int64(v)))
		case int64:
			status = C.ddb_stmt_bind_int64(s.stmt, idx, C.int64_t(v))
		case float64:
			status = C.ddb_stmt_bind_float64(s.stmt, idx, C.double(v))
		case bool:
			vi := 0
			if v {
				vi = 1
			}
			status = C.ddb_stmt_bind_bool(s.stmt, idx, C.uint8_t(vi))
		case string:
			cs := C.CString(v)
			status = C.ddb_stmt_bind_text(s.stmt, idx, cs, C.size_t(len(v)))
			C.free(unsafe.Pointer(cs))
		case []byte:
			if len(v) == 0 {
				status = C.ddb_stmt_bind_blob(s.stmt, idx, nil, 0)
			} else {
				status = C.ddb_stmt_bind_blob(s.stmt, idx, (*C.uint8_t)(unsafe.Pointer(&v[0])), C.size_t(len(v)))
			}
		case time.Time:
			// Microseconds since Unix epoch UTC
			micros := v.UnixNano() / 1e3
			status = C.ddb_stmt_bind_timestamp_micros(s.stmt, idx, C.int64_t(micros))
		case Decimal:
			scale := v.Scale
			if scale < 0 {
				scale = 0
			}
			if scale > 255 {
				scale = 255
			}
			status = C.ddb_stmt_bind_decimal(s.stmt, idx, C.int64_t(v.Unscaled), C.uint8_t(scale))
		default:
			return fmt.Errorf("unsupported type: %T", v)
		}
		if status != C.DDB_OK {
			return statusError(status, s.query)
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

	var hasRow C.uint8_t
	status := C.ddb_stmt_step(s.stmt, &hasRow)
	if status != C.DDB_OK {
		return nil, statusError(status, s.query)
	}

	var affected C.uint64_t
	status = C.ddb_stmt_affected_rows(s.stmt, &affected)
	if status != C.DDB_OK {
		return nil, statusError(status, s.query)
	}
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

// RebindInt64Execute rebinds the first parameter as int64 and re-executes
// the statement in a single cgo crossing, returning affected rows.
func (s *stmtStruct) RebindInt64Execute(value int64) (int64, error) {
	if s.stmt == nil {
		return 0, errors.New("statement is closed")
	}
	var affected C.uint64_t
	status := C.ddb_stmt_rebind_int64_execute(s.stmt, C.int64_t(value), &affected)
	if status != C.DDB_OK {
		return 0, statusError(status, s.query)
	}
	return int64(affected), nil
}

// ExecuteBatchI64 executes the prepared statement as a batch insert,
// binding each element of values to $1 in sequence.
func (s *stmtStruct) ExecuteBatchI64(values []int64) (int64, error) {
	if s.stmt == nil {
		return 0, errors.New("statement is closed")
	}
	if len(values) == 0 {
		return 0, nil
	}
	status := C.ddb_stmt_reset(s.stmt)
	if status != C.DDB_OK {
		return 0, statusError(status, s.query)
	}
	status = C.ddb_stmt_clear_bindings(s.stmt)
	if status != C.DDB_OK {
		return 0, statusError(status, s.query)
	}
	var affected C.uint64_t
	status = C.ddb_stmt_execute_batch_i64(s.stmt, C.size_t(len(values)), (*C.int64_t)(&values[0]), &affected)
	if status != C.DDB_OK {
		return 0, statusError(status, s.query)
	}
	return int64(affected), nil
}

// RebindTextInt64Execute rebinds text and int64 parameters and re-executes.
func (s *stmtStruct) RebindTextInt64Execute(text string, intValue int64) (int64, error) {
	if s.stmt == nil {
		return 0, errors.New("statement is closed")
	}
	ct := C.CString(text)
	defer C.free(unsafe.Pointer(ct))
	var affected C.uint64_t
	status := C.ddb_stmt_rebind_text_int64_execute(s.stmt, ct, C.size_t(len(text)), C.int64_t(intValue), &affected)
	if status != C.DDB_OK {
		return 0, statusError(status, s.query)
	}
	return int64(affected), nil
}

// RebindInt64TextExecute rebinds int64 and text parameters and re-executes.
func (s *stmtStruct) RebindInt64TextExecute(intValue int64, text string) (int64, error) {
	if s.stmt == nil {
		return 0, errors.New("statement is closed")
	}
	ct := C.CString(text)
	defer C.free(unsafe.Pointer(ct))
	var affected C.uint64_t
	status := C.ddb_stmt_rebind_int64_text_execute(s.stmt, C.int64_t(intValue), ct, C.size_t(len(text)), &affected)
	if status != C.DDB_OK {
		return 0, statusError(status, s.query)
	}
	return int64(affected), nil
}

// BindAndStepRowView binds an int64 to $1, steps, and returns the row view in one cgo crossing.
func (s *stmtStruct) BindAndStepRowView(index int, value int64) (hasRow bool, views *C.ddb_value_view_t, count C.size_t, err error) {
	if s.stmt == nil {
		return false, nil, 0, errors.New("statement is closed")
	}
	var hasRowByte C.uint8_t
	status := C.ddb_stmt_bind_int64_step_row_view(s.stmt, C.size_t(index), C.int64_t(value), &views, &count, &hasRowByte)
	if status != C.DDB_OK {
		return false, nil, 0, statusError(status, s.query)
	}
	return hasRowByte != 0, views, count, nil
}

// CopyValue copies a single column value from the current row.
func (s *stmtStruct) CopyValue(colIndex int) (*C.ddb_value_t, error) {
	if s.stmt == nil {
		return nil, errors.New("statement is closed")
	}
	val := &C.ddb_value_t{}
	status := C.ddb_stmt_value_copy(s.stmt, C.size_t(colIndex), val)
	if status != C.DDB_OK {
		return nil, statusError(status, s.query)
	}
	return val, nil
}

// FetchRowViews fetches up to maxRows rows in a batch, reducing cgo crossings.
func (s *stmtStruct) FetchRowViews(includeCurrent bool, maxRows int) (views *C.ddb_value_view_t, rowCount C.size_t, colCount C.size_t, err error) {
	if s.stmt == nil {
		return nil, 0, 0, errors.New("statement is closed")
	}
	var inc C.uint8_t
	if includeCurrent {
		inc = 1
	}
	status := C.ddb_stmt_fetch_row_views(s.stmt, inc, C.size_t(maxRows), &views, &rowCount, &colCount)
	if status != C.DDB_OK {
		return nil, 0, 0, statusError(status, s.query)
	}
	return views, rowCount, colCount, nil
}

type rows struct {
	s   *stmtStruct
	ctx context.Context
}

func (r *rows) Columns() []string {
	var count C.size_t
	status := C.ddb_stmt_column_count(r.s.stmt, &count)
	if status != C.DDB_OK {
		return []string{}
	}
	cols := make([]string, count)
	for i := 0; i < int(count); i++ {
		var name *C.char
		status = C.ddb_stmt_column_name_copy(r.s.stmt, C.size_t(i), &name)
		if status != C.DDB_OK || name == nil {
			continue
		}
		cols[i] = C.GoString(name)
		freeAPIString(name)
	}
	return cols
}

func (r *rows) Close() error {
	// Make statement reusable (and release any held read snapshot).
	if r.s != nil && r.s.stmt != nil {
		C.ddb_stmt_reset(r.s.stmt)
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

	var views *C.ddb_value_view_t
	var count C.size_t
	var hasRow C.uint8_t

	// Fused step+row_view: single cgo crossing instead of two
	status := C.ddb_stmt_step_row_view(r.s.stmt, &views, &count, &hasRow)
	if status != C.DDB_OK {
		return statusError(status, r.s.query)
	}
	if hasRow == 0 {
		return io.EOF
	}
	if count == 0 {
		return nil
	}
	goViews := unsafe.Slice((*C.ddb_value_view_t)(unsafe.Pointer(views)), int(count))
	for i := 0; i < int(count) && i < len(dest); i++ {
		v := goViews[i]
		if v.tag == C.DDB_VALUE_NULL {
			dest[i] = nil
			continue
		}
		switch v.tag {
		case C.DDB_VALUE_INT64:
			dest[i] = int64(v.int64_value)
		case C.DDB_VALUE_BOOL:
			dest[i] = v.bool_value != 0
		case C.DDB_VALUE_FLOAT64:
			dest[i] = float64(v.float64_value)
		case C.DDB_VALUE_TEXT:
			if v.len == 0 || v.data == nil {
				dest[i] = ""
				continue
			}
			dest[i] = C.GoStringN((*C.char)(unsafe.Pointer(v.data)), C.int(v.len))
		case C.DDB_VALUE_BLOB:
			if v.len == 0 || v.data == nil {
				dest[i] = []byte{}
				continue
			}
			dest[i] = C.GoBytes(unsafe.Pointer(v.data), C.int(v.len))
		case C.DDB_VALUE_UUID:
			dest[i] = C.GoBytes(unsafe.Pointer(&v.uuid_bytes[0]), 16)
		case C.DDB_VALUE_DECIMAL:
			dest[i] = Decimal{
				Unscaled: int64(v.decimal_scaled),
				Scale:    int(v.decimal_scale),
			}
		case C.DDB_VALUE_TIMESTAMP_MICROS:
			micros := int64(v.timestamp_micros)
			dest[i] = time.Unix(micros/1_000_000, (micros%1_000_000)*1_000).UTC()
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
