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
	"net/netip"
	"net/url"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
	"unsafe"
)

const writeQueueTimeoutDefault = ^uint64(0)

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
	var options string
	useWriteQueue := false
	var queueDefaultTimeoutMs *uint64

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

		if rawQuery != "" {
			query, err := url.ParseQuery(rawQuery)
			if err != nil {
				return nil, err
			}
			if opt := query.Get("options"); opt != "" {
				options += opt
			}
			if enabledValue, ok := query["write_queue_enabled"]; ok && len(enabledValue) > 0 {
				enabled, err := strconv.ParseBool(enabledValue[0])
				if err != nil {
					return nil, fmt.Errorf("invalid write_queue_enabled value %q: %w", enabledValue[0], err)
				}
				if enabled {
					useWriteQueue = true
				}
				options = appendOption(options, "write_queue_enabled", fmt.Sprintf("%v", enabled))
			}
			if capacityValue, ok := query["write_queue_capacity"]; ok && len(capacityValue) > 0 {
				if _, err := strconv.ParseUint(capacityValue[0], 10, 64); err != nil {
					return nil, fmt.Errorf("invalid write_queue_capacity value %q: %w", capacityValue[0], err)
				}
				useWriteQueue = true
				options = appendOption(options, "write_queue_capacity", capacityValue[0])
			}
			if timeoutValue, ok := query["write_queue_default_timeout_ms"]; ok && len(timeoutValue) > 0 {
				parsed, err := strconv.ParseUint(timeoutValue[0], 10, 64)
				if err != nil {
					return nil, fmt.Errorf("invalid write_queue_default_timeout_ms value %q: %w", timeoutValue[0], err)
				}
				useWriteQueue = true
				tmp := parsed
				queueDefaultTimeoutMs = &tmp
				options = appendOption(options, "write_queue_default_timeout_ms", timeoutValue[0])
			}
			if value, ok := query["write_queue_group_commit"]; ok && len(value) > 0 {
				commit, err := strconv.ParseBool(value[0])
				if err != nil {
					return nil, fmt.Errorf("invalid write_queue_group_commit value %q: %w", value[0], err)
				}
				useWriteQueue = true
				options = appendOption(options, "write_queue_group_commit", fmt.Sprintf("%v", commit))
			}
			if maxBatchValue, ok := query["write_queue_max_batch"]; ok && len(maxBatchValue) > 0 {
				if _, err := strconv.ParseUint(maxBatchValue[0], 10, 64); err != nil {
					return nil, fmt.Errorf("invalid write_queue_max_batch value %q: %w", maxBatchValue[0], err)
				}
				useWriteQueue = true
				options = appendOption(options, "write_queue_max_batch", maxBatchValue[0])
			}
			if value, ok := query["write_queue_max_group_delay_us"]; ok && len(value) > 0 {
				if _, err := strconv.ParseUint(value[0], 10, 64); err != nil {
					return nil, fmt.Errorf("invalid write_queue_max_group_delay_us value %q: %w", value[0], err)
				}
				useWriteQueue = true
				options = appendOption(options, "write_queue_max_group_delay_us", value[0])
			}
		}
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
	usingOptions := options != ""
	if usingOptions {
		cOptions := C.CString(options)
		defer C.free(unsafe.Pointer(cOptions))

		switch mode {
		case "create":
			if queryHasQueueOpenSupport() {
				status = C.ddb_db_create_with_options(cPath, cOptions, &db)
			} else {
				status = C.ddb_db_create(cPath, &db)
			}
		case "open":
			if queryHasQueueOpenSupport() {
				status = C.ddb_db_open_with_options(cPath, cOptions, &db)
			} else {
				status = C.ddb_db_open(cPath, &db)
			}
		default:
			if queryHasQueueOpenSupport() {
				status = C.ddb_db_open_or_create_with_options(cPath, cOptions, &db)
			} else {
				status = C.ddb_db_open_or_create(cPath, &db)
			}
		}
	} else {
		switch mode {
		case "create":
			status = C.ddb_db_create(cPath, &db)
		case "open":
			status = C.ddb_db_open(cPath, &db)
		default:
			status = C.ddb_db_open_or_create(cPath, &db)
		}
	}
	if status != C.DDB_OK || db == nil {
		return nil, statusError(status, "")
	}

	conn := &conn{db: db, useWriteQueue: useWriteQueue}
	if queueDefaultTimeoutMs != nil {
		conn.writeQueueDefaultMs = queueDefaultTimeoutMs
	}

	return conn, nil
}

func (c *connector) Driver() driver.Driver {
	return &Driver{}
}

type DecentDBError struct {
	Code    int
	Message string
	SQL     string
	Err     error
}

type Decimal struct {
	Unscaled int64
	Scale    int
}

type EnumValue struct {
	TypeID  uint64
	LabelID uint64
}

type IntervalValue struct {
	Months int32
	Days   int32
	Micros int64
}

type GeometryWKB []byte

type GeographyWKB []byte

const (
	valueTagEnum              = 11
	valueTagIPAddr            = 12
	valueTagCIDR              = 13
	valueTagDate              = 14
	valueTagTime              = 15
	valueTagTimestamptzMicros = 16
	valueTagInterval          = 17
	valueTagMACAddr           = 18
)

func (e *DecentDBError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("decentdb error %d: %s (%s) %v", e.Code, e.Message, e.SQL, e.Err)
	}
	return fmt.Sprintf("decentdb error %d: %s", e.Code, e.Message)
}

func (e *DecentDBError) Unwrap() error {
	return e.Err
}

var (
	ErrBusy        = errors.New("decentdb is busy")
	ErrTimeout     = errors.New("decentdb operation timed out")
	ErrCanceled    = errors.New("decentdb request was canceled")
	ErrQueueFull   = errors.New("decentdb queue is full")
	ErrQueueClosed = errors.New("decentdb queue is closed")
	ErrQueueClose  = ErrQueueClosed
)

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
	case C.DDB_ERR_UNSUPPORTED_FORMAT_VERSION:
		return 8
	case C.DDB_ERR_BUSY:
		return 9
	case C.DDB_ERR_TIMEOUT:
		return 10
	case C.DDB_ERR_CANCELED:
		return 11
	case C.DDB_ERR_QUEUE_FULL:
		return 12
	case C.DDB_ERR_QUEUE_CLOSED:
		return 13
	default:
		return 6
	}
}

func statusError(status C.ddb_status_t, sql string) error {
	msg := C.GoString(C.ddb_last_error_message())
	code := statusCode(status)
	v := &DecentDBError{Code: code, Message: msg, SQL: sql}
	switch status {
	case C.DDB_ERR_BUSY:
		v.Err = ErrBusy
	case C.DDB_ERR_TIMEOUT:
		v.Err = ErrTimeout
	case C.DDB_ERR_CANCELED:
		v.Err = ErrCanceled
	case C.DDB_ERR_QUEUE_FULL:
		v.Err = ErrQueueFull
	case C.DDB_ERR_QUEUE_CLOSED:
		v.Err = ErrQueueClosed
	}
	if v.Err != nil {
		return fmt.Errorf("%w: %w", v.Err, v)
	}
	return v
}

func freeAPIString(ptr *C.char) {
	if ptr == nil {
		return
	}
	p := ptr
	C.ddb_string_free(&p)
}

func appendOption(options string, key string, value string) string {
	part := fmt.Sprintf("%s=%s", key, value)
	if options == "" {
		return part
	}
	return options + " " + part
}

func queryHasQueueOpenSupport() bool {
	return true
}

type conn struct {
	db                  *C.ddb_db_t
	useWriteQueue       bool
	writeQueueDefaultMs *uint64
}

// DB provides direct access to DecentDB-specific operations beyond
// the standard database/sql interface.
type DB struct {
	c      *conn
	closed uint32
}

type WriteQueueMetrics struct {
	Capacity            uint64
	CurrentDepth        uint64
	Admitted            uint64
	Rejected            uint64
	TimedOut            uint64
	Canceled            uint64
	Executed            uint64
	Committed           uint64
	Failed              uint64
	GroupCommitBatches  uint64
	GroupCommitSyncs    uint64
	GroupCommitMaxBatch uint64
	GroupCommitCommits  uint64
	PhysicalSyncsSaved  uint64
	TotalQueueWaitNS    uint64
}

// Watch is an in-process reactive subscription handle.
type Watch struct {
	ptr    *C.ddb_watch_t
	closed uint32
}

type closeHook func()

var dbCloseHook atomic.Pointer[closeHook]

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
	runtime.SetFinalizer(wrapper, func(d *DB) {
		if atomic.LoadUint32(&d.closed) == 1 {
			return
		}
		_ = d.Close()
	})
	return wrapper, nil
}

// Close closes the database.
func (d *DB) Close() error {
	if !atomic.CompareAndSwapUint32(&d.closed, 0, 1) {
		return nil
	}
	runtime.SetFinalizer(d, nil)
	if hook := dbCloseHook.Load(); hook != nil {
		(*hook)()
	}
	return d.c.Close()
}

// ExecQueued executes a single SQL statement through the engine write queue,
// converting driver args to queue parameters and honoring context deadline.
func (d *DB) ExecQueued(ctx context.Context, query string, args ...driver.Value) (int64, error) {
	if d.closed != 0 {
		return 0, driver.ErrBadConn
	}
	return d.c.execQueuedDriverValues(ctx, query, args)
}

// ExecQueuedDefaultTimeout executes through the write queue using the
// connection's configured default timeout.
func (d *DB) ExecQueuedDefaultTimeout(query string, args ...driver.Value) (int64, error) {
	return d.ExecQueued(context.Background(), query, args...)
}

// WriteQueueMetrics returns engine write queue metrics.
func (d *DB) WriteQueueMetrics() (WriteQueueMetrics, error) {
	if d.closed != 0 {
		return WriteQueueMetrics{}, driver.ErrBadConn
	}
	return d.c.writeQueueMetrics()
}

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

// GetToolingMetadataJson returns the stable tooling metadata contract as JSON.
func (d *DB) GetToolingMetadataJson() (string, error) { return d.c.GetToolingMetadataJson() }

// DescribeQueryJson returns the stable non-executing query contract as JSON.
func (d *DB) DescribeQueryJson(sql string) (string, error) { return d.c.DescribeQueryJson(sql) }

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

// WatchTableJson subscribes to committed changes for one or more tables.
func (d *DB) WatchTableJson(tables []string) (*Watch, error) {
	return d.c.WatchTableJson(tables)
}

// WatchRangeJson subscribes to committed changes inside a primary-key JSON range.
func (d *DB) WatchRangeJson(table string, lower any, upper any) (*Watch, error) {
	return d.c.WatchRangeJson(table, lower, upper)
}

// WatchQueryJson subscribes to a SELECT query and receives an initial result
// followed by invalidation events for dependent tables.
func (d *DB) WatchQueryJson(sqlText string, params []any) (*Watch, error) {
	return d.c.WatchQueryJson(sqlText, params)
}

// ChangeStreamJson subscribes to ordered committed change events.
func (d *DB) ChangeStreamJson(tables []string) (*Watch, error) {
	return d.c.ChangeStreamJson(tables)
}

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

type queuedArgSet struct {
	Values  []C.ddb_value_t
	Buffers []unsafe.Pointer
}

func (q *queuedArgSet) Free() {
	for _, b := range q.Buffers {
		C.free(b)
	}
	q.Values = nil
	q.Buffers = nil
}

func convertQueueArgs(args []driver.NamedValue) (*queuedArgSet, error) {
	if len(args) == 0 {
		return &queuedArgSet{}, nil
	}
	out := &queuedArgSet{
		Values:  make([]C.ddb_value_t, len(args)),
		Buffers: make([]unsafe.Pointer, 0, len(args)),
	}

	for i, arg := range args {
		switch value := arg.Value.(type) {
		case nil:
			out.Values[i].tag = C.DDB_VALUE_NULL
		case int:
			out.Values[i].tag = C.DDB_VALUE_INT64
			out.Values[i].int64_value = C.int64_t(int64(value))
		case int64:
			out.Values[i].tag = C.DDB_VALUE_INT64
			out.Values[i].int64_value = C.int64_t(value)
		case float64:
			out.Values[i].tag = C.DDB_VALUE_FLOAT64
			out.Values[i].float64_value = C.double(value)
		case bool:
			out.Values[i].tag = C.DDB_VALUE_BOOL
			if value {
				out.Values[i].bool_value = C.uint8_t(1)
			}
		case string:
			out.Values[i].tag = C.DDB_VALUE_TEXT
			if len(value) > 0 {
				ptr := C.CBytes([]byte(value))
				out.Values[i].data = (*C.uint8_t)(ptr)
				out.Values[i].len = C.size_t(len(value))
				out.Buffers = append(out.Buffers, ptr)
			}
		case []byte:
			out.Values[i].tag = C.DDB_VALUE_BLOB
			if len(value) > 0 {
				ptr := C.CBytes(value)
				out.Values[i].data = (*C.uint8_t)(ptr)
				out.Values[i].len = C.size_t(len(value))
				out.Buffers = append(out.Buffers, ptr)
			}
		case Decimal:
			out.Values[i].tag = C.DDB_VALUE_DECIMAL
			out.Values[i].decimal_scaled = C.int64_t(value.Unscaled)
			scale := value.Scale
			if scale < 0 {
				scale = 0
			} else if scale > 255 {
				scale = 255
			}
			out.Values[i].decimal_scale = C.uint8_t(scale)
		case GeometryWKB:
			out.Values[i].tag = C.DDB_VALUE_GEOMETRY
			if len(value) > 0 {
				ptr := C.CBytes(value)
				out.Values[i].data = (*C.uint8_t)(ptr)
				out.Values[i].len = C.size_t(len(value))
				out.Buffers = append(out.Buffers, ptr)
			}
		case GeographyWKB:
			out.Values[i].tag = C.DDB_VALUE_GEOGRAPHY
			if len(value) > 0 {
				ptr := C.CBytes(value)
				out.Values[i].data = (*C.uint8_t)(ptr)
				out.Values[i].len = C.size_t(len(value))
				out.Buffers = append(out.Buffers, ptr)
			}
		case time.Time:
			out.Values[i].tag = C.DDB_VALUE_TIMESTAMP_MICROS
			out.Values[i].timestamp_micros = C.int64_t(value.UnixNano() / 1e3)
		default:
			return nil, fmt.Errorf("unsupported parameter type %T", arg.Value)
		}
	}

	return out, nil
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

func (c *conn) queueTimeoutFromContext(ctx context.Context) C.uint64_t {
	if c == nil {
		return C.uint64_t(writeQueueTimeoutDefault)
	}
	if ctx == nil || ctx == context.Background() || ctx == context.TODO() {
		if c.writeQueueDefaultMs != nil {
			return C.uint64_t(*c.writeQueueDefaultMs)
		}
		return C.uint64_t(writeQueueTimeoutDefault)
	}

	if deadline, ok := ctx.Deadline(); ok {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return 0
		}
		remainingMs := uint64(remaining.Milliseconds())
		if remainingMs < 1 {
			remainingMs = 1
		}
		if c.writeQueueDefaultMs == nil {
			return C.uint64_t(remainingMs)
		}
		if *c.writeQueueDefaultMs == 0 {
			return 0
		}
		if *c.writeQueueDefaultMs <= remainingMs {
			return C.uint64_t(*c.writeQueueDefaultMs)
		}
		return C.uint64_t(remainingMs)
	}

	if c.writeQueueDefaultMs != nil {
		return C.uint64_t(*c.writeQueueDefaultMs)
	}
	return C.uint64_t(writeQueueTimeoutDefault)
}

func (c *conn) writeQueueMetrics() (WriteQueueMetrics, error) {
	if c.db == nil {
		return WriteQueueMetrics{}, driver.ErrBadConn
	}
	var cMetrics C.ddb_write_queue_metrics_t
	status := C.ddb_db_write_queue_metrics(c.db, &cMetrics)
	if status != C.DDB_OK {
		return WriteQueueMetrics{}, statusError(status, "write_queue_metrics")
	}
	return WriteQueueMetrics{
		Capacity:            uint64(cMetrics.capacity),
		CurrentDepth:        uint64(cMetrics.current_depth),
		Admitted:            uint64(cMetrics.admitted),
		Rejected:            uint64(cMetrics.rejected),
		TimedOut:            uint64(cMetrics.timed_out),
		Canceled:            uint64(cMetrics.canceled),
		Executed:            uint64(cMetrics.executed),
		Committed:           uint64(cMetrics.committed),
		Failed:              uint64(cMetrics.failed),
		GroupCommitBatches:  uint64(cMetrics.group_commit_batches),
		GroupCommitSyncs:    uint64(cMetrics.group_commit_syncs),
		GroupCommitMaxBatch: uint64(cMetrics.group_commit_max_batch),
		GroupCommitCommits:  uint64(cMetrics.group_commit_commits_covered),
		PhysicalSyncsSaved:  uint64(cMetrics.physical_syncs_saved),
		TotalQueueWaitNS:    uint64(cMetrics.total_queue_wait_ns),
	}, nil
}

func newWatch(ptr *C.ddb_watch_t) *Watch {
	watch := &Watch{ptr: ptr}
	runtime.SetFinalizer(watch, func(w *Watch) {
		_ = w.Close()
	})
	return watch
}

func watchTimeoutMillis(timeout time.Duration) C.uint32_t {
	if timeout <= 0 {
		return 0
	}
	ms := uint64(timeout.Milliseconds())
	if ms == 0 {
		ms = 1
	}
	max := uint64(^uint32(0))
	if ms > max {
		ms = max
	}
	return C.uint32_t(ms)
}

// NextJson returns the next watch event as JSON. ok is false when the timeout
// expires without an event.
func (w *Watch) NextJson(timeout time.Duration) (jsonText string, ok bool, err error) {
	if w == nil || atomic.LoadUint32(&w.closed) == 1 || w.ptr == nil {
		return "", false, driver.ErrBadConn
	}
	var ptr *C.char
	status := C.ddb_watch_next_json(w.ptr, watchTimeoutMillis(timeout), &ptr)
	if status == C.DDB_ERR_TIMEOUT {
		return "", false, nil
	}
	if status != C.DDB_OK {
		return "", false, statusError(status, "watch next")
	}
	defer freeAPIString(ptr)
	return C.GoString(ptr), true, nil
}

// Next returns the next watch event decoded from JSON. ok is false when the
// timeout expires without an event.
func (w *Watch) Next(timeout time.Duration) (event map[string]any, ok bool, err error) {
	jsonText, ok, err := w.NextJson(timeout)
	if err != nil || !ok {
		return nil, ok, err
	}
	if err := json.Unmarshal([]byte(jsonText), &event); err != nil {
		return nil, false, err
	}
	return event, true, nil
}

// Close releases the native watch handle.
func (w *Watch) Close() error {
	if w == nil || !atomic.CompareAndSwapUint32(&w.closed, 0, 1) {
		return nil
	}
	runtime.SetFinalizer(w, nil)
	if w.ptr == nil {
		return nil
	}
	ptr := w.ptr
	status := C.ddb_watch_close(&ptr)
	w.ptr = ptr
	if status != C.DDB_OK {
		return statusError(status, "watch close")
	}
	return nil
}

func (c *conn) watchRequestPayload(request map[string]any) (*C.char, func(), error) {
	if c.db == nil {
		return nil, nil, driver.ErrBadConn
	}
	payload, err := json.Marshal(request)
	if err != nil {
		return nil, nil, err
	}
	cPayload := C.CString(string(payload))
	return cPayload, func() { C.free(unsafe.Pointer(cPayload)) }, nil
}

func (c *conn) WatchTableJson(tables []string) (*Watch, error) {
	cPayload, cleanup, err := c.watchRequestPayload(map[string]any{"tables": tables})
	if err != nil {
		return nil, err
	}
	defer cleanup()
	var watch *C.ddb_watch_t
	status := C.ddb_db_watch_table_json(c.db, cPayload, &watch)
	if status != C.DDB_OK || watch == nil {
		return nil, statusError(status, "watch create")
	}
	return newWatch(watch), nil
}

func (c *conn) WatchRangeJson(table string, lower any, upper any) (*Watch, error) {
	request := map[string]any{
		"table":           table,
		"lower_inclusive": true,
		"upper_inclusive": true,
	}
	if lower != nil {
		request["lower"] = lower
	}
	if upper != nil {
		request["upper"] = upper
	}
	cPayload, cleanup, err := c.watchRequestPayload(request)
	if err != nil {
		return nil, err
	}
	defer cleanup()
	var watch *C.ddb_watch_t
	status := C.ddb_db_watch_range_json(c.db, cPayload, &watch)
	if status != C.DDB_OK || watch == nil {
		return nil, statusError(status, "watch create")
	}
	return newWatch(watch), nil
}

func (c *conn) WatchQueryJson(sqlText string, params []any) (*Watch, error) {
	request := map[string]any{"sql": sqlText}
	if params != nil {
		request["params"] = params
	}
	cPayload, cleanup, err := c.watchRequestPayload(request)
	if err != nil {
		return nil, err
	}
	defer cleanup()
	var watch *C.ddb_watch_t
	status := C.ddb_db_watch_query_json(c.db, cPayload, &watch)
	if status != C.DDB_OK || watch == nil {
		return nil, statusError(status, "watch create")
	}
	return newWatch(watch), nil
}

func (c *conn) ChangeStreamJson(tables []string) (*Watch, error) {
	request := map[string]any{}
	if tables != nil {
		request["tables"] = tables
	}
	cPayload, cleanup, err := c.watchRequestPayload(request)
	if err != nil {
		return nil, err
	}
	defer cleanup()
	var watch *C.ddb_watch_t
	status := C.ddb_db_change_stream_json(c.db, cPayload, &watch)
	if status != C.DDB_OK || watch == nil {
		return nil, statusError(status, "watch create")
	}
	return newWatch(watch), nil
}

func (c *conn) execQueuedNamed(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if c.db == nil {
		return nil, driver.ErrBadConn
	}

	for _, arg := range args {
		if arg.Ordinal <= 0 {
			return nil, fmt.Errorf("invalid parameter index %d", arg.Ordinal)
		}
	}

	queueArgs, err := convertQueueArgs(args)
	if err != nil {
		return nil, err
	}
	defer queueArgs.Free()

	cQuery := C.CString(query)
	defer C.free(unsafe.Pointer(cQuery))

	var result *C.ddb_result_t
	var values *C.ddb_value_t
	if len(queueArgs.Values) > 0 {
		values = &queueArgs.Values[0]
	}
	status := C.ddb_db_execute_queued(
		c.db,
		cQuery,
		values,
		C.size_t(len(queueArgs.Values)),
		c.queueTimeoutFromContext(ctx),
		&result,
	)
	if status != C.DDB_OK {
		return nil, statusError(status, query)
	}
	defer C.ddb_result_free(&result)

	var affected C.uint64_t
	status = C.ddb_result_affected_rows(result, &affected)
	if status != C.DDB_OK {
		return nil, statusError(status, query)
	}
	return driver.RowsAffected(affected), nil
}

func (c *conn) execQueuedDriverValues(ctx context.Context, query string, args []driver.Value) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if c.db == nil {
		return 0, driver.ErrBadConn
	}
	namedArgs := make([]driver.NamedValue, len(args))
	for i, value := range args {
		namedArgs[i] = driver.NamedValue{Ordinal: i + 1, Value: value}
	}
	result, err := c.execQueuedNamed(ctx, query, namedArgs)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
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

// GetToolingMetadataJson returns the stable tooling metadata contract as JSON.
func (c *conn) GetToolingMetadataJson() (string, error) {
	if c.db == nil {
		return "", driver.ErrBadConn
	}
	var ptr *C.char
	status := C.ddb_db_get_tooling_metadata_json(c.db, &ptr)
	if status != C.DDB_OK {
		return "", statusError(status, "")
	}
	defer freeAPIString(ptr)
	return C.GoString(ptr), nil
}

// DescribeQueryJson returns the stable non-executing query contract as JSON.
func (c *conn) DescribeQueryJson(sql string) (string, error) {
	if c.db == nil {
		return "", driver.ErrBadConn
	}
	cSQL := C.CString(sql)
	defer C.free(unsafe.Pointer(cSQL))
	var ptr *C.char
	status := C.ddb_db_describe_query_json(c.db, cSQL, &ptr)
	if status != C.DDB_OK {
		return "", statusError(status, sql)
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
	case C.DDB_VALUE_GEOMETRY, C.DDB_VALUE_GEOGRAPHY:
		if val.data == nil || val.len == 0 {
			return []byte{}
		}
		return C.GoBytes(unsafe.Pointer(val.data), C.int(val.len))
	case C.DDB_VALUE_UUID:
		return C.GoBytes(unsafe.Pointer(&val.uuid_bytes[0]), 16)
	case C.DDB_VALUE_DECIMAL:
		return Decimal{Unscaled: int64(val.decimal_scaled), Scale: int(val.decimal_scale)}
	case C.DDB_VALUE_TIMESTAMP_MICROS:
		return decodeTimestampMicrosValue(int64(val.timestamp_micros))
	case C.DDB_VALUE_ENUM, C.DDB_VALUE_IPADDR, C.DDB_VALUE_CIDR,
		C.DDB_VALUE_DATE, C.DDB_VALUE_TIME, C.DDB_VALUE_TIMESTAMPTZ_MICROS,
		C.DDB_VALUE_INTERVAL, C.DDB_VALUE_MACADDR:
		return decodeSemanticTag(
			uint32(val.tag),
			uint64(val.enum_type_id),
			uint64(val.enum_label_id),
			uint8(val.ip_family),
			uint8(val.cidr_prefix_len),
			ipCIDRBytesFromValue(val),
			int32(val.date_days),
			int64(val.time_micros),
			int64(val.timestamptz_micros),
			int32(val.interval_months),
			int32(val.interval_days),
			int64(val.interval_micros),
		)
	default:
		return nil
	}
}

func ipCIDRBytesFromValue(val C.ddb_value_t) [16]byte {
	var out [16]byte
	for i := 0; i < len(out); i++ {
		out[i] = byte(val.ip_cidr_addr_bytes[i])
	}
	return out
}

func ipCIDRBytesFromView(val C.ddb_value_view_t) [16]byte {
	var out [16]byte
	for i := 0; i < len(out); i++ {
		out[i] = byte(val.ip_cidr_addr_bytes[i])
	}
	return out
}

func decodeMACAddrString(length uint8, addrBytes [16]byte) interface{} {
	if length != 6 && length != 8 {
		return nil
	}
	out := make([]byte, 0, int(length)*3-1)
	for i := 0; i < int(length); i++ {
		if i > 0 {
			out = append(out, ':')
		}
		out = append(out, "0123456789abcdef"[addrBytes[i]>>4])
		out = append(out, "0123456789abcdef"[addrBytes[i]&0x0f])
	}
	return string(out)
}

func decodeIPAddrString(family uint8, addrBytes [16]byte) interface{} {
	switch family {
	case 4:
		var addr4 [4]byte
		for i := 0; i < len(addr4); i++ {
			addr4[i] = addrBytes[i]
		}
		return netip.AddrFrom4(addr4).String()
	case 6:
		var addr6 [16]byte
		for i := 0; i < len(addr6); i++ {
			addr6[i] = addrBytes[i]
		}
		return netip.AddrFrom16(addr6).String()
	default:
		return nil
	}
}

func decodeCIDRString(family uint8, prefixLen uint8, addrBytes [16]byte) interface{} {
	var addr netip.Addr
	switch family {
	case 4:
		var addr4 [4]byte
		for i := 0; i < len(addr4); i++ {
			addr4[i] = addrBytes[i]
		}
		addr = netip.AddrFrom4(addr4)
	case 6:
		var addr6 [16]byte
		for i := 0; i < len(addr6); i++ {
			addr6[i] = addrBytes[i]
		}
		addr = netip.AddrFrom16(addr6)
	default:
		return nil
	}

	prefix := netip.PrefixFrom(addr, int(prefixLen))
	if !prefix.IsValid() {
		return nil
	}
	return prefix.Masked().String()
}

func decodeDateDaysValue(days int32) time.Time {
	return time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC).AddDate(0, 0, int(days))
}

func decodeTimeMicrosValue(micros int64) time.Duration {
	return time.Duration(micros) * time.Microsecond
}

func decodeTimestampMicrosValue(micros int64) time.Time {
	return time.UnixMicro(micros).UTC()
}

func decodeSemanticTag(
	tag uint32,
	enumTypeID uint64,
	enumLabelID uint64,
	ipFamily uint8,
	cidrPrefixLen uint8,
	ipCIDRAddrBytes [16]byte,
	dateDays int32,
	timeMicros int64,
	timestamptzMicros int64,
	intervalMonths int32,
	intervalDays int32,
	intervalMicros int64,
) interface{} {
	switch tag {
	case valueTagEnum:
		return EnumValue{
			TypeID:  enumTypeID,
			LabelID: enumLabelID,
		}
	case valueTagIPAddr:
		return decodeIPAddrString(ipFamily, ipCIDRAddrBytes)
	case valueTagCIDR:
		return decodeCIDRString(ipFamily, cidrPrefixLen, ipCIDRAddrBytes)
	case valueTagDate:
		return decodeDateDaysValue(dateDays)
	case valueTagTime:
		return decodeTimeMicrosValue(timeMicros)
	case valueTagTimestamptzMicros:
		return decodeTimestampMicrosValue(timestamptzMicros)
	case valueTagInterval:
		return IntervalValue{
			Months: intervalMonths,
			Days:   intervalDays,
			Micros: intervalMicros,
		}
	case valueTagMACAddr:
		return decodeMACAddrString(ipFamily, ipCIDRAddrBytes)
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

func isLikelyWriteQuery(query string) bool {
	normalized := strings.TrimSpace(strings.ToUpper(strings.TrimSuffix(strings.TrimSpace(query), ";")))
	if normalized == "" {
		return false
	}
	writePrefixes := []string{
		"INSERT ",
		"INSERT\t", "UPDATE ", "UPDATE\t", "DELETE ", "DELETE\t", "CREATE ",
		"CREATE\t", "ALTER ", "ALTER\t", "DROP ", "DROP\t", "TRUNCATE ",
		"TRUNCATE\t", "RENAME ", "RENAME\t", "REINDEX ", "REINDEX\t", "VACUUM ",
		"VACUUM\t", "ANALYZE ", "ANALYZE\t", "ATTACH ", "ATTACH\t", "DETACH ",
		"DETACH\t", "INSERT", "UPDATE", "DELETE", "CREATE", "ALTER", "DROP", "TRUNCATE", "VACUUM",
	}
	for _, prefix := range writePrefixes {
		if strings.HasPrefix(normalized, prefix) {
			return true
		}
	}
	return false
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
	if c.useWriteQueue && isLikelyWriteQuery(query) {
		return c.execQueuedNamed(ctx, query, args)
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
		case GeometryWKB:
			if len(v) == 0 {
				status = C.ddb_stmt_bind_geometry_wkb(s.stmt, idx, nil, 0)
			} else {
				var pinner runtime.Pinner
				pinner.Pin(&v[0])
				defer pinner.Unpin()
				status = C.ddb_stmt_bind_geometry_wkb(s.stmt, idx, (*C.uint8_t)(unsafe.Pointer(&v[0])), C.size_t(len(v)))
			}
		case GeographyWKB:
			if len(v) == 0 {
				status = C.ddb_stmt_bind_geography_wkb(s.stmt, idx, nil, 0)
			} else {
				var pinner runtime.Pinner
				pinner.Pin(&v[0])
				defer pinner.Unpin()
				status = C.ddb_stmt_bind_geography_wkb(s.stmt, idx, (*C.uint8_t)(unsafe.Pointer(&v[0])), C.size_t(len(v)))
			}
		case []byte:
			if len(v) == 0 {
				status = C.ddb_stmt_bind_blob(s.stmt, idx, nil, 0)
			} else {
				var pinner runtime.Pinner
				pinner.Pin(&v[0])
				defer pinner.Unpin()
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
		case C.DDB_VALUE_GEOMETRY, C.DDB_VALUE_GEOGRAPHY:
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
			dest[i] = decodeTimestampMicrosValue(int64(v.timestamp_micros))
		case C.DDB_VALUE_ENUM, C.DDB_VALUE_IPADDR, C.DDB_VALUE_CIDR,
			C.DDB_VALUE_DATE, C.DDB_VALUE_TIME, C.DDB_VALUE_TIMESTAMPTZ_MICROS,
			C.DDB_VALUE_INTERVAL, C.DDB_VALUE_MACADDR:
			dest[i] = decodeSemanticTag(
				uint32(v.tag),
				uint64(v.enum_type_id),
				uint64(v.enum_label_id),
				uint8(v.ip_family),
				uint8(v.cidr_prefix_len),
				ipCIDRBytesFromView(v),
				int32(v.date_days),
				int64(v.time_micros),
				int64(v.timestamptz_micros),
				int32(v.interval_months),
				int32(v.interval_days),
				int64(v.interval_micros),
			)
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
