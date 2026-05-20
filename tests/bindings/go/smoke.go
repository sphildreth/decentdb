package main

/*
#cgo linux CFLAGS: -I${SRCDIR}/../../../include
#cgo linux LDFLAGS: -L${SRCDIR}/../../../target/debug -ldecentdb -Wl,-rpath,${SRCDIR}/../../../target/debug
#include <stdlib.h>
#include "decentdb.h"
*/
import "C"

import (
	"fmt"
	"strings"
	"unsafe"
)

func check(status C.ddb_status_t, context string) {
	if status != C.DDB_OK {
		message := C.ddb_last_error_message()
		if message == nil {
			panic(fmt.Sprintf("%s failed with status %d", context, uint32(status)))
		}
		panic(fmt.Sprintf("%s failed with status %d: %s", context, uint32(status), C.GoString(message)))
	}
}

func main() {
	var db *C.ddb_db_t
	var result *C.ddb_result_t
	var rows C.size_t

	path := C.CString(":memory:")
	defer C.free(unsafe.Pointer(path))
	check(C.ddb_db_open_or_create(path, &db), "open_or_create")

	create := C.CString("CREATE TABLE smoke (id INT64 PRIMARY KEY, name TEXT)")
	defer C.free(unsafe.Pointer(create))
	check(C.ddb_db_execute(db, create, nil, 0, &result), "create")
	check(C.ddb_result_free(&result), "free create")

	insert := C.CString("INSERT INTO smoke (id, name) VALUES (1, 'go-smoke')")
	defer C.free(unsafe.Pointer(insert))
	check(C.ddb_db_execute(db, insert, nil, 0, &result), "insert")
	check(C.ddb_result_free(&result), "free insert")

	queuedInsert := C.CString("INSERT INTO smoke (id, name) VALUES (2, 'go-queued')")
	defer C.free(unsafe.Pointer(queuedInsert))
	check(C.ddb_db_execute_queued(db, queuedInsert, nil, 0, C.uint64_t(^uint64(0)), &result), "queued insert")
	check(C.ddb_result_free(&result), "free queued insert")
	var metrics C.ddb_write_queue_metrics_t
	check(C.ddb_db_write_queue_metrics(db, &metrics), "queue metrics")
	if metrics.admitted != 1 || metrics.committed != 1 || metrics.failed != 0 {
		panic(fmt.Sprintf("unexpected queue metrics admitted=%d committed=%d failed=%d",
			uint64(metrics.admitted), uint64(metrics.committed), uint64(metrics.failed)))
	}

	var watch *C.ddb_watch_t
	watchRequest := C.CString(`{"sql":"SELECT id, name FROM smoke ORDER BY id"}`)
	defer C.free(unsafe.Pointer(watchRequest))
	check(C.ddb_db_watch_query_json(db, watchRequest, &watch), "watch query")
	var watchEvent *C.char
	check(C.ddb_watch_next_json(watch, 1000, &watchEvent), "watch initial")
	if !strings.Contains(C.GoString(watchEvent), `"type":"initial"`) {
		panic("unexpected initial watch event")
	}
	check(C.ddb_string_free(&watchEvent), "free watch initial")

	watchInsert := C.CString("INSERT INTO smoke (id, name) VALUES (3, 'go-watch')")
	defer C.free(unsafe.Pointer(watchInsert))
	check(C.ddb_db_execute(db, watchInsert, nil, 0, &result), "watch insert")
	check(C.ddb_result_free(&result), "free watch insert")
	check(C.ddb_watch_next_json(watch, 1000, &watchEvent), "watch invalidate")
	invalidate := C.GoString(watchEvent)
	if !strings.Contains(invalidate, `"type":"invalidate"`) || !strings.Contains(invalidate, `"smoke"`) {
		panic(fmt.Sprintf("unexpected invalidate watch event: %s", invalidate))
	}
	check(C.ddb_string_free(&watchEvent), "free watch invalidate")
	if status := C.ddb_watch_next_json(watch, 1, &watchEvent); status != C.DDB_ERR_TIMEOUT {
		panic(fmt.Sprintf("expected watch timeout, got %d", uint32(status)))
	}
	check(C.ddb_watch_close(&watch), "watch close")

	selectSQL := C.CString("SELECT id, name FROM smoke")
	defer C.free(unsafe.Pointer(selectSQL))
	check(C.ddb_db_execute(db, selectSQL, nil, 0, &result), "select")
	check(C.ddb_result_row_count(result, &rows), "row count")
	if rows != 3 {
		panic(fmt.Sprintf("expected 3 rows, got %d", uint64(rows)))
	}
	check(C.ddb_result_free(&result), "free select")

	badSQL := C.CString("SELECT * FROM nope")
	defer C.free(unsafe.Pointer(badSQL))
	status := C.ddb_db_execute(db, badSQL, nil, 0, &result)
	if status != C.DDB_ERR_SQL {
		panic(fmt.Sprintf("expected SQL error, got %d", uint32(status)))
	}
	if message := C.GoString(C.ddb_last_error_message()); message == "" || !contains(message, "nope") {
		panic(fmt.Sprintf("unexpected error message %q", message))
	}

	check(C.ddb_db_free(&db), "free db")
}

func contains(haystack, needle string) bool {
	return len(needle) == 0 || (len(haystack) >= len(needle) && (func() bool {
		for i := 0; i+len(needle) <= len(haystack); i++ {
			if haystack[i:i+len(needle)] == needle {
				return true
			}
		}
		return false
	})())
}
