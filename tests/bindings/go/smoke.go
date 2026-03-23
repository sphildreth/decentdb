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
	"os"
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

	selectSQL := C.CString("SELECT id, name FROM smoke")
	defer C.free(unsafe.Pointer(selectSQL))
	check(C.ddb_db_execute(db, selectSQL, nil, 0, &result), "select")
	check(C.ddb_result_row_count(result, &rows), "row count")
	if rows != 1 {
		panic(fmt.Sprintf("expected 1 row, got %d", uint64(rows)))
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
