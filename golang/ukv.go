package main

/*
#cgo CFLAGS: -g -Wall -I${SRCDIR}/../include
#cgo LDFLAGS: -L${SRCDIR}/../build/lib -lukv -lstdc++
#include "ukv.h"
#include <stdlib.h>
*/
import "C"
import (
	"errors"
	"unsafe"
)

/**
 * This class is modeled after Redis client and other ORMs:
 * https://github.com/go-redis/redis
 * https://bun.uptrace.dev
 *
 * Further Reading:
 * https://blog.marlin.org/cgo-referencing-c-library-in-go
 */
type database struct {
	raw C.ukv_t
}

func (db *database) Reconnect(config string) error {

	error_c := C.ukv_error_t(nil)
	config_c := C.CString(config)
	C.ukv_open(config_c, &db.raw, &error_c)
	C.free(unsafe.Pointer(config_c))

	if error_c != nil {
		error_go := C.GoString(error_c)
		C.ukv_error_free(db.raw, error_c)
		return errors.New(error_go)
	}

	return nil
}

func (db *database) Set(key uint64, value *[]byte) error {

	// Passing values without copies seems essentially impossible
	// and causes: "cgo argument has Go pointer to Go pointer"
	// when we try to take pass pointer to to first object in `[]byte`.
	// https://stackoverflow.com/a/64867672
	error_c := C.ukv_error_t(nil)
	key_c := C.ukv_key_t(10)
	value_go := C.CBytes(*value)
	value_ptr_c := C.ukv_val_ptr_t(value_go)
	value_length_c := C.ukv_val_len_t(len(*value))

	C.ukv_write(
		// Inputs:
		db.raw, &key_c, C.size_t(1),
		// Configs:
		(*C.ukv_column_t)(nil), C.size_t(0), (C.ukv_options_write_t)(nil),
		// Data:
		&value_ptr_c, &value_length_c, &error_c)
	C.free(value_go)

	if error_c != nil {
		error_go := C.GoString(error_c)
		C.ukv_error_free(db.raw, error_c)
		return errors.New(error_go)
	}

	return nil
}

func (db *database) Get(key uint64) ([]byte, error) {

	// Even though we can't properly write without a single copy
	// from Go layer, but we can read entries from C-allocated buffers.
	error_c := C.ukv_error_t(nil)
	key_c := C.ukv_key_t(key)
	pulled_value_c := C.ukv_val_ptr_t(nil)
	pulled_value_length_c := C.ukv_val_len_t(0)
	arena_c := (C.ukv_arena_ptr_t)(nil)
	arena_length_c := (C.size_t)(0)

	C.ukv_read(
		// Inputs:
		db.raw, &key_c, C.size_t(1),
		// Configs:
		(*C.ukv_column_t)(nil), C.size_t(0), (C.ukv_options_read_t)(nil),
		// Temporary Memory:
		&arena_c, &arena_length_c,
		// Data:
		&pulled_value_c, &pulled_value_length_c, &error_c)

	pulled_value_go := C.GoBytes(unsafe.Pointer(pulled_value_c), C.int(pulled_value_length_c))
	C.ukv_read_free(db.raw, arena_c, arena_length_c)
	// We can also jsut cast it to slice without copies
	// pulled_value_go := (*[1 << 30]byte)(unsafe.Pointer(pulled_value_c))[:arena_length_c:arena_length_c]

	if error_c != nil {
		error_go := C.GoString(error_c)
		C.ukv_error_free(db.raw, error_c)
		return nil, errors.New(error_go)
	}

	return pulled_value_go, nil

}

func main() {

	db := database{}
	db.Reconnect("")
	db.Set(10, &[]byte{10, 23})
	db.Get(10)

	// TODO: Outgoing channel for batch reads
	// TODO: Internal memory handle inside `database` to store reads
}
