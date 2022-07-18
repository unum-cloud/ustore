package ukv

/*
#cgo CFLAGS: -g -Wall -I${SRCDIR}/../include
#cgo LDFLAGS: -L${SRCDIR}/../build/lib -lukv_stl -lstdc++

#include "ukv/ukv.h"
#include <stdlib.h>

ukv_val_len_t dereference_index(ukv_val_len_t lens, ukv_size_t idx) { return lens[idx]; }
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
type DataBase struct {
	raw C.ukv_t
}

func forwardError(error_c C.ukv_error_t) error {
	if error_c != nil {
		error_go := C.GoString(error_c)
		C.ukv_error_free(error_c)
		return errors.New(error_go)
	}
	return nil
}

func cleanTape(db *DataBase, arena_c C.ukv_arena_t) {
	C.ukv_arena_free(db.raw, arena_c)
}

func (db *DataBase) ReConnect(config string) error {

	error_c := C.ukv_error_t(nil)
	config_c := C.CString(config)
	defer C.free(unsafe.Pointer(config_c))

	C.ukv_open(config_c, &db.raw, &error_c)
	return forwardError(error_c)
}

func (db *DataBase) Close() {
	if db.raw != nil {
		C.ukv_free(db.raw)
		db.raw = nil
	}
}

func (db *DataBase) Set(key uint64, value *[]byte) error {

	// Passing values without copies seems essentially impossible
	// and causes: "cgo argument has Go pointer to Go pointer"
	// when we try to take pass pointer to to first object in `[]byte`.
	// https://stackoverflow.com/a/64867672
	error_c := C.ukv_error_t(nil)
	key_c := C.ukv_key_t(key)
	collection_c := (*C.ukv_collection_t)(nil)
	options_c := C.ukv_options_t(C.ukv_options_default_k)
	value_go := C.CBytes(*value)
	value_ptr_c := C.ukv_val_ptr_t(value_go)
	value_length_c := C.ukv_val_len_t(len(*value))
	value_offset_c := C.ukv_val_len_t(0)
	arena_c := (C.ukv_arena_t)(nil)
	defer C.free(value_go)
	defer cleanTape(db, arena_c)

	C.ukv_write(
		db.raw, nil, 1,
		collection_c, 0,
		&key_c, 0,
		&value_ptr_c, 0,
		&value_offset_c, 0,
		&value_length_c, 0,
		options_c, &arena_c, &error_c)
	return forwardError(error_c)
}

func (db *DataBase) Delete(key uint64) error {

	// Passing values without copies seems essentially impossible
	// and causes: "cgo argument has Go pointer to Go pointer"
	// when we try to take pass pointer to to first object in `[]byte`.
	// https://stackoverflow.com/a/64867672
	error_c := C.ukv_error_t(nil)
	key_c := C.ukv_key_t(key)
	collection_c := (*C.ukv_collection_t)(nil)
	options_c := C.ukv_options_t(C.ukv_options_default_k)
	value_ptr_c := C.ukv_val_ptr_t(nil)
	value_length_c := C.ukv_val_len_t(0)
	value_offset_c := C.ukv_val_len_t(0)
	arena_c := (C.ukv_arena_t)(nil)
	defer cleanTape(db, arena_c)

	C.ukv_write(
		db.raw, nil, 1,
		collection_c, 0,
		&key_c, 0,
		&value_ptr_c, 0,
		&value_offset_c, 0,
		&value_length_c, 0,
		options_c, &arena_c, &error_c)
	return forwardError(error_c)
}

func (db *DataBase) Get(key uint64) ([]byte, error) {

	// Even though we can't properly write without a single copy
	// from Go layer, but we can read entries from C-allocated buffers.
	error_c := C.ukv_error_t(nil)
	key_c := C.ukv_key_t(key)
	collection_c := (*C.ukv_collection_t)(nil)
	options_c := C.ukv_options_t(C.ukv_options_default_k)
	pulled_values_lengths_c := (*C.ukv_val_len_t)(nil)
	pulled_values_c := (C.ukv_val_ptr_t)(nil)
	arena_c := (C.ukv_arena_t)(nil)
	defer cleanTape(db, arena_c)

	C.ukv_read(
		db.raw, nil, 1,
		collection_c, 0,
		&key_c, 0,
		options_c,
		&pulled_values_lengths_c,
		&pulled_values_c,
		&arena_c,
		&error_c)

	error_go := forwardError(error_c)
	if error_go != nil {
		return nil, error_go
	}

	// We can also jsut cast it to slice without copies
	// pulled_value_go := (*[1 << 30]byte)(unsafe.Pointer(pulled_value_c))[:tape_length_c:tape_length_c]
	pulled_value_length_c := C.dereference_index(pulled_values_lengths_c, 0)
	if pulled_value_length_c == C.ukv_val_len_missing_k {
		return nil, nil
	} else {
		pulled_value_go := C.GoBytes(unsafe.Pointer(pulled_values_c), C.int(pulled_value_length_c))
		return pulled_value_go, nil
	}
}

func (db *DataBase) Contains(key uint64) (bool, error) {

	// Even though we can't properly write without a single copy
	// from Go layer, but we can read entries from C-allocated buffers.
	error_c := C.ukv_error_t(nil)
	key_c := C.ukv_key_t(key)
	collection_c := (*C.ukv_collection_t)(nil)
	options_c := C.ukv_options_t(C.ukv_option_read_lengths_k)
	pulled_values_lengths_c := (*C.ukv_val_len_t)(nil)
	pulled_values_c := (C.ukv_val_ptr_t)(nil)
	arena_c := (C.ukv_arena_t)(nil)
	defer cleanTape(db, arena_c)

	C.ukv_read(
		db.raw, nil, 1,
		collection_c, 0,
		&key_c, 0,
		options_c,
		&pulled_values_lengths_c,
		&pulled_values_c,
		&arena_c,
		&error_c)

	error_go := forwardError(error_c)
	if error_go != nil {
		return false, error_go
	}

	return C.dereference_index(pulled_values_lengths_c, 0) != C.ukv_val_len_missing_k, nil
}
