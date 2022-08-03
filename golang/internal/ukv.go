package ukv

/*
#cgo CFLAGS: -g -Wall -I${SRCDIR}/../../include
#include "ukv/db.h"
#include <stdlib.h>

ukv_val_len_t dereference_index(ukv_val_len_t* lens, ukv_size_t idx) { return lens[idx]; }

typedef void (*open_fn)(ukv_str_view_t, ukv_t*, ukv_error_t*);
typedef void (*error_free_fn)(ukv_error_t);
typedef void (*arena_free_fn)(ukv_t const, ukv_arena_t);
typedef void (*free_fn)(ukv_t);
typedef void (*read_fn)(ukv_t const, ukv_txn_t const, ukv_size_t const,
    ukv_collection_t const*, ukv_size_t const, ukv_key_t const*,
    ukv_size_t const, ukv_options_t const, ukv_val_len_t**,
    ukv_val_ptr_t*, ukv_arena_t*, ukv_error_t*);
typedef void (*write_fn)(ukv_t const, ukv_txn_t const,ukv_size_t const,
    ukv_collection_t const*, ukv_size_t const, ukv_key_t const*,
    ukv_size_t const, ukv_val_ptr_t const*, ukv_size_t const,
    ukv_val_len_t const*, ukv_size_t const, ukv_val_len_t const*,
    ukv_size_t const, ukv_options_t const, ukv_arena_t*, ukv_error_t*);


void u_open(void* fn, ukv_str_view_t c_config, ukv_t* c_db, ukv_error_t* c_error) {
	open_fn func = (open_fn)(fn);
	(*func)(c_config,c_db,c_error);
}

void u_error_free(void* fn, ukv_error_t c_error) {
	error_free_fn func = (error_free_fn)(fn);
	(*func)(c_error);
}

void u_arena_free(void* fn, ukv_t const c_db, ukv_arena_t c_arena) {
	arena_free_fn func = (arena_free_fn)(fn);
	(*func)(c_db, c_arena);
}

void u_free(void*fn, ukv_t c_db) {
	free_fn func = (free_fn)(fn);
	(*func)(c_db);
}

void u_read(void* fn, ukv_t const c_db, ukv_txn_t const c_txn, ukv_size_t const c_tasks_count,
		ukv_collection_t const* c_cols, ukv_size_t const c_cols_stride, ukv_key_t const* c_keys,
		ukv_size_t const c_keys_stride, ukv_options_t const c_options, ukv_val_len_t** c_found_lengths,
		ukv_val_ptr_t* c_found_values, ukv_arena_t* c_arena, ukv_error_t* c_error) {

	read_fn func = (read_fn)(fn);
	(*func)(c_db, c_txn, c_tasks_count, c_cols, c_cols_stride, c_keys, c_keys_stride,
			c_options, c_found_lengths, c_found_values, c_arena, c_error);
}

void u_write(void* fn, ukv_t const c_db, ukv_txn_t const c_txn, ukv_size_t const c_tasks_count,
		ukv_collection_t const* c_cols, ukv_size_t const c_cols_stride, ukv_key_t const* c_keys,
		ukv_size_t const c_keys_stride, ukv_val_ptr_t const c_vals, ukv_size_t const c_vals_stride,
		ukv_val_len_t const* c_offs, ukv_size_t const c_offs_stride, ukv_val_len_t const* c_lens,
		ukv_size_t const c_lens_stride, ukv_options_t const c_options, ukv_arena_t* c_arena, ukv_error_t* c_error) {

	write_fn func = (write_fn)(fn);
	ukv_val_ptr_t * val_ptr = (ukv_val_ptr_t*)c_vals;

	(*func)(c_db, c_txn, c_tasks_count, c_cols, c_cols_stride, c_keys, c_keys_stride, val_ptr,
			c_vals_stride, c_offs, c_offs_stride, c_lens, c_lens_stride, c_options, c_arena, c_error);
}

bool is_null(ukv_val_ptr_t ptr, int len) {
	return *(*(char**)ptr + len) == 0;
}

 const ukv_size_t size_of_key = sizeof(ukv_key_t);
 const ukv_size_t size_of_len = sizeof(ukv_val_len_t);
*/
import "C"
import (
	"errors"
	"unsafe"
)

type UKV_val_len_t = C.ukv_val_len_t

type BackendInterface struct {
	UKV_error_free      unsafe.Pointer
	UKV_arena_free      unsafe.Pointer
	UKV_open            unsafe.Pointer
	UKV_free            unsafe.Pointer
	UKV_read            unsafe.Pointer
	UKV_write           unsafe.Pointer
	UKV_val_len_missing UKV_val_len_t
}

/**
 * This class is modeled after Redis client and other ORMs:
 * https://github.com/go-redis/redis
 * https://bun.uptrace.dev
 *
 * Further Reading:
 * https://blog.marlin.org/cgo-referencing-c-library-in-go
 */
type DataBase struct {
	Backend BackendInterface
	raw     C.ukv_t
}

func forwardError(db *DataBase, error_c C.ukv_error_t) error {
	if error_c != nil {
		error_go := C.GoString(error_c)
		C.u_error_free(db.Backend.UKV_error_free, error_c)
		return errors.New(error_go)
	}
	return nil
}

func freeArena(db *DataBase, arena_c C.ukv_arena_t) {
	C.u_arena_free(db.Backend.UKV_arena_free, db.raw, arena_c)
}

func (db *DataBase) ReConnect(config string) error {

	error_c := C.ukv_error_t(nil)
	config_c := C.CString(config)
	defer C.free(unsafe.Pointer(config_c))

	C.u_open(db.Backend.UKV_open, config_c, &db.raw, &error_c)
	return forwardError(db, error_c)
}

func (db *DataBase) Close() {
	if db.raw != nil {
		C.u_free(db.Backend.UKV_free, db.raw)
		db.raw = nil
	}
}

func (db *DataBase) Set(key uint64, value []byte) error {

	// Passing values without copies seems essentially impossible
	// and causes: "cgo argument has Go pointer to Go pointer"
	// when we try to take pass pointer to to first object in `[]byte`.
	// https://stackoverflow.com/a/64867672
	error_c := C.ukv_error_t(nil)
	key_c := C.ukv_key_t(key)
	collection_c := (*C.ukv_collection_t)(nil)
	options_c := C.ukv_options_t(C.ukv_options_default_k)
	val_ptr := C.ukv_val_ptr_t(unsafe.Pointer(&value[0]))
	value_ptr_c := C.ukv_val_ptr_t(unsafe.Pointer(&val_ptr))
	value_length_c := C.ukv_val_len_t(len(value))
	value_offset_c := C.ukv_val_len_t(0)
	arena_c := (C.ukv_arena_t)(nil)
	defer freeArena(db, arena_c)

	C.u_write(db.Backend.UKV_write,
		db.raw, nil, 1,
		collection_c, 0,
		&key_c, 0,
		value_ptr_c, 0,
		&value_offset_c, 0,
		&value_length_c, 0,
		options_c, &arena_c, &error_c)
	return forwardError(db, error_c)
}

func (db *DataBase) SetBatch(keys []uint64, values [][]byte) error {

	error_c := C.ukv_error_t(nil)
	keys_c := (*C.ukv_key_t)(unsafe.Pointer(&keys[0]))
	collection_c := (*C.ukv_collection_t)(nil)
	options_c := C.ukv_options_t(C.ukv_options_default_k)
	value_ptr_c := C.ukv_val_ptr_t(unsafe.Pointer(&values[0]))
	task_count_c := C.size_t(len(values))

	offsets := make([]C.ukv_val_len_t, task_count_c)
	lens := make([]C.ukv_val_len_t, task_count_c)

	lens[0] = C.ukv_val_len_t(len(values[0]))
	offsets[0] = 0
	for i := C.size_t(1); i < task_count_c; i++ {
		lens[i] = C.ukv_val_len_t(len(values[i]))
		new_offs := offsets[i-1] + lens[i-1]
		for C.is_null(value_ptr_c, C.int(new_offs)) {
			new_offs++
		}
		offsets[i] = new_offs
	}

	arena_c := (C.ukv_arena_t)(nil)
	defer freeArena(db, arena_c)

	C.u_write(db.Backend.UKV_write,
		db.raw, nil, task_count_c,
		collection_c, 0,
		keys_c, C.size_of_key,
		value_ptr_c, 0,
		&offsets[0], C.size_of_len,
		&lens[0], C.size_of_len,
		options_c, &arena_c, &error_c)
	return forwardError(db, error_c)
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
	defer freeArena(db, arena_c)

	C.u_write(db.Backend.UKV_write,
		db.raw, nil, 1,
		collection_c, 0,
		&key_c, 0,
		value_ptr_c, 0,
		&value_offset_c, 0,
		&value_length_c, 0,
		options_c, &arena_c, &error_c)
	return forwardError(db, error_c)
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
	defer freeArena(db, arena_c)

	C.u_read(db.Backend.UKV_read,
		db.raw, nil, 1,
		collection_c, 0,
		&key_c, 0,
		options_c,
		&pulled_values_lengths_c,
		&pulled_values_c,
		&arena_c,
		&error_c)

	error_go := forwardError(db, error_c)
	if error_go != nil {
		return nil, error_go
	}

	// We can also jsut cast it to slice without copies
	// pulled_value_go := (*[1 << 30]byte)(unsafe.Pointer(pulled_value_c))[:tape_length_c:tape_length_c]
	pulled_value_length_c := C.dereference_index(pulled_values_lengths_c, 0)
	if pulled_value_length_c == db.Backend.UKV_val_len_missing {
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
	defer freeArena(db, arena_c)

	C.u_read(db.Backend.UKV_read,
		db.raw, nil, 1,
		collection_c, 0,
		&key_c, 0,
		options_c,
		&pulled_values_lengths_c,
		&pulled_values_c,
		&arena_c,
		&error_c)

	error_go := forwardError(db, error_c)
	if error_go != nil {
		return false, error_go
	}

	return C.dereference_index(pulled_values_lengths_c, 0) != db.Backend.UKV_val_len_missing, nil
}
