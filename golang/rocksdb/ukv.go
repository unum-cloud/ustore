package ukv

/*
#cgo LDFLAGS: -L${SRCDIR}/../../lib -lukv_embedded_rocksdb -lrocksdb -ldl -lm -lstdc++
#cgo CFLAGS: -g -Wall -I${SRCDIR}/../../include

#include "ukv/db.h"
#include <stdlib.h>
*/
import "C"
import (
	u "github.com/unum-cloud/UKV/golang/internal"
)

type RocksDB struct {
	u.DataBase
}

func CreateDB() RocksDB {
	backend := u.BackendInterface{
		UKV_error_free:      C.ukv_error_free,
		UKV_arena_free:      C.ukv_arena_free,
		UKV_open:            C.ukv_database_init,
		UKV_free:            C.ukv_database_free,
		UKV_read:            C.ukv_read,
		UKV_write:           C.ukv_write,
		UKV_val_len_missing: u.ukv_length_t(C.ukv_length_missing_k)}

	db := RocksDB{DataBase: u.DataBase{Backend: backend}}
	return db
}
