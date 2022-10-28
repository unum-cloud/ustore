package ukv_test

import (
	"testing"

	utest "github.com/unum-cloud/UKV/golang/internal/testing"
	ukv "github.com/unum-cloud/UKV/golang/umem"
)

func TestDataBaseSimple(t *testing.T) {
	db := ukv.CreateDB()
	utest.DataBaseSimpleTest(&db, t)
}

func TestDataBaseBatchInsert(t *testing.T) {
	db := ukv.CreateDB()
	utest.DataBaseBatchInsertTest(&db, t)
}
