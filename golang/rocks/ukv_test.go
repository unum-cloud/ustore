package ukv_test

import (
	"testing"

	ukv "github.com/unum-cloud/UKV/golang/rocks"
)

func TestDataBaseSimple(t *testing.T) {

	db := ukv.CreateDB()
	if err := db.ReConnect(""); err != nil {
		t.Fatalf("Couldn't open db: %s", err)
	}

	defer db.Close()
	if err := db.Set(42, []byte{1, 1, 1}); err != nil {
		t.Fatalf("Couldn't set value: %s", err)
	}

	if _, err := db.Get(42); err != nil {
		t.Fatalf("Couldn't get value: %s", err)
	}

	if _, err := db.Contains(42); err != nil {
		t.Fatalf("Couldn't check value existance: %s", err)
	}
}

func TestDataBaseBatchInsert(t *testing.T) {

	db := ukv.CreateDB()
	if err := db.ReConnect(""); err != nil {
		t.Fatalf("Couldn't open db: %s", err)
	}

	defer db.Close()
	keys := []uint64{4, 6, 8}
	values := [][]byte{
		[]byte("Hello"),
		[]byte("This"),
		[]byte("Day")}

	if err := db.SetBatch(keys, values); err != nil {
		t.Fatalf("Couldn't set value: %s", err)
	}

	for i := 0; i < len(keys); i++ {
		val, err := db.Get(keys[i])
		if err != nil {
			t.Fatalf("Couldn't get value: %s", err)
		}
		if string(val) != string(values[i]) {
			t.Fatalf("Wrong Value: Expected: %s, Got: %s", string(values[i]), string(val))
		}
	}

	if _, err := db.Get(42); err != nil {
		t.Fatalf("Couldn't get value: %s", err)
	}

	if _, err := db.Contains(42); err != nil {
		t.Fatalf("Couldn't check value existance: %s", err)
	}
}
