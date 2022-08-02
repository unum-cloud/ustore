package ukv_test

import (
	"testing"

	ukv "github.com/unum-cloud/UKV/golang/stl"
)

func TestDataBaseSimple(t *testing.T) {

	db := ukv.CreateDB()
	if err := db.ReConnect(""); err != nil {
		t.Fatalf("Couldn't open db: %s", err)
	}

	defer db.Close()
	if err := db.Set(42, &[]byte{1, 1, 1}); err != nil {
		t.Fatalf("Couldn't set value: %s", err)
	}

	if _, err := db.Get(42); err != nil {
		t.Fatalf("Couldn't get value: %s", err)
	}

	if _, err := db.Contains(42); err != nil {
		t.Fatalf("Couldn't check value existance: %s", err)
	}
}
