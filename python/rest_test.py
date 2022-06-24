
import pytest
import ukv


def test_batch_reads():
    keys_count = 100
    db = ukv.DataBase()

    with ukv.Transaction(db) as txn:
        for index in range(keys_count):
            txn.set(index, str(index).encode())
    for index in range(keys_count):
        assert db.get(index) == str(index).encode()
