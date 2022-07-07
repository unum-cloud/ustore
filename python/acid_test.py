
import pytest
import ukv.stl as ukv


def test_transaction_set():
    keys_count = 100
    db = ukv.DataBase()

    with ukv.Transaction(db) as txn:
        for index in range(keys_count):
            txn.set(index, str(index).encode())
    for index in range(keys_count):
        assert db.get(index) == str(index).encode()


def test_transaction_remove():
    keys_count = 100
    db = ukv.DataBase()
    for index in range(keys_count):
        db.set(index, str(index).encode())

    with ukv.Transaction(db) as txn:
        for index in range(keys_count):
            del txn[index]
    for index in range(keys_count):
        assert index not in db


def test_set_with_2_transactions():
    keys_count = 100

    db = ukv.DataBase()
    txn1 = ukv.Transaction(db)
    txn2 = ukv.Transaction(db)

    for index in range(0, keys_count, 2):
        txn1.set(index, str(index).encode())
    for index in range(1, keys_count, 2):
        txn2.set(index, str(index).encode())

    txn1.commit()
    txn2.commit()

    for index in range(keys_count):
        assert db.get(index) == str(index).encode()


def test_remove_with_2_transactions():
    keys_count = 100

    db = ukv.DataBase()
    for index in range(keys_count):
        db.set(index, str(index).encode())

    txn1 = ukv.Transaction(db)
    txn2 = ukv.Transaction(db)
    for index in range(0, keys_count, 2):
        del txn1[index]
    for index in range(1, keys_count, 2):
        del txn2[index]

    txn1.commit()
    txn2.commit()

    for index in range(keys_count):
        assert index not in db


def test_set_with_2_interleaving_transactions():
    keys_count = 100

    db = ukv.DataBase()
    txn1 = ukv.Transaction(db)
    txn2 = ukv.Transaction(db)

    for index in range(keys_count):
        txn1.set(index, 'a'.encode())
    with pytest.raises(Exception):
        for index in range(keys_count):
            txn2.set(index, 'b'.encode())

        txn2.commit()
        txn1.commit()


def test_remove_with_2_interleaving_transactions():
    keys_count = 100

    db = ukv.DataBase()
    for index in range(keys_count):
        db.set(index, str(index).encode())

    txn1 = ukv.Transaction(db)
    txn2 = ukv.Transaction(db)
    for index in range(keys_count):
        del txn1[index]
    with pytest.raises(Exception):
        for index in range(keys_count):
            del txn2[index]

        txn2.commit()
        txn1.commit()


def test_transaction_with_multiple_collections():
    coll_count = 100
    keys_count = 100
    db = ukv.DataBase()

    with ukv.Transaction(db) as txn:
        for col_index in range(coll_count):
            for index in range(keys_count):
                txn.set(str(col_index), index, str(index).encode())

    with ukv.Transaction(db) as txn:
        for col_index in range(coll_count):
            for index in range(keys_count):
                assert txn.get(str(col_index), index) == str(index).encode()
                assert txn[str(col_index)][index] == str(index).encode()
                assert index in db[str(col_index)]


def test_conflict():
    # Set with db befor get
    db1 = ukv.DataBase()
    txn = ukv.Transaction(db1)
    db1.set(0, "value".encode())
    with pytest.raises(Exception):
        txn.get(0)

    # Set with transaction before get
    db2 = ukv.DataBase()
    txn1 = ukv.Transaction(db2)
    with ukv.Transaction(db2) as txn2:
        txn2.set(0, 'value'.encode())
    with pytest.raises(Exception):
        txn1.get(0)


def test_transparent_reads():
    db = ukv.DataBase()
    txn = ukv.Transaction(db)
    txn.get(0)
    txn.set(1, "value".encode())
    db.set(0, "value".encode())
    with pytest.raises(Exception):
        txn.commit()
