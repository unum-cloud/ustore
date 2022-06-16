import ukv


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


def test_multiple_transactions():
    keys_count = 100

    def set_same_value(value):
        with ukv.Transaction(db) as txn:
            for index in range(keys_count):
                txn.set(index, value.encode())

    def check_db(db, value):
        for index in range(keys_count):
            assert db.get(index) == value.encode()

    db = ukv.DataBase()

    set_same_value('a')
    check_db(db, 'a')

    set_same_value('b')
    check_db(db, 'b')

    set_same_value('c')
    check_db(db, 'c')
