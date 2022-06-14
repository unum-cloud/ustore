import ukv


def test_explicit(col):

    col.set(3, 'x'.encode())
    col.set(4, 'y'.encode())
    assert col.get(3) == 'x'.encode()
    assert col.get(4) == 'y'.encode()


def test_operators(col):

    assert 1 not in col
    assert 2 not in col

    col[1] = 'a'.encode()
    col[2] = 'bb'.encode()
    assert 1 in col
    assert 2 in col
    assert col[1] == 'a'.encode()
    assert col[2] == 'bb'.encode()

    del col[1]
    del col[2]
    assert 1 not in col
    assert 2 not in col


def test_main_collection():
    db = ukv.DataBase()
    test_explicit(db)
    test_operators(db)


def test_named_collections():
    db = ukv.DataBase()
    col_sub = db['sub']
    col_dub = db['dub']
    test_explicit(col_sub)
    test_explicit(col_dub)
    test_operators(col_sub)
    test_operators(col_dub)


def test_main_collection_txn():

    with ukv.DataBase() as db:
        with ukv.Transaction(db) as txn:
            test_explicit(txn)
            test_operators(txn)
