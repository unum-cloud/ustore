import ukv.stl as ukv


def only_explicit(col):

    col.set(3, 'x'.encode())
    col.set(4, 'y'.encode())
    assert col.get(3) == 'x'.encode()
    assert col.get(4) == 'y'.encode()


def only_operators(col):

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
    only_explicit(db)
    only_operators(db)


def test_named_collections():
    db = ukv.DataBase()
    col_sub = db['sub']
    col_dub = db['dub']
    only_explicit(col_sub)
    only_explicit(col_dub)
    only_operators(col_sub)
    only_operators(col_dub)


def test_main_collection_txn():

    db = ukv.DataBase()

    txn = ukv.Transaction(db)
    only_explicit(txn)
    txn.commit()

    txn = ukv.Transaction(db)
    only_operators(txn)
    txn.commit()


def test_main_collection_txn_ctx():

    with ukv.DataBase() as db:
        with ukv.Transaction(db) as txn:
            only_explicit(txn)

    with ukv.DataBase() as db:
        with ukv.Transaction(db) as txn:
            only_operators(txn)
