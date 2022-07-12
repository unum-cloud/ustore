import ukv.stl as ukv


def only_explicit(col):

    col.set(3, b'x')
    col.set(4, b'y')
    assert col.get(3) == b'x'
    assert col.get(4) == b'y'
    assert col.get(4) != b'yy'

def only_overwrite(col):

    col.set(7, b'y')
    assert col.get(7) == b'y'
    assert col.get(7) != b'yy'

    # Overwrite with a different length string
    col.set(7, b'jjjjjjjj')
    assert col.get(7) == b'jjjjjjjj'
    col.set(7, b'yy')
    assert col.get(7) == b'yy'


def only_operators(col):

    col[1] = b'a'
    col[2] = b'bb'
    assert 1 in col
    assert 2 in col
    assert col[1] == b'a'
    assert col[2] == b'bb'

    del col[1]
    del col[2]
    assert 1 not in col
    assert 2 not in col


def test_main_collection():
    db = ukv.DataBase()
    only_explicit(db)
    only_overwrite(db)
    only_operators(db)


def test_named_collections():
    db = ukv.DataBase()
    col_sub = db['sub']
    col_dub = db['dub']
    only_explicit(col_sub)
    only_explicit(col_dub)
    only_overwrite(col_sub)
    only_overwrite(col_dub)
    only_operators(col_sub)
    only_operators(col_dub)


def test_main_collection_txn(): 

    db = ukv.DataBase()

    txn = ukv.Transaction(db)
    only_explicit(txn)
    txn.commit()

    txn = ukv.Transaction(db)
    only_overwrite(txn)
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
