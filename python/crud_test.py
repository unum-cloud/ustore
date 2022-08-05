import pytest
import numpy as np

import ukv.stl as ukv


def only_explicit(col):
    col.set(3, b'x')
    col.set(4, b'y')
    assert col.has_key(3)
    assert col.has_key(4)

    assert col.get(3) == b'x'
    assert col.get(4) == b'y'
    assert col.get(4) != b'yy'

    col.pop(3)
    col.pop(4)
    assert not col.has_key(3)
    assert not col.has_key(4)


def only_explicit_batch(col):
    col.set((3, 4), (b'xx', b'yy'))
    col.set([3, 4], (b'x', b'y'))
    col.get((3, 4))
    assert col.has_key((3, 4)) == (True, True)
    assert col.get((3, 4)) == (b'x', b'y')

    col.set((3, 4), None)
    assert col.has_key((3, 4)) == (False, False)

    col.pop((3, 4))


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


def only_overwrite(col):
    col.set(7, b'y')
    assert col.get(7) == b'y'
    assert col.get(7) != b'yy'

    # Overwrite with a different length string
    col.set(7, b'jjjjjjjj')
    assert col.get(7) == b'jjjjjjjj'
    col.set(7, b'yy')
    assert col.get(7) == b'yy'


def batch_insert(col):
    return
    col.clear()
    count_keys: int = 20
    keys: list[int] = list(range(1, count_keys + 1))
    keeper = []

    for i in keys[:count_keys//2]:
        keeper.append((f'{i}' * i).encode())

    for i in keys[count_keys//2:]:
        keeper.append((f'{i}' * int(i-count_keys//2)).encode())

    keys = np.array(keys, dtype=np.uint64)
    values = np.array(keeper)

    col.set(keys, values)

    for i in keys[:count_keys//2]:
        assert col.get(i) == (f'{i}' * i).encode()

    for i in keys[count_keys//2:]:
        assert col.get(i) == (f'{i}' * int(i-count_keys//2)).encode()


def scan(col):
    col.clear()

    col[10] = b'a'
    col[20] = b'aa'
    col[30] = b'aaa'
    col[40] = b'aaaa'
    col[50] = b'aaaaa'
    col[60] = b'aaaaaa'

    keys = col.scan(10, 6)
    assert np.array_equal(keys, [10, 20, 30, 40, 50, 60])

    keys = col.scan(20, 5)
    assert np.array_equal(keys, [20, 30, 40, 50, 60])

    keys = col.scan(30, 1)
    assert np.array_equal(keys, [30])

    keys = col.scan(40, 2)
    assert np.array_equal(keys, [40, 50])

    keys = col.scan(60, 1)
    assert np.array_equal(keys, [60])


def iterate(col):
    col.clear()
    col[1] = b'a'
    col[2] = b'aa'
    col[3] = b'aaa'
    col[4] = b'aaaa'
    col[5] = b'aaaaa'
    col[6] = b'aaaaaa'

    # Iterate keys
    iterated_keys = []
    for key in col.keys:
        iterated_keys.append(key)

    assert iterated_keys == [1, 2, 3, 4, 5, 6]
    assert np.array_equal(col.keys[2:4], [3, 4])
    with pytest.raises(Exception):
        col.keys[4:2]
    iterated_keys.clear()
    for key in col.keys.since(3):
        iterated_keys.append(key)
    assert iterated_keys == [3, 4, 5, 6]
    iterated_keys.clear()
    for key in col.keys.until(4):
        iterated_keys.append(key)
    assert iterated_keys == [1, 2, 3, 4]

    # Iterate items
    iterated_items = []
    for item in col.items:
        iterated_items.append(item)

    assert iterated_items == [
        (1, b'a'), (2, b'aa'), (3, b'aaa'), (4, b'aaaa'), (5, b'aaaaa'), (6, b'aaaaaa')]
    iterated_items.clear()
    for item in col.items.since(3):
        iterated_items.append(item)
    assert iterated_items == [
        (3, b'aaa'), (4, b'aaaa'), (5, b'aaaaa'), (6, b'aaaaaa')]
    iterated_items.clear()
    for item in col.items.until(4):
        iterated_items.append(item)
    assert iterated_items == [
        (1, b'a'), (2, b'aa'), (3, b'aaa'), (4, b'aaaa')]


def test_main_collection():
    db = ukv.DataBase().main
    only_explicit(db)
    only_explicit_batch(db)
    only_overwrite(db)
    only_operators(db)
    batch_insert(db)
    scan(db)
    iterate(db)


def test_named_collections():
    db = ukv.DataBase()
    col_sub = db['sub']
    col_dub = db['dub']
    scan(col_sub)
    scan(col_dub)
    iterate(col_sub)
    iterate(col_dub)
    only_explicit(col_sub)
    only_explicit(col_dub)
    only_overwrite(col_sub)
    only_overwrite(col_dub)
    only_operators(col_sub)
    only_operators(col_dub)
    batch_insert(col_sub)
    batch_insert(col_dub)


def test_main_collection_txn():

    db = ukv.DataBase()

    txn = ukv.Transaction(db)
    only_explicit(txn.main)
    txn.commit()

    txn = ukv.Transaction(db)
    only_explicit_batch(txn.main)
    txn.commit()

    txn = ukv.Transaction(db)
    only_overwrite(txn.main)
    txn.commit()

    txn = ukv.Transaction(db)
    only_operators(txn.main)
    txn.commit()


def test_main_collection_txn_ctx():

    with ukv.DataBase() as db:
        with ukv.Transaction(db) as txn:
            only_explicit(txn.main)

    with ukv.DataBase() as db:
        with ukv.Transaction(db) as txn:
            only_operators(txn.main)

    with ukv.DataBase() as db:
        with ukv.Transaction(db) as txn:
            only_explicit_batch(txn.main)
