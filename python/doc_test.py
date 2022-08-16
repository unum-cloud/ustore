import json

import ukv.stl as ukv


def only_json(col):
    docs = col.docs

    original = {
        'US': 'US',
        'Japan': '日本',
        'Armenia': 'Հայաստան',
    }
    docs[1] = original
    assert 1 in col

    retrieved = docs[1]
    assert original == retrieved, 'Failed to recover document'

    docs[1].patch([
        {'op': 'add', 'path': '/swag', 'value': 'maximum'},
        # {'op': 'replace', 'path': '/baz', 'value': 'boo'},
        # {'op': 'remove', 'path': '/foo'}
    ])

    docs[1].merge({
        '/swag': 'ultimate'
    })

    # Batch field lookup
    # Supporting JSON-Pointers
    docs['1/US']


def only_table(col):
    docs = col.docs
    docs[1] = {'name': 'Lex', 'lastname': 'Fridman', 'tweets': 2221}
    docs[2] = {'name': 'Andrew', 'lastname': 'Huberman', 'tweets': 3935}
    docs[3] = {'name': 'Joe', 'lastname': 'Rogan', 'tweets': 45900}

    table = col.table

    # Single-type tables
    assert table[['tweets']].astype('int32').loc[1, 2, 3] \
        == [2221, 3935, 45900]
    assert table[['name']].astype('string').head(4) \
        == ['Lex', 'Andrew', 'Joe', None]
    assert table[['tweets']].astype('string').head(4) \
        == ['2221', '3935', '45900', None]

    results = table.astype({
        'name': 'string',
        'tweets': 'int64'
    }).loc[1, 2, 3]
    assert results[0] == ['Lex', 'Andrew', 'Joe']
    assert results[1] == [2221, 3935, 45900]

    results = table.loc[100:].head(10)


def testing():
    db = ukv.DataBase()
    only_json(db.main)
    only_table(db.main)
