import json

import ukv.stl as ukv


def only_json(col):

    original = json.dumps({
        'US': 'US',
        'Japan': '日本',
        'Armenia': 'Հայաստան'
    }).encode()

    col[1] = original
    assert 1 in col

    retrieved = json.loads(col[1].decode())
    assert original == retrieved, 'Failed to recover document'

    # Batch lookups, via lists AND via tuples
    col[(1, 2, 3, 4)]
    col[[1, 2, 3, 4]]

    # Batch checks and deletions
    (1, 2, 3, 4) in col
    del col[(1, 2, 3, 4)]

    # Batch field lookup
    # Supporting JSON-Pointers
    col[1 / '/US']
    col[1]['US']
    col[(1, 2, 3)][('US', 'Phone', 'Name')]

    # Listing a range of documents
    col[1:]
    col[1:].keys()
    col[1:1000]


def testing():
    main = ukv.DataBase().main()
    only_json(main)
