import json
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.csv as csv
import pyarrow.dataset as ds
import ukv.umem as ukv
pa.get_include()


def create_table(db):
    col = db.main
    docs = col.docs
    docs[0] = {'name': 'Lex', 'lastname': 'Fridman', 'tweets': 2221}
    docs[1] = {'name': 'Andrew', 'lastname': 'Huberman', 'tweets': 3935}
    docs[2] = {'name': 'Joe', 'lastname': 'Rogan', 'tweets': 45900}
    return col.table


def test_to_arrow():
    db = ukv.DataBase()
    table = create_table(db)
    # Tweets
    df_tweets = pd.DataFrame({'tweets': [2221, 3935, 45900]}, dtype=np.int32)
    assert table[['tweets']].astype('int32').loc([0, 1, 2]).to_arrow() \
        == pa.RecordBatch.from_pandas(df_tweets)

    # Names
    df_names = pd.DataFrame({'name': [b'Lex', b'Andrew', b'Joe']})
    assert table[['name']].astype(
        'bytes').to_arrow() == pa.RecordBatch.from_pandas(df_names)

    # Tweets and Names
    df_names_and_tweets = pd.DataFrame([
        {'name': 'Lex', 'tweets': 2221},
        {'name': 'Andrew', 'tweets': 3935},
        {'name': 'Joe', 'tweets': 45900}
    ])
    schema = pa.schema([
        pa.field('name', pa.binary()),
        pa.field('tweets', pa.int32())])

    assert table.astype({'name': 'bytes', 'tweets': 'int32'}
                        ).to_arrow() == pa.RecordBatch.from_pandas(df_names_and_tweets, schema)

    # Surnames and Names
    df_names_and_tweets = pd.DataFrame([
        {'name': 'Lex', 'lastname': 'Fridman'},
        {'name': 'Andrew', 'lastname': 'Huberman'},
        {'name': 'Joe', 'lastname': 'Rogan'}
    ])
    schema = pa.schema([
        pa.field('name', pa.binary()),
        pa.field('lastname', pa.binary())])

    assert table.astype({'name': 'bytes', 'lastname': 'bytes'}
                        ).to_arrow() == pa.RecordBatch.from_pandas(df_names_and_tweets, schema)

    # All
    all = pd.DataFrame([
        {'name': 'Lex', 'tweets': 2221, 'lastname': 'Fridman'},
        {'name': 'Andrew', 'tweets': 3935, 'lastname': 'Huberman'},
        {'name': 'Joe', 'tweets': 45900, 'lastname': 'Rogan'}
    ])
    schema = pa.schema([
        pa.field('name', pa.binary()),
        pa.field('tweets', pa.int32()),
        pa.field('lastname', pa.binary())])

    assert table.astype({'name': 'bytes', 'tweets': 'int32', 'lastname': 'bytes'}
                        ).to_arrow() == pa.RecordBatch.from_pandas(all, schema)

    db.clear()


def test_update():
    db = ukv.DataBase()
    col = db.main
    docs = col.docs
    table = col.table

    docs[0] = {'name': 'Lex', 'lastname': 'Fridman', 'tweets': 2221}
    docs[1] = {'name': 'Andrew', 'lastname': 'Huberman', 'tweets': 3935}
    docs[2] = {'name': 'Joe', 'lastname': 'Rogan', 'tweets': 45900}

    tweets = pa.array([2, 4, 5])
    names = pa.array(["Jack", "Charls", "Sam"])
    column_names = ["tweets", "name"]

    modifier = pa.RecordBatch.from_arrays([tweets, names], names=column_names)
    table.loc(slice(0, 2)).update(modifier)

    assert docs[0] == {'name': 'Jack', 'lastname': 'Fridman', 'tweets': 2}
    assert docs[1] == {'name': 'Charls', 'lastname': 'Huberman', 'tweets': 4}
    assert docs[2] == {'name': 'Sam', 'lastname': 'Rogan', 'tweets': 5}

    db.clear()


def test_csv():
    db = ukv.DataBase()
    table = create_table(db)
    table.loc([0, 1])
    table.astype({'name': 'str', 'tweets': 'int64'}
                 ).to_csv("tmp/pandas.csv")

    df = table.astype({'name': 'str', 'tweets': 'int64'}
                      ).to_arrow()

    exported_df = csv.read_csv("tmp/pandas.csv").to_batches()[0]
    assert df == exported_df

    db.clear()


def test_parquet():
    db = ukv.DataBase()
    table = create_table(db)
    table.loc([0, 1])
    table.astype({'name': 'str', 'tweets': 'int32'}
                 ).to_parquet("tmp/pandas.parquet")

    df = table.astype({'name': 'str', 'tweets': 'int32'}
                      ).to_arrow()

    exported_df = next(ds.dataset("tmp/pandas.parquet",
                       format="parquet").to_batches())
    assert df == exported_df

    db.clear()


def test_json():
    db = ukv.DataBase()
    table = create_table(db)
    table.loc([0, 1, 2])
    table.astype({'name': 'str', 'tweets': 'int32'})

    expected_json = '''{"name":{"0":"Lex","1":"Andrew","2":"Joe"},"tweets":{"0":2221,"1":3935,"2":45900}}'''
    exported_json = table.to_json()
    assert exported_json == expected_json

    table.to_json("tmp/pandas.json")
    expected_json = json.loads(expected_json)
    f = open("tmp/pandas.json")
    exported_json = json.load(f)
    f.close()
    assert exported_json == expected_json

    db.clear()


def test_merge():
    db = ukv.DataBase()
    col1 = db['table1']
    docs1 = col1.docs
    table1 = col1.table
    docs1[0] = {'name': 'Lex', 'tweets': 0}
    docs1[1] = {'name': 'Andrew', 'tweets': 1}
    docs1[2] = {'name': 'Joe', 'tweets': 2}

    col2 = db['table2']
    docs2 = col2.docs
    table2 = col2.table
    docs2[0] = {'name': 'Lex', 'lastname': 'Fridman', 'tweets': 10}
    docs2[1] = {'name': 'Charls', 'lastname': 'Huberman'}
    docs2[3] = {'name': 'Carl', 'lastname': 'Rogan', 'tweets': 3}

    table1.loc([0, 1, 2])
    table2.loc([0, 1, 2, 3])
    table1.merge(table2)

    expected = pa.RecordBatch.from_pandas(pd.DataFrame([
        {'name': 'Lex',  'tweets': 10, 'lastname': 'Fridman'},
        {'name': 'Charls', 'tweets': 1, 'lastname': 'Huberman'},
        {'name': 'Joe', 'tweets': 2, 'lastname': None},
        {'name': 'Carl', 'tweets': 3, 'lastname': 'Rogan'},
    ]))

    table1.loc([0, 1, 2, 3])
    exported = table1.astype(
        {'name': 'str', 'tweets': 'int64', 'lastname': 'str'}).to_arrow()

    assert expected == exported
