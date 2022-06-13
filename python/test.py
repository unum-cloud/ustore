import ukv


if __name__ == '__main__':
    db = ukv.DataBase()
    with db as col:
        print('opened')

        col[1] = 'a'.encode()
        col[2] = 'bb'.encode()
        col[3] = 'c'.encode()
        print('filled')

        print(col[1])
        print(col[2])
        print(col[3])
        print('exported')

        col.set('doubled', 1, 'aa'.encode())
        col.set('doubled', 2, 'bbbb'.encode())
        col.set('doubled', 3, 'cc'.encode())
        print(col.get('doubled', 1))
        print(col.get('doubled', 2))
        print(col.get('doubled', 3))

    # with ukv.DataBase() as db:

    #     with ukv.Transaction(db, 10) as txn:
    #         col = txn['main']

    #         col[1] = 'a'.encode()
    #         col[2] = 'bb'.encode()
    #         col[3] = 'c'.encode()
    #         print('filled')

    #         print(col[1])
    #         print(col[2])
    #         print(col[3])
    #         print('exported')
