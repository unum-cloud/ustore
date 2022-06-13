import ukv


if __name__ == '__main__':
    col = ukv.UKV()
    col[1] = 'a'.encode()
    col[2] = 'bb'.encode()
    col[3] = 'c'.encode()

    print(col[1])
    print(col[2])
    print(col[3])
