def set_bit(i, n):
    """
    set bit n, n unchanged, return a new n
    """
    return i | (1 << n)


def get_bits(i, start, len):
    """
    Get part of a integer i, from bits start, length is len

    """
    return (i >> start) & ((1 << len) - 1)

def hexprint(data):
    """
    data will be aligned to 8 bytes first, then print out with line numbers
    hex values, and accsii chars

    """
    from binascii import hexlify
    #padding
    mod = len(data) % 8
    if mod != 0:
        for i in range(8 - mod):
            data = data + '\x00'
    ln = len(data) / 8
    for n in range(ln):
        line = data[n * 8: (n + 1) * 8]
        hd = hexlify(line).upper()
        print '%4x' % n, hd[:8], hd[8:], '   ',
        for c in line:
            if c.isalpha():
                print c,
        print


def split_records(data, record_size):
    """
    Split records array into a record list
    """
    n = len(data) / record_size
    for i in range(n):
        yield data[i * record_size : (i + 1) * record_size ]

def get_record(data, record_size, index):
    """
    get one record
    """
    return data[index * record_size : (index + 1) * record_size ]




if __name__ == '__main__':
    a = get_bits(0x62c3a, 0, 63)
    print '%x' % (a << 9)

    i = 3
    print set_bit(i, 0), set_bit(i, 1), set_bit(i, 7), set_bit(i, 8)

    for a in split_records('abcdefgh', 2):
        print a
