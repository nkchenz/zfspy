from util import *

def lzjb_decompress(encoded_data):
    """
    We dont care about the decode data length, we treat the encoded_data as all useful
    and decode them all. Please feed us well format data, and cut the decoded data
    to the length you like.

    If wrong format found,  return None
    """
    data = ''
    l = len(encoded_data)
    i = 0
    while i < l:
        # 8 bits repsent 8 item,  the two bytes of copy item is only ONE item, carefull!
        map = ord(encoded_data[i]) # get map
        i = i + 1
        for n in range(8):
            if i >= l:
                #print 'possible corrupt data, not enough item found n=', n
                return data
            if get_bits(map, n, 1) == 0:
                data = data + encoded_data[i] # origial data
                i = i + 1
            else:
                # this is a copy item here
                matched_len = get_bits(ord(encoded_data[i]), 2, 6) + 3 # high 6 bits
                # low 2 bits of the first byte and the second byte
                offset = (get_bits(ord(encoded_data[i]), 0, 2) << 8) + ord(encoded_data[i+1])
                i = i + 2
                p = len(data) # current data pointer
                for nn in range(matched_len):
                    copy_p = p + nn - offset 
                    if copy_p < 0:
                        #print 'crrupt data, i=', i
                        return None 
                    data = data + data[copy_p]
    return data

if __name__ == '__main__':
    E = '\x40yadda \x20\x06,\x20+blah\x1c\x05'
    print lzjb_decompress(E)
    E = '\x00\x00'
    print len(lzjb_decompress(E))
