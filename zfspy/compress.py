"""
ZFSpy: Python bindings for ZFS

Copyright (C) 2008 Chen Zheng <nkchenz@gmail.com>

This file is licensed under the terms of the GNU General Public License
version 2. This program is licensed "as is" without any warranty of any
kind, whether express or implied.

lzjb for Python

"""

from util import *

max_offset = 1023
max_matched_len = 66

def find_match(data, start, str):
    if start >= str:
        return (None, None)
    str_len = len(data) - str
    if str_len < 3: # we must ensure we have at least 3 char to match
        return (None, None)
    # see if we can find the next 3 chars from start, we do not care about  multi ocurrences
    p = data[start:str].find(data[str:str+3])
    if p < 0:
        return (None, None)
    p = p + start # offset in data
    matched_len = 0
    # find the longest common beginning 
    while matched_len < str_len:
        if data[p + matched_len] != data[str + matched_len]:
            break
        else:
            matched_len = matched_len + 1
    if matched_len > max_matched_len:
        matched_len = max_matched_len
    return (str - p, matched_len)
        

def lzjb_compress(data):
    """
    for each map, there are 8 items, a item is:
        a match:
                look backward max_offset at most
                match length greater than 3, and less than max_offset
        else just a literal item  

    """
    encoded_data = ''
    l = len(data)
    i = 0
    while i < l:
        map = 0 
        cache = ''

        for n in range(8):
            search_start = i - max_offset
            if search_start < 0:
                search_start = 0
            offset, matched_len = find_match(data, search_start, i)
            if matched_len: # match
                cache = cache + chr(((matched_len - 3) << 2) | (offset >> 8)) + chr(get_bits(offset, 0, 8))
                i = i + matched_len
                map = set_bit(map, n)
            else: # literal
                cache = cache + data[i]
                i = i +1
            # there are less than 8 items
            if i >= l:
                encoded_data = encoded_data + chr(map) + cache
                return encoded_data
        # save the control byte map and 8 items cache
        encoded_data = encoded_data + chr(map) + cache
    
    return encoded_data

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
            if get_bits(map, n, 1) == 0:
                if i >= l:
                    #print 'possible corrupt data, not enough item found n=', n
                    return data
                data = data + encoded_data[i] # origial data
                i = i + 1
            else:
                if i + 1 >= l:
                    return None #it's surpposed that still 2 bytes left, must be corrupt 
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
    M = 'yadda yadda yadda,+blah+blah+blah'
    print 'E:'
    hexprint(E)
    print 'compressE:'
    hexprint(lzjb_compress(M))
    print 'M:'
    hexprint(M)
    print 'decompressM:'
    hexprint(lzjb_decompress(E))
