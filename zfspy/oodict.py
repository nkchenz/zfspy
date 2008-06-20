"""
ZFSpy: Python bindings for ZFS

Copyright (C) 2008 Chen Zheng <nkchenz@gmail.com>

This file is licensed under the terms of the GNU General Public License
version 2. This program is licensed "as is" without any warranty of any
kind, whether express or implied.
"""

class OODict(dict):
    """
    OODict
        OO style dict

    Examples:
        >>> a = OODict()
        >>> a.fish = 'fish'
        >>> a['fish']
        'fish'
        >>> a['water'] = 'water'
        >>> a.water
        'water'
        >>> a.test = {'value': 1}
        >>> a.test2 = OODict({'name': 'test2', 'value': 2})
        >>> a.test, a.test2.name, a.test2.value
        (1, 'test2', 2)
    """
    def __init__(self, data = {}):
        dict.__init__(self, data)

    def __getattr__(self, key):
        value = self.__getitem__(key)
        # if value is the only key in object, it can be omited
        if isinstance(value, dict) and value.keys() == ['value']:
            return value['value']
        else:
            return value

    def __setattr__(self, key, value):
        self.__setitem__(key, value)


if __name__ == "__main__":
    import doctest
    doctest.testmod()
