"""
ZFSpy: Python bindings for ZFS

Copyright (C) 2008 Chen Zheng <nkchenz@gmail.com>

This file is licensed under the terms of the GNU General Public License
version 2. This program is licensed "as is" without any warranty of any
kind, whether express or implied.
"""
import conf

class ZPool(object):

    def __init__(self):
        self.pools = None
        pass

    def import_cached(self):
        """
        Import zpool config cache from ZPOOL_CACHE
        """

    def status(self):
        pass



if __name__ == '__main__':
        print 'zpool selftest'
        print conf.ZPOOL_CACHE
