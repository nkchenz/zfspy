"""
ZFSpy: Python bindings for ZFS

Copyright (C) 2008 Chen Zheng <nkchenz@gmail.com>

This file is licensed under the terms of the GNU General Public License
version 2. This program is licensed "as is" without any warranty of any
kind, whether express or implied.
"""
import os
from nvpair import NVPair
import conf

class ZPool(object):
    """
    ZPool
        the class of a zpool
    
        You can create a empty zpool, all will be default value, or 
        initilize it with given nvpair data    
        
    """

    def __init__(self, data = None):
        # there maybe some other pool attrs we do not cover, please advice
        self.name = ''
        self.version = 1
        self.state = 0
        self.txg = 0
        self.pool_guid = 0
        self.hostid = 0
        self.hostname = ''
        self.vdev_tree = None
        if data: 
            for k, v in data.items():
                self.__setattr__(k, v)


    def datasets(self):
        """
        Get all datasets in this pool

        Returns
            zfspy.DataSet
        """
        pass


    def status(self):
        return ['active', 'exported', 'destroyed'][self.state]


    @classmethod
    def import_cached(cls, cf = conf.ZPOOL_CACHE):
        """
        Import all the pools from zpool cache

        Returns
            [zfspy.ZPool]
        """
        try:
            st = os.stat(cf)
        except:
            print 'can\'t access %s' % cf
            return None

        pools = []
        xdr = NVPair.unpack_file(cf)
        for pool in NVPair.strip(xdr['value']).values():
            pools.append(ZPool(pool)) 
        return pools

    def __repr__(self):
        return '<zfspy.ZPool \'%s %x\'>' % (self.name, self.pool_guid)


if __name__ == '__main__':
    ZPool.import_cached()
    import doctest
    doctest.testmod()
