"""
ZFSpy: Python bindings for ZFS

Copyright (C) 2008 Chen Zheng <nkchenz@gmail.com>

This file is licensed under the terms of the GNU General Public License
version 2. This program is licensed "as is" without any warranty of any
kind, whether express or implied.
"""
import os
from nvpair import NVPair
from oodict import OODict
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
        self.vdev = OODict() 
        if data: 
            for k, v in data.items():
                self.__setattr__(k, v)

            # Gather vdev infos, vdev['disk'], vdev['file'], vdev['mirror'], etc.
            for inter in self.vdev_tree.children:
                if 'children' in inter:
                    devs = []
                    for leaf in inter.children:
                         devs.append(leaf.path)
                else:
                    devs = inter.path
                self.vdev.setdefault(inter.type, []).append(devs)


    def datasets(self):
        """
        Get all datasets in this pool

        Returns
            zfspy.DataSet
        """
        pass


    def status(self):
        print '  pool:', self.name
        print ' state:', ['ACTIVE', 'EXPORTED', 'DESTROYED'][self.state]
        print 'config:'
        print '       ', self.vdev_tree.type.upper()
        
        if 'children' in self.vdev_tree:
            for inter in self.vdev_tree.children:
                print '         ', 
                if inter.is_log:
                    print 'LOG',
                print inter.type.upper(),
                if 'children' in inter:
                    print
                    for leaf in inter.children:
                         print '           ', leaf.type.upper(), leaf.path
                else:
                    print inter.path


    @classmethod
    def import_cached(cls, cf = conf.ZPOOL_CACHE):
        """
        Import all the pools from zpool cache

        Returns
            [ZPool]
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
        return '<ZPool \'%s\'>' % self.name


if __name__ == '__main__':
    pools = ZPool.import_cached()
    for pool in pools:
        pool.status()
        print pool.vdev
    
    import doctest
    doctest.testmod()
