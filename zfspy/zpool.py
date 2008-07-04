"""
ZFSpy: Python bindings for ZFS

Copyright (C) 2008 Chen Zheng <nkchenz@gmail.com>

This file is licensed under the terms of the GNU General Public License
version 2. This program is licensed "as is" without any warranty of any
kind, whether express or implied.
"""
import os
import conf
from nvpair import NVPair
from oodict import OODict
from spa import *
from dmu import *
from zap import *
from dsl import *
from zpl import *

class ZPool(object):
    """
    ZPool
        the class of a zpool
    
        You can create a empty zpool, all will be default value, or 
        initilize it with given nvpair data    
        
    """

    def __init__(self, data = None):
        if data: 
            for k, v in data.items():
                self.__setattr__(k, v)
            self.spa = SPA(self.vdev_tree)

    def load(self):
        """
        Load root dataset from disks
        """
        vdev, self.ubbest = self.spa.find_ubbest()
        data = ZIO.read_blk(vdev, self.ubbest.ub_rootbp)
        self.mos = OBJSet(vdev, data)
        self.object_directory = ZAP.from_dnode(self.mos, 1)

        # get config, sync_bplist  here

        # get root_dataset
        self.dsl_dir = DSL_Dir(self.mos, self.object_directory.entries.root_dataset)


    def status(self):
        print '  pool:', self.name
        print ' state:', ['ACTIVE', 'EXPORTED', 'DESTROYED'][self.state]
        if self.spa:
            self.spa.status()
    
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
    
    import doctest
    doctest.testmod()
