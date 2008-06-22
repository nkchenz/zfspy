"""
ZFSpy: Python bindings for ZFS

Copyright (C) 2008 Chen Zheng <nkchenz@gmail.com>

This file is licensed under the terms of the GNU General Public License
version 2. This program is licensed "as is" without any warranty of any
kind, whether express or implied.
"""
import os
from nvpair import NVPair, StreamUnpacker
from oodict import OODict

class UberBlock(OODict):
    """ 
    uberblock:  168B
        uint64_t    ub_magic        0x00bab10c
        uint64_t    ub_version             0x1
        uint64_t    ub_txg   
        uint64_t    ub_guid_sum     checksum of all the leaf vdevs's guid
        uint64_t    ub_timestamp    
        blkptr_t    ub_rootbp       point to MOS
    """

    def __init__(self, data = None):
        if data:
            su = StreamUnpacker(data)
            self.ub_magic, self.ub_version, self.ub_txg, self.ub_guid_sum, self.ub_timestamp = su.repeat('uint64', 5)
            self.ub_rootbp = data[48:168]


class VDevLabel(object):
    """
    VDevLabel
    block device:
        L0 L1 BootBlock.... L2 L3


    sizeof BootBlock = 4M - L0 * 2
    four identical vdev_label L0 L1 L2 L3

    vdev_label:     256K
        blank       8K
        boot header 8K
        xdr nvlist      112K
        uberblock array, 128K

    """

    def __init__(self, dev = None):
        self.boot_header = None
        self.nvlist = {}
        self.active_uberblock = 0
        self.dev = ''
        if dev:
            self.dev = dev
            self._from_dev(dev)

    def _from_dev(self, dev):
        """
        Read vdev_label from dev 
        """
        f = open(dev, 'rb')
        f.read(8192)
        self.boot_header = f.read(8192)
        self.nvlist = NVPair.unpack(f.read(112 * 1024))
        self.data = NVPair.strip(self.nvlist['value'])
        # find the biggest ub_txg
        ub1 = UberBlock(f.read(168))
        for i in range(128 * 1024 / 168):
            ub2 = UberBlock(f.read(168))
            if ub1.ub_txg > ub2.ub_txg:
                break
            ub1 = ub2
        if ub1.ub_txg > ub2.ub_txg:
            self.active_uberblock = ub1
        else:
            self.active_uberblock = ub2
        

    def __repr__(self):
        return '<VDevLabel \'%s\'>' % self.dev


if __name__ == '__main__':
    from pprint import pprint
    vl = VDevLabel('/chenz/disk3')

    pprint(vl.nvlist)
    pprint(vl.data)
    print vl.dev
    print vl.active_uberblock
    
    import doctest
    doctest.testmod()
