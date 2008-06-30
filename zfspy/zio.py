"""
ZFSpy: Python bindings for ZFS

Copyright (C) 2008 Chen Zheng <nkchenz@gmail.com>

This file is licensed under the terms of the GNU General Public License
version 2. This program is licensed "as is" without any warranty of any
kind, whether express or implied.
"""

from compress import *

class ZIO:
    
    @classmethod
    def read_blk(cls, vdev, bp):
        """
        read the block pointed to by bp
            @vdev       the vdev_tree in vdev_label, we need it to find the real dev
                        by dva[0].vdev index
            @bp         block pointer
        """
        dva = bp.dva[0]
        dev = vdev.children[dva.vdev].path
        offset = dva.offset + (1 << 22)
        data = cls.read(dev, offset, bp.psize)
        if bp.comp == 'on' or bp.comp == 'lzjb':
            data = lzjb_decompress(data)[:bp.lsize]
        return data
        
    @classmethod
    def read(cls, dev, offset, size, how = 0):
        """
        read size data from dev + offset
           offset default relative to beginning
           how = 2 for end of the dev, otherwise not defined
        """
        f = open(dev, 'rb')
        f.seek(offset, how)
        data = f.read(size)
        f.close()
        return data
