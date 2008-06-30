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
from spa import SPA
from dmu import OBJSet
from zio import ZIO
from util import *

ZAP_OBJ_TYPE = [
'DMU_OT_OBJECT_DIRECTORY',
'DMU_OT_DSL_DIR_CHILD_MAP',
'DMU_OT_DSL_DS_SNAP_MAP',
'DMU_OT_DSL_PROPS',
'DMU_OT_DIRECTORY_CONTENTS',
'DMU_OT_MASTER_NODE',
'DMU_OT_DELETE_QUEUE',
'DMU_OT_ZVOL_PROP',
]

ZBT_MICRO = (1 << 63) + 3
ZBT_HEADER = (1 << 63) + 1
ZBT_LEAF = (1 << 63) + 0

MZAP_ENT_LEN = 64
MZAP_NAME_LEN = (MZAP_ENT_LEN - 8 - 4 - 2)

class ZAP(object):
    """
    microzap:
        uint64 type
        uint64 salt
        48b    padding
        mzap_ent_phys_t array


    mzap_ent_phys: 
        uint64_t mze_value;
        uint32_t mze_cd;
        uin16_t mze_pad;
        char mze_name[MZAP_NAME_LEN];
    """

    def __init__(self, data):
        self.type = StreamUnpacker(data[:8]).uint64()
        if self.type == ZBT_MICRO:
            debug('found mzap')
            self._mzap(data)

    def _mzap(self, data):
        self.salt = StreamUnpacker(data[8:16]).uint64()
        self.entries = OODict()
        for ent in split_records(data[64:], MZAP_ENT_LEN):
            value = StreamUnpacker(ent[:8]).uint64()
            name = ent[14:MZAP_NAME_LEN].strip('\00')
            if name:
                self.entries[name] = value
        debug(self.entries)

    def __repr__(self):
        return '<ZAP>'

if __name__ == '__main__':
    spa = SPA()
    labels = spa.load_labels('/chenz/disk1')
    l1 = labels[0] 
    bp = l1.ubbest.ub_rootbp
    vdev = l1.data.vdev_tree
    data = ZIO.read_blk(vdev, bp)
    mos = OBJSet(vdev, data)

    obj_dir = mos.meta_dnode.get_object(1)
    debug('object_directory: %s' % obj_dir)
    value = ZIO.read_blk(obj_dir.vdev, obj_dir.blkptr[0])
    mzap = ZAP(value)

    import doctest
    doctest.testmod()
