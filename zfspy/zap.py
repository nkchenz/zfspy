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
from dsl import DSLDataSet, DSLDir

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
            debug('mzap init')
            self._mzap(data)

    def _mzap(self, data):
        self.salt = StreamUnpacker(data[8:16]).uint64()
        self.entries = OODict()
        for ent in split_records(data[64:], MZAP_ENT_LEN):
            value = StreamUnpacker(ent[:8]).uint64()
            name = ent[14:MZAP_NAME_LEN].strip('\00')
            if name:
                self.entries[name] = value
        debug('mzap entries: %s' % self.entries)

    def __repr__(self):
        return '<ZAP %s>' % self.entries

if __name__ == '__main__':
    spa = SPA()
    labels = spa.load_labels('/chenz/disk3')
    l1 = labels[0] 
    bp = l1.ubbest.ub_rootbp
    vdev = l1.data.vdev_tree
    data = ZIO.read_blk(vdev, bp)
    mos = OBJSet(vdev, data)

    obj_dir = mos.get_object(1)
    debug('object_directory: %s' % obj_dir)
    value = ZIO.read_blk(obj_dir.vdev, obj_dir.blkptr[0])
    mzap = ZAP(value)

    root_dataset = mos.get_object(mzap.entries.root_dataset)
    debug('root_dataset: %s' % root_dataset)

    config = mos.get_object(mzap.entries.config)
    debug('config: %s' % config)

    sync_bplist = mos.get_object(mzap.entries.sync_bplist)
    debug('sync_bplist: %s' % sync_bplist)

    ds_dir = DSLDir(root_dataset.bonus)
    debug('root dsl dir: %s' % ds_dir)

    dnode = mos.get_object(ds_dir.dd_props_zapobj)
    bp = dnode.blkptr[0]
    value = ZIO.read_blk(mos.vdev, bp)
    props = ZAP(value)
    debug('props: %s' % props)

    dnode = mos.get_object(ds_dir.dd_child_dir_zapobj)
    bp = dnode.blkptr[0]
    value = ZIO.read_blk(mos.vdev, bp)
    child_dir = ZAP(value)
    debug('child_dir: %s' % child_dir)

    dnode = mos.get_object(ds_dir.dd_head_dataset_obj)
    active_dataset = DSLDataSet(dnode.bonus)
    debug('active dataset: %s' % active_dataset)

    import doctest
    doctest.testmod()
