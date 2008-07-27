"""
ZFSpy: Python bindings for ZFS

Copyright (C) 2008 Chen Zheng <nkchenz@gmail.com>

This file is licensed under the terms of the GNU General Public License
version 2. This program is licensed "as is" without any warranty of any
kind, whether express or implied.
"""
from nvpair import NVPair, StreamUnpacker
from util import *
from oodict import *
from zio import *

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
        else:
            # fat zap here
            print 'type=%x zap found' % self.type
            hexprint(data)
            pass

    def _mzap(self, data):
        self.salt = StreamUnpacker(data[8:16]).uint64()
        self.entries = OODict()
        for ent in split_records(data[64:], MZAP_ENT_LEN):
            value = StreamUnpacker(ent[:8]).uint64()
            name = ent[14:MZAP_NAME_LEN].strip('\00')
            if name:
                self.entries[name] = value
        debug('mzap entries: %s' % self.entries)

    @classmethod
    def from_dnode(cls, objset, i):
        dnode = objset.get_object(i)
        bp = dnode.blkptr[0]
        zap = ZAP(ZIO.read_blk(objset.vdev, bp))
        zap.dnode = dnode # we'd better to save the dnode we came from
        return zap

    def _fatzap(self, data):
        """
        zap_block_type
        zap_magic 0x2f52AB2AB
        zap_table:
            zt_blk
            zt_num_blks
            zt_shif

            zt_nextblk
            zt_copied

        zap_freeblk
        zap_num_leafs
        zap_num_entries
        zap_salt
        zap_pad 8181
        zap_leafs 8192
        """
        pass

    def __repr__(self):
        return '<ZAP %s>' % self.entries

if __name__ == '__main__':
    import doctest
    doctest.testmod()
