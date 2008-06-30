"""
ZFSpy: Python bindings for ZFS

Copyright (C) 2008 Chen Zheng <nkchenz@gmail.com>

This file is licensed under the terms of the GNU General Public License
version 2. This program is licensed "as is" without any warranty of any
kind, whether express or implied.
"""
from oodict import OODict
from util import *
from nvpair import NVPair, StreamUnpacker
from spa import SPA, BlockPtr
from zio import ZIO

DNODE_CORE_SIZE = 64
DNODE_SIZE = 512
BlockPtr_SIZE = 128
ZIL_HEADER_SIZE = 192

DMU_OBJTYPE = [
'DMU_OT_NONE',
'DMU_OT_OBJECT_DIRECTORY',
'DMU_OT_OBJECT_ARRAY',
'DMU_OT_PACKED_NVLIST',
'DMU_OT_NVLIST_SIZE',
'DMU_OT_BPLIST',
'DMU_OT_BPLIST_HDR',
'DMU_OT_SPACE_MAP_HEADER',
'DMU_OT_SPACE_MAP',
'DMU_OT_INTENT_LOG',
'DMU_OT_DNODE',
'DMU_OT_OBJSET',
'DMU_OT_DSL_DATASET',
'DMU_OT_DSL_DATASET_CHILD_MAP',
'DMU_OT_OBJSET_SNAP_MAP',
'DMU_OT_DSL_PROPS',
'DMU_OT_DSL_OBJSET',
'DMU_OT_ZNODE',
'DMU_OT_ACL',
'DMU_OT_PLAIN_FILE_CONTENTS',
'DMU_OT_DIRECTORY_CONTENTS',
'DMU_OT_MASTER_NODE',
'DMU_OT_DELETE_QUEUE',
'DMU_OT_ZVOL',
'DMU_OT_ZVOL_PROP',
]

DMU_OBJSET_TYPE = [
'DMU_OST_NONE',
'DMU_OST_META',
'DMU_OST_ZFS',
'DMU_OST_ZVOL',
'DMU_OST_OTHER',         # /* For testing only! */
'DMU_OST_ANY',           # /* Be careful! */
'DMU_OST_NUMTYPES'
]


class DNode(OODict):
    """
    Objects are Dnodes

    dnode_phys_t
        uint8_t  dn_type;
        uint8_t  dn_indblkshift;
        uint8_t  dn_nlevels
        uint8_t  dn_nblkptr;
        uint8_t  dn_bonustype;
        uint8_t  dn_checksum;
        uint8_t  dn_compress;
        uint8_t  dn_pad[1];
        uint16_t dn_datablkszsec;
        uint16_t dn_bonuslen;
        uint8_t  dn_pad2[4];
        uint64_t dn_maxblkid;
        uint64_t dn_secphys;
        uint64_t dn_pad3[4];
        blkptr_t dn_blkptr[N];
        uint8_t  dn_bonus[BONUSLEN]

    """

    def __init__(self, vdev, data):
        if not data:
            return
        self.vdev = vdev
        su = StreamUnpacker(data)
        self.type, self.indblkshift, self.nlevels, self.nblkptr, self.bonustype, self.checksum, self.compress, pad = su.repeat('uint8', 8)
        self.type = DMU_OBJTYPE[self.type]
        self.datablkszsec, self.bonuslen = su.repeat('uint16', 2)
        su.rewind(-4)
        self.maxblkid, self.secphys, pad = su.repeat('uint64', 3)
        bonus_offset = DNODE_CORE_SIZE + BlockPtr_SIZE * self.nblkptr
        self.blkptr = []
        for blk in split_records(data[DNODE_CORE_SIZE : bonus_offset], BlockPtr_SIZE):
            bp = BlockPtr(blk)
            if not bp.is_hole():
                self.blkptr.append(bp)
        self.bonus = data[bonus_offset : bonus_offset + self.bonuslen]

    def get_object(self, index):
        """
        Get the object by index from level 0 blocks
        
        indirect block onlys contain block pointers, its size is 1 << indblkshift. level 0 block
        contains dnode_phys_t, is data block, size is datablkszsec << 9. object index is only about
        level 0 blocks.

        maxblkid is the max id of level 0 blocks, so the max object number in this dnode is
            (datablkszsec << 9) / DNODE_SIZE * maxblkid
        """
        bp_per_indirectblk = (1 << self.indblkshift) / BlockPtr_SIZE
        object_per_level0blk = (self.datablkszsec << 9) / DNODE_SIZE
        if index < 0 or index >= object_per_level0blk * self.maxblkid: #invalid index range
            debug('object index %d out of range' % index)
            return None
       
        # compute offset of every level from bottom to top
        map = []
        blkid = index / object_per_level0blk
        offset = index %  object_per_level0blk
        map.append((blkid, offset))
        for level in range(1, self.nlevels):
            blkid, offset = blkid / bp_per_indirectblk, blkid % bp_per_indirectblk
            map.append((blkid, offset))
        #print 'maxblkid:', self.maxblkid, 'bp_per_indirectblk:', bp_per_indirectblk, 'object_per_level0blk:', object_per_level0blk
        debug('levels offset for object %d: %s' % (index, map))

        # top level only can have 3 blocks at most, its blkid must be less than 
        # the number of real blkptr we have. if it's greater than 3, it means that
        # we should have one more level then.
        toplevel_blkid = map[-1][0]
        if toplevel_blkid >= len(self.blkptr):
            return None

        bp = self.blkptr[toplevel_blkid]
        levels = range(self.nlevels)
        levels.reverse()
        for level in levels: 
            # offset in blk really matters, blkid in level doesn't
            offset = map[level][1]
            debug('level: %d  offset: %d' % (level, offset ))
            blk_data = ZIO.read_blk(self.vdev, bp)
            if level == 0:            
                return DNode(self.vdev, get_record(blk_data, DNODE_SIZE, offset))
            bp = BlockPtr(get_record(blk_data, BlockPtr_SIZE, offset))

class OBJSet(object):
    """
    1k
    objset_phys_t
        dnode_phys_t metadnode
        zil_header_t os_zil_header
        uint64_t os_type
        
    """
    def __init__(self, vdev, data):
        self.vdev = vdev
        self.meta_dnode = DNode(self.vdev, data[0:DNODE_SIZE])
        zil_header_end = DNODE_SIZE + ZIL_HEADER_SIZE
        self.zil_header = data[DNODE_SIZE: zil_header_end]
        self.os_type = DMU_OBJSET_TYPE[StreamUnpacker(data[zil_header_end: zil_header_end + 8]).uint64()]

    def __repr__(self):
        pass

if __name__ == '__main__':
    """
    very important entry
    lib/libzpool/spa.c          spa_load 
    lib/libzpool/dsl_poolc      dsl_pool_open
    dmu_objset_open_impl return a objset, the arcread does the real dirty io work
    
    arc_read -> zio_read -> zio_wait-> zio_excute
    
    """
    spa = SPA()
    labels = spa.load_labels('/chenz/disk1')
    l1 = labels[0] 
    bp = l1.ubbest.ub_rootbp
    vdev = l1.data.vdev_tree
    data = ZIO.read_blk(vdev, bp)


    mos = OBJSet(vdev, data)
    print mos.os_type
    print mos.meta_dnode

    obj_dir = mos.meta_dnode.get_object(1)
    print obj_dir
    
    import doctest
    doctest.testmod()
