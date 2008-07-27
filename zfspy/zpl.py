"""
ZFSpy: Python bindings for ZFS

Copyright (C) 2008 Chen Zheng <nkchenz@gmail.com>

This file is licensed under the terms of the GNU General Public License
version 2. This program is licensed "as is" without any warranty of any
kind, whether express or implied.
"""
from os.path import normpath, join
from zap import *
from nvpair import *
from oodict import OODict

ZPL_FILE_TYPE = {
0x1: 'S_IFIFO',
0x2: 'S_IFCHR',
0x4: 'S_IFDIR',
0x6: 'S_IFBLK',
0x8: 'S_IFREG',
0xa: 'S_IFLNK',
0xc: 'S_IFSOCK',
0xd: 'S_IFDOOR',
0xe: 'S_IFPORT',
}

ZPL_FLAG ={
'ZFS_XATTR':       0x1,     #     /* is an extended attribute */
'ZFS_INHERIT_ACE':     0x2, #     /* ace has inheritable ACEs */
'ZFS_ACL_TRIVIAL':     0x4, #    /* files ACL is trivial */
'ZFS_ACL_OBJ_ACE':     0x8, #    /* ACL has CMPLX Object ACE *
'ZFS_ACL_PROTECTED':   0x10, #        /* ACL protected */
'ZFS_ACL_DEFAULTED':   0x20, #        /* ACL should be defaulted */
'ZFS_ACL_AUTO_INHERIT':    0x40, #        /* ACL should be inherited */
'ZFS_BONUS_SCANSTAMP': 0x80 
}

ACL_T_SIZE = 16


class ZFile(OODict):

    def read(self):
        id = 0
        blk_size = self.dnode.datablkszsec << 9
        data = ''
        remain_len = self.znode.size 
        while True:
            buf = self.dnode.get_blk(id)
            if id == self.dnode.maxblkid:
                data += buf[:remain_len]
                return data
            else:
                data += buf
                remain_len -= blk_size
            id += 1

class ZDir(OODict):

    def __init__(self):
        self.entries = None

    def read(self):
        bp = self.dnode.blkptr[0]
        zap = ZAP(ZIO.read_blk(self.dnode.vdev, bp))
        # I can't find it in the code, but I can tell that the highest 2bits are flags
        # not part of object id
        for k, v in zap.entries.items():
            zap.entries[k] = get_bits(v, 0, 62)
        self.entries = zap.entries

    def get_child(self, name):
        # load contents first if haven't 
        if not self.entries:
            self.read()
        if name not in self.entries:
            return None
        return self.entries[name]


class ACL(OODict):
    def __init__(self, data):
        f = StreamUnpacker(data)
        self.z_acl_extern_obj = f.uint64()
        self.z_acl_count = f.uint32()
        self.z_acl_version = f.uint16()

        self.z_ace_data = []
        for data in split_records(data[16:], ACL_T_SIZE):
            acl = OODict()
            f = StreamUnpacker(data)
            acl.a_who = f.uint64()
            acl.a_access_mask = f.uint32()
            acl.a_flags, acl.a_type = f.repeat('uint16', 2)
            self.z_ace_data.append(acl)

class ZNode(OODict):
    def __init__(self, data):
        f = StreamUnpacker(data[:18*8])
        self.atime = list(f.repeat('uint64', 2))
        self.mtime = list(f.repeat('uint64', 2))
        self.ctime = list(f.repeat('uint64', 2))
        self.crtime = list(f.repeat('uint64', 2))
        self.gen, self.mode, self.size, self.parent, \
        self.links, self.xattr, self.rdev, self.flag,\
        self.uid, self.gid = f.repeat('uint64', 10)

        self.type = ZPL_FILE_TYPE[get_bits(self.mode, 12, 4)]
        self.acl = ACL(data[176:])

class ZFS(object):
    """
    DMU_OST_ZFS: OBJSet

    master_node index 1, contains attributes: DELETE_QUEUE, VERSION, and ROOT
        
    all fs objects contain a znode_phys_t in its dnode bonus buffer
    zfs_znode_acl 

    literal meaning, stands for Zfs FS
    """

    def __init__(self, objset):
        self.objset = objset
        debug('%s' % objset)
        self.master_node = ZAP.from_dnode(self.objset, 1)
        self.version = self.master_node.entries.VERSION

    def open(self, path):
        """
        Open dir or file by path

        Returns:
            File
        """
        # we use zap read all the children entry at one time, it will be a disaster if there
        # are hundreds of thousands of entries
        obj_id = self.lookup(path)
        if not obj_id:
            return None
        return self.open_obj(obj_id)

    def open_obj(self, obj_id):
        """
        Open dir or file by its object id
        """
        dnode = self.objset.get_object(obj_id)
        znode = ZNode(dnode.bonus)
        if dnode.type == 'DMU_OT_DIRECTORY_CONTENTS':
            f = ZDir()
        else:
            if dnode.type == 'DMU_OT_PLAIN_FILE_CONTENTS':
                f = ZFile()
            else:
                return None
        f.dnode = dnode
        f.znode = znode
        return f

    def lookup(self, path):
        """
        Look up a file object id by its path    
        """
        path = normpath(join('/', path))
        debug('lookup %s' % path)
        levels = path.split('/')
        levels.pop(0)
        id = self.master_node.entries.ROOT
        if not levels[0]:
            return id # we are root
        for level in levels:
            parent = id
            dir = self.open_obj(parent)
            if not isinstance(dir, ZDir):
                return None
            id = dir.get_child(level)
            debug('name: %s id=%s' % (level, id))
            if not id: # not found
                return None
        return id

    def __repr__(self):
        return '<ZFS>'

if __name__ == '__main__':
    
    import doctest
    doctest.testmod()
