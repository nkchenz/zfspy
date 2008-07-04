"""
ZFSpy: Python bindings for ZFS

Copyright (C) 2008 Chen Zheng <nkchenz@gmail.com>

This file is licensed under the terms of the GNU General Public License
version 2. This program is licensed "as is" without any warranty of any
kind, whether express or implied.
"""
from nvpair import StreamUnpacker
from oodict import OODict
from spa import BlockPtr
from dmu import OBJSet
from zap import *

class DSL_DataSet(OODict):
    """
    prev_snap, next_snap both point to another dsl_dataset
    ds_bp points to a ZPL FS object
    """
    def __init__(self, objset, obj_index):
        self.objset = objset
        self.dnode = objset.get_object(obj_index)
        data = self.dnode.bonus
        f = StreamUnpacker(data)
        self.ds_dir_obj, self.ds_prev_snap_obj, self.ds_prev_snap_txg, self.ds_next_snap_obj, \
        self.ds_snapnames_zapobj, self.ds_num_children, self.ds_creation_time, self.ds_creation_txg, \
        self.ds_deadlist_obj, self.ds_used_bytes, self.ds_compressed_bytes, self.ds_uncompressed_bytes, \
        self.ds_unique_bytes, self.ds_fsid_guid, self.ds_guid, self.ds_flags = f.repeat('uint64', 16)
        self.ds_bp = BlockPtr(data[128:])

        self._load()

    def _load(self):
        self.snapnames = ZAP.from_dnode(self.objset, self.ds_snapnames_zapobj)


    def prev_snap(self):
        """Return a prev snap dataset, should check whether we are at end of the list"""
        if self.ds_prev_snap_obj == 0:
            return None
        return DSL_DataSet(self.objset, self.ds_prev_snap_obj)

    def next_snap(self):
        """Return a next snap dataset"""
        if self.ds_next_snap_obj == 0:
            return None
        return DSL_DataSet(self.objset, self.ds_next_snap_obj)

    def __repr__(self):
        return '<DSL_DataSet \'%s\'>' % self.ds_guid

class DSL_Dir(OODict):
    """
    object_directory.root_dataset, its object type is DMU_OT_DSL_DIR. The root DSL 
    directory is a special object whose contents reference all top level datasets
    within the pool.

        dd_head_dataset_obj active dataset
        dd_child_dir_zapobj child datasets zap object, point to another dsl_dir
        dd_props_zapobj     non-inherited local properties of all the datasets
        
    src/lib/libzfscommon/include/sys/dsl_dir.h
    """
    def __init__(self, objset, obj_index):
        self.objset = objset
        self.dnode = objset.get_object(obj_index) # dnode of this object
        # dsl_dir and dsl_dataset are in the bonus buffer of a dnode
        data = self.dnode.bonus
        f = StreamUnpacker(data)
    	self.dd_creation_time, self.dd_head_dataset_obj, self.dd_parent_obj, \
        self.dd_origin_obj, self.dd_child_dir_zapobj, \
        self.dd_used_bytes, self.dd_compressed_bytes, self.dd_uncompressed_bytes, \
        self.dd_quota, self.dd_reserved, self.dd_props_zapobj, self.dd_deleg_zapobj = f.repeat('uint64', 12)

        self._load()

    def _load(self):
        """
        Load Dir info
        """
        self.head_dataset = DSL_DataSet(self.objset, self.dd_head_dataset_obj)
        # we are not sure that it's a mzap here
        self.child_dir = ZAP.from_dnode(self.objset, self.dd_child_dir_zapobj)
        self.props = ZAP.from_dnode(self.objset, self.dd_props_zapobj)

    def __repr__(self):
        return '<DSL_Dir>'

if __name__ == '__main__':
    
    import doctest
    doctest.testmod()
