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

class DSLDataSet(OODict):
    """
    """

    def __init__(self, data):
        f = StreamUnpacker(data)
        self.ds_dir_obj, self.ds_prev_snap_obj, self.ds_prev_snap_txg, self.ds_next_snap_obj, \
        self.ds_snapnames_zapobj, self.ds_num_children, self.ds_creation_time, self.ds_creation_txg, \
        self.ds_deadlist_obj, self.ds_used_bytes, self.ds_compressed_bytes, self.ds_uncompressed_bytes, \
        self.ds_unique_bytes, self.ds_fsid_guid, self.ds_guid, self.ds_flags = f.repeat('uint64', 16)
        self.ds_bp = BlockPtr(data[128:])

class DSLDir(OODict):
    """
    object_directory.root_dataset, its object type is DMU_OT_DSL_DIR. The root DSL 
    directory is a special object whose contents reference all top level datasets
    within the pool.

        dd_head_dataset_obj active dataset
        dd_child_dir_zapobj child datasets zap object, point to another dsl_dir
        dd_props_zapobj     non-inherited local properties of all the datasets
        
    src/lib/libzfscommon/include/sys/dsl_dir.h
    """
    def __init__(self, data):
        f = StreamUnpacker(data)
    	self.dd_creation_time, self.dd_head_dataset_obj, self.dd_parent_obj, \
        self.dd_origin_obj, self.dd_child_dir_zapobj, \
        self.dd_used_bytes, self.dd_compressed_bytes, self.dd_uncompressed_bytes, \
        self.dd_quota, self.dd_reserved, self.dd_props_zapobj, self.dd_deleg_zapobj = f.repeat('uint64', 12)

if __name__ == '__main__':
    
    import doctest
    doctest.testmod()
