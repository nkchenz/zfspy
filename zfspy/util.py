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

def import_zpool_cache():
    """
    Import all the pools from zpool cache

    Returns
        [ZPool]
    """
    cf = conf.ZPOOL_CACHE
    st = os.stat(cf)
    if not st:
        print cf, 'not found, create your zfs first with zfs-fuse'
        return
    return NVPair().unpack_file(cf)
    
