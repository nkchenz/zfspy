#from __init__ import *
from zfspy import *

from pprint import pprint

conf.debug = False

# get zpool info from /etc/zfs/zpool.cache
print 'all pools found:'
pools = ZPool.import_cached()
print pools

app = pools[0]
print 'pool info:'
print 'name=%s pool_guid=%s hostname=%s hostid=%s version=%s' % \
          (app.name, app.pool_guid, app.hostname, app.hostid, app.version)

# load spa info from the disks
app.load()
print 'vdevs of %s:' % app.name
pprint(app.spa.vdev)

print 'status:'
app.status()

print app.ubbest
print app.mos
print app.object_directory

dir = app.dsl_dir 
print dir
print dir.props
print dir.child_dir

# list all the children datasets, all n-v of zap object are stored in its 'entries' OODict
for ds, obj_index in dir.child_dir.entries.items():
    print 'name:', ds, 'obj_index:', obj_index

ds = dir.head_dataset
print ds
print ds.snapnames

"""DataSet
    dsl_dataset
    objset DMU_OST_ZFS
    fs     current active fs
    child  children datasets

FS
    snap   snaps of this dataset
    master_node: delq, version, root
    type 'filesystem', 'snapshot', 'clone'
    open  open dir or file,  each dir is a DMU_OT_DIRECTORY
    read  
    touch 
    create
    diff
    rm

File:
    basic file object

print fs.root
dir = fs.open('kernel/linux')

for file in fs.ls(dir):
    print file

fs.read(file)
fs.diff(snapa, snapb)
"""
