from __init__ import *
#from zfspy import *

from pprint import pprint

conf.debug = True

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

fs = ds.active_fs
print fs.master_node
print fs.version

# open the top directory
root = fs.open('')
print root.dnode
print root.znode
print root.type
print root.znode.acl

# get children files
for f, i in root.entries.items():
    print f, i 

fs.ls('a/b/c')
fs.ls('/ab/c/d')

"""
fs.read(file)
fs.diff(snapa, snapb)
"""
