#from __init__ import *
from zfspy import *

from pprint import pprint

#conf.debug = True

# get zpool info from /etc/zfs/zpool.cache
print 'all pools found:'
pools = ZPool.import_cached()
print pools

app = pools[0]
print 'pool info:'
print 'name=%s pool_guid=%s hostname=%s hostid=%s version=%s' % \
          (app.name, app.pool_guid, app.hostname, app.hostid, app.version)

pprint(app.spa.vdev)
# load spa info from the disks
app.load()
print 'vdevs of %s:' % app.name

print 'status:'
app.status()

print app.mos
print app.object_directory

dir = app.dsl_dir 
print dir
print dir.props
print dir.child_dir

print dir.lookup_dataset('not_exists')
print dir.lookup_dataset('src')
print dir.lookup_dataset('src/a/b')

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
root = fs.open('/')
#print root.dnode
#print root.znode
#print root.znode.acl

# get children files, read dir
root.read()
print root.entries

f = fs.open('git/git.c')
print f.dnode
print f.znode.size
print f.read()

f = fs.open('linux-2.6.18-53.1.19.el5.tar.bz2')
print f.dnode
print f.znode.size

tmp = open('linux-2.6.18-53.1.19.el5.tar.bz2', 'w+')
tmp.write(f.read())

"""
fs.diff(snapa, snapb)
"""
