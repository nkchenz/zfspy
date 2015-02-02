import platform

if platform.system() == "FreeBSD":
    ZPOOL_CACHE = '/boot/zfs/zpool.cache'
else:
    ZPOOL_CACHE = '/etc/zfs/zpool.cache'

debug = False
