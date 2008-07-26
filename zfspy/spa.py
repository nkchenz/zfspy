"""
ZFSpy: Python bindings for ZFS

Copyright (C) 2008 Chen Zheng <nkchenz@gmail.com>

This file is licensed under the terms of the GNU General Public License
version 2. This program is licensed "as is" without any warranty of any
kind, whether express or implied.
"""
import os
from nvpair import NVPair, StreamUnpacker
from oodict import OODict
from util import *
from zio import ZIO

UBERBLOCK_SHIFT = 10
UBERBLOCK_SIZE = 1 << UBERBLOCK_SHIFT
VDEV_UBERBLOCK_COUNT = 128 << 10 >> UBERBLOCK_SHIFT
SPA_MINBLOCKSHIFT = 9
VDEVLABEL_SIZE = 256 << 10

class UberBlock(OODict):
    """ 
    uberblock:  168B
        uint64_t    ub_magic        0x00bab10c
        uint64_t    ub_version             0x1
        uint64_t    ub_txg   
        uint64_t    ub_guid_sum     checksum of all the leaf vdevs's guid
        uint64_t    ub_timestamp    
        blkptr_t    ub_rootbp       point to MOS

    Accordding to lib/libzfscommon/include/sys/vdev_impl.h
        #define VDEV_UBERBLOCK_SHIFT(vd)    \
            MAX((vd)->vdev_top->vdev_ashift, UBERBLOCK_SHIFT)

    minimum allocatable unit for top level vdev is ashift, currently '10' for a RAIDz configuration, 
    '9' otherwise.
    
    lib/libzfscommon/include/sys/uberblock_impl.h 
         #define UBERBLOCK_SHIFT     10          /* up to 1K */
    
    So uberblock elements in array are all aligned to 1K, be carefull! 
    """

    def __init__(self, data):
        if data:
            su = StreamUnpacker(data)
            self.ub_magic, self.ub_version, self.ub_txg, self.ub_guid_sum, self.ub_timestamp = su.repeat('uint64', 5)
    
    def valid(self):
        """check whether this ub is valid"""
        return self.ub_magic ==  0x00bab10c or self.ub_magic ==  0x0cb1ba00

    def better_than(self, ub):
        """if self is better than ub, return True, else False"""
        if self.ub_txg > ub.ub_txg:
            return True
        if self.ub_txg < ub.ub_txg:
            return False
        # txgs are equal, then compare timestamp
        if self.ub_timestamp > ub.ub_timestamp:
            return True
        else:
            return False
             
    def __repr__(self):
        return '<UberBlock \'ub_txg %s ub_timestamp %s\'>' % (self.ub_txg, self.ub_timestamp)


class BlockPtr(OODict):
    """
    block: 128 b
        3 dvas
        E      1bit   little endian 1, big 0
        level    7bit
        type   1b
        cksum 1b
        comp   1b
        PSIZE  2b  physical size
        LSIZE  2b  logical size
        padding 24b
        birth txg 8b
        fill count 8b
        checksum 32b

    dva:  
        vdev   4b   from lib/libzpool/vdev.c vdev_lookup_top you can see
                    vdev is just the array index of root vdev's children
        grid   1b
        asize  3b
        G      1bit, gang block is a block which contains block pointers
        offset 63bit

    physical block address = offset << 9 + 4M

    You can use BlockPtr() to create a empty block_ptr, please remeber
    to initialize all its members. Always call with initial data is a preferd
    way, zero data for empty block_ptr 
    """


    def __init__(self, data = None):
        if data:
            self.dva = []
            dva_size = 16 
            for dva in split_records(data[0 : dva_size * 3], dva_size):
                self.dva.append(self._parse_dva(dva))
            su = StreamUnpacker(data[dva_size * 3 :])
            i = su.uint64()
            #see lib/libzfscommon/include/sys/spa.h
            self.lsize = (get_bits(i, 0, 16) + 1) << SPA_MINBLOCKSHIFT
            self.psize = (get_bits(i, 16, 16) + 1) << SPA_MINBLOCKSHIFT
            self.comp = get_bits(i, 32, 8)
            self.cksum = get_bits(i, 40, 8)
            self.type = get_bits(i, 48, 8)
            self.level = get_bits(i, 56, 5)
            if get_bits(i, 63, 1):
                self.endian = '<' # little endian
            else:
                self.endian = '>' # big
            self.cksum = ['unknown', 'on', 'off', 'label', 'gang header', 'zilog', 'fletcher2', 'fletcher4', 'SHA-256'][self.cksum]
            self.comp = ['unknown', 'on', 'off', 'lzjb'][self.comp]
            self.type = self.type
            su.rewind(-24) # skip 24b paddings
            self.birth_txg, self.fill_count = su.repeat('uint64', 2)
            self.checksum = []
            for i in range(4):
                self.checksum.append(su.uint64())

    def _parse_dva(self, data):
        dva = OODict()
        su = StreamUnpacker(data)
        i = su.uint64()
        dva.asize = get_bits(i, 0, 24) << SPA_MINBLOCKSHIFT
        dva.grid = get_bits(i, 24, 8)
        dva.vdev = get_bits(i, 32, 32)
        i = su.uint64()
        dva.offset = get_bits(i, 0, 63) << SPA_MINBLOCKSHIFT
        if get_bits(i, 63, 1):
            dva.G = True
        else:
            dva.G = False
        return dva

    def is_hole(self):
        """
        lib/libzfscommon/include/sys/spa.h:275:#define  BP_IS_HOLE(bp)      ((bp)->blk_birth == 0)
        """
        return self.birth_txg == 0

    def __repr__(self):
        s = ''
        for i in range(3):
            dva = self.dva[i]
            s = s + 'DVA[%d]=<%s:%x:%x G=%s> ' % (i, dva.vdev, dva.offset, dva.asize, dva.G)
        s = s + '%s %s type=%d birth=%d fill=%d lsize=%d psize=%d ' % (self.cksum, self.comp, self.type, self.birth_txg, self.fill_count, self.lsize, self.psize)
        a = []
        for i in self.checksum:
            a.append('%x' % i)
        s = s + 'chksum=' + ':'.join(a) 
        return '<BlockPtr %s>' % s 


class VDevLabel(object):
    """
    VDevLabel
    block device:
        L0 L1 BootBlock.... L2 L3

    sizeof BootBlock = 4M - L0 * 2
    four identical vdev_label L0 L1 L2 L3

    vdev_label:     256K
        blank       8K
        boot header 8K
        xdr nvlist      112K
        uberblock array, 128K  each elements is aligned by 1K

    """

    def __init__(self, data = None):
        self.boot_header = None
        self.nvlist = {}
        self.uberblocks = 0
        self.data = ''
        if data:
            self._from_data(data)

    def _from_data(self, data):
        self.boot_header = data[8 << 10: 16 << 10]
        self.nvlist = NVPair.unpack(data[16 << 10: 128 << 10])
        self.data = NVPair.strip(self.nvlist['value'])
        # find the active uberblock
        debug('find ubbest')
        ub_array = data[128 << 10 :] 
        ubbest = None
        i = 0
        for data in split_records(ub_array, UBERBLOCK_SIZE):
            ub = UberBlock(data)
            ub.index = i
            i = i + 1
            if not ub.valid():
                continue
            if not ubbest:
                ubbest = ub
            if ub.better_than(ubbest):
                ubbest = ub
            debug('current index=%d txg=%d timestamp=%d ubbest index=%d txg=%d timestamp=%d' % \
                        (ub.index, ub.ub_txg, ub.ub_timestamp, ubbest.index, ubbest.ub_txg, ubbest.ub_timestamp))
        # use index here so we don't have to parse blockptr for every ub, that saves a lot
        data = get_record(ub_array, UBERBLOCK_SIZE, ubbest.index)
        ubbest.ub_rootbp = BlockPtr(data[40: 168])
        self.ubbest = ubbest
         
    def __repr__(self):
        return '<VDevLabel \'txg %s\'>' % self.data.txg 


class SPA(object):

    def __init__(self, vdev_tree):
        self.vdev = vdev_tree

    def find_ubbest(self):
        # Fixme: which dev should we load from?
        dev = self.vdev.children[1]
        if 'children' not in dev:
            path = dev.path
        else:
            path = dev.children[0].path
        labels = self.load_labels(path)
        l1 = labels[0] 

        # vdev tree in label is used
        debug('vdev_tree of label1: %s' % l1.data.vdev_tree)
        return (l1.data.vdev_tree, l1.ubbest)

    def load_labels(self, dev):
        """
        Load vdev label informations, return the four labels

        Return
            [VDevLabel]
        """
        l = []
        l.append(VDevLabel(ZIO.read(dev, 0, VDEVLABEL_SIZE)))
        l.append(VDevLabel(ZIO.read(dev, VDEVLABEL_SIZE, VDEVLABEL_SIZE)))
        l.append(VDevLabel(ZIO.read(dev, -VDEVLABEL_SIZE * 2, VDEVLABEL_SIZE, 2)))
        l.append(VDevLabel(ZIO.read(dev, -VDEVLABEL_SIZE, VDEVLABEL_SIZE, 2)))
        return l

    def status(self):
        print 'config:'
        print '       ', self.vdev.type.upper()
        if 'children' in self.vdev:
            for inter in self.vdev.children:
                print '         ', 
                if inter.is_log:
                    print 'LOG',
                print inter.type.upper(),
                if 'children' in inter:
                    print
                    for leaf in inter.children:
                         print '           ', leaf.type.upper(), leaf.path
                else:
                    print inter.path


    def __repr__(self):
        return '<SPA>' 


if __name__ == '__main__':
    from pprint import pprint

    import doctest
    doctest.testmod()
