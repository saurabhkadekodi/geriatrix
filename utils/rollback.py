#!/usr/bin/python3

#
# Copyright (c) 2018 Carnegie Mellon University.
#
# All rights reserved.
#
# Use of this source code is governed by a BSD-style license that can be
# found in the LICENSE file. See the AUTHORS file for names of contributors.
#

import argparse
from io import BytesIO

parser = argparse.ArgumentParser()
parser.add_argument("blkparse", help="blkparse file containing only write operations from blktrace dump, filtered with blkparse -a write ...")
parser.add_argument("offset", help="offset of the partition in blocks", type=int)
parser.add_argument("image", help="original image file path used to dd on partition for running benchmark")
parser.add_argument("partition", help="device file of the partition on which the benchmark was executed")
args = parser.parse_args()

def parse_blkparse(blkparse, offset):
    r = set()
    nwrites = 0
    with open(blkparse, "r") as file:
        for line in file:
            fields = line.split()
            if len(fields) < 10: #or fields[5] != 'C':
                continue
            if fields[6][0] != 'W':
                continue
            assert fields[6][0] == 'W', fields
            block = (int(fields[7]) - offset) // 8
            nblocks = int(fields[9]) // 8
            nblocks += 1
            for blk in range(block, block + nblocks):
                r.add(blk)
    return (r, nwrites)

def restore_image(written_blocks, nwrites):
    block_size = 4096
    with open(args.image, "rb") as image_file:
        with open(args.partition, "wb") as partition:
            for block in written_blocks:
                image_file.seek(block * block_size, 0) # seek to byte offset within file
                data = image_file.read(block_size) # read data block
                partition.seek(block * block_size, 0) # seek to byte offset in partition
                bytes_written = partition.write(data)
            partition.close()


if __name__ == "__main__":
    written_blocks, nwrites = parse_blkparse(args.blkparse, args.offset)
    restore_image(written_blocks, nwrites)
    print("total blocks replaced: ", len(written_blocks))
