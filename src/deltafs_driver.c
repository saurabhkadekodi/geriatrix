/*
 * Copyright (c) 2018 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

/*
 * this file provides optional DELTAFS support for geriatrix by
 * providing a deltafs backen_driver.  it is only compiled if DELTAFS
 * support is enabled...
 */

#include <assert.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/stat.h>

#include <deltafs/deltafs_api.h>

#include "backend_driver.h"

/* can't map open directly, since the signatures differ slightly */
static int dback_open(const char *path, int falgs, ...) {
    return(deltafs_open(path, falgs, 0600));
}

/* deltafs doesn't have an access function, but it has stat */
static int dback_access(const char *path, int mode) {
    struct stat st;
    assert(mode == F_OK);   /* only support F_OK, check if file is present */
    return(deltafs_stat(path, &st));
}

/* 
 * deltafs version of fake posix_fallocate... (XXX could make shared
 * version of this, but too small to bother with?)
 */
static int dback_fallocate(int fd, off_t offset, off_t len) {
    struct stat st;
    off_t newlen, curoff, lastoff, ptr;
    ssize_t rv;

    newlen = offset + len;

    if (deltafs_fstat(fd, &st) < 0)
        return(errno);

    if (st.st_size > newlen)        /* not growing it, assume ok */
        return(0);

    if (deltafs_ftruncate(fd, newlen) < 0)   /* grow it */
        return(errno);

    curoff = ((st.st_size + (st.st_blksize-1)) / st.st_blksize) * st.st_blksize;
    lastoff = ((newlen + (st.st_blksize-1)) / st.st_blksize) * st.st_blksize;

    for (ptr = curoff ; ptr < lastoff ; ptr += st.st_blksize) {
        rv = deltafs_pwrite(fd, "", 1, ptr);
        if (rv < 0)
            return(errno);
        if (rv == 0)
            return(EIO);
    }

    return(0);
}

/*
 * here is the main driver structure....
 */
struct backend_driver deltafs_backend_driver = {
    dback_open, deltafs_close, deltafs_write, dback_access, deltafs_unlink, 
    deltafs_mkdir, dback_fallocate, deltafs_stat, deltafs_chmod,
};
