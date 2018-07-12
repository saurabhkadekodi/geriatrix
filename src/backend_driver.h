/*
 * Copyright (c) 2018 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

/*
 * all I/O to the backend is routed through the backend driver.
 * this should allow us to port geriatrix to filesystem that are
 * not accessed through the kernel's POSIX system call API.
 */

#ifndef BACKEND_
#define BACKEND_

/*
 * the signature on these is setup so that we can directly plug in
 * libc's posix calls for the default posix environment.
 */
struct backend_driver {
    int (*bd_open)(const char *path, int flags, ...);
    int (*bd_close)(int fd);
    ssize_t (*bd_write)(int fd, const void *buf, size_t nbytes);
    int (*bd_access)(const char *path, int mode);
    int (*bd_unlink)(const char *path);
    int (*bd_mkdir)(const char *path, mode_t mode);
    int (*bd_fallocate)(int fd, off_t offset, off_t len);
    int (*bd_stat)(const char *path, struct stat *st);
    int (*bd_chmod)(const char *path, mode_t mode);
};

#endif /* BACKEND_ */
