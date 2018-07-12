/*
 * Copyright (c) 2018 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include <iostream>

#ifndef FILE_
#define FILE_
class File {
  public:
    size_t size;
    std::string path;
    uint64_t age;
    int depth; // id of the dir_bucket_keys
    File *prev;
    File *next;
    File *size_next;
    File *size_prev;
    File *dir_next;
    File *dir_prev;
    size_t blk_size;
    long blk_count;

    File(const char *name) {
      this->path = name;
      this->size = 0;
      this->age = 0;
      this->depth = 0;
      this->prev = this->next = NULL;
      this->size_next = this->size_prev = NULL;
      this->dir_next = this->dir_prev = NULL;
    }

    File(const char *name, size_t size, uint64_t age, int depth) {
      this->path = name;
      this->size = size;
      this->age = age;
      this->prev = this->next = NULL;
      this->size_next = this->size_prev = NULL;
      if(this->size == 0) {
        this->blk_size = 4096;
        this->blk_count = 0;
      } else if(this->size >= 4096) {
        this->blk_size = 4096;
        this->blk_count = this->size / 4096;
      } else if(this->size >= 1024) {
        this->blk_size = 1024;
        this->blk_count = this->size / 1024;
      } else {
        this->blk_size = this->size;
        this->blk_count = 1;
      }
      this->depth = depth;
      this->dir_next = this->dir_prev = NULL;
    }

    int createFile();
    int deleteFile();
    int accessFile();

    void operator=(const File &f) {
      size = f.size;
      path = f.path;
      age = f.age;
      depth = f.depth;
      blk_size = f.blk_size;
      blk_count = f.blk_count;
      prev = NULL;
      next = NULL;
      size_next = NULL;
      size_prev = NULL;
      dir_next = NULL;
      dir_prev = NULL;
    }

    bool operator<(File f) const {
      if(this->age < f.age) {
        return true;
      }
      return false;
    }

    friend std::ostream& operator<< (std::ostream &out, const File &f) {
      out << "(path = " << f.path << ", age = " << f.age << ", size = " <<
        f.size << ", depth = " << f.depth << ")";
      return out;
    }
};

#endif
