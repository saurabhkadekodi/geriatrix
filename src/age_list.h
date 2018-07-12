/*
 * Copyright (c) 2018 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "file.h"

#ifndef AGE_LIST_
#define AGE_LIST_
class AgeList {
  public:
    File *fs;
    size_t size;
    uint64_t count;
    uint64_t total_size;

    AgeList(uint64_t size) {
      fs = new File("0", 0, 0, 0);
      fs->prev = fs;
      fs->next = fs;
      this->size = size;
    }

    void addFile(File *f) {
      f->next = fs;
      f->prev = fs->prev;
      fs->prev->next = f;
      fs->prev = f;
      count++;
    }

    void deleteFile(File *f) {
      if(fs->next == f) {
        fs->next = f->next;
      } else if(fs->prev == f) {
        fs->prev = f->prev;
      }
      f->prev->next = f->next;
      f->next->prev = f->prev;
      f->next = f->prev = NULL;
      count--;
    }
};
#endif  /* AGE_LIST_ */
