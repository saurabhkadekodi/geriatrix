/*
 * Copyright (c) 2018 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "dir_bucket.h"
#include <boost/unordered_map.hpp>

#ifndef SIZE_BUCKET_
#define SIZE_BUCKET_
using namespace boost::unordered;
class SizeBucket {
  public:
    uint64_t count; // count of files of particular size
    File *start; // oldest file of particular size in bucket
    double ideal_fraction; // ideal fraction of total files in this bucket
    double actual_fraction; // current fraction of total files
    uint64_t size; // size of the files in this size bucket
    int id; // id of size bucket
    unordered_map<int, DirBucket> *db; // depth to DirBucket map
    size_t *size_arr;

    SizeBucket(uint64_t size, int id, size_t *size_arr) {
      this->size = size;
      this->size_arr = size_arr;
      count = 0;
      start = NULL;
      ideal_fraction = 0;
      actual_fraction = 0;
      this->id = id;
      db = NULL;
    }

    void operator=(const SizeBucket &b) {
      count = b.count;
      start = b.start;
      actual_fraction = b.actual_fraction;
      ideal_fraction = b.ideal_fraction;
      size = b.size;
      id = b.id;
      db = b.db;
      size_arr = b.size_arr;
    }

    void addFile(File *f, uint64_t live_file_count) {
      count++;

      auto d = (db->find(f->depth))->second;
      db->erase(f->depth);
      d.count++;
      if(d.count == 1) {
        d.start = f;
      }
      db->insert(std::pair<size_t, DirBucket>(f->depth, d));

      actual_fraction = ((double) count / live_file_count);
      if(start == NULL) {
        assert(count == 1);
        start = f;
        f->size_next = f;
        f->size_prev = f;
      } else {
        f->size_next = start;
        f->size_prev = start->size_prev;
        start->size_prev->size_next = f;
        start->size_prev = f;
      }
    }

    void deleteFile(File *f, uint64_t live_file_count) {
      /*
       * Steps in deleting file from SizeBucket
       * 1. adjust count.
       * 2. adjust actual fraction.
       * 3. adjust start if necessary.
       */
      count--;
      actual_fraction = ((double) count / live_file_count);

      auto d = (db->find(f->depth))->second;
      db->erase(f->depth);
      d.count--;
      if(d.count == 0) {
        d.start = NULL;
      } else if(d.start == f) {
        d.start = f->dir_next;
      }
      db->insert(std::pair<size_t, DirBucket>(f->depth, d));

      if(count == 0) {
        this->start = NULL;
      } else if(this->start == f) {
        this->start = f->size_next;
      }
      f->size_prev->size_next = f->size_next;
      f->size_next->size_prev = f->size_prev;
      f->size_next = f->size_prev = NULL;
    }

    std::string replace(unordered_map<int, std::string>& size_bucket_keys) {
      auto key = size_bucket_keys[this->id];
      auto new_key = this->getKey();
      size_bucket_keys[this->id] = new_key;
      return key;
    }

    std::string getKey() {
      auto difference = this->ideal_fraction - this->actual_fraction;
      return (std::to_string(size_arr[this->id]) + " " + std::to_string(difference));
    }

    File *getFileToDelete(int depth) {
      auto d = (db->find(depth))->second;
      return d.getFileToDelete(depth);
    }

    void reKey(uint64_t live_file_count, unordered_map<int, std::string>& size_bucket_keys) {
      actual_fraction = ((double) count / live_file_count);
      auto key = size_bucket_keys[this->id];
      auto new_key = this->getKey();
      size_bucket_keys[this->id] = new_key;
    }

};
#endif
