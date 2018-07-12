/*
 * Copyright (c) 2018 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include <boost/unordered_map.hpp>
#include <boost/random.hpp>

#include "file.h"

#ifndef DIR_BUCKET_
#define DIR_BUCKET_
static int global_live_depth = 0;

using namespace boost::unordered;
struct DirBucket {
  public:
    uint64_t count; // count of files of particular size
    double ideal_fraction; // ideal fraction of total files in this bucket
    double actual_fraction; // current fraction of total files
    int id; // id of dir bucket
    int depth; // depth of the dir (root has depth 0)
    File *start; // oldest file of particular depth
    std::string prefix; // prefix path for this dir bucket
    uint64_t sibling_dirs; // count of number of dirs at this level
    int *dir_arr; // dir arr from input

    DirBucket(int id) {
      count = 0;
      ideal_fraction = 0.0;
      actual_fraction = 0.0;
      this->id = id;
      depth = 0;
      sibling_dirs = 0;
    }

    DirBucket(int depth, uint32_t sibling_dirs, int id, std::string
              mount_point, int fake, int *dir_arr,
              int (*mkpath)(const char *, mode_t)) {
      count = 0;
      ideal_fraction = 0.0;
      actual_fraction = 0.0;
      this->id = id;
      this->depth = depth;
      this->sibling_dirs = sibling_dirs;
      this->dir_arr = dir_arr;
      prefix = "";
      auto slash = "/";
      if(this->depth > 0) {
        std::string dirs = "";
        for(auto i=1; i<=this->depth-1; i++) {
          if(i != 1) {
            dirs += "/";
          }
          dirs += "d" + std::to_string(i);
        }
        prefix = dirs;
        if(sibling_dirs == 0) {
          if(global_live_depth < this->depth) {

            std::string full_file_path = mount_point + slash + prefix +
                "/d" + std::to_string(this->depth);
            prefix += "/d" + std::to_string(this->depth);
            if(!fake) {
                int rv = (*mkpath)(full_file_path.c_str(), 0777);
                assert(rv == 0);
            }

          }
        } else {
          for(uint32_t j=1; j<=sibling_dirs; j++) {
            if(this->depth == 1) {
              slash = "";
            }
            if(global_live_depth < this->depth) {
              std::string full_file_path = mount_point + slash +
                prefix + "/d" + std::to_string(j);
              if(!fake) {
                int rv = (*mkpath)(full_file_path.c_str(), 0777);
                assert(rv == 0);
              }
            }
          }
        }
        global_live_depth = this->depth;
      }
      start = NULL;
    }

    void operator=(const DirBucket &b) {
      count = b.count;
      actual_fraction = b.actual_fraction;
      ideal_fraction = b.ideal_fraction;
      start = b.start;
      id = b.id;
      depth = b.depth;
      prefix = b.prefix;
      sibling_dirs = b.sibling_dirs;
    }

    std::string getKey() {
      auto difference = this->actual_fraction - this->ideal_fraction;
      return (std::to_string(dir_arr[this->id]) + " " +
          std::to_string(difference));
    }

    std::string replace(unordered_map<int, std::string>& dir_bucket_keys) {
      auto key = dir_bucket_keys[this->id];
      auto new_key = this->getKey();
      dir_bucket_keys[this->id] = new_key;
      return key;
    }

    void addFile(File *f, uint64_t live_file_count) {
      this->count++;
      actual_fraction = ((double) count / live_file_count);
      if(start == NULL) {
        assert(count == 1);
        start = f;
        f->dir_next = f;
        f->dir_prev = f;
      } else {
        assert(count > 1);
        f->dir_next = start;
        f->dir_prev = start->dir_prev;
        start->dir_prev->dir_next = f;
        start->dir_prev = f;
      }
    }

    void deleteFile(File *f, uint64_t live_file_count) {
      assert(this->count > 0);
      this->count--;
      actual_fraction = ((double) count / live_file_count);
      if(this->count == 0) {
        this->start = NULL;
      } else if(this->start == f) {
        this->start = f->dir_next;
      }
      f->dir_prev->dir_next = f->dir_next;
      f->dir_next->dir_prev = f->dir_prev;
      f->dir_next = f->dir_prev = NULL;
    }

    File *getFileToDelete(int depth) {
      if(this->count == 0) {
        return NULL;
      }
      boost::random::mt19937 gen{static_cast<std::uint32_t>(this->count)};
      boost::random::uniform_int_distribution<> dist{1,
        static_cast<int>(this->count)};
      auto rand = dist(gen);
      auto f = this->start;
      for(auto i=1; i<rand; i++) {
        f = f->dir_next;
      }
      return f;
    }

    void reKey(uint64_t live_file_count, unordered_map<int,
        std::string>&dir_bucket_keys) {
      actual_fraction = ((double) count / live_file_count);
      auto key = dir_bucket_keys[this->id];
      auto new_key = this->getKey();
      dir_bucket_keys[this->id] = new_key;
    }
};
#endif
