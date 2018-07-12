/*
 * Copyright (c) 2018 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "size_bucket.h"

#ifndef AGE_BUCKET_
#define AGE_BUCKET_
using namespace boost::unordered;
class AgeBucket {
  public:
    File *f; // pointer to start of file as per bucket cutoff
    unordered_map<size_t, SizeBucket> *sb;
    uint64_t count; // count of all files in bucket
    uint64_t cutoff; // cutoff for bucket
    double ideal_fraction; // ideal fraction of total files in this bucket
    double actual_fraction; // current fraction of total files
    bool youngest_bucket; // flag to indicate youngest bucket
    int id; // bucket id
    double ratio; // the ratio of what % of files
    File *last; // youngest file in bucket

    AgeBucket() {
      sb = NULL;
      count = 0;
      cutoff = 0;
      actual_fraction = 0;
      ideal_fraction = 0;
      youngest_bucket = false;
      id = 0;
      ratio = 0;
      f = NULL;
      last = NULL;
    }

    AgeBucket(int id) {
      sb = NULL;
      count = 0;
      cutoff = 0;
      actual_fraction = 0;
      ideal_fraction = 0;
      youngest_bucket = false;
      this->id = id;
      ratio = 0;
      f = NULL;
      last = NULL;
    }

    /*
     * This operator overloading is for the comparison of the buckets in the
     * flat_set.
     */
    bool operator<(AgeBucket b) const {
      auto v1 = ideal_fraction - actual_fraction;
      auto v2 = b.ideal_fraction - b.actual_fraction;
      if(v1 < 0) {
        v1 *= -1;
      }
      if(v2 < 0) {
        v2 *= -1;
      }
      if(v1 < v2) {
        return true;
      }
      return false;
    }

    void operator=(const AgeBucket &b) {
      count = b.count;
      cutoff = b.cutoff;
      actual_fraction = b.actual_fraction;
      ideal_fraction = b.ideal_fraction;
      sb = b.sb;
      f = b.f;
      youngest_bucket = b.youngest_bucket;
      id = b.id;
      ratio = b.ratio;
      last = b.last;
    }

    void addFile(File *f, uint64_t live_file_count, bool at_front) {
      count++;
      auto s = (sb->find(f->size))->second;
      sb->erase(f->size);

      auto d = (s.db->find(f->depth))->second;
      s.db->erase(f->depth);
      d.addFile(f, live_file_count);
      s.db->insert(std::pair<int, DirBucket>(f->depth, d));

      s.count++;
      if(s.count == 1) {
        assert(s.start == NULL);
        s.start = f;
      }
      sb->insert(std::pair<size_t, SizeBucket>(f->size, s));
      actual_fraction = ((double) count / live_file_count);

      if(this->f == NULL) {
        this->f = f;
        assert(this->last == NULL);
        this->last = f;
      }

      if(at_front) {
        //FIXME: probably assert to see if f->next = this->f
        this->f = f;
      } else {
        this->last = f;
      }
    }

    void deleteFile(File *f, uint64_t live_file_count) {
      /*
       * Steps in deleting file from AgeBucket.
       * 1. adjust count.
       * 2. adjust actual_fraction.
       * 3. remove file from size bucket.
       * 4. adjust f if necessary.
       * 5. adjust last if necessary.
       */
      this->count--;
      actual_fraction = ((double) count / live_file_count);
      auto s = (sb->find(f->size))->second;
      sb->erase(f->size);

      auto d = (s.db->find(f->depth))->second;
      s.db->erase(f->depth);
      d.deleteFile(f, live_file_count);
      s.db->insert(std::pair<int, DirBucket>(f->depth, d));

      s.count--;
      if(s.count == 0) {
        s.start = NULL;
      } else if(s.start == f) {
        s.start = f->size_next;
      }
      sb->insert(std::pair<size_t, SizeBucket>(f->size, s));
      if(count == 0) {
        this->f = NULL;
        this->last = NULL;
      } else if(this->f == f) {
        this->f = f->next;
      }

      if((this->last == f) && (count > 0)) {
        this->last = f->prev;
      }
    }

    std::string replace(unordered_map<int, std::string>& age_bucket_keys) {
      auto key = age_bucket_keys[this->id];
      auto new_key = this->getKey();
      age_bucket_keys[this->id] = new_key;
      return key;
    }

    std::string getKey() {
      auto difference = this->actual_fraction - this->ideal_fraction;
      return (std::to_string(this->id) + " " + std::to_string(difference));
    }

    File * getFileToDelete(size_t size, int dir) {
      auto s = (sb->find(size))->second;
      return s.getFileToDelete(dir);
    }
};
#endif
