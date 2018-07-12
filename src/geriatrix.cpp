/*
 * Copyright (c) 2018 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "geriatrix.h"

#ifdef NEED_POSIX_FALLOCATE
/*
 * fake posix_fallocate by ftruncating the file larger and touching
 * a byte in each block.... returns 0 on success, errno on fail(!!!)
 * (this is at the top of the file so it can be included in the
 * posix driver if needed...)
 */
static int posix_fallocate(int fd, off_t offset, off_t len) {
    struct stat st;
    off_t newlen, curoff, lastoff, ptr;
    ssize_t rv;

    newlen = offset + len;

    if (fstat(fd, &st) < 0)
        return(errno);

    if (st.st_size > newlen)        /* not growing it, assume ok */
        return(0);

    if (ftruncate(fd, newlen) < 0)   /* grow it */
        return(errno);

    curoff = ((st.st_size + (st.st_blksize-1)) / st.st_blksize) * st.st_blksize;
    lastoff = ((newlen + (st.st_blksize-1)) / st.st_blksize) * st.st_blksize;

    for (ptr = curoff ; ptr < lastoff ; ptr += st.st_blksize) {
        if (lseek(fd, ptr, SEEK_SET) < 0)
            return(errno);
        rv = write(fd, "", 1);    /* writes a null */
        if (rv < 0)
            return(errno);
        if (rv == 0)
            return(EIO);
    }

    return(0);
}
#endif

/*
 * backend configuration -- all filesystem aging I/O is routed here!
 */

/* posix driver (the default) */
static struct backend_driver posix_backend_driver = {
    open, close, write, access, unlink, mkdir, posix_fallocate, stat, chmod,
};

#ifdef DELTAFS     /* optional backend for cmu's deltafs */
extern struct backend_driver deltafs_backend_driver;
#endif

/* g_backend is the backend we are using (default=posix) */
static struct backend_driver *g_backend = &posix_backend_driver;

/*
 * mkdir_path(path,mode): make an entire path(ala "mkdir -p").
 * ret 0 on sucess, -1 on error (errno set by mkdir).
 */
static int mkdir_path(const char *path, mode_t mode) {
  char *pcopy, *slash;
  mode_t parentmode;
  int done, olderrno, rv;
  struct stat st;

  /* make a copy of it, since we change it (its from a c++ string) */
  slash = pcopy = strdup(path);
  if (pcopy == NULL)
    return(-1);
  parentmode = mode | S_IWUSR | S_IXUSR;

  for (done = 0 ; done == 0 ; /*null*/ ) {
    slash += strspn(slash, "/");   /* first char that is not a "/" */
    slash += strcspn(slash, "/");  /* next "/" or end of string */
    if (*slash == '\0') done = 1;  /* hit last directory? */

    *slash = '\0';
    rv = g_backend->bd_mkdir(pcopy, done ? mode : parentmode);
    if (rv < 0) {
      olderrno = errno;
      if (g_backend->bd_stat(pcopy, &st) < 0) {   /* not there */
        errno = olderrno;
        done = -1;
        break;
      }
      if (!S_ISDIR(st.st_mode)) {
        errno = ENOTDIR;
        done = -1;
        break;
      }
      /* could have already be there */
    } else if (done) {    /* final directory created, apply mode */
      /* needed if trying to set setuid/setgid/sticky bits */
      if ((mode & ~(S_IRWXU|S_IRWXG|S_IRWXO)) != 0 &&
        g_backend->bd_chmod(path, mode) == -1) {
        done = -1;
        break;
      }
    }
    if (!done) *slash = '/';
  }

  free(pcopy);
  return( done < 0 ? -1 : 0);
}

void issueCreate(const char *path, size_t len) {
  int fd, rv = 1;
  fd = g_backend->bd_open(path, O_RDWR|O_CREAT, 0600);
  assert(fd > -1);
  if(len > 0) {
    do {
      if(rv < 0) {
        sleep(1);
      }
      rv = g_backend->bd_fallocate(fd, 0, len);
    } while(rv != 0);
    if(rv != 0) {
      fprintf(stderr,
          "issueCreate: fallocate(%s): %s with params fd: %d, len:%lu\n",
          path, strerror(rv), fd, len);
      abort();
    }
  }
  rv = g_backend->bd_close(fd);
  assert(rv == 0);
  return;
}

void issueAccess(const char *path) {
  int retval = 0;
  do {
    retval = g_backend->bd_access(path, F_OK);
    if(retval == -1) {
      if(errno == EACCES || errno == ENOENT) {
        continue;
      } else {
        assert(0);
      }
    }
    return;
  } while(1);
}

void issueDelete(const char *path) {
  int rv;
  issueAccess(path);
  rv = g_backend->bd_unlink(path);
  assert(rv == 0);
  return;
}

/**
 * Create a file.
 */
int File::createFile() {
  std::string slash = "";
  if(depth > 1) {
    slash = "/";
  }
  std::string path = mount_point + slash + this->path;
  size_t size = this->blk_size * this->blk_count;
  if(!fake)
    pool->enqueue([path, size] { issueCreate(path.c_str(), size); });
  return 0;
}

/**
 * Access file
 */
int File::accessFile() {
  std::string slash = "";
  if(depth != 0) {
    slash = "/";
  }
  std::string full_path = mount_point + slash + this->path;
  int retval = g_backend->bd_access(full_path.c_str(), F_OK);
  if(retval == -1) {
    if(errno == EACCES || errno == ENOENT) {
      return -1;
    } else {
      assert(0);
    }
  }
  return 0;
}

/**
 * Delete a file.
 */
int File::deleteFile() {
  std::string slash = "";
  if(depth != 0) {
    slash = "/";
  }
  std::string full_path = mount_point + slash + this->path;
  if(!fake)
    pool->enqueue([full_path] { issueDelete(full_path.c_str()); });
  return 0;
}

struct BucketCompare {
  bool operator()(const std::string& lhs, const std::string& rhs) const {
    double v1 = 0, v2 = 0;
    long l_id = -1, r_id = -1;
    int i = 0;
    boost::char_separator<char> sep(" ");
    boost::tokenizer<boost::char_separator<char>> tok_l(lhs, sep);
    for (auto it = tok_l.begin(); it != tok_l.end(); ++it) {
      if(i == 0) {
        l_id = std::stol(*it);
        i++;
        continue;
      }
      v1 = std::stod(*it);
      break;
    }
    boost::tokenizer<boost::char_separator<char>> tok_r(rhs, sep);
    i = 0;
    for (auto it = tok_r.begin(); it != tok_r.end(); ++it) {
      if(i == 0) {
        r_id = std::stol(*it);
        i++;
        continue;
      }
      v2 = std::stod(*it);
      break;
    }
    if(v1 < v2) {
      return true;
    } else if ((v1 == v2) && (l_id < r_id)) {
      return true;
    } else if ((v1 == v2) && (l_id > r_id)) {
      return false;
    }
    return false;
  }
};

AgeList *global_file_list;
flat_map<std::string, AgeBucket, BucketCompare> age_buckets;
flat_map<std::string, SizeBucket, BucketCompare> *size_buckets;
flat_map<std::string, DirBucket, BucketCompare> *dir_buckets;

void readDistribution(void *input, distribution_type_t type) {
  int count = 0;
  struct dir *d = NULL;
  struct age *a = NULL;
  struct size *s = NULL;
  switch(type) {
    case DIRS: {
                 d = (struct dir *) input;
                 std::ifstream infile(d->in_file);
                 infile >> count;
                 NUM_DIRS = count;
                 d->arr = (int *) malloc(sizeof(int) * count);
                 d->distribution = (double *) malloc(sizeof(double) * count);
                 d->subdir_arr = (uint32_t *) malloc (sizeof(uint32_t) * count);
                 for(auto i=0; i<count; i++) {
                   infile >> d->arr[i];
                   infile >> d->distribution[i];
                   infile >> d->subdir_arr[i];
                 }
               } break;

    case SIZES: {
                  s = (struct size *) input;
                  std::ifstream infile(s->in_file);
                  infile >> count;
                  NUM_SIZES = count;
                  s->arr = (size_t *) malloc(sizeof(size_t) * count);
                  s->cutoffs = (double *) malloc(sizeof(double) * count);
                  s->distribution = (double *) malloc(sizeof(double) * count);
                  for(auto i=0; i<count; i++) {
                    infile >> s->arr[i];
                    infile >> s->distribution[i];
                    if(i == 0) {
                      s->cutoffs[i] = s->distribution[i];
                    } else {
                      s->cutoffs[i] = s->cutoffs[i-1] + s->distribution[i];
                    }
                  }
                } break;

    case AGES: {
                 a = (struct age *) input;
                 std::ifstream infile(a->in_file);
                 infile >> count;
                 NUM_AGES = count;
                 a->distribution = (double *) malloc(sizeof(double) * count);
                 a->cutoffs = (double *) malloc(sizeof(double) * count);
                 for(auto i=0; i<count; i++) {
                   infile >> a->cutoffs[i];
                   infile >> a->distribution[i];
                 }
               } break;
  }
}

void init(struct age *a_grp, struct size *s_grp, struct dir *d_grp) {
  int i, j, k;
  readDistribution(a_grp, AGES);
  readDistribution(s_grp, SIZES);
  readDistribution(d_grp, DIRS);
  global_file_list = new AgeList(0);

  for(i=0; i<NUM_AGES; i++) {
    total_age_weight += a_grp->distribution[i];
  }

  for(i=0; i<NUM_SIZES; i++) {
    total_size_weight += s_grp->distribution[i];
  }

  for(i=0; i<NUM_DIRS; i++) {
    total_dir_weight += d_grp->distribution[i];
  }

  size_buckets = new flat_map<std::string, SizeBucket, BucketCompare>;
  for(i=0; i<NUM_SIZES; i++) {
    SizeBucket s(s_grp->arr[i], i, s_grp->arr);
    s.db = new unordered_map<int, DirBucket>();
    for(j=0; j<NUM_DIRS; j++) {
      DirBucket d(d_grp->arr[j], d_grp->subdir_arr[j], j,
                  mount_point, fake, d_grp->arr, mkdir_path);
      s.db->insert(std::pair<int, DirBucket>(d_grp->arr[j], d));
    }
    s.ideal_fraction = s_grp->distribution[i] / total_size_weight;
    s_grp->bucket_keys[i] = s.getKey();
    size_buckets->insert(std::pair<std::string,
        SizeBucket>(s_grp->bucket_keys[i], s));
  }

  for(i=0; i<NUM_AGES; i++) {
    AgeBucket b(i);
    b.sb = new unordered_map<size_t, SizeBucket>();
    b.f = NULL;
    for(j=0; j<NUM_SIZES; j++) {
      SizeBucket s(s_grp->arr[j], j, s_grp->arr);
      s.db = new unordered_map<int, DirBucket>();
      for(k=0; k<NUM_DIRS; k++) {
        DirBucket d(d_grp->arr[k], d_grp->subdir_arr[k], k, mount_point,
                    fake, d_grp->arr, mkdir_path);
        s.db->insert(std::pair<int, DirBucket>(d_grp->arr[k], d));
      }
      b.sb->insert(std::pair<size_t, SizeBucket>(s_grp->arr[j], s));
    }
    b.ideal_fraction = a_grp->distribution[i] / total_age_weight;
    if(i == 0) {
      b.youngest_bucket = true;
    }
    b.ratio = 1 - (a_grp->cutoffs[i] / a_grp->cutoffs[NUM_AGES-1]);
    a_grp->bucket_keys[i] = b.getKey();
    age_buckets.insert(std::pair<std::string,
        AgeBucket>(a_grp->bucket_keys[i], b));
  }

  dir_buckets = new flat_map<std::string, DirBucket, BucketCompare>;
  for(i=0; i<NUM_DIRS; i++) {
    DirBucket d(d_grp->arr[i], d_grp->subdir_arr[i], i, mount_point,
                fake, d_grp->arr, mkdir_path);
    d.ideal_fraction = d_grp->distribution[i] / total_dir_weight;
    d_grp->bucket_keys[i] = d.getKey();
    dir_buckets->insert(std::pair<std::string,
        DirBucket>(d_grp->bucket_keys[i], d));
  }
}

void destroy() {
  delete dist;
  delete pool;
  delete global_file_list;
  //ProfilerStop();
}

void dumpSizeBuckets(char *f = NULL) {
  FILE *fp = NULL;
  if(f) {
    fp = fopen(f, "w");
    fprintf(fp, "SIZE FRACTION TYPE\n");
  }

  if(f) {
    auto current_id = 0;
    while(current_id < NUM_SIZES) {
      auto it = size_buckets->rbegin();
      auto s = it->second;
      while(s.id != current_id) {
        it++;
        s = it->second;
      }
      fprintf(fp, "%" PRIu64 " %f IDEAL\n", s.size, s.ideal_fraction);
      fprintf(fp, "%" PRIu64 " %f ACTUAL\n", s.size, s.actual_fraction);
      current_id++;
    }
    fclose(fp);
  }
}

void dumpDirBuckets(char *f = NULL) {
  FILE *fp = NULL;
  if(f) {
    fp = fopen(f, "w");
    fprintf(fp, "DEPTH FRACTION TYPE\n");
  }

  if(f) {
    auto current_id = 0;
    while(current_id < NUM_DIRS) {
      auto it = dir_buckets->rbegin();
      auto d = it->second;
      while(d.id != current_id) {
        it++;
        d = it->second;
      }
      fprintf(fp, "%d %f IDEAL\n", d.depth, d.ideal_fraction);
      fprintf(fp, "%d %f ACTUAL\n", d.depth, d.actual_fraction);
      current_id++;
    }
    fclose(fp);
  }
}

double calculateChiMeanSquared(std::list<double> expected,
    std::list<double> actual, char *age_dump_file,
    char *size_dump_file, char *dir_dump_file) {
  double chi_2 = 0.0;
  for(auto e_it = expected.begin(), a_it = actual.begin();
      e_it != expected.end(); e_it++, a_it++) {
    chi_2 += pow(double((*e_it - *a_it)), 2) / *e_it;
  }

  auto goodness_of_my_dist = cdf(*dist, chi_2);
  if(goodness_of_my_dist <= goodness_measure) {
    dumpSizeBuckets(size_dump_file);
    dumpDirBuckets(dir_dump_file);
    auto current_id = 0;
    auto fp = fopen(age_dump_file, "w");
    fprintf(fp, "BUCKET FRACTION TYPE\n");
    while(current_id < NUM_AGES) {
      auto it = age_buckets.rbegin();
      auto a = it->second;
      while(a.id != current_id) {
        it++;
        a = it->second;
      }
      fprintf(fp, "%d %f IDEAL\n", a.id, a.ideal_fraction);
      fprintf(fp, "%d %f ACTUAL\n", a.id, a.actual_fraction);
      current_id++;
    }
    return goodness_of_my_dist;
  }
  return 0;
}


int dumpAgeBuckets(char *age_dump_file = NULL, char *size_dump_file = NULL,
    char *dir_dump_file = NULL, int only_calculate_accuracy = 0) {
  FILE *fp = NULL;
  if(age_dump_file) {
    fp = fopen(age_dump_file, "w");
    fprintf(fp, "BUCKET FRACTION TYPE\n");
  }

  std::list <double> expected;
  std::list <double> actual;

  if(age_dump_file) {
    auto current_id = 0;
    while(current_id < NUM_AGES) {
      auto it = age_buckets.rbegin();
      auto a = it->second;
      while(a.id != current_id) {
        it++;
        a = it->second;
      }
      fprintf(fp, "%d %f IDEAL\n", a.id, a.ideal_fraction);
      fprintf(fp, "%d %f ACTUAL\n", a.id, a.actual_fraction);
      expected.push_back(a.ideal_fraction);
      actual.push_back(a.actual_fraction);
      current_id++;
    }
    fclose(fp);
  } else {
    if(!only_calculate_accuracy) {
      std::cout << std::endl << 
        "************ AGE BUCKET DUMP *************" << std::endl;
    }
    auto it = age_buckets.rbegin();
    uint64_t oldest = 0; uint64_t youngest = 0;
    while(it != age_buckets.rend()) {
      oldest = 0;
      youngest = 0;
      auto a = it->second;
      if(a.f != NULL) {
        oldest = a.f->age;
      }

      if(a.last != NULL) {
        youngest = a.last->age;
      }
      expected.push_back(a.ideal_fraction);
      actual.push_back(a.actual_fraction);

      if(!only_calculate_accuracy) {
        std::cout << "Bucket = " << a.id << ", Ideal Ratio = " <<
          a.ideal_fraction << ", Actual Ratio = " << a.actual_fraction <<
          ", Count = " << a.count << ", Cutoff = " << a.cutoff <<
          ", Oldest file = " << oldest << ", Youngest file = "
          << youngest << std::endl;
      }
      it++;
    }
  }

  if(confidence > 0.0) {
    return calculateChiMeanSquared(expected, actual, age_dump_file,
        size_dump_file, dir_dump_file);
  }
  return 0;
}

float tossCoin() {
  return ((float) (rand() % 100)) / 100;
}

void reAge(struct age *a_grp, uint64_t future_tick = 0) {
  int i = 0;
  AgeBucket a, *ab = new AgeBucket[NUM_AGES];
  unordered_map<int, AgeBucket> age_bucket_map;

  for(i=0; i<NUM_AGES; i++) {
    ab[i] = (age_buckets.find(a_grp->bucket_keys[i]))->second;
    // revise cutoffs
    if(future_tick == 0) {
      ab[i].cutoff = ab[i].ratio * tick;
    } else {
      ab[i].cutoff = ab[i].ratio * future_tick;
    }
  }

  /*
   * Note that the loop below goes to n-1 age buckets.
   */
  for(i=0; i<NUM_AGES-1; i++) {
    if(ab[i].last == NULL) {
      continue;
    }

    while(ab[i].count > 0) {
      auto f = ab[i].f;
      if(f->age >= ab[i].cutoff) {
        break;
      }

      ab[i].deleteFile(f, global_live_file_count);
      ab[i+1].addFile(f, global_live_file_count, false);
    }
  }

  for(i=0; i<NUM_AGES; i++) {
    auto old_key = ab[i].replace(a_grp->bucket_keys);
    age_buckets.erase(old_key);
    age_buckets.insert(std::pair<std::string, AgeBucket>(a_grp->bucket_keys[i],
          ab[i]));
  }

  delete [] ab;
}

void dumpStats(struct age *a, struct size *s, struct dir *d) {
  std::cout << "============= OVERALL STATISTICS ===============" << std::endl;
  std::cout << " Total runtime = " << runtime << " mins." << std::endl;
  std::cout << " Total number of operations = " << tick << std::endl;
  std::cout << " Number of disk overwrites = " << runs << std::endl;
  std::cout << " Total aging workload created = " <<
    workload_size / 1048576 << " MB" << std::endl;
  if (confidence > 0) {
    std::cout << " Confidence achieved (chi-squared measure) = " <<
      confidence << std::endl;
  } else {
    std::cout << " Perfect convergence achieved" << std::endl;
  }
  std::cout << " Size distribution dumped in " << s->out_file << std::endl;
  std::cout << " Dir depth distribution dumped in " << d->out_file << std::endl;
  std::cout << " Age distribution dumped in " << a->out_file << std::endl;
  std::cout << "================================================" << std::endl;
}

size_t createFile(int size_arr_position, struct age *a_grp, struct size *s_grp,
    struct dir *d_grp, int *create_succeeded) {
  /*
   * In this function, we need to do the following tasks:
   *
   * 1. find what size file we need to create.
   * 2. find the dir depth we have to create it at
   * 3. create a file of that size.
   * 4. increment global_live_file_count
   * 5. adjust dir_buckets
   * 6. add file to global_file_list
   * 7. adjust size_buckets
   * 8. adjust age_buckets
   *
   * IMPORTANT: order of the steps is necessary.
   */

  // step 1
  SizeBucket sb(0, 0, s_grp->arr);
  if(size_arr_position >= 0) {
    auto it = size_buckets->find(s_grp->bucket_keys[size_arr_position]);
    assert(it != size_buckets->end());
    sb = it->second;
  } else {
    // the size bucket farthest away from its ideal fraction
    auto it = size_buckets->rbegin();
    while((it->second.size + live_data_size) >= total_disk_capacity) {
      if (it == size_buckets->rend()) {
        std::cout << "Cannot create a single file, exhausted all options!"
          << std::endl;
        *create_succeeded = -1;
        return 0;
      }
      it++;
    }
    sb = it->second;
  }

  // step 2
  auto d_it = dir_buckets->begin();
  auto d = d_it->second;

  // step 3
  char name[PATH_MAX];
  std::string sibling_dir = "";
  if((d.depth > 0) && (d.sibling_dirs > 0)) {
    boost::random::mt19937 gen{static_cast<std::uint64_t>(rand())};
    boost::random::uniform_int_distribution<std::uint64_t> dist{1,
      static_cast<std::uint64_t>(d.sibling_dirs)};
    auto rand_subdir = dist(gen);
    sibling_dir += "d" + std::to_string(rand_subdir) + "/";
  }
  snprintf(name, PATH_MAX, "%s/%s%" PRIu64, d.prefix.c_str(),
           sibling_dir.c_str(), tick);
  File *f = new File(name, sb.size, tick, d.depth); // step 2
  auto retval = f->createFile();
  assert(retval == 0);
  auto ret_size = f->size;

  // step 4
  global_live_file_count++;

  // step 5
  dir_buckets->erase(d_grp->bucket_keys[d.id]);
  d.count++;
  d.reKey(global_live_file_count, d_grp->bucket_keys);
  dir_buckets->insert(std::pair<std::string,
      DirBucket>(d_grp->bucket_keys[d.id], d));
  auto old_dir_buckets = dir_buckets;
  d_it = old_dir_buckets->begin();
  dir_buckets = new flat_map<std::string, DirBucket, BucketCompare>;
  while(d_it != old_dir_buckets->end()) {
    d = d_it->second;
    d.reKey(global_live_file_count, d_grp->bucket_keys);
    dir_buckets->insert(std::pair<std::string,
        DirBucket>(d_grp->bucket_keys[d.id], d));
    d_it++;
  }
  delete old_dir_buckets;

  // step 6
  global_file_list->addFile(f);

  // step 7
  size_buckets->erase(s_grp->bucket_keys[sb.id]);
  sb.addFile(f, global_live_file_count);
  s_grp->bucket_keys[sb.id] = sb.getKey();
  size_buckets->insert(std::pair<std::string,
      SizeBucket>(s_grp->bucket_keys[sb.id], sb));
  auto old_size_buckets = size_buckets;
  auto s_it = old_size_buckets->begin();
  size_buckets = new flat_map<std::string, SizeBucket, BucketCompare>;
  while(s_it != old_size_buckets->end()) {
    auto s = s_it->second;
    s.reKey(global_live_file_count, s_grp->bucket_keys);
    size_buckets->insert(std::pair<std::string,
        SizeBucket>(s_grp->bucket_keys[s.id], s));
    s_it++;
  }
  delete old_size_buckets;

  // step 8
  // youngest bucket
  AgeBucket ab = ((age_buckets.find(a_grp->bucket_keys[0]))->second);
  age_buckets.erase(a_grp->bucket_keys[ab.id]);
  ab.addFile(f, global_live_file_count, false);
  a_grp->bucket_keys[ab.id] = ab.getKey();
  age_buckets.insert(std::pair<std::string,
      AgeBucket>(a_grp->bucket_keys[ab.id], ab));
  return ret_size;
}

size_t deleteFile(struct age *a_grp, struct size *s_grp, struct dir *d_grp) {
  /*
   * When deleting a file, we perform the following operations:
   *
   * 1. find what size file we should delete.
   * 2. choose bucket to delete it from.
   * 3. choose which position in the bucket to delete it from.
   * 4. perform unlink operation.
   * 5. decrement global_live_file_count.
   * 6.  adjust dir buckets
   * 7. adjust age_buckets
   * 8. adjust size_buckets
   * 9. remove file from global_file_list
   *
   * NOTE: ORDER IS IMPORTANT
   */

  // step 1
  File *f = NULL;
  SizeBucket sb(0, 0, s_grp->arr); // dummy SizeBucket
  AgeBucket ab;
  DirBucket db(0, 0, 0, mount_point, fake,
               d_grp->arr, mkdir_path); // dummy DirBucket

  auto a_it = age_buckets.rbegin();

  // select age bucket
  do {
    ab = a_it->second;
    auto s_it = size_buckets->begin();
    // select size bucket
    do {
      sb = s_it->second;
      auto d_it = dir_buckets->rbegin();
      // select dir bucket
      do {
        db = d_it->second;
        f = ab.getFileToDelete(sb.size, db.depth);
        d_it++;
      } while((f == NULL) && (d_it != dir_buckets->rend()));
      s_it++;
    } while((f == NULL) && (s_it != size_buckets->end()));
    a_it++;
  } while((f == NULL) && (a_it != age_buckets.rend()));

  if(f == NULL) {
    std::cout << "Cannot delete a single file of any size!" << std::endl;
    exit(1);
  }

  auto ret_size = f->size;

  // step 4
  auto retval = f->deleteFile();
  assert(retval == 0);

  // step 5
  global_live_file_count--;

  // step 6
  dir_buckets->erase(d_grp->bucket_keys[db.id]);
  db.count--;
  db.reKey(global_live_file_count, d_grp->bucket_keys);
  dir_buckets->insert(std::pair<std::string,
      DirBucket>(d_grp->bucket_keys[db.id], db));
  auto old_dir_buckets = dir_buckets;
  auto d_it = old_dir_buckets->begin();
  dir_buckets = new flat_map<std::string, DirBucket, BucketCompare>;
  while(d_it != old_dir_buckets->end()) {
    auto d = d_it->second;
    d.reKey(global_live_file_count, d_grp->bucket_keys);
    dir_buckets->insert(std::pair<std::string,
        DirBucket>(d_grp->bucket_keys[d.id], d));
    d_it++;
  }
  delete old_dir_buckets;

  // step 7
  age_buckets.erase(a_grp->bucket_keys[ab.id]);
  ab.deleteFile(f, global_live_file_count);
  a_grp->bucket_keys[ab.id] = ab.getKey();
  age_buckets.insert(std::pair<std::string,
      AgeBucket>(a_grp->bucket_keys[ab.id], ab));

  // step 8
  size_buckets->erase(s_grp->bucket_keys[sb.id]);
  sb.deleteFile(f, global_live_file_count);
  s_grp->bucket_keys[sb.id] = sb.getKey();
  size_buckets->insert(std::pair<std::string,
      SizeBucket>(s_grp->bucket_keys[sb.id], sb));
  auto old_size_buckets = size_buckets;
  auto s_it = old_size_buckets->begin();
  size_buckets = new flat_map<std::string, SizeBucket, BucketCompare>;
  while(s_it != old_size_buckets->end()) {
    auto s = s_it->second;
    s.reKey(global_live_file_count, s_grp->bucket_keys);
    size_buckets->insert(std::pair<std::string,
        SizeBucket>(s_grp->bucket_keys[s.id], s));
    s_it++;
  }
  delete old_size_buckets;

  // step 9
  global_file_list->deleteFile(f);

  delete f;
  return ret_size;
}

uint64_t calculateT(struct age *a_grp) {
  uint64_t T = 0;
  int64_t t = 0;
  int i = 0;
  auto s_i = 0.0;
  for(i=0; i < NUM_AGES - 1; i++) {
    auto a = (age_buckets.find(a_grp->bucket_keys[i]))->second;

    if (i == 0) {
      s_i = 1 - a.ratio;
    } else {
      auto younger_a = (age_buckets.find(a_grp->bucket_keys[i-1]))->second;
      s_i = younger_a.ratio - a.ratio;
    }
    t = 2 * K * ((double)a.ideal_fraction / s_i);
    if (T < t) {
      T = t;
    }
  }

  auto a = (age_buckets.find(a_grp->bucket_keys[i]))->second;
  auto younger_a = (age_buckets.find(a_grp->bucket_keys[i-1]))->second;
  s_i = younger_a.ratio - a.ratio;
  t = ((double)(2 * K * (a.ideal_fraction - 1) + K) / s_i);
  if (t > 0 && T < t) {
    T = t;
  }

  if (s_i * T <= K) {
    T = (K / s_i);
  }
  return T;
}

int performOp(bool create, int size_arr_position,
    int idle_injections, struct age *a, struct size *s, struct dir *d) {
  tick++;
  int create_succeeded = 0;
  if(create) {
    auto data_added = createFile(size_arr_position, a, s, d, &create_succeeded);
    if(create_succeeded == 0) {
      live_data_size += data_added;
      workload_size += data_added;
    }
  }
  if (!create || create_succeeded == -1) {
    live_data_size -= deleteFile(a, s, d);
  }
  return 0;
}

int performRapidAging(size_t till_size, int idle_injections,
    struct age *a, struct size *s, struct dir *d) {
  boost::random::mt19937 gen{static_cast<std::uint64_t>(total_size_weight)};
  boost::random::uniform_int_distribution<std::uint64_t> dist{1,
    static_cast<std::uint64_t>(total_size_weight)};
  while(live_data_size < till_size) {
    auto rand = dist(gen);
    int j = 0;
    while(rand > s->cutoffs[j]) {
      j++;
      if(j == NUM_SIZES) {
        j--;
        break;
      }
    }
    performOp(true, j, idle_injections, a, s, d);
  }
  return 0;
}

int performStableAging(size_t till_size, int idle_injections,
    struct age *a, struct size *s, struct dir *d, int runs) {
  auto future_tick = calculateT(a);
  reAge(a, future_tick);
  int confidence_met = 0;
  AGING_TRIGGER trigger = none;
  do {
    if(tossCoin() < 0.5) {
      performOp(true, -1, idle_injections, a, s, d); // create file
    } else {
      performOp(false, -1, idle_injections, a, s, d); // delete file
    }
    reAge(a, future_tick);

    if((tick % 10000) == 0) {
      auto end = std::chrono::high_resolution_clock::now();
      auto millis = std::chrono::duration<double,
           std::milli>(end - start).count();
      runtime = ((millis / 1000) / 60);
      std::cout << "Workload = " << workload_size / 1048576 << " MB, Runtime = "
        << runtime << " mins., Convergence ops = " << future_tick <<
        ", Operations = " << tick << "..." << std::endl;
      dumpSizeBuckets();
      dumpDirBuckets();
      auto confidence_met = dumpAgeBuckets(a->out_file, s->out_file,
          d->out_file);
      if(confidence > 0 && confidence_met == 1) {
        return 1;
      }
    }
    if(tick >= future_tick) {
      trigger = convergence;
      std::cout <<
        "Aging stopped due to perfect convergence in relative age distribution."
        << std::endl;
    } else if(workload_size >= till_size) {
      std::cout << "Aging stopped because of reaching intended workload size."
        << std::endl;
      trigger = workload;
    } else if(runtime >= runtime_max) {
      std::cout << "Aging stopped because of reaching runtime limit."
        << std::endl;
      trigger = exec_time;
    } else if(confidence > 0 && confidence_met == 1) {
      std::cout << "Aging stopped because of meeting intended aging accuracy."
        << std::endl;
      trigger = accuracy;
    }
  } while(trigger == none);
  return trigger;
}

void usage() {
  std::cout << std::endl;
  std::cout << "geriatrix " << std::endl;
  std::cout << "        -n <disk size in bytes>" << std::endl;
  std::cout << "        -u <utilization fraction>" << std::endl;
  std::cout << "        -r <random seed>" << std::endl;
  std::cout << "        -m <mount point>" << std::endl;
  std::cout << "        -a <age distribution file>" << std::endl;
  std::cout << "        -s <size distribution file>" << std::endl;
  std::cout << "        -d <dir distribution file>" << std::endl;
  std::cout << "        -x <age distribution out file>" << std::endl;
  std::cout << "        -y <size distribution out file>" << std::endl;
  std::cout << "        -z <dir distribution out file>" << std::endl;
  std::cout << "        -t <t-way concurrency>" << std::endl;
  std::cout << "        -i <num runs>" << std::endl;
  std::cout << "        -f <0 / 1 fake>" << std::endl;
  std::cout << "        -p <0 / 1 idle time>" << std::endl;
  std::cout << "        -c <confidence fraction between 0 and 1>" << std::endl;
  std::cout << "        -q <0 / 1 ask before quitting>" << std::endl;
  std::cout << "        -w <num mins>" << std::endl;
  std::cout << "        -b <backend (posix, deltafs, etc.)>" << std::endl;
  std::cout << std::endl;
}

int resumeAgingQuery(uint64_t total_disk_capacity, double runtime) {
  auto decision = 'x';
  std::cout << "=================== Aging trigger fired  ====================="
    << std::endl;
  if(confidence > 0) {
    std::cout << "Accuracy at this point = " <<
      dumpAgeBuckets(NULL, NULL, NULL, 1) << std::endl;
  } else {
    std::cout << "Perfect convergence mode selected." << std::endl;
  }
  std::cout << "Number of disk overwrites = " <<
    (double)workload_size / total_disk_capacity << std::endl;
  std::cout << "Runtime till now = " << runtime << " mins." << std::endl;
  do {
    std::cout << "Do you want to resume aging (y / n): ";
    std::cin >> decision;
    std::cout << std::endl;
  } while(!((decision == 'y') || (decision == 'n')));
  if(decision == 'y') {
    std::cout << "=================================================="
      << std::endl;
    if(confidence > 0) {
      std::cout << "Current confidence level set = " << confidence << "."
        << std::endl;
      std::cout << "Enter new confidence level (fraction between 0 and 1): ";
      std::cin >> confidence;
      std::cout << std::endl;
    }
    std::cout << "Aging currently ran for " << runtime << " mins." << std::endl;
    std::cout <<
      "How many more mins do you want to age if confidence is not met: ";
    std::cin >> runtime_max;
    start = std::chrono::high_resolution_clock::now();
    std::cout << std::endl;
    std::cout << "Number of disk overwrites = " << runs << std::endl;
    std::cout << "How many more disk overwrites do you want to age for: ";
    auto more_runs = 0;
    std::cin >> more_runs;
    runs += more_runs;
    std::cout << std::endl;
    std::cout << "Happy Aging!!!" << std::endl;
    std::cout << "=================================================="
      << std::endl;
    return 1;
  }
  return 0;
}

void handler(int signo){
  dumpAgeBuckets(a.out_file);
  dumpSizeBuckets(s.out_file);
  dumpDirBuckets(d.out_file);
  dumpStats(&a, &s, &d);
  destroy();
  exit(0);
}

int main(int argc, char *argv[]) {
  if(argc != 37) {
    usage();
    exit(1);
  }
  char *mybackend = NULL;
  //uint64_t total_disk_capacity = 0;
  double utilization = 0.0;
  int seed = 0;
  int option = 0;
  int concurrency = 0;
  int idle_injections = 0;
  int query_before_quitting = 0;
  while((option = getopt(argc, argv,
                         "n:u:r:m:a:s:d:x:y:z:t:i:f:p:c:q:w:b:")) != EOF) {
    switch(option) {
      case 'n': total_disk_capacity = strtoull(optarg, NULL, 10); break;
      case 'u': utilization = strtod(optarg, NULL); break;
      case 'r': seed = atoi(optarg); break;
      case 'm': mount_point = optarg; break;
      case 'a': a.in_file = optarg; break;
      case 's': s.in_file = optarg; break;
      case 'd': d.in_file = optarg; break;
      case 'x': a.out_file = optarg; break;
      case 'y': s.out_file = optarg; break;
      case 'z': d.out_file = optarg; break;
      case 't': concurrency = atoi(optarg); break;
      case 'i': runs = atoi(optarg); break;
      case 'f': fake = atoi(optarg); break;
      case 'p': idle_injections = atoi(optarg); break;
      case 'c': confidence = strtod(optarg, NULL); break;
      case 'q': query_before_quitting = atoi(optarg); break;
      case 'w': runtime_max = atoi(optarg); break;
      case 'b': mybackend = optarg; break;
    }
  }
  if (!mybackend || strcmp(mybackend, "posix") == 0) {
      /* do nothing, posix is the default */
  } else if (strcmp(mybackend, "deltafs") == 0) {
#ifdef DELTAFS
      g_backend = &deltafs_backend_driver;
#else
      fprintf(stderr, "error: DELTAFS not enabled in this binary\n");
      exit(1);
#endif
  }

  assert(total_disk_capacity > 0);
  srand(seed);

  init(&a, &s, &d); // initialize the data structures for aging
  if(confidence > 0.0) {
    // initialize the chi-squared value for accuracy comparison.
    dist = new boost::math::chi_squared(NUM_AGES - 1);
    goodness_measure = cdf(*dist, confidence);
  }
  pool = new ThreadPool(concurrency);
  performRapidAging(total_disk_capacity * utilization, idle_injections,
      &a, &s, &d);
  K = tick;
  struct sigaction sigIntHandler;

  sigIntHandler.sa_handler = handler;
  sigemptyset(&sigIntHandler.sa_mask);
  sigIntHandler.sa_flags = 0;
  sigaction(SIGINT, &sigIntHandler, NULL);

  do {
    performStableAging(total_disk_capacity * runs, idle_injections,
        &a, &s, &d, runs);
  } while(query_before_quitting && resumeAgingQuery(total_disk_capacity,
        runtime));
  handler(0);
  return 0;
}
