/*
 * Copyright (c) 2018 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include <iostream>
#include <stdlib.h>
#include <string>
#include <boost/container/vector.hpp>
#include <boost/unordered_map.hpp>
#include <boost/container/flat_map.hpp>
#include <boost/tokenizer.hpp>
#include <boost/random.hpp>
#include <boost/math/distributions/chi_squared.hpp>
#include <chrono>
#include <fstream>
#include <getopt.h>
#include <atomic>
#include <mutex>
#include <thread>
#include <fcntl.h>
#include <inttypes.h>
#include <unistd.h>
#include <sys/stat.h>
#include <signal.h>
//#include <gperftools/profiler.h>
#include "ThreadPool.h"

#include "age_bucket.h"
#include "age_list.h"
#include "backend_driver.h"

using namespace boost::container;
using namespace boost::unordered;
using namespace std::chrono;

std::string mount_point = "";
int NUM_DIRS = 0;
int NUM_SIZES = 0;
int NUM_AGES = 0;
int fake = 0;

typedef enum {
  DIRS,
  SIZES,
  AGES
} distribution_type_t;

struct size {
  char *in_file;
  char *out_file;
  double *distribution;
  size_t *arr;
  double *cutoffs;
  unordered_map<int, std::string> bucket_keys;
} s;

struct dir {
  char *in_file;
  char *out_file;
  double *distribution;
  int *arr;
  uint32_t *subdir_arr;
  unordered_map<int, std::string> bucket_keys;
} d;

struct age {
  char *in_file;
  char *out_file;
  double *distribution;
  double *cutoffs;
  unordered_map<int, std::string> bucket_keys;
} a;

double confidence = 0.0;
boost::math::chi_squared *dist;
double goodness_measure = 0.0;
auto start = std::chrono::high_resolution_clock::now();
int runtime_max = 0;
double runtime = 0;
int runs = 0;
uint64_t K = 0;

ThreadPool *pool;

uint64_t tick = 0;
uint64_t global_live_file_count = 0;
uint64_t total_age_weight = 0;
uint64_t total_size_weight = 0;
uint64_t total_dir_weight = 0;
size_t total_disk_capacity = 0;
size_t live_data_size = 0;
size_t workload_size = 0;

enum AGING_TRIGGER {none, convergence, exec_time, workload, accuracy};

