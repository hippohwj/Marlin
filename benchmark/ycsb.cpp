//
// Created by Zhihan Guo on 3/30/23.
//
#include "ycsb/ycsb.h"
#include "ycsb/YCSBConfig.h"

using namespace arboretum;

int main(int argc, char* argv[]) {
  auto bench_config = new (MemoryAllocator::Alloc(sizeof(YCSBConfig))) YCSBConfig(argc, argv);
  bench_config->runtime_ = 20;
  g_storage_data_loaded = true; // TODO: add warm-up period and collect after buffer is full.
//  bench_config->num_rows_ = 1024 * 132;
//  g_granule_id = 2; // 1024 * 132
//  bench_config->num_rows_ = 1024 * 1024 * 10;
//  g_granule_id = 0; // 1024 * 1024 * 10
  bench_config->num_rows_ = 1024 * 1024;
  g_granule_id = 1; // 1024 * 1024
  bench_config->read_perc_ = 0.5;
  bench_config->zipf_theta_ = 0;
  bench_config->num_workers_ = 8;
  g_index_type = arboretum::REMOTE;
  g_buf_type = arboretum::NOBUF;
//  g_index_type = arboretum::BTREE;
//  g_buf_type = arboretum::OBJBUF;
  // currently assume 1-1 mapping between clients and db server threads
  g_num_worker_threads = bench_config->num_workers_;
  auto db = new (MemoryAllocator::Alloc(sizeof(ARDB))) ARDB;
  ycsb::ycsb(db, bench_config);
  return 0;
}

