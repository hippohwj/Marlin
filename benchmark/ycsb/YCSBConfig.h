//
// Created by Zhihan Guo on 3/31/23.
//

#ifndef ARBORETUM_BENCHMARK_YCSB_YCSBCONFIG_H_
#define ARBORETUM_BENCHMARK_YCSB_YCSBCONFIG_H_

#include "common/BenchConfig.h"
using namespace std;
namespace arboretum {


#define YCSB_CONFIG(x) x(uint64_t, num_rows_, 1024)     \
x(size_t, num_dataload_nodes_, 3)                       \
x(size_t, migrate_cache_enable_, 0)                       \
x(size_t, scale_node_id_, 3)                       \
x(size_t, num_req_per_query_, 16)                       \
x(uint64_t, runtime_, 20) x(uint64_t, warmup_time_, 10) \
x(double, req_remote_perc_, 0)    \
x(double, req_hotspot_perc_, 0.8)    \
x(double, zipf_theta_, 0) x(double, read_perc_, 0.5)    \
x(size_t, tbl_name_, 1) \
x(uint64_t, time_before_migration_, 10) \
x(uint64_t, time_after_migration_, 20) \
BENCH_CONFIG(x)

class YCSBConfig : public BenchConfig {
 public:
  YCSBConfig(int argc, char * argv[]) : BenchConfig() {
    if (argc > 1) {
      std::strncpy(g_config_fname, argv[1], 100);
    }
    LOG_DEBUG("Loading YCSB Config from file: %s", g_config_fname);
    read_json(g_config_fname, config_);
    auto config_name = config_.get<std::string>("config_name");
    LOG_DEBUG("Loading YCSB Config: %s", config_name.c_str());
    for (ptree::value_type &item : config_.get_child("ycsb_config")){
      YCSB_CONFIG(IF_GLOBAL_CONFIG3)
    }
    num_workers_ = g_num_worker_threads;
    YCSB_CONFIG(PRINT_BENCH_CONFIG)
    cout << "hello mark" << endl;
  };
  
  string GetTblName() {
    return "M_TBL_" + to_string(tbl_name_) + "G";
  }


  YCSB_CONFIG(DECL_BENCH_CONFIG)
};

}

#endif //ARBORETUM_BENCHMARK_YCSB_YCSBCONFIG_H_
