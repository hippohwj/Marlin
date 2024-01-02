#ifndef ARBORETUM_BENCHMARK_YCSB_YCSBLOADBALANCEWORKLOAD_H_
#define ARBORETUM_BENCHMARK_YCSB_YCSBLOADBALANCEWORKLOAD_H_

#include "common/Workload.h"
#include "ycsb/YCSBWorkload.h"
#include "YCSBConfig.h"

namespace arboretum {

class YCSBLoadBalanceWorkload : public YCSBWorkload {
 public:
  explicit YCSBLoadBalanceWorkload(ARDB *db, YCSBConfig *config);
  YCSBQuery * GenQuery() override;
};

}
#endif //ARBORETUM_BENCHMARK_YCSB_YCSBLOADBALANCEWORKLOAD_H_
