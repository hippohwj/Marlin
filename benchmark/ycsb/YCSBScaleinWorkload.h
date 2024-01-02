#ifndef ARBORETUM_BENCHMARK_YCSB_YCSBSCALEINWORKLOAD_H_
#define ARBORETUM_BENCHMARK_YCSB_YCSBSCALEINWORKLOAD_H_

#include "common/Workload.h"
#include "ycsb/YCSBWorkload.h"
#include "YCSBConfig.h"

namespace arboretum {

class YCSBScaleinWorkload : public YCSBWorkload {
 public:
  explicit YCSBScaleinWorkload(ARDB *db, YCSBConfig *config);
  YCSBQuery * GenQuery() override;
};

}
#endif //ARBORETUM_BENCHMARK_YCSB_YCSBSCALEINWORKLOAD_H_
