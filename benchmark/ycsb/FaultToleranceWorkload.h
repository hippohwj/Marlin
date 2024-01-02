#ifndef ARBORETUM_BENCHMARK_YCSB_FAULTTOLERANCEWORKLOAD_H_
#define ARBORETUM_BENCHMARK_YCSB_FAULTTOLERANCEWORKLOAD_H_

#include "common/Workload.h"
#include "YCSBConfig.h"
#include "YCSBWorkload.h"
#include "ScaleoutWorkload.h"
#include "common/BenchWorker.h"
#include <unordered_map>


namespace arboretum {


class FaultToleranceWorkload : public ScaleoutWorkload {
 public:
  explicit FaultToleranceWorkload(ARDB *db, YCSBConfig *config);
  static uint32_t granule_num_per_node;
  void Init() override;
  void InitComplement() override;

  void GenLockTable() override;
  // static void Execute(Workload * workload, BenchWorker * worker);
  std::string Name() override { return "FaultTolerance"; }


protected:
  void GenQueries() override;

};

}
#endif //ARBORETUM_BENCHMARK_YCSB_SCALEINWORKLOAD_H_   