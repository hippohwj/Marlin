#ifndef ARBORETUM_BENCHMARK_YCSB_LOADBALANCEWORKLOAD_H_
#define ARBORETUM_BENCHMARK_YCSB_LOADBALANCEWORKLOAD_H_

#include "common/Workload.h"
#include "YCSBConfig.h"
#include "YCSBWorkload.h"
#include "ScaleoutWorkload.h"
#include "common/BenchWorker.h"
#include <unordered_map>


namespace arboretum {


class LoadBalanceWorkload : public ScaleoutWorkload {
 public:
  explicit LoadBalanceWorkload(ARDB *db, YCSBConfig *config);
  static uint32_t granule_num_per_node;
  void Init() override;
  void InitComplement() override;

  void GenLockTable() override;
  // static void Execute(Workload * workload, BenchWorker * worker);
  std::string Name() override { return "Loadbalance"; }


protected:
  void GenQueries() override;

};

}
#endif //ARBORETUM_BENCHMARK_YCSB_LOADBALANCEWORKLOAD_H_   