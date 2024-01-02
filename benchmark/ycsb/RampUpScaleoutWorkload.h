#ifndef ARBORETUM_BENCHMARK_YCSB_RAMPUPSCALEOUTWORKLOAD_H_
#define ARBORETUM_BENCHMARK_YCSB_RAMPUPSCALEOUTWORKLOAD_H_

#include "common/Workload.h"
#include "YCSBConfig.h"
#include "YCSBWorkload.h"
#include "ScaleoutWorkload.h"
#include "common/BenchWorker.h"
#include <unordered_map>


namespace arboretum {


class RampUpScaleoutWorkload : public ScaleoutWorkload {
 public:
  explicit RampUpScaleoutWorkload(ARDB *db, YCSBConfig *config);
  static uint32_t granule_num_in_cluster;
  void Init() override;
  void InitComplement() override;

  void GenLockTable() override;
  // static void Execute(Workload * workload, BenchWorker * worker);
  std::string Name() override { return "RampUpScaleout"; }


protected:
  void GenQueries() override;
  void GenGranulesToMigrate();
  std::vector<uint64_t> GranulesToMigrate;
};

}
#endif //ARBORETUM_BENCHMARK_YCSB_RAMPUPSCALEOUTWORKLOAD_H_   