#ifndef ARBORETUM_BENCHMARK_YCSB_SYNTHETICWORKLOAD_H_
#define ARBORETUM_BENCHMARK_YCSB_SYNTHETICWORKLOAD_H_

#include "common/Workload.h"
#include "YCSBConfig.h"
#include "YCSBWorkload.h"
#include "ScaleoutWorkload.h"
#include "common/BenchWorker.h"
#include <unordered_map>


namespace arboretum {


class SyntheticWorkload : public ScaleoutWorkload {
 public:
  explicit SyntheticWorkload(ARDB *db, YCSBConfig *config);
  void Init() override;
  void InitComplement() override;
  void LoadData(string& tbl_name) override;
  void GenLockTable() override;
  std::string Name() override { return "Synthetic"; }
  std::vector<uint64_t>* GenGranulesToMigr() {
    return &granules_to_migr;
  }



protected:
  uint32_t granule_num_per_node;
  void GenQueries() override;
  std::vector<uint64_t> granules_to_migr;

};

}
#endif //ARBORETUM_BENCHMARK_YCSB_SYNTHETICWORKLOAD_H_  