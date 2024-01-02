#ifndef ARBORETUM_BENCHMARK_YCSB_SCALEOUTWORKLOAD_H_
#define ARBORETUM_BENCHMARK_YCSB_SCALEOUTWORKLOAD_H_

#include "common/Workload.h"
#include "YCSBConfig.h"
#include "YCSBWorkload.h"
#include "common/BenchWorker.h"
#include <unordered_map>


namespace arboretum {


class ScaleoutWorkload : public Workload {
 public:
  explicit ScaleoutWorkload(ARDB *db, YCSBConfig *config);
  virtual void Init();
  virtual void InitComplement();
  static std::vector<uint64_t> SOGranulesToMigrate;
  static uint32_t granule_num_per_node;

  void LoadData(string& tbl_name) override;
  virtual void GenLockTable();
  static void Execute(Workload * workload, BenchWorker * worker);
  virtual std::string Name() { return "Scaleout"; }


protected:
  virtual void GenQueries();
  unordered_map<uint32_t, vector<YCSBQuery *> * > all_queries_;
  uint64_t time_before_migr_{0};
  uint64_t time_after_migr_{0};

};

}
#endif //ARBORETUM_BENCHMARK_YCSB_SCALEOUTWORKLOAD_H_   