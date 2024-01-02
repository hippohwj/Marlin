//
// Created by Zhihan Guo on 3/31/23.
//

#ifndef ARBORETUM_BENCHMARK_WORKLOAD_H_
#define ARBORETUM_BENCHMARK_WORKLOAD_H_

#include "db/ARDB.h"
#include "local/ISchema.h"
#include "ycsb/YCSBConfig.h"


namespace arboretum {

class Workload {
 public:
  explicit Workload(ARDB * db, YCSBConfig *config) : db_(db), config_(config) {};
  void InitSchema(std::istream &in);
  YCSBConfig * GetConfig() {
    return config_;
  };

  void LoadSysTBL();
  virtual void LoadData(string& tbl_name) = 0;
  virtual void LoadDataForMigr(string &tbl_name, std::vector<uint64_t> * granules_to_migr){};
  // virtual void LoadMetaCache();

 protected:
  // vector<ISchema *> schemas_{};
  unordered_map<string, ISchema *> schemas_{};
  unordered_map<string, OID> tables_{};
  vector<OID> indexes_{};
  ARDB * db_;
  YCSBConfig *config_;
};

}

#endif //ARBORETUM_BENCHMARK_WORKLOAD_H_
