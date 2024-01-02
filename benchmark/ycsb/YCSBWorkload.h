//
// Created by Zhihan Guo on 3/31/23.
//

#ifndef ARBORETUM_BENCHMARK_YCSB_YCSBWORKLOAD_H_
#define ARBORETUM_BENCHMARK_YCSB_YCSBWORKLOAD_H_

#include "common/Workload.h"
#include "YCSBConfig.h"

namespace arboretum {

struct YCSBRequest {
  // uint64_t key{140214049647936};
  uint64_t key;
  uint32_t value;
  AccessType ac_type;
  LockType lock_type = LockType::NO_WAIT;
  uint64_t src_node{UINT64_MAX};
};

struct YCSBQuery {
  YCSBRequest * requests;
  OID tbl_id;
  size_t req_cnt;
  QueryType q_type = QueryType::REGULAR;
  bool is_all_remote_readonly;
};

class ARDB;
class BenchWorker;
class YCSBWorkload : public Workload {
 public:
  explicit YCSBWorkload(ARDB *db, YCSBConfig *config);
  void LoadData(string& tbl_name) override;
  void LoadDataForMigr(string &tbl_name, std::vector<uint64_t> * granules_to_migr) override;
  #if REMOTE_STORAGE_TYPE == REMOTE_STORAGE_AZURE
  void BatchLoad();
  #endif

  virtual YCSBQuery * GenQuery();

  void GenRequest(YCSBQuery * query, size_t &cnt);
  static void Execute(Workload * workload, BenchWorker * worker);

 protected:
  struct YCSBtuple {
    int64_t pkey;
    char data1[100];
  };

  // query generation and key distribution
  uint64_t zipf(uint64_t n, double theta);
  uint64_t the_n{0};
  double denom{0};
  double zeta_2_theta{0};

  void CalculateDenom();
  static double zeta(uint64_t n, double theta);
};

}
#endif //ARBORETUM_BENCHMARK_YCSB_YCSBWORKLOAD_H_
