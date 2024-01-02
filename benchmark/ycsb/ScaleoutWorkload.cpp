
#include "ScaleoutWorkload.h"
#include "ITxn.h"
#include "MigrationTxn.h"
#include "PostheatTxn.h"
#include "common/GlobalData.h"
#include "common/OptionalGlobalData.h"
#include "ycsb/YCSBConfig.h"
#include "transport/rpc_client.h"
#include "db/txn/TxnTable.h"
#include "common/GlobalData.h"
#include "db/ClusterManager.h"

using namespace std;
namespace arboretum {
//TODO(Hippo): move this hard code
// // std::vector<uint64_t> ScaleoutWorkload::GranulesToMigrate = {24, 25, 26, 27 ,28, 29, 30, 31, 56, 57, 58, 59, 60, 61, 62, 63, 88, 89, 90, 91, 92, 93, 94, 95}; 
// std::vector<uint64_t> ScaleoutWorkload::GranulesToMigrate = {0, 1, 2, 3, 4, 5, 6, 7, 8, 32, 33, 34, 35, 36, 37, 38, 39, 64, 65, 66, 67, 68, 69, 70, 71}; 
// uint32_t ScaleoutWorkload::granule_num_per_node = 8;

// uint32_t ScaleoutWorkload::granule_num_per_node = 16;
// std::vector<uint64_t> ScaleoutWorkload::GranulesToMigrate = [] {
//     vector<uint64_t> v;
//     for (int nodeid = 0; nodeid < 3; nodeid++) {
//         uint64_t start_idx = nodeid * 32;
//         for (uint64_t i = 0; i < granule_num_per_node; i++) {
//             v.push_back(start_idx + i);
//         }
//     }

//     return v;
// }();


// single node migration
uint32_t ScaleoutWorkload::granule_num_per_node = 32;
// uint32_t ScaleoutWorkload::granule_num_per_node = 1;
std::vector<uint64_t> ScaleoutWorkload::SOGranulesToMigrate = [] {
    vector<uint64_t> v;
    // for (uint64_t i = 0; i < granule_num_per_node; i++) {
    //   for (int nodeid = 0; nodeid < 3; nodeid++) {
    //      uint64_t start_idx = nodeid * 128;
    //      v.push_back(start_idx + i);
    //   }
    // }

    for (int nodeid = 0; nodeid < 3; nodeid++) {
        uint64_t start_idx = nodeid * 128;
        for (uint64_t i = 0; i < granule_num_per_node; i++) {
            v.push_back(start_idx + i);
        }
    }

    return v;
}();

// std::vector<uint64_t> ScaleoutWorkload::GranulesToMigrate = {24, 25, 26, 27 ,28, 29, 30, 31, 56, 57, 58, 59, 60, 61, 62, 63, 88, 89, 90, 91, 92, 93, 94, 95}; 
// uint32_t ScaleoutWorkload::granule_num_per_node = 32;


ScaleoutWorkload::ScaleoutWorkload(ARDB *db, YCSBConfig *config) : Workload(db, config) {
  time_before_migr_ = config->time_before_migration_; 
  time_after_migr_ = config->time_after_migration_;
}

void ScaleoutWorkload::Init() {
  LOG_INFO("Scaleout Init...")
  string tbl_name = config_->GetTblName();
  std::ifstream in("configs/"+ tbl_name);
  InitSchema(in);
  LoadSysTBL();
  if (config_->migrate_cache_enable_) {
     LoadData(tbl_name);
  }
  GenLockTable();
  GenQueries();
}
  // lock_tbl_[tid] = NEW(std::atomic<uint32_t>)(0);

void ScaleoutWorkload::InitComplement() {
  LOG_INFO("Scaleout Complement Init...")
  M_ASSERT(config_->migrate_cache_enable_ == 0, "only support migration_cache_enable_ = 0");
  GenLockTable();
  GenQueries();
}

void ScaleoutWorkload::GenLockTable() {
    auto user_table = db_->GetTable(0);
    for (auto gid: SOGranulesToMigrate) {
       auto start_key = g_granule_size_mb*1024 * gid;
       for (int i = 0; i < g_granule_size_mb*1024; i++) {
            // user_table->Putputstart_key + i] = NEW(std::atomic<uint32_t>)(0);
            user_table->PutLock(start_key + i,  NEW(std::atomic<uint32_t>)(0));
       }
    }
}

void ScaleoutWorkload::LoadData(string& tbl_name) {
  char data[schemas_[tbl_name]->GetTupleSz()];
  // strcpy(&data[schemas_[0]->GetFieldOffset(1)], "init");
  uint64_t rows_per_node = config_-> num_rows_;

  // load data/cache
  for (auto gid: SOGranulesToMigrate) {
       auto start_key = g_granule_size_mb*1024 * gid;
       for (int i = 0; i < g_granule_size_mb*1024; i++) {
          uint64_t primary_key = start_key + i;
          // set primary key
          schemas_[tbl_name]->SetPrimaryKey(static_cast<char *>(data), primary_key);
          schemas_[tbl_name]->SetNumericField(1, data, primary_key);
          db_->InsertTuple(tables_[tbl_name], primary_key, data, schemas_[tbl_name]->GetTupleSz());
       }
    
  }

  if (g_storage_data_loaded && g_check_preloaded_data) {
    // TODO(Hippo): generate granule id properly
    M_ASSERT((uint64_t)db_->CheckStorageSize() == config_->num_rows_,
             "Size of data inserted does not match!");
  }
  LOG_INFO("Finished loading YCSB workload and checked durability");
}

void ScaleoutWorkload::GenQueries() {
  LOG_INFO("Scaleout Workload Generate Queries...")
  // auto tbl_id = db_->GetGranuleTable()->GetTableId();
  // auto query_tbl_name = config_->GetTblName(); 
  auto g_tbl_name = db_->GetGranuleTable()->GetTableName();
  auto tbl_id = db_->GetGranuleTableID(); 
  auto post_heat_tbl_id = tables_[config_->GetTblName()];
  // M_ASSERT(g_migr_threads_num <= granule_num_per_node, "maximum migr thread num should be %d, current is %d", granule_num_per_node, g_migr_threads_num);
  LockType lock_type =
      (g_migr_wait_die_enable) ? LockType::WAIT_DIE : LockType::NO_WAIT;
  uint32_t query_num = SOGranulesToMigrate.size() / g_migr_threads_num;
  uint32_t node_num = SOGranulesToMigrate.size() / granule_num_per_node;
  uint32_t granule_num_per_node_per_thd = (query_num == 1)? 1: (query_num / node_num);

  for (int i = 0; i < g_migr_threads_num; i++) {
    vector<YCSBQuery *> *queries = new vector<YCSBQuery *>();
    for (int j = 0; j < node_num; j++) {
          for (int z = 0; z < granule_num_per_node_per_thd; z++) {
              uint32_t start_idx = j * granule_num_per_node + granule_num_per_node_per_thd * i;
              auto granule_id = SOGranulesToMigrate[z + start_idx]; 
              auto query = NEW(YCSBQuery);
              query->tbl_id = tbl_id;
              query->req_cnt = 1;
              query->q_type = QueryType::MIGR;
              query->requests = NEW_SZ(YCSBRequest, query->req_cnt);
              query->is_all_remote_readonly = false;
              // query to migrate
              // query->requests[0].key = GranulesToMigrate[i];
              query->requests[0].key = granule_id;
              query->requests[0].value = g_scale_node_id;
              query->requests[0].ac_type = AccessType::UPDATE;
              query->requests[0].lock_type = lock_type;
              queries->push_back(query);
              if (g_migr_postheat_enable) {
                  //post-heat query
              auto query = NEW(YCSBQuery);
              query->tbl_id = post_heat_tbl_id;
              query->req_cnt = 1;
              query->q_type = QueryType::GRANULE_SCAN;
              query->requests = NEW_SZ(YCSBRequest, query->req_cnt);
              query->is_all_remote_readonly = true;
              query->requests[0].key = granule_id;
              query->requests[0].value = g_scale_node_id;
              query->requests[0].ac_type = AccessType::READ;
              query->requests[0].lock_type = lock_type;
              queries->push_back(query);
              }
          }
    }

    // for (int j = 0; j < query_num; j++) {
    //       auto query = NEW(YCSBQuery);
    //       query->tbl_id = tbl_id;
    //       query->req_cnt = 1;
    //       query->requests = NEW_SZ(YCSBRequest, query->req_cnt);
    //       query->is_all_remote_readonly = false;
    //       // query to migrate
    //       // query->requests[0].key = GranulesToMigrate[i];
    //       query->requests[0].key = GranulesToMigrate[j + query_num * i];
    //       query->requests[0].value = g_scale_node_id;
    //       query->requests[0].ac_type = AccessType::UPDATE;
    //       query->requests[0].lock_type = lock_type;
    //       queries->push_back(query);
    // }
    all_queries_.insert(std::make_pair(i, queries));
  }

  // printout queries
  for (auto it = all_queries_.begin(); it != all_queries_.end(); ++it) {
    auto queries = it->second;
    cout<< "thread_id: " << it->first << " get queries( ";
    for(auto query = queries->begin(); query != queries->end(); ++query) {
      cout << (*query)->requests[0].key << " ";

    } 
    cout << " )"<< endl;
  }
}

void ScaleoutWorkload::Execute(Workload *input_workload, BenchWorker *worker) {
  vector<float> migr_txn_latencies;
  ScaleoutWorkload * workload = (ScaleoutWorkload *)input_workload;
  string thd_name = workload->Name() + "WorkerThread";
  arboretum::Worker::SetThdId(worker->worker_id_);
  arboretum::Worker::SetMaxThdTxnId(0);
  arboretum::ARDB::InitRand(worker->worker_id_);
  LOG_INFO("%s-%u starts execution!", thd_name.c_str(), arboretum::Worker::GetThdId());
  uint64_t origin = GetSystemClock();
  auto origin_chrono = std::chrono::high_resolution_clock::now();


  // std::this_thread::sleep_for(std::chrono::seconds(workload->GetConfig()->time_before_migration_));
  std::this_thread::sleep_for(std::chrono::seconds(workload->time_before_migr_));
  if (g_rampup_scaleout_enable) {
      while (!cluster_manager->MigrationDone((g_node_id - 1) * g_migr_threads_num)) {
          std::this_thread::sleep_for(std::chrono::seconds(1));
      }
  }
  YCSBQuery * query;
  bool restart = false;
  // uint64_t txn_starttime;
  // uint64_t starttime;
  auto starttime_chrono = std::chrono::high_resolution_clock::now();
  auto txn_starttime_chrono = std::chrono::high_resolution_clock::now();
  auto critical_end = std::chrono::high_resolution_clock::now();
  auto critical_stt = std::chrono::high_resolution_clock::now();
  size_t cur_query_id = 0;
  auto queries = *(workload->all_queries_[worker->worker_id_ - g_num_worker_threads - 1]);
  //sleep for lag
  if (g_migr_conc_lag_ms > 0) {
      usleep(g_migr_conc_lag_ms * 1000 * (worker->worker_id_ - 1));
  }

  //sleep to enable interwave migr
  if (g_migr_interweave_enable) {
     std::this_thread::sleep_for(std::chrono::milliseconds(120)*(worker->worker_id_ - g_num_worker_threads - 1));
  }
  LOG_INFO("Start Migration!");
  uint64_t exe_stt = 0; 
  uint64_t exe_stp = 0;
  uint64_t commit_stp = 0;

  // auto myid = this_thread::get_id();
  auto myid = arboretum::Worker::GetThdId();
stringstream ss;
ss << myid;
string mystring = ss.str();
    cout<< "thread_id: " << mystring << " get queries( ";
    for(auto query = queries.begin(); query != queries.end(); ++query) {
      cout << (*query)->requests[0].key << " ";

    } 
    cout << " )"<< endl;
  
  TxnType migr_type = TxnType::MIGRATION; 

  if (g_migr_dynamast) {
    migr_type = TxnType::DYNAMAST;
  } else if (g_migr_albatross) {
    migr_type = TxnType::ALBATROSS; 
  }

  while (cur_query_id < queries.size()) {
    if (!restart) {
      query = queries[cur_query_id];
      // txn_starttime = GetSystemClock();
      txn_starttime_chrono = std::chrono::high_resolution_clock::now(); 
      // starttime = txn_starttime;
      starttime_chrono = txn_starttime_chrono;
      cout << "XXX: start migrate for granule " << query->requests[0].key << endl;
    } else {
      // starttime = GetSystemClock();
      starttime_chrono = std::chrono::high_resolution_clock::now(); 
    }
    RC rc = OK;
    OID txn_id;
    if (query->q_type == QueryType::MIGR) {
      MigrationTxn *txn =
          (MigrationTxn *)workload->db_->StartTxn(migr_type);
      txn->setRetry(restart);
      txn_table->add_txn(txn);
      auto t1 = txn_table->get_txn(txn->GetTxnId());
      exe_stt = GetSystemClock();
      rc = txn->Preheat(query);
      critical_stt = std::chrono::high_resolution_clock::now();
      if (rc == OK) {
        rc = txn->Execute(query);
      }
      exe_stp = GetSystemClock();
      // deal with commit or abort through Commit()
      rc = txn->Commit(rc);
      auto commit_stp_chro = std::chrono::high_resolution_clock::now();
      commit_stp = GetSystemClock();
      critical_end = commit_stp_chro;
      string req = "scale out main";
      if (rc == COMMIT) {
        txn->Postheat();
      }
      txn_id = txn->GetTxnId();
      txn_table->remove_txn(txn, req);
      // rc = arboretum::ARDB::CommitTxn(txn, rc);
      DEALLOC(txn);
      if (rc == COMMIT) {
        auto commit_latency = std::chrono::duration_cast<std::chrono::nanoseconds>(commit_stp_chro - txn_starttime_chrono).count();
        cout << "XXX: finish migrate txn " << txn_id << " for granule "
                   << query->requests[0].key << "with ms " << nano_to_ms(commit_latency) << endl;
      }

    } else if (query->q_type == QueryType::GRANULE_SCAN) {
      PostheatTxn *txn =
          (PostheatTxn *)workload->db_->StartTxn(TxnType::POSTHEAT);
      txn_table->add_txn(txn);
      auto t1 = txn_table->get_txn(txn->GetTxnId());
      auto exe_stt = GetSystemClock();
      if (rc == OK) {
        rc = txn->Execute(query);
      }
      auto exe_stp = GetSystemClock();
      // deal with commit or abort through Commit()
      if (rc == OK) {
        rc = COMMIT;
      } else {
        M_ASSERT(false, "hasn't support abort yet");
      }
      rc = txn->Commit(rc);
      auto commit_stp = GetSystemClock();
      string req = "scale out main";
      txn_table->remove_txn(txn, req);
      // rc = arboretum::ARDB::CommitTxn(txn, rc);
      DEALLOC(txn);
      if (rc == COMMIT) {
              cout << "XXX: finish postheat for granule "
                   << query->requests[0].key << endl;
      }

    } else {
      M_ASSERT(false, "unknown query type for scaleoutworkload");
    }

    restart = (rc == ABORT)? true: false;
    if (g_warmup_finished) {
      // auto endtime = GetSystemClock();
      auto endtime_chrono = std::chrono::high_resolution_clock::now(); 
      if (rc == ABORT) {
        worker->benchstats_.int_stats_.abort_cnt_++;
      } else {
        cur_query_id++;
        // stats
        worker->benchstats_.int_stats_.commit_cnt_++;
        Worker::commit_count_++;
        // auto commit_latency = endtime - txn_starttime;
        auto commit_latency = std::chrono::duration_cast<std::chrono::nanoseconds>(endtime_chrono - txn_starttime_chrono).count();
        auto critical_latency = std::chrono::duration_cast<std::chrono::nanoseconds>(critical_end - critical_stt).count();
        Worker::dbstats_.int_stats_.txn_time_ns_ += commit_latency;
        Worker::dbstats_.int_stats_.txn_exe_time_ns_ += exe_stp - exe_stt;
        Worker::dbstats_.int_stats_.txn_commit_time_ns_ += commit_stp - exe_stp;
        Worker::dbstats_.int_stats_.txn_count_ += 1;
        if (query->q_type == QueryType::MIGR) {
        LOG_INFO("XXX albatross finish migr txn %d with %0.2f ms(exec %0.2f ms, commit %0.2f ms, critical %.2f)", txn_id, nano_to_ms(commit_latency), nano_to_ms(exe_stp - exe_stt), nano_to_ms(commit_stp - exe_stp), nano_to_ms(critical_latency));
        }
    
        if (g_migr_postheat_enable) {
            if (query->q_type == QueryType::MIGR) {
                cout << "XXX migration start time "
                     << std::chrono::duration_cast<std::chrono::milliseconds>(
                            txn_starttime_chrono - origin_chrono)
                            .count() * 1.0 / 1000
                     << endl;
                Worker::migrate_starts.push_back(
                    std::chrono::duration_cast<std::chrono::milliseconds>(
                        txn_starttime_chrono - origin_chrono)
                        .count()*1.0/1000);
                if (!g_faulttolerance_enable) {
                    Worker::migrate_critical_starts.push_back(
                        std::chrono::duration_cast<std::chrono::milliseconds>(
                            critical_stt - origin_chrono)
                            .count() *
                        1.0 / 1000);
                    Worker::migrate_critical_ends.push_back(
                        std::chrono::duration_cast<std::chrono::milliseconds>(
                            critical_end - origin_chrono)
                            .count() *
                        1.0 / 1000);
                }

            } else if (query->q_type == QueryType::GRANULE_SCAN) {
                Worker::migrate_ends.push_back(
                    std::chrono::duration_cast<std::chrono::milliseconds>(
                        endtime_chrono - origin_chrono)
                        .count()* 1.0 / 1000);
            }

        } else {
            if (query->q_type == QueryType::MIGR) {
                migr_txn_latencies.push_back(nano_to_ms(commit_latency));
                cout << "XXX migration start time "
                     << std::chrono::duration_cast<std::chrono::milliseconds>(
                            txn_starttime_chrono - origin_chrono)
                            .count() * 1.0 /1000
                     << endl;
                Worker::migrate_starts.push_back(
                    std::chrono::duration_cast<std::chrono::milliseconds>(
                        txn_starttime_chrono - origin_chrono)
                        .count()*1.0/1000);
                // Worker::migrate_critical_starts.push_back(nano_to_s(critical_stt
                // - origin));
                Worker::migrate_critical_starts.push_back(
                    std::chrono::duration_cast<std::chrono::milliseconds>(
                        critical_stt - origin_chrono)
                        .count()*1.0/1000);
                Worker::migrate_critical_ends.push_back(
                    std::chrono::duration_cast<std::chrono::milliseconds>(
                        critical_end - origin_chrono)
                        .count()*1.0/1000);
                Worker::migrate_ends.push_back(
                    std::chrono::duration_cast<std::chrono::milliseconds>(
                        endtime_chrono - origin_chrono)
                        .count()*1.0/1000);
            }
        }
        if (g_synthetic_enable && (g_migr_sleep_ms > 0)) {
          // control migr rate
          std::this_thread::sleep_for(std::chrono::milliseconds(g_migr_sleep_ms));
        }

        // worker->benchstats_.vec_stats_.commit_latency_ns_.push_back(commit_latency);
        // if (worker->benchstats_.int_stats_.commit_cnt_ % 10000 == 0) {
        //   LOG_DEBUG("thread %u finishes %u txns", worker->GetThdId(),
        //             worker->benchstats_.int_stats_.commit_cnt_);
        // }
      }
      // stats
      // auto latency = endtime - starttime;
      auto latency = std::chrono::duration_cast<std::chrono::microseconds>(endtime_chrono - starttime_chrono).count();
      worker->benchstats_.int_stats_.thd_runtime_ns_ += latency;
    }
  }
  LOG_INFO("Migration Finished!");
  // sleep(workload->time_after_migr_);
  LOG_INFO("%s-%u finishes execution with %u txns!", thd_name.c_str(), Worker::GetThdId(), worker->benchstats_.int_stats_.commit_cnt_);
  worker_thread_done();
  
  // send migr_thread_done to all nodes
   SundialRequest request;
   SundialResponse response;
   cout << "XXX send migration end" << endl;
   request.set_request_type(SundialRequest::MIGR_END_REQ);
   //only end sync req will add legal node id
   request.set_node_id(g_node_id);
   request.set_thd_id(arboretum::Worker::GetThdId());
   

   // Notify all nodes the completion of the current migr thread.
   for (uint32_t i = 0; i < g_num_nodes; i++) {
       rpc_client->sendRequest(i, request, response);
   }

  // summarize stats
  arboretum::Worker::latch_.lock();
  Worker::migr_txn_latency_accum.insert(Worker::migr_txn_latency_accum.end(), migr_txn_latencies.begin(), migr_txn_latencies.end());
  Worker::SumStats();
  worker->SumStats();
  arboretum::Worker::latch_.unlock();
}

}