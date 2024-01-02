//
// Created by Zhihan Guo on 3/31/23.
//

#include <cmath>
#include "YCSBWorkload.h"
#include "LoadBalanceWorkload.h"
#include "common/BenchWorker.h"
#include "ITxn.h"
#include "common/GlobalData.h"
#include "common/OptionalGlobalData.h"
#include "db/txn/TxnTable.h"

namespace arboretum {
const bool assert= false;

YCSBWorkload::YCSBWorkload(ARDB *db, YCSBConfig *config) : Workload(db, config) {
  config_ = config;
  string tbl_name = config->GetTblName();
  std::ifstream in("configs/"+ tbl_name);
  InitSchema(in);
  LoadSysTBL();
  // only load data on the nodes where original data generate
  if (g_node_id != g_client_node_id && g_node_id < config_->num_dataload_nodes_) {
    LoadData(tbl_name);
  }
  if (config_->zipf_theta_ != 0) {
    CalculateDenom();
  }
}

void YCSBWorkload::Execute(Workload *input_workload, BenchWorker *worker) {
  std::vector<float> user_txn_latencies;
  YCSBWorkload * workload = (YCSBWorkload *)input_workload;
  arboretum::Worker::SetThdId(worker->worker_id_);
  arboretum::Worker::SetMaxThdTxnId(0);
  arboretum::ARDB::InitRand(worker->worker_id_);
  LOG_INFO("Thread-%u starts execution!", arboretum::Worker::GetThdId());
  if (g_rampup_scaleout_enable) {
    std::this_thread::sleep_for(std::chrono::milliseconds((worker->worker_id_ - 1) * g_rampup_inteval_ms));
  }
  YCSBQuery * query;
  int retry_cnt = 0;
  uint64_t txn_starttime;
  uint64_t starttime;
  bool is_gtbl_up = false;
  while (!g_terminate_exec) {
     //dynamast check
    if (g_migr_dynamast) {
      bool is_migr_triggered = false;
      {
        std::lock_guard<std::mutex> lock(mm_mutex);        
        is_migr_triggered = (mm_count!=0);
      }
      if (is_migr_triggered) {
        {
          std::lock_guard<std::mutex> lock(an_mutex);        
          an_count--;
        }
        active_thread.notify_all();
        {
          std::unique_lock<std::mutex> lock(mm_mutex);
          migr_mark.wait(lock, []{return (mm_count == 0);});
          lock.unlock();
          std::lock_guard<std::mutex> an_lock(an_mutex);        
          an_count++; 
        }

      } 
    }


    if (retry_cnt == 0) {
      query = workload->GenQuery();
      txn_starttime = GetSystemClock();
      starttime = txn_starttime;
    } else {
      if (is_gtbl_up) {
         retry_cnt --;
      } else {
        double sleep_ms = pow(2, retry_cnt - 1) * 2; 
        int sleep_ms_int = static_cast<int>((sleep_ms > 100 )? 100: sleep_ms); 
        LOG_INFO("XXX Albatross backoff for abort with %d ms", sleep_ms_int);
        usleep(sleep_ms_int * 1000);
      }

      starttime = GetSystemClock();
    }
    RC rc;
    auto txn = workload->db_->StartTxn();
    txn_table->add_txn(txn);
    // auto t1 = txn_table->get_txn(txn->GetTxnId());

    auto exe_stt = GetSystemClock();
    rc = txn->Execute(query);
    bool abort_dur_exe = false;
    if (rc == GTBL_UP) {
      rc = ABORT;
      is_gtbl_up = true;
    }
    if (rc == ABORT ) {
      abort_dur_exe = true; 
    }
    auto exe_stp = GetSystemClock(); 
    // deal with commit or abort through Commit()
    rc = txn->Commit(rc);
    auto commit_stp = GetSystemClock();
    // if (rc == OK) {
      //  rc = txn->Commit();
    // } 
    // if (rc == ABORT) {
    //    txn->Abort();
    // }
    string req = "ycsb main"; 
    auto txn_id = txn->GetTxnId();
    txn_table->remove_txn(txn, req);
    // rc = arboretum::ARDB::CommitTxn(txn, rc);
    DEALLOC(txn);
    if (rc == ABORT) {
      retry_cnt++;
    } else {
      retry_cnt=0; 
    }
    if (g_warmup_finished) {
      auto endtime = GetSystemClock();
      if (rc == ABORT && !is_gtbl_up) {
        // if (abort_dur_exe) {
        //   LOG_INFO("XXX Albatross txn %d abort during execution", txn_id);
        // } else {
        //   LOG_INFO("XXX Albatross txn %d abort during commit", txn_id);
        // }
        worker->benchstats_.int_stats_.abort_cnt_++;
        Worker::abort_count_++;
      } else if (rc== COMMIT){
        // stats
        worker->benchstats_.int_stats_.commit_cnt_++;
        Worker::commit_count_++;
        // auto commit_latency = endtime - txn_starttime;
        auto commit_latency = endtime - starttime;
        Worker::dbstats_.int_stats_.txn_time_ns_ += commit_latency;

        Worker::dbstats_.int_stats_.txn_exe_time_ns_ += exe_stp - exe_stt;
        Worker::dbstats_.int_stats_.txn_commit_time_ns_ += commit_stp - exe_stp;
        Worker::dbstats_.int_stats_.txn_count_ += 1;
        if (dur_migr == 1) {
           user_txn_latencies.push_back(nano_to_ms(commit_latency)); 
        }
        // LOG_INFO("XXX albatross finish txn %d with %0.2f ms(exec %0.2f ms, commit %0.2f ms)", txn_id, nano_to_ms(commit_latency), nano_to_ms(exe_stp - exe_stt), nano_to_ms(commit_stp - exe_stp));
        // worker->benchstats_.vec_stats_.commit_latency_ns_.push_back(commit_latency);
        // if (worker->benchstats_.int_stats_.commit_cnt_ % 10000 == 0) {
        //   LOG_DEBUG("thread %u finishes %u txns", worker->GetThdId(),
        //             worker->benchstats_.int_stats_.commit_cnt_);
        // }
      }
      // stats
      auto latency = endtime - starttime;

      worker->benchstats_.int_stats_.thd_runtime_ns_ += latency;
      
    }
  }
  LOG_INFO("Thread-%u finishes execution with %u txns!", Worker::GetThdId(),  worker->benchstats_.int_stats_.commit_cnt_);
  worker_thread_done();
  // summarize stats
  arboretum::Worker::latch_.lock();
  Worker::user_txn_latency_accum.insert(Worker::user_txn_latency_accum.end(), user_txn_latencies.begin(), user_txn_latencies.end());
  Worker::SumStats();
  worker->SumStats();
  arboretum::Worker::latch_.unlock();
}


void YCSBWorkload::LoadDataForMigr(string &tbl_name, std::vector<uint64_t> * granules_to_migr) {
   char data[schemas_[tbl_name]->GetTupleSz()];
  // load data/cache
  for (auto gid: *granules_to_migr) {
       auto start_key = g_granule_size_mb*1024 * gid;
       for (int i = 0; i < g_granule_size_mb*1024; i++) {
          uint64_t primary_key = start_key + i;
          // set primary key
          schemas_[tbl_name]->SetPrimaryKey(static_cast<char *>(data), primary_key);
          schemas_[tbl_name]->SetNumericField(1, data, primary_key);
          db_->InsertTuple(tables_[tbl_name], primary_key, data, schemas_[tbl_name]->GetTupleSz());
       }
    
  }

  LOG_INFO("Finished loading YCSB workload and checked durability");

}

void YCSBWorkload::LoadData(string &tbl_name) {
#if REMOTE_STORAGE_TYPE == REMOTE_STORAGE_REDIS
  char data[schemas_[tbl_name]->GetTupleSz()];
  // strcpy(&data[schemas_[0]->GetFieldOffset(1)], "init");
  uint64_t rows_per_node = config_->num_rows_;

  // load data
  for (int64_t key = 0; (uint64_t)key < rows_per_node; key++) {
    uint64_t start_key = rows_per_node * g_node_id;
    uint64_t primary_key = start_key + key;
    // LOG_INFO("primary key is %d", primary_key);
    // set primary key
    uint64_t granule_id = GetGranuleID(primary_key);
    schemas_[tbl_name]->SetPrimaryKey(static_cast<char *>(data), primary_key);
    schemas_[tbl_name]->SetNumericField(1, data, primary_key);
    db_->InsertTuple(tables_[tbl_name], primary_key, data,
                     schemas_[tbl_name]->GetTupleSz());

    // uint64_t primary_key = key * g_num_nodes + g_node_id;
  }

  if (g_storage_data_loaded && g_check_preloaded_data) {
    // TODO(Hippo): generate granule id properly
    M_ASSERT((uint64_t)db_->CheckStorageSize() == config_->num_rows_,
             "Size of data inserted does not match!");
  }
  LOG_INFO("Finished loading YCSB workload and checked durability");
#elif REMOTE_STORAGE_TYPE == REMOTE_STORAGE_AZURE
  int batch_async_size = 6;
  if (!g_storage_data_loaded) {
    if (g_buf_type == NOBUF) {
      auto starttime =  std::chrono::high_resolution_clock::now();
      auto sz = schemas_[tbl_name]->GetTupleSz();
      char data[sz];
      // strcpy(&data[schemas_[0]->GetFieldOffset(1)], "init");
      uint64_t rows_per_node = config_->num_rows_;
      size_t batch_sz = 64;
      auto num_batches = 0;
      if (((g_granule_size_mb * 1024) % batch_sz) != 0) {
        LOG_ERROR(
            "partition size must be multiples of %zu, parition size is %d mb",
            batch_sz, g_granule_size_mb);
      }

      size_t progress = 0;
      unordered_map<uint64_t, string> batch;

      // load data
      uint64_t start_key = (g_load_data_gb_st !=0 )? (g_load_data_gb_st * 1024*1024 + rows_per_node * g_node_id): rows_per_node * g_node_id;
      size_t last_partition = GetGranuleID(start_key);;

      uint64_t primary_key;
      for (int64_t key = 0; (uint64_t)key < rows_per_node; key++) {
        primary_key = start_key + key;
        // set primary key
        schemas_[tbl_name]->SetPrimaryKey(static_cast<char *>(data),
                                          primary_key);
        schemas_[tbl_name]->SetNumericField(1, data, primary_key);
        OID fake_tbl_id = 0;
        auto total_tuple_sz = schemas_[tbl_name]->GetTupleSz();
        auto tuple = new (MemoryAllocator::Alloc(sizeof(ITuple) + total_tuple_sz)) ITuple(0, primary_key);
        tuple->SetData(data, sz);
        batch.insert({primary_key, std::string(reinterpret_cast<char *>(tuple),
                          total_tuple_sz)});
        auto part = GetGranuleID(primary_key);
        if ((key + 1) % batch_sz == 0 && key != 0) {
            if (part != last_partition) {
                LOG_INFO("Finish loading granule/partition %d ",
                         last_partition);
                last_partition = part;
            }
            db_->BatchInsert(tables_[tbl_name], part, batch);
            cout<< "XXX: insert batch " << num_batches << endl; 

            batch.clear();
            // init_part = (key + 1) / g_partition_sz;
            num_batches++;
            if (num_batches % batch_async_size == 0) {
                // LOG_INFO("Waiting for batch number %d", num_batches/8);
                cout << "XXX: Waiting for batch number"<< num_batches/batch_async_size << endl; 
                db_->WaitForAsyncBatchLoading(batch_async_size);
            }
        }

        // uint64_t primary_key = key * g_num_nodes + g_node_id;
      }
      // insert last batch
      if (!batch.empty()) {
        auto part = GetGranuleID(primary_key);
        db_->BatchInsert(tables_[tbl_name], part, batch);
        cout<< "XXX: insert batch " << num_batches << endl; 
        last_partition = part;
        num_batches++;
        batch.clear();
        LOG_INFO("Finish loading granule/partition %d ", last_partition);
        cout<< "Finish loading granule/partition " << last_partition << endl; 
      } else {
        LOG_INFO("Finish loading granule/partition %d ", last_partition);
        cout<< "Finish loading granule/partition " << last_partition << endl; 
      }
      cout << "XXX: Waiting for batch number"<< num_batches/batch_async_size << endl; 
      db_->WaitForAsyncBatchLoading(num_batches % batch_async_size);

      // if (g_storage_data_loaded && g_check_preloaded_data) {
      //   // TODO(Hippo): generate granule id properly
      //   M_ASSERT((uint64_t)db_->CheckStorageSize() == config_->num_rows_,
      //            "Size of data inserted does not match!");
      // }
      auto endtime = std::chrono::high_resolution_clock::now();
      int duration = std::chrono::duration_cast<std::chrono::seconds>(endtime - starttime).count();
      LOG_INFO("Finished loading YCSB workload and checked durability with batch number %d, takes %d s", num_batches, duration);

    } else if (g_buf_type == OBJBUF) { 
      char data[schemas_[tbl_name]->GetTupleSz()];
      // strcpy(&data[schemas_[0]->GetFieldOffset(1)], "init");
      uint64_t rows_per_node = config_->num_rows_;

      // load data
      for (int64_t key = 0; (uint64_t)key < rows_per_node; key++) {
        uint64_t start_key = rows_per_node * g_node_id;
        uint64_t primary_key = start_key + key;
        // LOG_INFO("primary key is %d", primary_key);
        // set primary key
        uint64_t granule_id = GetGranuleID(primary_key);
        schemas_[tbl_name]->SetPrimaryKey(static_cast<char *>(data),
                                          primary_key);
        schemas_[tbl_name]->SetNumericField(1, data, primary_key);
        db_->InsertTuple(tables_[tbl_name], primary_key, data,
                         schemas_[tbl_name]->GetTupleSz());

        // uint64_t primary_key = key * g_num_nodes + g_node_id;
      }

      if (g_storage_data_loaded && g_check_preloaded_data) {
        // TODO(Hippo): generate granule id properly
        M_ASSERT((uint64_t)db_->CheckStorageSize() == config_->num_rows_,
                 "Size of data inserted does not match!");
      }
      LOG_INFO("Finished loading YCSB workload and checked durability");
    } else {
      M_ASSERT(false, "unknown g_buf_type");
    }
  }

#endif
}

// #if REMOTE_STORAGE_TYPE == REMOTE_STORAGE_AZURE
// void YCSBWorkload::BatchLoad() {
//   auto sz = schemas_[0]->GetTupleSz();
//   char data[sz];
//   strcpy(&data[schemas_[0]->GetFieldOffset(1)], "init");
//   std::multimap<std::string, std::string> batch;
//   size_t init_part = 0;
//   size_t progress = 0;
//   auto num_batches = 0;
//   size_t batch_sz = 64;
//   if (((g_granule_size_mb*1024) % batch_sz) != 0) {
//     LOG_ERROR("partition size must be multiples of %zu, parition size is %d mb", batch_sz,  g_granule_size_mb);
//   }
//   for (int64_t key = config_->loading_startkey; (uint64_t) key < config_->num_rows_; key++) {
//     if (key % (config_->num_rows_ / batch_sz) == 0 && progress <= batch_sz) {
//       LOG_DEBUG("Loading progress: %3zu %% (current key = %ld) ", progress, key);
//       progress++;
//     }
//     // set primary key
//     schemas_[0]->SetPrimaryKey(data, key);
//     batch.insert({std::to_string(key), std::string(data, sz)});
//     auto part = key / g_partition_sz;
//     if ((key + 1) % batch_sz == 0 && key != 0) {
//       if (part != init_part) {
//         LOG_ERROR("current part id %zu for key (%ld) does not match starting part id %zu in the batch",
//                   part, key, init_part);
//       }
//       db_->BatchInsert(tables_["MAIN_TABLE"], part, batch);
//       batch.clear();
//       init_part = (key + 1) / g_partition_sz;
//       num_batches++;
//       if (num_batches % 8 == 0) {
//         db_->WaitForAsyncBatchLoading(8);
//       }
//     }
//   }
//   db_->FinishLoadingData(tables_["MAIN_TABLE"]);
// }
// #endif


// YCSBQuery *YCSBWorkload::GenQuery() {
//   auto query = NEW(YCSBQuery);
//   query->req_cnt = config_->num_req_per_query_;
//   query->requests = NEW_SZ(YCSBRequest, query->req_cnt);
//   size_t cnt = 0;
//   while (cnt < query->req_cnt) {
//     GenRequest(query, cnt);
//   }
//   return query;
// }


YCSBQuery *YCSBWorkload::GenQuery() {
  auto query = NEW(YCSBQuery);
  query->tbl_id = tables_[config_->GetTblName()]; 
  query->req_cnt = config_->num_req_per_query_;
  query->requests = NEW_SZ(YCSBRequest, query->req_cnt);
  query->is_all_remote_readonly= false;
  query->q_type = QueryType::REGULAR;
  size_t _request_cnt = 0;
  M_ASSERT(config_->num_req_per_query_ <= 64, "Change the following constant if num_req_per_query_ > 64");
  uint64_t all_keys[64];
  uint64_t table_size = config_->num_rows_;
  if (g_loadbalance_enable) {
      while (_request_cnt < config_->num_req_per_query_) {
      YCSBRequest & req = query->requests[_request_cnt];
      // for loadBalance workload, config_->req_remote_perc_ should be a high value like 0.8
      // double lottery = ARDB::RandDouble();
      // bool remote = (lottery < config_->req_remote_perc_);
      // uint32_t node_id;
      // if (remote) {
      //   if (g_node_id == g_scale_node_id) {
      //     node_id = ARDB::RandUint64(1, g_num_nodes - 1);  
      //   } else {
      //     node_id = g_scale_node_id;
      //   }
      // } else {
      //   node_id = g_node_id;
      // }
 
      // uint64_t row_id = (config_->zipf_theta_ == 0)? ARDB::RandUint64(0, table_size - 1):zipf(table_size - 1,config_->zipf_theta_);






      // uint64_t row_id = zipf(table_size - 1,config_->zipf_theta_);
      // uint64_t row_id = ARDB::RandUint64(0, table_size - 1);
      double lottery = ARDB::RandDouble();
      bool remote = (lottery < config_->req_remote_perc_);
      uint64_t primary_key;
      if (remote) {
          if (g_node_id == g_scale_node_id) {
              uint32_t node_id = ARDB::RandUint64(1, g_num_nodes - 1);
              primary_key = config_->num_rows_ * node_id +
                            ARDB::RandUint64(0, table_size - 1);
          } else {
              if (ARDB::RandDouble() < 0.95) {
                  int migr_gran_num_per_node =
                      LoadBalanceWorkload::granule_num_per_node / g_num_nodes;
                  int migr_gran_num_per_thd =
                      migr_gran_num_per_node / g_migr_threads_num;
                  std::vector<uint64_t> granules_to_migr;
                  for (int i = 0; i < migr_gran_num_per_node; i++) {
                      uint64_t start_gran_id =
                          g_node_id * migr_gran_num_per_node;
                      granules_to_migr.push_back(start_gran_id + i);
                  }

                  uint64_t granule_id = granules_to_migr[ARDB::RandUint64(
                      0, granules_to_migr.size() - 1)];
                  uint64_t row_id =
                      ARDB::RandUint64(0, g_granule_size_mb * 1024 - 1);
                  primary_key = g_granule_size_mb * 1024 * granule_id + row_id;
              } else {
                  primary_key = config_->num_rows_ * g_scale_node_id +
                                ARDB::RandUint64(0, table_size - 1);
              }
          }
      } else {
         primary_key = config_->num_rows_ * g_node_id + ARDB::RandUint64(0, table_size - 1);
      }

      // uint64_t row_id = zipf(table_size * config_->num_dataload_nodes_ - 1,config_->zipf_theta_);
      // uint64_t primary_key = row_id;
      double r = ARDB::RandDouble();
      req.ac_type = (r < config_->read_perc_)? READ : UPDATE;
      req.key = primary_key;
      req.value = 0;
      // remove duplicates
      bool exist = false;
      for (uint32_t i = 0; i < _request_cnt; i++)
          if (all_keys[i] == req.key)
              exist = true;
      if (!exist)
          all_keys[_request_cnt ++] = req.key;
  }

  } else if (g_scaleout_enable) {
      while (_request_cnt < config_->num_req_per_query_) {
          YCSBRequest &req = query->requests[_request_cnt];
          bool remote = (g_num_nodes > 1)
                            ? (ARDB::RandDouble() < config_->req_remote_perc_)
                            : false;
          // if (remote) {
          //     Worker::dbstats_.int_stats_.remote_req_count_ += 1;
          // } else {
          //     Worker::dbstats_.int_stats_.local_req_count_ += 1;
          // }

          uint32_t node_id;
          if (remote) {
              node_id = (g_node_id + ARDB::RandUint64(
                                         1, config_->num_dataload_nodes_ - 1)) %
                        config_->num_dataload_nodes_;
              M_ASSERT(node_id != g_node_id, "Wrong remote query node id");
              Worker::dbstats_.int_stats_.remote_req_count_ += 1;
          } else {
              node_id = g_node_id;
              Worker::dbstats_.int_stats_.local_req_count_ += 1;
          }

          uint64_t row_id = (config_->zipf_theta_ == 0)
                                ? ARDB::RandUint64(0, table_size - 1)
                                : zipf(table_size - 1, config_->zipf_theta_);

          // uint64_t row_id = zipf(table_size - 1,config_->zipf_theta_);
          // uint64_t row_id = ARDB::RandUint64(0, table_size - 1);
          uint64_t primary_key = config_->num_rows_ * node_id + row_id;

          // uint64_t row_id = zipf(table_size * config_->num_dataload_nodes_ -
          // 1,config_->zipf_theta_); uint64_t primary_key = row_id;
          double r = ARDB::RandDouble();
          req.ac_type = (r < config_->read_perc_) ? READ : UPDATE;
          req.key = primary_key;
          req.value = 0;
          // remove duplicates
          bool exist = false;
          for (uint32_t i = 0; i < _request_cnt; i++)
              if (all_keys[i] == req.key) exist = true;
          if (!exist) all_keys[_request_cnt++] = req.key;
      }
  } else if (g_scalein_enable) {
    M_ASSERT(false, "to support");
  } else if (g_faulttolerance_enable) {
    M_ASSERT(false, "to support");
  }

  return query;
}


// YCSBQuery *YCSBWorkload::GenQuery() {
//   auto query = NEW(YCSBQuery);
//   query->tbl_id = tables_[config_->GetTblName()]; 
//   query->req_cnt = config_->num_req_per_query_;
//   query->requests = NEW_SZ(YCSBRequest, query->req_cnt);
//   query->is_all_remote_readonly= true;
//   size_t _request_cnt = 0;
//   M_ASSERT(config_->num_req_per_query_ <= 64, "Change the following constant if num_req_per_query_ > 64");
//   uint64_t all_keys[64];
//   bool has_remote = false;
//   uint64_t table_size = config_->num_rows_;
//   // for (uint32_t tmp = 0; tmp < config_->num_req_per_query_; tmp ++) {
//   while (_request_cnt < config_->num_req_per_query_) {
//       YCSBRequest & req = query->requests[_request_cnt];
//       bool remote = (g_num_nodes > 1)? (ARDB::RandDouble() < config_->req_remote_perc_) : false;
//       uint32_t node_id;
//       if (remote) {
//           //TODO(hippo): remove node id which will be provide by system table(granule table)
//           node_id = (g_node_id + ARDB::RandUint64(1, g_num_nodes - 1)) % g_num_nodes;
//           has_remote = true;
//           Worker::dbstats_.int_stats_.remote_req_count_ += 1;
//       } else {
//           node_id = g_node_id;
//           Worker::dbstats_.int_stats_.local_req_count_ += 1;
//       } 
//       // #if SINGLE_PART_ONLY
//       // uint64_t row_id = zipf(table_size / g_num_worker_threads - 1, g_zipf_theta);
//       // row_id = row_id * g_num_worker_threads + GET_THD_ID;
//       // assert(row_id < table_size);
//       // #else
//       uint64_t row_id = zipf(table_size - 1,config_->zipf_theta_);
//       // #endif
//       // uint64_t primary_key = row_id * g_num_nodes + node_id;
//       uint64_t primary_key = table_size * node_id + row_id;
//       M_ASSERT(primary_key < table_size * (node_id + 1), "primary_key is too large, primary_key=%ld, node_id=%d\n", primary_key, node_id);
//       M_ASSERT(primary_key >= table_size * node_id, "primary_key is too small, primary_key=%ld, node_id=%d\n", primary_key, node_id);
//       // bool readonly = (row_id == 0)? false :
//       //                 (int(row_id * config_->read_perc_) > int((row_id - 1) * config_->read_perc_));
//       //TODO(Hippo); remove this hack and support read-only txn
//       bool readonly = false;
//       if (readonly)
//           req.ac_type = READ;
//       else {
//           double r = ARDB::RandDouble();
//           req.ac_type = (r <config_->read_perc_)? READ : UPDATE;
//       }
//       if (req.ac_type == UPDATE && remote)
//           query->is_all_remote_readonly = false;

//       req.key = primary_key;
//       req.value = 0;
//       // remove duplicates
//       bool exist = false;
//       for (uint32_t i = 0; i < _request_cnt; i++)
//           if (all_keys[i] == req.key)
//               exist = true;
//       if (!exist)
//           all_keys[_request_cnt ++] = req.key;
//   }
//   if (!has_remote)
//         query->is_all_remote_readonly = false;
//   // // Sort the requests in key order.
//   // if (g_sort_key_order) {
//   //     for (int i = _request_cnt - 1; i > 0; i--)
//   //         for (int j = 0; j < i; j ++)
//   //             if (_requests[j].key > _requests[j + 1].key) {
//   //                 RequestYCSB tmp = _requests[j];
//   //                 _requests[j] = _requests[j + 1];
//   //                 _requests[j + 1] = tmp;
//   //             }
//   //     for (uint32_t i = 0; i < _request_cnt - 1; i++)
//   //         assert(_requests[i].key < _requests[i + 1].key);
//   // }
//   return query;
// }

// void YCSBWorkload::GenRequest(YCSBQuery * query, size_t &cnt) {
//   // TODO: add (bool) remote and g_node_id info in the future
//   YCSBRequest & req = query->requests[cnt];
//   auto g_num_nodes = 1;
//   auto node_id = 0;
//   uint64_t row_id = zipf(config_->num_rows_ - 1, config_->zipf_theta_);
//   uint64_t primary_key = row_id * g_num_nodes + node_id;
//   bool readonly = row_id != 0 && (int((int32_t) row_id * config_->read_perc_) >
//       int(((int32_t) row_id - 1) * config_->read_perc_));
//   if (readonly)
//     req.ac_type = READ;
//   else {
//     double r = ARDB::RandDouble();
//     req.ac_type = (r < config_->read_perc_) ? READ : UPDATE;
//   }
//   req.key = primary_key;
//   req.value = 0;
//   // remove duplicates
//   bool exist = false;
//   for (uint32_t i = 0; i < cnt; i++)
//     if (query->requests[i].key == req.key)
//       exist = true;
//   if (!exist)
//     cnt++;
// }



void YCSBWorkload::CalculateDenom() {
  assert(the_n == 0);
  uint64_t table_size = config_->num_rows_;
  M_ASSERT(table_size % g_num_worker_threads == 0, "Table size must be multiples of worker threads");
  the_n = table_size / g_num_worker_threads - 1;
  denom = zeta(the_n, config_->zipf_theta_);
  zeta_2_theta = zeta(2, config_->zipf_theta_);
}

uint64_t YCSBWorkload::zipf(uint64_t n, double theta) {
  assert(theta == config_->zipf_theta_);
  double alpha = 1 / (1 - theta);
  double zetan = denom;
  double eta = (1 - pow(2.0 / n, 1 - theta)) /
      (1 - zeta_2_theta / zetan);
  double u = ARDB::RandDouble();
  double uz = u * zetan;
  if (uz < 1) return 0;
  if (uz < 1 + pow(0.5, theta)) return 1;
  return (uint64_t) (n * pow(eta * u - eta + 1, alpha));
}

double YCSBWorkload::zeta(uint64_t n, double theta) {
  double sum = 0;
  for (uint64_t i = 1; i <= n; i++)
    sum += pow(1.0 / (int32_t) i, theta);
  return sum;
}

} // namespace