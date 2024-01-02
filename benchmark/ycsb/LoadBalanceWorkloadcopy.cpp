
// #include "LoadBalanceWorkload.h"
// #include "ITxn.h"
// #include "MigrationTxn.h"
// #include "PostheatTxn.h"
// #include "common/GlobalData.h"
// #include "common/OptionalGlobalData.h"
// #include "ycsb/YCSBConfig.h"
// #include "db/txn/TxnTable.h"

// namespace arboretum {

// // single node migration
// uint32_t LoadBalanceWorkload::granule_num_per_node = 32 * 3;

// LoadBalanceWorkload::LoadBalanceWorkload(ARDB *db, YCSBConfig *config) : ScaleoutWorkload(db, config) {
// }

// void LoadBalanceWorkload::Init() {
//   LOG_INFO("LoadBalance Init...")
//   string tbl_name = config_->GetTblName();
//   std::ifstream in("configs/"+ tbl_name);
//   InitSchema(in);
//   LoadSysTBL();
//   M_ASSERT(config_->migrate_cache_enable_ == 0, "only support migration_cache_enable_ = 0");
//   GenLockTable();
//   GenQueries();
// }

// void LoadBalanceWorkload::InitComplement() {
//   LOG_INFO("LoadBalance Complement Init...")
//   M_ASSERT(config_->migrate_cache_enable_ == 0, "only support migration_cache_enable_ = 0");
//   GenLockTable();
//   GenQueries();
// }

// void LoadBalanceWorkload::GenLockTable() {
//     auto user_table = db_->GetTable(0);
//     int migr_node_num = g_num_nodes - 2;
//     int migr_gran_num_per_node = granule_num_per_node / migr_node_num;
//     int migr_gran_num_per_thd = migr_gran_num_per_node / g_migr_threads_num;
//     std::vector<uint64_t> granules_to_migr;
//     for (int i = 0; i < migr_gran_num_per_node; i++) {
//       uint64_t start_gran_id = g_node_id * migr_gran_num_per_node;
//       granules_to_migr.push_back(start_gran_id + i);
//     }
//     for (auto gid: granules_to_migr) {
//        auto start_key = g_granule_size_mb*1024 * gid;
//        for (int i = 0; i < g_granule_size_mb*1024; i++) {
//             // user_table->Putputstart_key + i] = NEW(std::atomic<uint32_t>)(0);
//             user_table->PutLock(start_key + i,  NEW(std::atomic<uint32_t>)(0));
//        }
//     }
// }

// void LoadBalanceWorkload::GenQueries() {
//     LOG_INFO("LoadBalance Workload Generate Queries...")
//     // auto tbl_id = db_->GetGranuleTable()->GetTableId();
//     auto g_tbl_name = db_->GetGranuleTable()->GetTableName();
//     auto tbl_id = db_->GetGranuleTableID(); 
//     cout << "XXX: gen query for table " << g_tbl_name << ", with tbl_id " << tbl_id << endl;
//     auto query_tbl_name = config_->GetTblName();
//     auto post_heat_tbl_id = tables_[config_->GetTblName()];
//     // M_ASSERT(g_migr_threads_num <= granule_num_per_node, "maximum migr thread
//     // num should be %d, current is %d", granule_num_per_node,
//     // g_migr_threads_num);
//     LockType lock_type =
//         (g_migr_wait_die_enable) ? LockType::WAIT_DIE : LockType::NO_WAIT;

//     int migr_node_num = g_num_nodes - 2;
//     int migr_gran_num_per_node = granule_num_per_node / migr_node_num;
//     int migr_gran_num_per_thd = migr_gran_num_per_node / g_migr_threads_num;
//     M_ASSERT((granule_num_per_node % migr_node_num) == 0,
//              "number of granules (%d) in scale node has to be distributed in "
//              "cluster evenly",
//              granule_num_per_node);
//     M_ASSERT((migr_gran_num_per_node % g_migr_threads_num) == 0,
//              "number of granules per node (%d) to be migrated has to be "
//              "distributed in threads (%d) evenly",
//              migr_gran_num_per_node, g_migr_threads_num);
//     std::vector<uint64_t> granules_to_migr;
//     for (int i = 0; i < migr_gran_num_per_node; i++) {
//       uint64_t start_gran_id = g_node_id * migr_gran_num_per_node;
//       granules_to_migr.push_back(start_gran_id + i);
//     }
//     cout << "XXX: " << granules_to_migr.size() << " granules to migr to node  " << g_node_id << " is ( ";
//     for (auto gid: granules_to_migr) {
//         cout << gid << " ";
//     }
//     cout << " )" << endl;

//     for (int i = 0; i < g_migr_threads_num; i++) {
//         vector<YCSBQuery *> *queries = new vector<YCSBQuery *>();
//         for (int j = 0; j < migr_gran_num_per_thd; j++) {
//             int start_idx = i * migr_gran_num_per_thd;
//             auto granule_id = granules_to_migr[start_idx + j];
//             auto query = NEW(YCSBQuery);
//             query->tbl_id = tbl_id;
//             query->req_cnt = 1;
//             query->q_type = QueryType::MIGR;
//             query->requests = NEW_SZ(YCSBRequest, query->req_cnt);
//             query->is_all_remote_readonly = false;
//             // query to migrate
//             query->requests[0].key = granule_id;
//             query->requests[0].value = g_node_id;
//             query->requests[0].ac_type = AccessType::UPDATE;
//             query->requests[0].lock_type = lock_type;
//             queries->push_back(query);
//             if (g_migr_postheat_enable) {
//                 // post-heat query
//                 auto query = NEW(YCSBQuery);
//                 query->tbl_id = post_heat_tbl_id;
//                 query->req_cnt = 1;
//                 query->q_type = QueryType::GRANULE_SCAN;
//                 query->requests = NEW_SZ(YCSBRequest, query->req_cnt);
//                 query->is_all_remote_readonly = true;
//                 query->requests[0].key = granule_id;
//                 query->requests[0].value = g_node_id;
//                 query->requests[0].ac_type = AccessType::READ;
//                 query->requests[0].lock_type = lock_type;
//                 queries->push_back(query);
//             }
//             all_queries_.insert(std::make_pair(i, queries));
//         }
//     }

//     // printout queries
//     for (auto it = all_queries_.begin(); it != all_queries_.end(); ++it) {
//         auto queries = it->second;
//         cout << "thread_id: " << it->first << " get queries( ";
//         for (auto query = queries->begin(); query != queries->end(); ++query) {
//             cout << (*query)->requests[0].key << " ";
//         }
//         cout << " )" << endl;
//     }
// }

// // void ScaleoutWorkload::Execute(Workload *input_workload, BenchWorker *worker) {
// //   ScaleoutWorkload * workload = (ScaleoutWorkload *)input_workload;
// //   arboretum::Worker::SetThdId(worker->worker_id_);
// //   arboretum::Worker::SetMaxThdTxnId(0);
// //   arboretum::ARDB::InitRand(worker->worker_id_);
// //   LOG_INFO("ScaleoutWorkerThread-%u starts execution!", arboretum::Worker::GetThdId());
// //   uint64_t origin = GetSystemClock();
// //   auto origin_chrono = std::chrono::high_resolution_clock::now();


// //   // sleep(workload->GetConfig()->time_before_migration_);
// //   std::this_thread::sleep_for(std::chrono::seconds(workload->GetConfig()->time_before_migration_));

// //   YCSBQuery * query;
// //   bool restart = false;
// //   // uint64_t txn_starttime;
// //   // uint64_t starttime;
// //   auto starttime_chrono = std::chrono::high_resolution_clock::now(); 
// //   auto txn_starttime_chrono = std::chrono::high_resolution_clock::now();
// //   auto critical_end = std::chrono::high_resolution_clock::now();
// //   auto critical_stt = std::chrono::high_resolution_clock::now();

// //   size_t cur_query_id = 0;
// //   auto queries = *(workload->all_queries_[worker->worker_id_]);
// //   //sleep for lag
// //   if (g_migr_conc_lag_ms > 0) {
// //       usleep(g_migr_conc_lag_ms * 1000 * (worker->worker_id_ - 1));
// //   }
// //   LOG_INFO("Start Migration!");
// //   uint64_t exe_stt = 0; 
// //   uint64_t exe_stp = 0;
// //   uint64_t commit_stp = 0;

// //   auto myid = this_thread::get_id();
// // stringstream ss;
// // ss << myid;
// // string mystring = ss.str();
// //     cout<< "thread_id: " << mystring << " get queries( ";
// //     for(auto query = queries.begin(); query != queries.end(); ++query) {
// //       cout << (*query)->requests[0].key << " ";

// //     } 
// //     cout << " )"<< endl;

// //   while (cur_query_id < queries.size()) {
// //     if (!restart) {
// //       query = queries[cur_query_id];
// //       // txn_starttime = GetSystemClock();
// //       txn_starttime_chrono = std::chrono::high_resolution_clock::now(); 
// //       // starttime = txn_starttime;
// //       starttime_chrono = txn_starttime_chrono;
// //     } else {
// //       // starttime = GetSystemClock();
// //       starttime_chrono = std::chrono::high_resolution_clock::now(); 
// //     }
// //     RC rc = OK;
// //     if (query->q_type == QueryType::MIGR) {
// //       MigrationTxn *txn =
// //           (MigrationTxn *)workload->db_->StartTxn(TxnType::MIGRATION);
// //       txn->setRetry(restart);
// //       txn_table->add_txn(txn);
// //       auto t1 = txn_table->get_txn(txn->GetTxnId());
// //       auto exe_stt = GetSystemClock();
// //       rc = txn->Preheat(query);
// //       critical_stt = std::chrono::high_resolution_clock::now();
// //       if (rc == OK) {
// //         rc = txn->Execute(query);
// //       }
// //       auto exe_stp = GetSystemClock();
// //       // deal with commit or abort through Commit()
// //       rc = txn->Commit(rc);
// //       auto commit_stp = std::chrono::high_resolution_clock::now();
// //       critical_end = commit_stp;
// //       string req = "scale out main";
// //       if (rc == COMMIT) {
// //         txn->Postheat();
// //       }
// //       txn_table->remove_txn(txn, req);
// //       // rc = arboretum::ARDB::CommitTxn(txn, rc);
// //       DEALLOC(txn);
// //       if (rc == COMMIT) {
// //               cout << "XXX: finish migrate for granule "
// //                    << query->requests[0].key << endl;
// //       }

// //     } else if (query->q_type == QueryType::GRANULE_SCAN) {
// //       PostheatTxn *txn =
// //           (PostheatTxn *)workload->db_->StartTxn(TxnType::POSTHEAT);
// //       txn_table->add_txn(txn);
// //       auto t1 = txn_table->get_txn(txn->GetTxnId());
// //       auto exe_stt = GetSystemClock();
// //       if (rc == OK) {
// //         rc = txn->Execute(query);
// //       }
// //       auto exe_stp = GetSystemClock();
// //       // deal with commit or abort through Commit()
// //       if (rc == OK) {
// //         rc = COMMIT;
// //       } else {
// //         M_ASSERT(false, "hasn't support abort yet");
// //       }
// //       rc = txn->Commit(rc);
// //       auto commit_stp = GetSystemClock();
// //       string req = "scale out main";
// //       txn_table->remove_txn(txn, req);
// //       // rc = arboretum::ARDB::CommitTxn(txn, rc);
// //       DEALLOC(txn);
// //       if (rc == COMMIT) {
// //               cout << "XXX: finish postheat for granule "
// //                    << query->requests[0].key << endl;
// //       }

// //     } else {
// //       M_ASSERT(false, "unknown query type for scaleoutworkload");
// //     }

// //     restart = (rc == ABORT)? true: false;
// //     if (g_warmup_finished) {
// //       // auto endtime = GetSystemClock();
// //       auto endtime_chrono = std::chrono::high_resolution_clock::now(); 
// //       if (rc == ABORT) {
// //         worker->benchstats_.int_stats_.abort_cnt_++;
// //       } else {
// //         cur_query_id++;
// //         // stats
// //         worker->benchstats_.int_stats_.commit_cnt_++;
// //         Worker::commit_count_++;
// //         // auto commit_latency = endtime - txn_starttime;
// //         auto commit_latency = std::chrono::duration_cast<std::chrono::microseconds>(endtime_chrono - txn_starttime_chrono).count();
// //         Worker::dbstats_.int_stats_.txn_time_ns_ += commit_latency;
// //         Worker::dbstats_.int_stats_.txn_exe_time_ns_ += exe_stp - exe_stt;
// //         Worker::dbstats_.int_stats_.txn_commit_time_ns_ += commit_stp - exe_stp;
// //         Worker::dbstats_.int_stats_.txn_count_ += 1;
// //         if (g_migr_postheat_enable) {
// //             if (query->q_type == QueryType::MIGR) {
// //                 cout << "XXX migration start time "
// //                      << std::chrono::duration_cast<std::chrono::milliseconds>(
// //                             txn_starttime_chrono - origin_chrono)
// //                             .count() * 1.0 / 1000
// //                      << endl;
// //                 Worker::migrate_starts.push_back(
// //                     std::chrono::duration_cast<std::chrono::milliseconds>(
// //                         txn_starttime_chrono - origin_chrono)
// //                         .count()*1.0/1000);
// //                 // Worker::migrate_critical_starts.push_back(nano_to_s(critical_stt
// //                 // - origin));
// //                 Worker::migrate_critical_starts.push_back(
// //                     std::chrono::duration_cast<std::chrono::milliseconds>(
// //                         critical_stt - origin_chrono)
// //                         .count()*1.0/1000);
// //                 Worker::migrate_critical_ends.push_back(
// //                     std::chrono::duration_cast<std::chrono::milliseconds>(
// //                         critical_end - origin_chrono)
// //                         .count() * 1.0/1000);
// //             } else if (query->q_type == QueryType::GRANULE_SCAN) {
// //                 Worker::migrate_ends.push_back(
// //                     std::chrono::duration_cast<std::chrono::milliseconds>(
// //                         endtime_chrono - origin_chrono)
// //                         .count()* 1.0 / 1000);
// //             }

// //         } else {
// //             if (query->q_type == QueryType::MIGR) {
// //                 cout << "XXX migration start time "
// //                      << std::chrono::duration_cast<std::chrono::milliseconds>(
// //                             txn_starttime_chrono - origin_chrono)
// //                             .count() * 1.0 /1000
// //                      << endl;
// //                 Worker::migrate_starts.push_back(
// //                     std::chrono::duration_cast<std::chrono::milliseconds>(
// //                         txn_starttime_chrono - origin_chrono)
// //                         .count()*1.0/1000);
// //                 // Worker::migrate_critical_starts.push_back(nano_to_s(critical_stt
// //                 // - origin));
// //                 Worker::migrate_critical_starts.push_back(
// //                     std::chrono::duration_cast<std::chrono::milliseconds>(
// //                         critical_stt - origin_chrono)
// //                         .count()*1.0/1000);
// //                 Worker::migrate_critical_ends.push_back(
// //                     std::chrono::duration_cast<std::chrono::milliseconds>(
// //                         critical_end - origin_chrono)
// //                         .count()*1.0/1000);
// //                 Worker::migrate_ends.push_back(
// //                     std::chrono::duration_cast<std::chrono::milliseconds>(
// //                         endtime_chrono - origin_chrono)
// //                         .count()*1.0/1000);

// //                 // cout << "XXX migration start time " <<
// //                 // nano_to_s(std::chrono::duration_cast<std::chrono::microseconds>(txn_starttime_chrono
// //                 // - origin_chrono).count()) << endl;
// //                 // Worker::migrate_starts.push_back(nano_to_s(std::chrono::duration_cast<std::chrono::microseconds>(txn_starttime_chrono
// //                 // - origin_chrono).count()));
// //                 // //
// //                 // Worker::migrate_critical_starts.push_back(nano_to_s(critical_stt
// //                 // - origin));
// //                 // Worker::migrate_ends.push_back(nano_to_s(std::chrono::duration_cast<std::chrono::microseconds>(endtime_chrono
// //                 // - origin_chrono).count()));

// //                 // Worker::migrate_starts.push_back(nano_to_s(txn_starttime -
// //                 // origin));
// //                 // Worker::migrate_critical_starts.push_back(nano_to_s(critical_stt
// //                 // - origin)); Worker::migrate_ends.push_back(nano_to_s(endtime
// //                 // - origin));
// //             }
// //         }

// //         // worker->benchstats_.vec_stats_.commit_latency_ns_.push_back(commit_latency);
// //         // if (worker->benchstats_.int_stats_.commit_cnt_ % 10000 == 0) {
// //         //   LOG_DEBUG("thread %u finishes %u txns", worker->GetThdId(),
// //         //             worker->benchstats_.int_stats_.commit_cnt_);
// //         // }
// //       }
// //       // stats
// //       // auto latency = endtime - starttime;
// //       auto latency = std::chrono::duration_cast<std::chrono::microseconds>(endtime_chrono - starttime_chrono).count();
// //       worker->benchstats_.int_stats_.thd_runtime_ns_ += latency;
// //     }
// //   }
// //   LOG_INFO("Migration Finished!");
// //   sleep(workload->GetConfig()->time_after_migration_);
// //   LOG_INFO("ScaleoutWorkerThread-%u finishes execution with %u txns!", Worker::GetThdId(),  worker->benchstats_.int_stats_.commit_cnt_);
// //   worker_thread_done();
// //   // summarize stats
// //   arboretum::Worker::latch_.lock();
// //   Worker::SumStats();
// //   worker->SumStats();
// //   arboretum::Worker::latch_.unlock();
// // }

// }