//
// Created by Zhihan Guo on 4/1/23.
//

#include "ITxn.h"
#include "CCManager.h"
#include "remote/IDataStore.h"
#include "remote/ILogStore.h"
#include "buffer/ObjectBufferManager.h"
#include "common/Worker.h"
#include "txn/TxnManager.h"
#include "log/LogEntry.h"
#include "transport/rpc_client.h"
#include "ClusterManager.h"
#include "ycsb/ScaleoutWorkload.h"
#include "db/txn/TxnTable.h"

namespace arboretum {
// std::vector<uint64_t> ScaleoutWorkload::GranulesToMigrate = {24, 25, 26, 27 ,28, 29, 30, 31, 56, 57, 58, 59, 60, 61, 62, 63, 88, 89, 90, 91, 92, 93, 94, 95}; 


ITxn::ITxn(OID txn_id, ARDB * db) : txn_id_(txn_id), db_(db) {
    rpc_semaphore_ = new SemaphoreSync();
    log_semaphore_ = new SemaphoreSync();
};

RC ITxn::Execute(YCSBQuery *query) {
    RC rc = OK;
    RC remote_rc = OK;
    auto response_rpc_endtime_chrono = std::chrono::high_resolution_clock::now(); 
    auto starttime_chrono = std::chrono::high_resolution_clock::now();
    // group requests by node id
    unordered_map<uint64_t, vector<YCSBRequest *>> node_requests_map;
    unordered_map<uint64_t, uint64_t> granule_node_map;
    for (uint32_t i = 0; i < query->req_cnt; i++) {
        YCSBRequest &req = query->requests[i];
        // search from G_TBL to get the corresponding node id for each request
        auto granule_id = GetGranuleID(req.key);
        uint64_t home_node;
        if (granule_node_map.find(granule_id) == granule_node_map.end()) {
            // home_node = granule_tbl_cache->get(granule_id);
            granule_tbl_cache->get(granule_id, home_node);
            granule_node_map[granule_id] = home_node;
        } else {
            home_node = granule_node_map[granule_id];
        }
        if (node_requests_map.find(home_node) == node_requests_map.end()) {
            // node_requests_map.insert(pair<uint64_t, vector<YCSBRequest *>>(
            //     home_node, vector<YCSBRequest *>()));
            node_requests_map[home_node] = vector<YCSBRequest *>();
        }
        node_requests_map[home_node].push_back(&req);
    }


    granule_node_map.clear();
    auto node_req_endtime_chrono = std::chrono::high_resolution_clock::now(); 
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(node_req_endtime_chrono - starttime_chrono).count();
    LOG_INFO("XXX albatross finish GenReqs for txn %d with %d ms", txn_id_, duration);
  

    // collect data from remote
    rc = send_remote_package(&node_requests_map, query->tbl_id);
    // LOG_INFO("finish execution for txn %d", txn_id_)
    auto read_rpc_endtime_chrono = std::chrono::high_resolution_clock::now(); 
      duration = std::chrono::duration_cast<std::chrono::milliseconds>(read_rpc_endtime_chrono - node_req_endtime_chrono).count();
      LOG_INFO("XXX albatross finish SendReadReq for txn %d with %d ms", txn_id_, duration);
    if (rc == ABORT || rc == FAIL) return rc;

    // collect data locally
    // TODO(Hippo): this is a hack; change to distributed calculation and commit
    // execute locally (including the updates for remote data,
    // remote data update will be sent back through prepare stage in 2PC)
    // M_ASSERT(false, "figure out to execute and record updates for remote")
    // if (node_requests_map.find(g_node_id) != node_requests_map.end()) {
    //     M_ASSERT(false, "shouldn't reach this branch");
    //     auto cur_node_reqs = node_requests_map[g_node_id];
    //     unordered_set<uint64_t> granule_set;
    //     bool has_wrong_member = false;
    //     std::tuple<uint64_t, uint64_t> memCorrection;
    //     for (auto it = cur_node_reqs.begin(); it != cur_node_reqs.end(); it++) {
    //         auto req = *it;
    //         // check whether the key's granule exist on this node and acquire
    //         // lock for the granule search from G_TBL to get the corresponding
    //         // node id for each request
    //         char *g_data;
    //         size_t g_size;
    //         auto g_table = db_->GetGranuleTable();
    //         auto granule_id = GetGranuleID(req->key);
    //         if (granule_set.find(granule_id) == granule_set.end()) {
    //             granule_set.insert(granule_id);
    //             rc = GetTuple(g_table, 0, SearchKey(granule_id),
    //                           AccessType::READ, g_data, g_size);
    //             if (rc == ABORT) {
    //                 // LOG_INFO("Abort for key %s", to_string(req.key).c_str());
    //                 break;
    //             }
    //             auto g_schema = g_table->GetSchema();
    //             auto val = &(g_data[g_schema->GetFieldOffset(1)]);
    //             uint64_t home_node = *((uint64_t *)val);
    //             if (home_node != g_node_id) {
    //                 // TODO(Hippo): remove this error assert and add correct
    //                 // response_type for non-global sys txn impl
    //                 memCorrection = make_tuple(granule_id, home_node);
    //                 has_wrong_member = true;
    //                 rc = ABORT;
    //                 break;
    //             }
    //         }

    //         char *data;
    //         size_t size;
    //         auto table = db_->GetTable(query->tbl_id);
    //         // TODO(Hippo): put correct idx_id here, if idx_id = 0, default is
    //         // searching from primary index
    //         rc = GetTuple(table, 0, SearchKey(req->key), req->ac_type, data,
    //                       size);

    //         if (rc == ABORT) {
    //             // LOG_INFO("Abort for key %d in txnid %d ", req->key, txn_id_);
    //             break;
    //         }
    //     }
    //     // TODO(Hippo): move it out from critical path
    //     if (has_wrong_member) {
    //         granule_tbl_cache->insert(get<0>(memCorrection),
    //                                   get<1>(memCorrection));
    //         LOG_INFO(
    //                 "Update local membership cache with (granule id %d, node "
    //                 "id %d)",
    //                 get<0>(memCorrection), get<1>(memCorrection));
    //     }
    // }
    
    remote_rc = ReceiveRemoteResponse();
    response_rpc_endtime_chrono = std::chrono::high_resolution_clock::now(); 
    duration = std::chrono::duration_cast<std::chrono::milliseconds>(response_rpc_endtime_chrono - read_rpc_endtime_chrono).count();
    std::ostringstream remain_req;
        for (auto &item: node_requests_map) {
            remain_req << "[node id: " << item.first << ", ";
            for (auto req: item.second) {
                remain_req <<" g" << GetGranuleID(req->key)<< " "; 
            }
            remain_req << " ]\n";
        }
    LOG_INFO("XXX albatross finish ReadReqResponse for txn %d with %d ms for (%s)", txn_id_, duration, remain_req.str());

    rc = (rc == ABORT || rc == FAIL || rc == GTBL_UP) ? rc: remote_rc;
    // LOG_DEBUG("XXX finish execution phase for txnid %d with status %s", txn_id_, (rc == ABORT)? "ABORT": "COMMIT");
    if (rc == ABORT || rc == FAIL || rc==GTBL_UP) return rc;

    // execute transactions and update locally
    for (size_t i = 0; i < query->req_cnt; i++) {
        YCSBRequest &req = query->requests[i];
        if (req.ac_type == UPDATE) {
            auto table = db_->GetTable(query->tbl_id);
            auto schema = table->GetSchema();
            auto write_cp = get_data_from_buffer(req.key, query->tbl_id);
            for (size_t cid = 1; cid < schema->GetColumnCnt(); cid++) {
                schema->SetNumericField(cid, write_cp->data_, GetTxnId());
            }
        }
    }
    auto exec_endtime_chrono = std::chrono::high_resolution_clock::now(); 
    duration = std::chrono::duration_cast<std::chrono::milliseconds>(exec_endtime_chrono - response_rpc_endtime_chrono).count();
    LOG_INFO("XXX albatross finish loca exec for txn %d with %d ms", txn_id_, duration);

    return rc;
}

RC ITxn::GetTuple(ITable *p_table, OID idx_id, SearchKey key, AccessType ac_type,
                  char *&data, size_t &sz, LockType lock_type, bool delta_access) {
  // this method only get 1 tuple
  int limit = 1;
  auto starttime = GetSystemClock();
  auto tuples = p_table->IndexSearch(key, idx_id, limit);
  ITuple * tuple;
  if (g_buf_type == OBJBUF) {
    tuple = arboretum::ObjectBufferManager::AccessTuple(tuples[0]);
  } else if (g_buf_type == NOBUF) {
    tuple = reinterpret_cast<ITuple *>(tuples[0].Get());
  } else {
    LOG_ERROR("not impl yet.");
  }
  auto endtime = GetSystemClock();
  if (g_warmup_finished) {
      Worker::dbstats_.int_stats_.idx_time_ns_ += endtime - starttime;
  }

  // if (p_table->IsGranuleTable() && std::find(ScaleoutWorkload::GranulesToMigrate.begin(), ScaleoutWorkload::GranulesToMigrate.end(), key.ToUInt64()) != ScaleoutWorkload::GranulesToMigrate.end()) {
  //     LOG_DEBUG("XXX Waiting to acquire lock for key (%s) with type %s for table %s in txnid %d with lock type %s", key.ToString().c_str() , ac_type == UPDATE ? "update": "read", p_table->GetTableName().c_str(), txn_id_, (lock_type == LockType::WAIT_DIE)?"wait-die":"no-wait");
  //     // LOG_DEBUG("XXX Waiting to acquire lock for key (%s) with type %s for table %s in txnid %d with lock type %s, thread id is %s", key.ToString().c_str() , ac_type == UPDATE ? "update": "read", p_table->GetTableName().c_str(), txn_id_, ((lock_type == LockType::WAIT_DIE)?"wait-die":"no-wait").c_str(), std::this_thread::get_id().c_str());
  // }

  std::atomic<uint32_t>& lock = p_table->GetLockState(tuple->GetTID());
  RC rc = CCManager::AcquireLock(lock, ac_type, lock_type);
  // Worker::stats_.int_stats_.cc_ns_ += GetSystemClock() - endtime;
  if (rc == ABORT) {
    // tuple is not added to access, must free here
    // if (p_table->IsGranuleTable()) {
    // LOG_DEBUG("Abort: fail to get lock for key (%s) in Granule %d with type %s for table %s in txnid %d", key.ToString().c_str(), GetGranuleID(key.ToUInt64()) , ac_type == UPDATE ? "update": "read", p_table->GetTableName().c_str(), txn_id_);
    // }
    FinishAccess(tuples[0], false);
    return rc;
  } else {
    // if (p_table->IsGranuleTable() && ac_type == UPDATE) {
    // if (p_table->IsGranuleTable() && std::find(ScaleoutWorkload::GranulesToMigrate.begin(), ScaleoutWorkload::GranulesToMigrate.end(), key.ToUInt64()) != ScaleoutWorkload::GranulesToMigrate.end()) {
      // LOG_DEBUG("XXX Acquire lock for key (%s) with type %s for table %s in txnid %d with lock type %s", key.ToString().c_str() , ac_type == UPDATE ? "update": "read", p_table->GetTableName().c_str(), txn_id_, (lock_type == LockType::WAIT_DIE)?"wait-die":"no-wait");
    //   // LOG_DEBUG("XXX Acquire lock for key (%s) with type %s for table %s in txnid %d with lock type %s, thread id is %s", key.ToString().c_str() , ac_type == UPDATE ? "update": "read", p_table->GetTableName().c_str(), txn_id_, (lock_type == LockType::WAIT_DIE)?"wait-die":"no-wait", std::this_thread::get_id().c_str());
    // }
  }
  Access ac = {ac_type, key, p_table, tuples, limit, lock_type!=LockType::NO_LOCK};
  sz = p_table->GetTupleDataSize();

  if (ac_type == UPDATE) {
    // since limit = 1, write copy here is a single copy. for cases with more than
    // one rows sharing same key, here should call new for each of them
    auto write_copy = new WriteCopy(sz);
    write_copy->Copy(tuple->GetData());
    ac.data_ = write_copy;
    data = write_copy->data_;
  } else {
    data = tuple->GetData();
  }
  if (!delta_access) {
    tuple_accesses_.push_back(ac);
  } else {
    preheat_delta_accesses_.push_back(ac); 
  }
  return rc;
}

RC ITxn::GetTupleOrInsert(ITable *p_table, OID idx_id, SearchKey key, AccessType ac_type,
                  char *&data, size_t &sz, LockType lock_type, char * preload_tuple) {
  M_ASSERT(g_buf_type == OBJBUF && ac_type == READ , "only support OBJBUF");
  // this method only get 1 tuple
  int limit = 1;
  auto starttime = GetSystemClock();
  // auto tuples = p_table->IndexSearch(key, idx_id, limit);
  auto tuples = p_table->IndexSearchOrFill(key, idx_id, limit, preload_tuple);
  ITuple * tuple;
  tuple = arboretum::ObjectBufferManager::AccessTuple(tuples[0]);
  auto endtime = GetSystemClock();
  if (g_warmup_finished) {
      Worker::dbstats_.int_stats_.idx_time_ns_ += endtime - starttime;
  }

  // std::atomic<uint32_t>& lock = p_table->GetLockState(tuple->GetTID());
  // RC rc = CCManager::AcquireLock(lock, ac_type, lock_type);
  // // Worker::stats_.int_stats_.cc_ns_ += GetSystemClock() - endtime;
  // if (rc == ABORT) {
  //   FinishAccess(tuples[0], false);
  //   return rc;
  // } else {
  // }
  Access ac = {ac_type, key, p_table, tuples, limit, false};
  sz = p_table->GetTupleDataSize();
  data = tuple->GetData();
  tuple_accesses_.push_back(ac);
  return OK;
}



RC ITxn::Commit(RC rc) {
  // 2 PC
  if (rc == OK) {
    // _prepare_start_time = get_sys_clock();
    rc = CommitP1();
    // LOG_DEBUG("XXX finish commit p1 for txnid %d with status %s", txn_id_, (rc == ABORT)? "ABORT": "COMMIT");
  }

  if (rc != FAIL) {
    // _commit_start_time = get_sys_clock();
    rc = CommitP2(rc);
    // LOG_DEBUG("XXX finish commit p2 for txnid %d with status %s", txn_id_, (rc == ABORT)? "ABORT": "COMMIT");
  }
  return rc;
}

RC ITxn::Abort() {
  cleanup(ABORT);
  return OK;
}

void ITxn::InsertTuple(ITable *&p_table, char *&data, size_t sz) {
  M_ASSERT(false, "Implement insert tuple");
}

 WriteCopy * ITxn::get_data_from_buffer(uint64_t key, uint32_t table_id) {
  for (auto & it : tuple_accesses_) {
      if (it.key_.ToUInt64() == key && it.GetTableId() == table_id) {
        return it.data_;
      }
  }
  for (auto &it : remote_tuple_accesses_) {
      if (it.key_.ToUInt64() == key && it.GetTableId() == table_id) {
        return it.data_;
      }
  }
  // data not found

  string access_str = "";
  for (auto & it : tuple_accesses_) {
       access_str = access_str + it.key_.ToString() + ",";
  }
  string remote_access_str="";
   for (auto &it : remote_tuple_accesses_) {
      remote_access_str = remote_access_str + it.key_.ToString() + ",";
  }
  cout << "tuple accesses keys: [" << access_str <<" ]"<< std::endl; 
  cout << "remote tuple accesses keys: [" << remote_access_str << " ]" << std::endl; 
  M_ASSERT(false, "data not found in txn local buffer for target key: %lu", key);
  return nullptr;
}




size_t ITxn::GenDataForP1Log(string& data_str) {
   return GenDataAndGranulesForP1Log(data_str, nullptr);
}

size_t ITxn::GenDataAndGranulesForP1Log(string& data_str, unordered_set<uint64_t> * granules) {
    // serialize log data
  vector<char> buffer;
  uint16_t wr_num = 0;
   for (auto access : tuple_accesses_) {
    for (int i = 0; i < access.num_rows_; i++) {
      // auto tuple = reinterpret_cast<ITuple *>(access.rows_[i].Get());
      //    auto tuple = new (MemoryAllocator::Alloc(GetTotalTupleSize()))
      //     ITuple(tbl_id_, tid);
      // tuple->SetData(data, sz);
      if (access.ac_ ==  AccessType::UPDATE) {
        auto origin_tuple = reinterpret_cast<ITuple *>(access.rows_[i].Get());
        auto tbl_id = origin_tuple->GetTableId(); 
        auto table = db_->GetTable(tbl_id); 
        auto schema = table->GetSchema();
        auto tuple = new (MemoryAllocator::Alloc(table->GetTotalTupleSize()))ITuple(tbl_id, origin_tuple->GetTID());
      //   auto table = access.tbl_;
      //   auto schema = access.tbl_->GetSchema();
      //   auto tuple = new (MemoryAllocator::Alloc(table->GetTotalTupleSize()))ITuple(access.tbl_->GetTableId(), origin_tuple->GetTID());
        tuple->SetData(access.data_[i].data_, access.data_[i].sz_);
        auto tuple_size = table->GetTotalTupleSize();
        // auto flush_key = tuple->GetPrimaryStorageKey(table->GetSchema());
        SearchKey search_key;
        tuple->GetPrimaryKey(schema, search_key);
        auto key = search_key.ToUInt64();
        if (granules) {
            auto granule_id = GetGranuleID(key);
            granules->insert(granule_id);
        }
        // append key
        buffer.insert(buffer.end(), CHAR_PTR_CAST(&key), CHAR_PTR_CAST(&key) + sizeof(uint64_t));
        // append value
        // char *value = tuple->GetData();
        // uint64_t v_size = access.tbl_->GetTotalTupleSize();
        // uint32_t value_size = v_size;
        // buffer.insert(buffer.end(),CHAR_PTR_CAST(&value_size), CHAR_PTR_CAST(&value_size) + sizeof(value_size));

        //TODO: only append tuple data rather than entire tuple
        buffer.insert(buffer.end(),CHAR_PTR_CAST(&tuple_size), CHAR_PTR_CAST(&tuple_size) + sizeof(tuple_size));
        char * tuple_data =  reinterpret_cast<char *>(tuple);
        buffer.insert(buffer.end(), tuple_data, tuple_data + tuple_size);
        DEALLOC(tuple);
        wr_num++;
      }
    }
  }
  // log data for prepare phase
  buffer.insert(buffer.begin(), CHAR_PTR_CAST(&wr_num), CHAR_PTR_CAST(&wr_num) + sizeof(wr_num));
  size_t size = buffer.size();
  string data(buffer.begin(), buffer.end());
  data_str = std::move(data);
  return size;
}


RC ITxn::Log(string &data, uint8_t status_new, string &lsn) {
  Log(data, status_new,lsn, nullptr);
}

RC ITxn::Log(string &data, uint8_t status_new, string &lsn, unordered_set<uint64_t> * granules){
  RC st = OK;
   // log data for prepare phase
  LogEntry * log_entry = new LogEntry(data, status_new, GetTxnId());
  if (granules) {
    log_entry->SetGranules(granules);
  }
  // string entry_str;
  // log_entry->to_string(entry_str);
  // LOG_INFO("XXX: log_entry is %s", entry_str.c_str());
  // LogEntry::SerializeLogData(GetTxnId(),TxnManager::State::COMMITTED, data_str, &log_entry);
  auto starttime = GetSystemClock();
  // g_data_store->Log(fake_node_id, log_entry);
  g_log_store->Log(g_node_id, log_entry);
  //TODO(Hippo): Optimize the phase to obtain lsn
  log_entry->GetLSN(lsn);
  if (!g_replay_from_buffer_enable) {
      delete log_entry;
  }
  
  if (g_warmup_finished) {
    auto endtime = GetSystemClock();
    Worker::dbstats_.int_stats_.log_time_ns_ += endtime - starttime;
    Worker::dbstats_.int_stats_.log_size_ += data.size();
    Worker::dbstats_.int_stats_.log_count_ += 1;
  }
  return st;
}

 RC ITxn::LogForP1(string& lsn) {
  string data;
  unordered_set<uint64_t> granules_set;
  auto log_size = GenDataAndGranulesForP1Log(data, &granules_set);
  return Log(data, TxnManager::State::PREPARED, lsn, &granules_set);
  // LOG_INFO("XXX: PREPARE log size is %d, log data str size is %d", log_size, data.length());
 }

 RC ITxn::LogForP2(RC rc, string& lsn) {
    string data="";
    return Log(data, (rc == COMMIT)? TxnManager::State::COMMITTED: TxnManager::State::ABORTED, lsn);
 }

bool ITxn::IsMigrTxn() {
  return is_migr_txn_;
}


 RC ITxn::CommitP1() {
        // Start Two-Phase Commit
    decision_ = COMMIT;
    SundialRequest::NodeData * participant;
    auto thd_id = arboretum::Worker::GetThdId();
    // send prepare request to participants
    for (auto it = _remote_nodes_involved.begin();
         it != _remote_nodes_involved.end(); it++) {
      // if any failed or aborted, txn must abort, cannot enter this function
      // assert(it->second->state == RUNNING);

      uint64_t node_id = it->first;
      SundialRequest &request = it->second->request;
      SundialResponse &response = it->second->response;
      request.Clear();
      response.Clear();
      request.set_txn_id(GetTxnId());
      request.set_request_type(SundialRequest::PREPARE_REQ);
      // request.set_node_id(it->first);
      request.set_node_id(g_node_id);
      request.set_coord_id(g_node_id);
      request.set_thd_id(thd_id);

      // attach coordinator
      participant = request.add_nodes();
      participant->set_nid(g_node_id);
      // attach participants
      // TODO(Hippo): differentiate read-only case
      // if (!is_txn_read_only()) {
      for (auto itr = _remote_nodes_involved.begin();
           itr != _remote_nodes_involved.end(); itr++) {
        if (itr->second->is_readonly) continue;
        participant = request.add_nodes();
        participant->set_nid(it->first);
      }
      // }

      // set data for the requests
      for (auto access : remote_tuple_accesses_) {
        if (access.node_id_ == node_id && access.ac_ == AccessType::UPDATE) {
            SundialRequest::TupleData *tuple = request.add_tuple_data();
            uint64_t tuple_size = access.GetDataSize();
            tuple->set_key(access.GetKeyUInt64());
            tuple->set_table_id(access.GetTableId());
            tuple->set_size(tuple_size);
            tuple->set_data(access.GetData(), tuple_size);
        }
      }

      rpc_semaphore_->incr();
      rpc_client->sendRequestAsync(this, it->first, request, response);
    }

    // // profile: # prepare phase
    // INC_INT_STATS(num_prepare, 1);

    //TODO(Hippo): support read-only shortcut for log
    // log for prepare phase(vote and transaction data)

    string lsn;
    if (g_node_id != g_client_node_id) {
       LogForP1(lsn);
    }

    // wait for vote
    rpc_semaphore_->wait();
    //TODO(Hippo): check _txn_state is necessary
    // _txn_state = PREPARED;
    return decision_;

 }

 RC ITxn::CommitP2(RC rc) {
  // TODO(Hippo): support remote_readonly optimization
    //   bool remote_readonly = is_read_only() && (rc == COMMIT);
    // if (remote_readonly && CC_ALG != OCC) {
    //     for (auto it = _remote_nodes_involved.begin();
    //          it != _remote_nodes_involved.end(); it++) {
    //         if (!(it->second->is_readonly)) {
    //             remote_readonly = false;
    //             break;
    //         }
    //     }
    // }
    // if (remote_readonly) { // no logging and remote message at all
    //     _finish_time = get_sys_clock();
    //     _cc_manager->cleanup(rc);
    //     _txn_state = (rc == COMMIT)? COMMITTED : ABORTED;
    //     return rc;
    // }

  // TODO(Hippo): enable log

    // rpc_log_semaphore->incr();
    // // 2pc: persistent decision
    // #if LOG_DEVICE == LOG_DVC_REDIS
    // redis_client->log_async(g_node_id, get_txn_id(), rc_to_state(rc));
    // rpc_log_semaphore->wait();
    // sendRemoteLogRequest(rc_to_state(rc), 1, g_node_id, SundialRequest::RESP_OK);
    // #endif
    // // finish after log is stable.
    // rpc_log_semaphore->wait();

  //TODO(Hippo): support time stats
    // // _finish_time = get_sys_clock();

    auto thd_id = arboretum::Worker::GetThdId();
    for (auto it = _remote_nodes_involved.begin(); it != _remote_nodes_involved.end(); it ++) {
        // No need to run this phase if the remote sub-txn has already committed
        // or aborted.
        if (it->second->state == TxnManager::State::ABORTED || it->second->state == TxnManager::State::COMMITTED) {
            // LOG_INFO("XXX don't need to send process decision to node %d with statue %s in txn %d", it->first, (it->second->state == TxnManager::State::ABORTED)? "ABORT": "COMMITTED",txn_id_);
            continue;
        }

        SundialRequest &request = it->second->request;
        SundialResponse &response = it->second->response;
        request.Clear();
        response.Clear();
        request.set_txn_id(GetTxnId());
        // request.set_node_id( it->first);
        request.set_node_id(g_node_id);
        request.set_coord_id(g_node_id);
        request.set_thd_id(thd_id);
        SundialRequest::RequestType type = (rc == COMMIT)?
            SundialRequest::COMMIT_REQ : SundialRequest::ABORT_REQ;
        request.set_request_type( type );
        rpc_semaphore_->incr();
        rpc_client->sendRequestAsync(this, it->first, request, response);
    }

    string lsn;
    if (g_node_id != g_client_node_id) {
      LogForP2(rc, lsn);
    }
    // LOG_INFO("XXX local process decision for txn %d with status %s", txn_id_, (rc == ABORT)? "ABORT": "COMMIT");

    cleanup(rc, lsn);
    rpc_semaphore_->wait();
    // _txn_state = (rc == COMMIT)? COMMITTED : ABORTED;
    return rc;
 }

 void ITxn::cleanup(RC rc) {
  assert(rc == ABORT);
  string fakelsn ="0";
  cleanup(rc, fakelsn);
 }

 void ITxn::cleanup(RC rc, string &lsn) {
    assert(rc == COMMIT || rc == ABORT);

    unordered_map<uint64_t, string> granule_tracker;
    for (auto access : tuple_accesses_) {
       if (rc == COMMIT && access.ac_ == UPDATE && (!access.tbl_->IsSysTable())) {
          granule_tracker[GetGranuleID(access.GetKeyUInt64())] = lsn;
       }
    }
    if (!granule_tracker.empty()) {
          granule_commit_tracker->InsertBatch(&granule_tracker);
   // //   cout << "granule_commit_tracker is "<<granule_commit_tracker->PrintStr() << endl;
    }



    // apply updates from txn local buffer to real data, release locks and clear
    // txn local buffer
    for (auto access : tuple_accesses_) {
        if (!access.is_migrated_) {
          for (int i = 0; i < access.num_rows_; i++) {
              auto tuple = reinterpret_cast<ITuple *>(access.rows_[i].Get());
              if (rc == COMMIT && access.ac_ == UPDATE) {
                tuple->SetData(access.data_[i].data_, access.data_[i].sz_);
                tuple->SetLSN(lsn);
              }
              auto lock_id = tuple->GetTID();
              bool dirty = (g_replay_enable)
                               ? false
                               : (rc == COMMIT) && (access.ac_ == UPDATE) &&
                                     !g_force_write;
              // for now, always flush (line: 60) before unpin.
              if (!access.is_migrated_) {
                 FinishAccess(access.rows_[i], dirty);  // unpin the data
              }
              if (access.need_release_lock) {
                CCManager::ReleaseLock(access.tbl_->GetLockState(lock_id),
                                     access.ac_);
                // LOG_INFO("XXX Release lock for key (%d) with type %s in txnid %d", access.key_.ToUInt64(), access.ac_ == UPDATE ? "update": "read", txn_id_);
              }

              // if (db_->GetTable(tuple->GetTableId())->IsGranuleTable() && access.ac_ == UPDATE) {
              // if (db_->GetTable(tuple->GetTableId())->IsGranuleTable() && std::find(ScaleoutWorkload::GranulesToMigrate.begin(), ScaleoutWorkload::GranulesToMigrate.end(), access.GetKeyUInt64()) != ScaleoutWorkload::GranulesToMigrate.end()) {
              //     LOG_INFO("XXX release lock for key (%lu) with type %s for table %s in txn id %d", access.GetKeyUInt64(), access.ac_ == UPDATE ? "update": "read", db_->GetTable(tuple->GetTableId())->GetTableName().c_str(), txn_id_); 
              // }
              delete &access.data_[i];
          }
        }
        // do not use delete since Free is already called in line 69
        DEALLOC(access.rows_);
    }
    for (auto access : remote_tuple_accesses_) {
        for (int i = 0; i < access.num_rows_; i++) {
            delete &access.data_[i];
        }
        DEALLOC(access.rows_);
    }
    tuple_accesses_.clear();
    remote_tuple_accesses_.clear();
 }

void ITxn::FinishAccess(SharedPtr &ptr, bool dirty) {
  if (g_buf_type == OBJBUF) {
    arboretum::ObjectBufferManager::FinishAccessTuple(ptr, dirty);
  } else if (g_buf_type == NOBUF) {
    ptr.Free();
  } else {
    LOG_ERROR("not impl yet.");
  }
}

bool ITxn::ContainsGranule(GID granuleid) {
  if (granule_involved_.find(granuleid) != granule_involved_.end()) {
    return true;
  } else {
    return false;
  }
}


void ITxn::MergeTxn(ITxn * txn) {
  for (auto access: *(txn->GetAccess())) {
    tuple_accesses_.push_back(access);
  }
}


void ITxn::Serialize(vector<uint8_t> * buffer, OID granule_id) {
    buffer->insert(buffer->end(), CHAR_PTR_CAST(&txn_id_), CHAR_PTR_CAST(&txn_id_) + sizeof(txn_id_));
    vector<uint8_t> tmp_buffer;
    uint8_t related_txn = ContainsGranule(granule_id)? 1:0;
    tmp_buffer.insert(tmp_buffer.end(), CHAR_PTR_CAST(&related_txn), CHAR_PTR_CAST(&related_txn) + sizeof(related_txn));

    uint32_t access_num = 0;
    for (auto& access: tuple_accesses_) {
      if (!access.is_migrated_) {
         access.Serialize(&tmp_buffer);
         access_num++;
      }
    }
    tmp_buffer.insert(tmp_buffer.begin(), CHAR_PTR_CAST(&access_num), CHAR_PTR_CAST(&access_num) + sizeof(access_num));
    buffer->insert(buffer->end(), tmp_buffer.begin(), tmp_buffer.end());

}


 const uint8_t * ITxn::Deserialize(const uint8_t * buffer) {
    const uint8_t *offset = buffer;
    std::memcpy(&txn_id_, offset, sizeof(txn_id_));
    offset = offset + sizeof(txn_id_);
    uint32_t access_num;
    std::memcpy(&access_num, offset, sizeof(access_num));
    offset = offset + sizeof(access_num);
    uint8_t related_txn; 
    std::memcpy(&related_txn, offset, sizeof(related_txn));
    offset = offset + sizeof(related_txn);
    if (related_txn == 1) {
      is_alb_related_txn_ = true;
    } else {
      M_ASSERT(related_txn==0, "unexpected related_txn value %d", related_txn);
      is_alb_related_txn_ = false;
    }
    for (int i = 0; i < access_num; i++) {
      Access ac;
      offset = ac.Deserialize(offset, db_);
      tuple_accesses_.push_back(ac);
    }
    return offset;
}



}