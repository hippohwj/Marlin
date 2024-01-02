#include "MigrationTxn.h"
#include "remote/ILogStore.h"
#include "transport/rpc_client.h"
#include "common/GlobalData.h"
#include "buffer/ObjectBufferManager.h"
#include "IDataStore.h"
#include "common/Worker.h"
#include "CCManager.h"

namespace arboretum {
RC MigrationTxn::Preheat(YCSBQuery *query) {
        RC rc = OK;
        auto thd_id = arboretum::Worker::GetThdId();

        if ((g_migr_preheat_enable || g_migr_hybridheat_enable) && !isRetry_) {
        // do cache preheat
        //TODO: extend to support multiple queries/granules per migration
        M_ASSERT(query->req_cnt == 1, "only support one query per migration txn for now");
        SundialResponse response;
        // auto start = GetSystemClock();
        for (uint32_t i = 0; i < query->req_cnt; i++) {
            YCSBRequest &req = query->requests[i];
            uint32_t src_node = (req.src_node == UINT64_MAX)? GetDataStoreNodeByGranuleID(req.key): req.src_node;
            uint32_t dest_node = req.value;
            uint32_t granule_id = req.key;
            SundialRequest request;
            request.set_txn_id(txn_id_);
            request.set_thd_id(thd_id);
            request.set_node_id(g_node_id);
            request.set_request_type(SundialRequest::CACHE_PREHEAT_REQ);
            SundialRequest::CachePreheatRequest *preheat_request = request.add_cache_preheat_reqs();
            preheat_request->set_granule_id(granule_id);
            rpc_semaphore_->incr(); 
            rpc_client->sendRequestAsync(this, src_node, request, response);
        }
        rpc_semaphore_->wait();
        if (response.response_type() ==  SundialResponse::ABORT_REQ) {
           M_ASSERT(false, "Unexpected abort");
        } else if (response.response_type() != SundialResponse::ACK) {
           M_ASSERT(false, "Unknown response type");
           return ABORT;
        }

        // auto dur = GetSystemClock() - start;
        // cout<< "XXX preheat req last " << nano_to_s(dur) << " seconds for granule " << query->requests[0].key << endl;

        // scan and fill in cache for target granule
        ITable * user_tbl = db_->GetUserTable();
        g_data_store->granuleScan(user_tbl->GetTableName(), query->requests[0].key, 2048,[&user_tbl](uint64_t key, char * preloaded_data){
            user_tbl->InsertPreheatTuple(preloaded_data);
        });

    }
    return rc;
}

bool MigrationTxn::IsMigrTxn() {
  return true;
}

RC MigrationTxn::Execute(YCSBQuery *query) {
    critical_start = std::chrono::high_resolution_clock::now();
    auto thd_id = arboretum::Worker::GetThdId();
    query_ = query;
    RC rc = OK;
    // group requests by node id
    unordered_map<uint64_t, vector<YCSBRequest *>> node_requests_map;

    if (!g_partial_migr_enable) {
        // global migration txn
        // TODO(Hippo): remove this hack and get the node lists of the cluster
        // from NodeTable(require table lock for NodeTable first)
        vector<uint64_t> nodes = {0, 1, 2, 3};
        for (uint64_t node_id : nodes) {
            node_requests_map.insert(pair<uint64_t, vector<YCSBRequest *>>(
                node_id, vector<YCSBRequest *>()));
        }

        for (uint32_t i = 0; i < query->req_cnt; i++) {
            YCSBRequest &req = query->requests[i];
            for (uint64_t node_id : nodes) {
                node_requests_map[node_id].push_back(&req);
            }
        }
    } else {
        // partial migration txn
        for (uint32_t i = 0; i < query->req_cnt; i++) {
            YCSBRequest &req = query->requests[i];
            
            uint32_t src_node = (req.src_node == UINT64_MAX)? GetDataStoreNodeByGranuleID(req.key): req.src_node;
            uint32_t dest_node = req.value;
            // cout << "XXX: migr txn src node is " << src_node << ", dest node is " << dest_node << endl;
            if (node_requests_map.find(src_node) == node_requests_map.end()) {
                vector<YCSBRequest *> reqs;
                reqs.push_back(&req);
                node_requests_map.insert(
                    pair<uint64_t, vector<YCSBRequest *>>(src_node, reqs));
            } else {
                node_requests_map[src_node].push_back(&req);
            }
            if (node_requests_map.find(dest_node) == node_requests_map.end()) {
                vector<YCSBRequest *> reqs;
                reqs.push_back(&req);
                node_requests_map.insert(
                    pair<uint64_t, vector<YCSBRequest *>>(dest_node, reqs));
            } else {
                node_requests_map[dest_node].push_back(&req);
            }
        }
    }

    // auto start = GetSystemClock();

    // acquire locks on each participant
    rc = send_remote_package(&node_requests_map, query->tbl_id);
    if (rc == ABORT || rc == FAIL) return rc;

    // collect data locally
    // TODO(Hippo): this is a hack; change to distributed calculation and commit
    // execute locally (including the updates for remote data,
    // remote data update will be sent back through prepare stage in 2PC)
    // M_ASSERT(false, "figure out to execute and record updates for remote")
    if (node_requests_map.find(g_node_id) != node_requests_map.end()) {
        auto cur_node_reqs = node_requests_map[g_node_id];
        for (auto it = cur_node_reqs.begin(); it != cur_node_reqs.end(); it++) {
            char *data;
            auto req = *it;
            size_t size;
            auto table = db_->GetTable(query->tbl_id);
            // TODO(Hippo): put correct idx_id here, if idx_id = 0, default is
            // searching from primary index
            rc = GetTuple(table, 0, SearchKey(req->key), req->ac_type, data,
                          size, req->lock_type);
            if (rc == ABORT) {
                // LOG_INFO("Abort for key %s", to_string(req.key).c_str());
                break;
            }
        }
    }

    RC remote_rc = ReceiveRemoteResponse(); 
    // auto dur = GetSystemClock() - start;
    // cout<< "XXX migr txn wait for read req response takes " << nano_to_s(dur) << " seconds for granule " << query->requests[0].key << endl;


    rc = (rc == ABORT || rc == FAIL) ? rc: remote_rc;

    if (rc == ABORT || rc == FAIL) return rc;

    // execute transactions and update locally
    for (size_t i = 0; i < query->req_cnt; i++) {
        YCSBRequest &req = query->requests[i];
        auto table = db_->GetTable(query->tbl_id);
        auto schema = table->GetSchema();
        vector<WriteCopy *> write_cps;
        get_data_from_buffer(req.key, query->tbl_id, write_cps);
        M_ASSERT((g_partial_migr_enable)? write_cps.size() == 2 : write_cps.size() == 4, "XXX doesn't find enough writes, current size %d", write_cps.size());
        for (auto write_cp: write_cps) {
            schema->SetNumericField(1, write_cp->data_, req.value);
        }
    }
    return rc;
}

void MigrationTxn::get_data_from_buffer(uint64_t key, uint32_t table_id,
                                        vector<WriteCopy *> &all_data) {
    for (auto &it : tuple_accesses_) {
        if (it.key_.ToUInt64() == key && it.GetTableId() == table_id) {
            all_data.push_back(it.data_);
        }
    }
    for (auto &it : remote_tuple_accesses_) {
        if (it.key_.ToUInt64() == key && it.GetTableId() == table_id) {
            all_data.push_back(it.data_);
        }
    }
    if (all_data.empty()) {
        // data not found
        string access_str = "";
        for (auto &it : tuple_accesses_) {
            access_str = access_str + it.key_.ToString() + ",";
        }
        string remote_access_str = "";
        for (auto &it : remote_tuple_accesses_) {
            remote_access_str = remote_access_str + it.key_.ToString() + ",";
        }
        // LOG_INFO("tuple accesses keys: %s", access_str.c_str());
        // LOG_INFO("remote tuple accesses keys: %s", remote_access_str.c_str());
        std::cout << "tuple accesses keys: [" << access_str <<" ]"<< std::endl; 
        std::cout << "remote tuple accesses keys: [" << remote_access_str << " ]" << std::endl; 
        M_ASSERT(false,
                 "data not found in txn local buffer for target key: %lu", key);
    }
}

RC MigrationTxn::Commit(RC rc) {
  // 2 PC
  if (rc == OK) {
    // _prepare_start_time = get_sys_clock();
    rc = CommitP1();
  }

  if (rc != FAIL) {
    // _commit_start_time = get_sys_clock();
    rc = CommitP2(rc);
  }

  if (rc == COMMIT) {
    // sync membership change to client server
    SundialRequest request;
    SundialResponse response;
    request.set_request_type(SundialRequest::MEM_SYNC_REQ);
    request.set_txn_id(txn_id_);

    for (size_t i = 0; i < query_->req_cnt; i++) {
        YCSBRequest &req = query_->requests[i];
        SundialRequest::MemSyncRequest *sync_req =
            request.add_mem_sync_reqs();
        sync_req->set_key(req.key);
        LOG_INFO("XXX Albatross send membership update rpc to client server for granule %d to node %d", req.key, req.value);
        sync_req->set_value(req.value);
    }
    rpc_semaphore_->incr();
    rpc_client->sendRequestAsync(nullptr, g_client_node_id, request, response);
    rpc_semaphore_->wait();
  }
  return rc;
}

 RC MigrationTxn::CommitP1() {
    // Start Two-Phase Commit
    decision_ = COMMIT;
    SundialRequest::NodeData * participant;
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
      request.set_node_id(it->first);
      request.set_coord_id(g_node_id);
      request.set_thd_id(arboretum::Worker::GetThdId());

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
    string data;
    auto log_size = GenDataForP1Log(data);
    LogForLocalAndSys(data, TxnManager::State::PREPARED, lsn);

    // wait for vote
    rpc_semaphore_->wait();

    //TODO(Hippo): check _txn_state is necessary
    // _txn_state = PREPARED;
    return decision_;

 }


 void MigrationTxn::LockDelta(RC rc) {
  assert(rc == COMMIT);
  if (!cache_preheat_delta_.empty()) {
        auto start = GetSystemClock();
        auto table = db_->GetUserTable();
        auto schema = table->GetSchema();
        auto tuple_size =  schema->GetTupleSz(); 
        M_ASSERT(table != nullptr, "table ptr is null");

        for (uint64_t key : cache_preheat_delta_) {
            char *data;
            size_t size;
            rc = GetTuple(table, 0, SearchKey(key), AccessType::UPDATE, data,
                    size, LockType::NO_WAIT, true);
            M_ASSERT(rc == OK, "unexpected rc for delta lock aquire");
        }
        auto dur = GetSystemClock() - start;
        LOG_INFO("XXX acquire row locks for preheat delta with item num %d taking %.2f us", cache_preheat_delta_.size(), nano_to_us(dur));
  }
 }

void MigrationTxn::ReleaseDeltaLock(RC rc) {

        for (auto access : preheat_delta_accesses_) {
        for (int i = 0; i < access.num_rows_; i++) {
            auto tuple = reinterpret_cast<ITuple *>(access.rows_[i].Get());
            auto lock_id = tuple->GetTID();
            FinishAccess(access.rows_[i], false);  // unpin the data
            if (access.need_release_lock) {
              CCManager::ReleaseLock(access.tbl_->GetLockState(lock_id),
                                   access.ac_);
            }
            delete &access.data_[i];
        }
        // do not use delete since Free is already called in line 69
        DEALLOC(access.rows_);
    }
    preheat_delta_accesses_.clear();
}
 

 void MigrationTxn::CleanupCache(RC rc) {
    //TODO: to support abort as well.
    assert(rc == COMMIT);
     // move delta process to migration txn
    if (!cache_preheat_delta_.empty()) {
        //  cache_preheat_mask->insertBatch(&cache_preheat_delta_);
        // evict dirty/obsolete entry from cache
        // TODO: remove this hack, get tbl from parameter
        // string tbl_name = "M_TBL_300G"; 
        // auto table = db_->GetTableByName(tbl_name);
        auto table = db_->GetUserTable();
        auto schema = table->GetSchema();
        auto tuple_size =  schema->GetTupleSz(); 
        M_ASSERT(table != nullptr, "table ptr is null");
        uint64_t gran_id = 100;

        for (uint64_t key : cache_preheat_delta_) {
            if (gran_id == 100) {
                gran_id = GetGranuleID(key);
                // cout<<"XXX preheat migration delete for granule " << gran_id << endl;
                LOG_INFO("XXX start preheat migration delete for granule (%lu)", gran_id);
            }
            table->Delete(SearchKey(key));
            // // char data[tuple_size];
            // schema->SetPrimaryKey(static_cast<char *>(data), key);
            // // fake data
            // schema->SetNumericField(1, data, key);
            // db_->IndexDelete(table, key, data,
            //                  schema->GetTupleSz());
        }
        LOG_INFO("XXX finish preheat migration delete for granule (%lu) with item num %d", gran_id, cache_preheat_delta_.size());
    }

 }

 RC MigrationTxn::CommitP2(RC rc) {
    for (auto it = _remote_nodes_involved.begin(); it != _remote_nodes_involved.end(); it ++) {
        // No need to run this phase if the remote sub-txn has already committed
        // or aborted.
        if (it->second->state == TxnManager::State::ABORTED || it->second->state == TxnManager::State::COMMITTED)
            continue;
        SundialRequest &request = it->second->request;
        SundialResponse &response = it->second->response;
        request.Clear();
        response.Clear();
        request.set_txn_id(GetTxnId());
        request.set_node_id( it->first);
        request.set_coord_id(g_node_id);
        request.set_thd_id(arboretum::Worker::GetThdId());
        SundialRequest::RequestType type = (rc == COMMIT)?
            SundialRequest::COMMIT_REQ : SundialRequest::ABORT_REQ;
        request.set_request_type( type );
        rpc_semaphore_->incr();
        rpc_client->sendRequestAsync(this, it->first, request, response);
    }

    string lsn;
    string data="";
    LogForLocalAndSys(data, (rc == COMMIT)? TxnManager::State::COMMITTED: TxnManager::State::ABORTED, lsn);
    if (g_migr_lock_early_release_enable) {
        // aquire row locks for delta
        LockDelta(rc);
        // release granule lock
        cleanup(rc, lsn);
        Worker::migr_critical_latency.push_back(
                    std::chrono::duration_cast<std::chrono::milliseconds>(
                        std::chrono::high_resolution_clock::now() - critical_start)
                        .count()*1.0);
        // wait for log replay sync
        rpc_semaphore_->wait();
        // cleanup cache
        CleanupCache(rc);
        // release row locks
        ReleaseDeltaLock(rc);
    } else {
      CleanupCache(rc);
      cleanup(rc, lsn);
      rpc_semaphore_->wait();
      Worker::migr_critical_latency.push_back(
                    std::chrono::duration_cast<std::chrono::milliseconds>(
                        std::chrono::high_resolution_clock::now() - critical_start)
                        .count()*1.0);

    }
    // _txn_state = (rc == COMMIT)? COMMITTED : ABORTED;
    return rc;
 }

 RC MigrationTxn::Postheat() {
    RC rc = OK;
    auto thd_id = arboretum::Worker::GetThdId();

    if (g_migr_hybridheat_enable) {
        // TODO(Hippo): remove the hardcode and get table from parameter
        // string tbl_name = "M_TBL_300G";
        // auto table = db_->GetTableByName(tbl_name);
        auto table = db_->GetUserTable();
        auto schema = table->GetSchema();
        auto tuple_size = schema->GetTupleSz();
        M_ASSERT(table != nullptr, "table ptr is null");
        uint64_t gran_id = 100;

        // fill delta to cache
        for (uint64_t key : cache_preheat_delta_) {
            if (gran_id == 100) {
                gran_id = GetGranuleID(key);
            }
            char *data;
            size_t size;
            // TODO(Hippo): put correct idx_id here, if idx_id = 0, default is
            // searching from primary index
            size_t idx_id = 0;
            auto tuples = table->IndexSearch(SearchKey(key), idx_id, 0);
            M_ASSERT(tuples != nullptr, "search returns null");

            // Should increase the usage count for postheat process
            // ITuple * tuple = nullptr;
            // if (g_buf_type == OBJBUF) {
            //     tuple = arboretum::ObjectBufferManager::AccessTuple(tuples[0]);
            // } else if (g_buf_type == NOBUF) {
            //     tuple = reinterpret_cast<ITuple *>(tuples[0].Get());
            // } else {
            //     LOG_ERROR("not impl yet.");
            // }
            // FinishAccess(access.rows_[i], dirty);  // unpin the data 
        }
        LOG_INFO(
            "XXX finish prostheat of hybrid migration for granule (%lu) with "
            "item num %d",
            gran_id, cache_preheat_delta_.size());
    }
    return rc;
 }

 RC MigrationTxn::LogForLocalAndSys(string &data, uint8_t status_new, string &lsn){
    RC st = OK;
    #if REMOTE_STORAGE_TYPE == REMOTE_STORAGE_REDIS
    // async log data for system log
    log_semaphore_->incr();
    // log data to system log
    LogEntry *log_entry =
        new LogEntry(data, status_new, GetTxnId());
    auto starttime = GetSystemClock();
    g_syslog_store->AsyncLog(g_sys_log_id, log_entry,
                          [this](cpp_redis::reply &reply) {
                            if (reply.is_error() || reply.is_null()) {
                               decision_ = ABORT;
                               log_semaphore_->decr();
                            } else {
                               log_semaphore_->decr();
                            }
                              //TODO(Hippo): properly set up lsn for the entry
                            //   log_entry->GetLSN(lsn);
                          });
    delete log_entry;

    // sync log for local
    Log(data, status_new, lsn);
    // wait for sys log return
    log_semaphore_->wait();
    #elif REMOTE_STORAGE_TYPE == REMOTE_STORAGE_AZURE
    //TODO: support async system log for azure log store
    Log(data, status_new, lsn);
    #endif
    return st;
 }

 void MigrationTxn::setRetry(bool retry) { isRetry_ = retry; }
}