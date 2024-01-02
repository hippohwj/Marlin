#include "DynamastMigrTxn.h"
#include "remote/ILogStore.h"
#include "transport/rpc_client.h"
#include "common/GlobalData.h"
#include "buffer/ObjectBufferManager.h"
#include "IDataStore.h"
#include "common/Worker.h"
#include "CCManager.h"

namespace arboretum {


bool DynamastMigrTxn::IsMigrTxn() {
  M_ASSERT(false, "figure out how to deal with IsMigrTxn() in Dynamast");
  return false;
}

RC DynamastMigrTxn::Execute(YCSBQuery *query) {
    critical_start = std::chrono::high_resolution_clock::now();
    auto thd_id = arboretum::Worker::GetThdId();
    query_ = query;
    RC rc = OK;
   
    //send migr start mark to client server
    cout<< "XXX dynamast send migr mark" << endl;
    SendMigrMark(true);
    // rpc_semaphore_->wait();
    cout<< "XXX dynamast receive migr mark response" << endl;

    // group requests by node id
    unordered_map<uint64_t, vector<YCSBRequest *>> node_requests_map;

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


RC DynamastMigrTxn::Commit(RC rc) {
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
     //send migr end mark to client server
     SendMigrMark(false);
    //  rpc_semaphore_->wait();

    // TODO(Hippo): remove the hack and retrieve data from
    //  Notify other nodes the completion of the current node.
    // for (uint32_t i = 0; i < g_num_nodes; i++) {
    //         if (i == g_node_id) {
    //             for (size_t j = 0; j < query_->req_cnt; j++) {
    //                 YCSBRequest &req = query_->requests[j];
    //                 granule_tbl_cache->insert(req.key, req.value);
    //             }
    //         } else {
    //             SundialRequest request;
    //             SundialResponse response;
    //             request.set_request_type(SundialRequest::MEM_SYNC_REQ);
    //             for (size_t i = 0; i < query_->req_cnt; i++) {
    //                 YCSBRequest &req = query_->requests[i];
    //                 SundialRequest::MemSyncRequest *sync_req =
    //                     request.add_mem_sync_reqs();
    //                 sync_req->set_key(req.key);
    //                 sync_req->set_value(req.value);
    //             }
    //             rpc_client->sendRequestAsync(nullptr, i, request, response);
    //         }
    // }
  }
  return rc;
}



RC DynamastMigrTxn::SendMigrMark(bool isStart) {
   // send migr mark to client server
//    g_client_node_id
   auto thd_id = arboretum::Worker::GetThdId();
   SundialResponse response;
   SundialRequest request;
   request.set_txn_id(txn_id_);
   request.set_thd_id(thd_id);
   request.set_node_id(g_node_id);
   if (isStart) {
       request.set_request_type(SundialRequest::DYNAMAST_MIGR_ST_REQ);
   } else {
       request.set_request_type(SundialRequest::DYNAMAST_MIGR_END_REQ);
   }
   rpc_semaphore_->incr();
   rpc_client->sendRequestAsync(this, g_client_node_id, request, response);
   rpc_semaphore_->wait();
}




}