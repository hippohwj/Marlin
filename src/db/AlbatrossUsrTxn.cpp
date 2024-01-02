#include "AlbatrossUsrTxn.h"
#include "remote/ILogStore.h"
#include "transport/rpc_client.h"
#include "common/GlobalData.h"
#include "buffer/ObjectBufferManager.h"
#include "IDataStore.h"
#include "common/Worker.h"
#include "CCManager.h"
#include <sstream>


namespace arboretum {

void AlbatrossUsrTxn::GenNodeReqMap(vector<YCSBRequest *>& requests, unordered_map<uint64_t, vector<YCSBRequest *>>& node_req_map) {
    unordered_map<uint64_t, uint64_t> granule_node_map;
    for (auto req: requests) {
        // YCSBRequest &req = query->requests[i];
        // search from G_TBL to get the corresponding node id for each request
        auto granule_id = GetGranuleID(req->key);
        uint64_t home_node;
        if (granule_node_map.find(granule_id) == granule_node_map.end()) {
            // home_node = granule_tbl_cache->get(granule_id);
            granule_tbl_cache->get(granule_id, home_node);
            granule_node_map[granule_id] = home_node;
        } else {
            home_node = granule_node_map[granule_id];
        }
        if (node_req_map.find(home_node) == node_req_map.end()) {
            // node_requests_map.insert(pair<uint64_t, vector<YCSBRequest *>>(
            //     home_node, vector<YCSBRequest *>()));
            node_req_map[home_node] = vector<YCSBRequest *>();
        }
        node_req_map[home_node].push_back(req);
    }
}

RC AlbatrossUsrTxn::Execute(YCSBQuery *query) {
    query_ = query;
    RC rc = OK;
    RC remote_rc = OK;
    // group requests by node id
    vector<YCSBRequest *> remaining_reqs;
    // auto response_rpc_endtime_chrono = std::chrono::high_resolution_clock::now(); 
    // auto starttime_chrono = std::chrono::high_resolution_clock::now();
    for (uint32_t i = 0; i < query->req_cnt; i++) {
        remaining_reqs.push_back(&(query->requests[i]));
    }
    int retry = 0;
    unordered_map<uint64_t, vector<YCSBRequest *>> node_requests_map;
    while(!remaining_reqs.empty()) {
      if (retry != 0) {
        usleep(100000);
      }
      node_requests_map.clear();
      GenNodeReqMap(remaining_reqs, node_requests_map);
      // if (retry > 0) {
      //   std::ostringstream remain_req;
      //   for (auto &item: node_requests_map) {
      //       remain_req << "[node id: " << item.first << ", ";
      //       for (auto req: item.second) {
      //           remain_req <<" g" << GetGranuleID(req->key)<< " "; 
      //       }
      //       remain_req << " ]\n";
      //   }
      //   cout<< "XXX AlbatrossUsrTxn " << txn_id_ << " execute retry " << retry << " with remains " << remain_req.str() << endl;
      // }
    //   auto node_req_endtime_chrono = std::chrono::high_resolution_clock::now(); 
    //   auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(node_req_endtime_chrono - starttime_chrono).count();
    //   LOG_INFO("XXX albatross finish GenNodeReqMap for txn %d with %d ms", txn_id_, duration);
  
      std::map<uint32_t, RemoteNodeInfo *> remote_rpc;
      rc = SendReadRequest(&node_requests_map, query->tbl_id, remote_rpc);
    //   auto read_rpc_endtime_chrono = std::chrono::high_resolution_clock::now(); 
    //   duration = std::chrono::duration_cast<std::chrono::milliseconds>(read_rpc_endtime_chrono - node_req_endtime_chrono).count();
    //   LOG_INFO("XXX albatross finish SendReadReq for txn %d with %d ms", txn_id_, duration);
      retry++;
      // LOG_INFO("finish execution for txn %d", txn_id_)
      if (rc == ABORT || rc == FAIL) return rc;
      remote_rc = ReceiveReadResponse(remaining_reqs, remote_rpc);
    //   response_rpc_endtime_chrono = std::chrono::high_resolution_clock::now(); 
    //   duration = std::chrono::duration_cast<std::chrono::milliseconds>(response_rpc_endtime_chrono - read_rpc_endtime_chrono).count();
    //   LOG_INFO("XXX albatross finish ReadReqResponse for txn %d with %d ms", txn_id_, duration);

      rc = (rc == ABORT || rc == FAIL || rc == GTBL_UP) ? rc: remote_rc;
      // LOG_DEBUG("XXX finish execution phase for txnid %d with status %s", txn_id_, (rc == ABORT)? "ABORT": "COMMIT");
      for (auto item: remote_rpc) {
        delete item.second;
      }
      if (rc == ABORT || rc == FAIL || rc == GTBL_UP) return rc;
    }
    if (retry >= 2) {
        cout<< "XXX AlbatrossUsrTxn " << txn_id_ << " execute retry " << retry << endl;
    }

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

    //  auto exec_endtime_chrono = std::chrono::high_resolution_clock::now(); 
    //  auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(exec_endtime_chrono - response_rpc_endtime_chrono).count();
    //  LOG_INFO("XXX albatross finish loca exec for txn %d with %d ms", txn_id_, duration);
    return rc;
}


 RC AlbatrossUsrTxn::CommitP1() {
        // Start Two-Phase Commit
    decision_ = COMMIT;
    remote_nodes_.clear();
    auto thd_id = arboretum::Worker::GetThdId();
    vector<YCSBRequest *> remaining_reqs;
        // std::map<uint32_t, RemoteNodeInfo *> _remote_nodes_involved;
    for (uint32_t i = 0; i < query_->req_cnt; i++) {
        remaining_reqs.push_back(&(query_->requests[i]));
    }
    int retry = 0;
    while (!remaining_reqs.empty()) {
      if (retry != 0) {
        usleep(100000);
      }
      unordered_map<uint64_t, vector<YCSBRequest *>> node_requests_map;
      GenNodeReqMap(remaining_reqs, node_requests_map);
      // if (retry > 0) {
      //   std::ostringstream remain_req;
      //   for (auto &item: node_requests_map) {
      //       remain_req << "[node id: " << item.first << ", ";
      //       for (auto req: item.second) {
      //           remain_req <<" g" << GetGranuleID(req->key)<< " "; 
      //       }
      //       remain_req << " ]\n";
      //   }
      //   cout<< "XXX AlbatrossUsrTxn " << txn_id_ << " commit retry " << retry << " with remains " << remain_req.str() << endl;
      // }
      // migr albatross needs retries
      std::map<uint32_t, RemoteNodeInfo *> remote_rpc;
      RC rc = SendPrepareRequest(&node_requests_map, remote_rpc);
      retry++;
      // LOG_INFO("finish execution for txn %d", txn_id_)
      if (rc == ABORT || rc == FAIL) return rc;
      decision_ = ReceivePrepareResponse(node_requests_map, remaining_reqs, remote_rpc);
      for (auto item: remote_rpc) {
        delete item.second;
      }
      if (decision_ == ABORT) {
        // LOG_INFO("XXX Albatross txn %d abort for commit prepare", txn_id_);
        return decision_;
      }
    }

    if (retry >= 2) {
        cout<< "XXX AlbatrossUsrTxn " << txn_id_ << " commit p1 retry " << retry << endl;
    }
    return decision_;

 }


  RC AlbatrossUsrTxn::CommitP2(RC rc) {
    auto thd_id = arboretum::Worker::GetThdId();
    vector<YCSBRequest *> remaining_reqs;
        // std::map<uint32_t, RemoteNodeInfo *> _remote_nodes_involved;
    for (uint32_t i = 0; i < query_->req_cnt; i++) {
        remaining_reqs.push_back(&(query_->requests[i]));
    }
    int retry = 0;
    while (!remaining_reqs.empty()) {
      if (retry != 0) {
        usleep(100000);
      }
      unordered_map<uint64_t, vector<YCSBRequest *>> node_requests_map;
      GenNodeReqMap(remaining_reqs, node_requests_map);
      std::map<uint32_t, RemoteNodeInfo *> remote_rpc;
      RC rc = SendCommitRequest(rc, &node_requests_map, remote_rpc);
      retry++;
      // LOG_INFO("finish execution for txn %d", txn_id_)
      ReceiveCommitResponse(node_requests_map, remaining_reqs, remote_rpc);
      for (auto item: remote_rpc) {
        delete item.second;
      }
    }

    if (retry >= 2) {
        cout<< "XXX AlbatrossUsrTxn " << txn_id_ << " commit p2 retry " << retry << endl;
    }
    return rc;
 }





 

//   RC AlbatrossUsrTxn::CommitP2(RC rc) {
//     auto thd_id = arboretum::Worker::GetThdId();
//     vector<RemoteNodeInfo *> rpcs_tracker;
//     string status = (rc == COMMIT)? "commit":"abort"; 
//     // LOG_INFO("XXX commit nodes num %d, with commit state: %s", remote_nodes_.size(), status.c_str());
//     for (auto node_id: remote_nodes_) {
//         RemoteNodeInfo * rpc = new RemoteNodeInfo; 
//         rpcs_tracker.push_back(rpc); 
//         rpc->request.set_txn_id(GetTxnId());
//         // request.set_node_id( it->first);
//         rpc->request.set_node_id(g_node_id);
//         rpc->request.set_coord_id(g_node_id);
//         rpc->request.set_thd_id(thd_id);
//         SundialRequest::RequestType type = (rc == COMMIT)?
//             SundialRequest::COMMIT_REQ : SundialRequest::ABORT_REQ;
//         rpc->request.set_request_type( type );
//         rpc_semaphore_->incr();
//         rpc_client->sendRequestAsync(this, node_id, rpc->request, rpc->response);
//     }

//     string lsn;
//     // LOG_INFO("XXX local process decision for txn %d with status %s", txn_id_, (rc == ABORT)? "ABORT": "COMMIT");
//     cleanup(rc, lsn);
//     rpc_semaphore_->wait();
//     for (auto item: rpcs_tracker) {
//         delete item;
//     }
//     // _txn_state = (rc == COMMIT)? COMMITTED : ABORTED;
//     return rc;
//  }

//  RC AlbatrossUsrTxn::CommitP2(RC rc) {
//     auto thd_id = arboretum::Worker::GetThdId();
//     string status = (rc == COMMIT)? "commit":"abort"; 
//     // LOG_INFO("XXX commit nodes num %d, with commit state: %s", remote_nodes_.size(), status.c_str());
//     int retry = 0;
//     while (!remote_nodes_.empty()) {
//       if (retry != 0) {
//         usleep(100000);
//       }
//       std::map<uint32_t, RemoteNodeInfo *> remote_rpc;

//       for (auto node_id: remote_nodes_) {
//         RemoteNodeInfo * rpc = new RemoteNodeInfo; 
//         rpc->request.set_txn_id(GetTxnId());
//         // request.set_node_id( it->first);
//         rpc->request.set_node_id(g_node_id);
//         rpc->request.set_coord_id(g_node_id);
//         rpc->request.set_thd_id(thd_id);
//         SundialRequest::RequestType type = (rc == COMMIT)?
//             SundialRequest::COMMIT_REQ : SundialRequest::ABORT_REQ;
//         rpc->request.set_request_type( type );
//         rpc_semaphore_->incr();
//         rpc_client->sendRequestAsync(this, node_id, rpc->request, rpc->response);
//         remote_rpc[node_id] = rpc;
//       }
//       retry++;
//       rpc_semaphore_->wait();
//         // clean up for
//         // store data from remote read reponse
//         for (auto it = remote_rpc.begin();
//              it != remote_rpc.end(); it++) {
//             uint64_t node_id = it->first;
//             SundialResponse &response = it->second->response;
//             if (response.response_type() == SundialResponse::ACK) {
//               remote_nodes_.erase(node_id);
//             } else if (response.response_type() == SundialResponse::SUSPEND) {

//             } else {
//               assert(false);
//             }
//         }
//     }

//     if (retry >= 2) {
//         cout<< "XXX AlbatrossUsrTxn " << txn_id_ << " commit p2 retry " << retry << endl;
//     }
//     return rc;
//  }






}