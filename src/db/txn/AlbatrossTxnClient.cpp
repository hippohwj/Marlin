#include "db/ITxn.h"
#include "transport/rpc_client.h"
#include "TxnManager.h"
#include "OptionalGlobalData.h"
#include "db/AlbatrossUsrTxn.h"
#include "db/AlbatrossMigrTxn.h"
#include "common/Worker.h"
#include "db/txn/TxnTable.h"

using sundial_rpc::SundialRequest;
using sundial_rpc::SundialResponse;
using sundial_rpc::SundialRPC;

using namespace std;
namespace arboretum
{
    RC AlbatrossUsrTxn::SendReadRequest(unordered_map<uint64_t, vector<YCSBRequest *>> * node_requests_map, OID tbl_id, std::map<uint32_t, RemoteNodeInfo *>& remote_nodes_involved) {
        auto thd_id = arboretum::Worker::GetThdId();
        for (auto it = node_requests_map->begin(); it != node_requests_map->end(); it++)
        {
            uint64_t node_id = it->first;
            if (node_id != g_node_id) {
            if (remote_nodes_involved.find(node_id) == remote_nodes_involved.end())
            {
                remote_nodes_involved[node_id] = new RemoteNodeInfo;
                remote_nodes_involved[node_id]->state = TxnManager::State::RUNNING;
                remote_nodes_involved[node_id]->is_readonly = true;
            }
            SundialRequest &request = remote_nodes_involved[node_id]->request;
            request.set_txn_id(txn_id_);
            // request.set_node_id(node_id);
            request.set_node_id(g_node_id);
            request.set_thd_id(thd_id);
            request.set_tbl_id(tbl_id);
            request.set_coord_id(g_node_id);
            request.set_request_type(SundialRequest::READ_REQ);
            for (auto it2 = it->second.begin(); it2 != it->second.end(); it2++)
            {
                SundialRequest::ReadRequest *read_request = request.add_read_requests();
                read_request->set_key((*it2)->key);
                // read_request->set_index_id((*it2)->index_id);
                // TODO(hippo): remove this hack for index_id; pass in real idx_id
                read_request->set_index_id(0);
                read_request->set_access_type((*it2)->ac_type);
                read_request->set_lock_type((*it2)->lock_type);
                if ((*it2)->ac_type != READ)
                {
                    remote_nodes_involved[node_id]->is_readonly = false;
                    // TODO(hippo): figure out whether it is necessary to set_txn_read_write
                    // set_txn_read_write();
                }
            }
            }
        }

        // send out remote requests asynchronously
        for (auto it = remote_nodes_involved.begin(); it != remote_nodes_involved.end(); it++)
        {
            rpc_semaphore_->incr();
            rpc_client->sendRequestAsync(this, it->first, it->second->request,
                                         it->second->response);
        }
        return OK;
    }

    RC AlbatrossUsrTxn::ReceiveReadResponse(vector<YCSBRequest *> &remaining_reqs, std::map<uint32_t, RemoteNodeInfo *>& remote_nodes_involved) {
        rpc_semaphore_->wait();
        // clean up for
        RC rc = OK;
        // store data from remote read reponse
        for (auto it = remote_nodes_involved.begin();
             it != remote_nodes_involved.end(); it++) {
            uint64_t node_id = it->first;
            // remote_nodes_.push_back(node_id);
            SundialResponse &response = it->second->response;
            if (response.response_type() == SundialResponse::RESP_OK) {
            remote_nodes_.insert(node_id);
            for (int i = 0; i < response.tuple_data_size(); i++) {
                // store remote read locally
                int limit = 1;
                auto tuple = response.tuple_data(i);
                auto key = tuple.key();
                for (auto it = remaining_reqs.begin(); it != remaining_reqs.end(); ) {
                   if ((*it)->key == key) {
                       it = remaining_reqs.erase(it); 
                   } else {
                       ++it;
                   }
                }
                // TODO(Hippo): replace table using real table from response
                // info
                Access access = {(AccessType)tuple.access_type(),
                                 SearchKey(key),
                                 db_->GetTable(tuple.table_id()),
                                 nullptr,
                                 limit,
                                 true,
                                 node_id};
                // Access *access = &(*remote_tuple_accesses_.rbegin());
                M_ASSERT(node_id != g_node_id, "node id %d should be equal to g_node_id %d", node_id, g_node_id);
                // access.home_node_id = node_id;
                access.rows_ = nullptr;
                auto sz = tuple.size();
                auto write_copy = new WriteCopy(sz);
                write_copy->Copy((void *)tuple.data().c_str());
                access.data_ = write_copy;
                remote_tuple_accesses_.push_back(access);
            }

            if (response.albatross_payload_size() > 0) {
                M_ASSERT(false, "unexpected payload branch for albatross");
            }

            if (response.cache_preheat_delta_size() > 0) {
                M_ASSERT(false, "unexpected cache preheat branch for albatross");
            }

            } else if (response.response_type() ==
                       SundialResponse::RESP_ABORT) {
            // _remote_nodes_involved[it->first]->state =
            //     TxnManager::State::ABORTED;
            // if aborted due to the outdated membership, update local
            // membership cache
            rc = ABORT;
            if (response.memb_data_size() > 0) {
                auto update_info = response.memb_data(0);
                granule_tbl_cache->insert(update_info.granid(),
                                          update_info.node_id());
                LOG_INFO(
                    "Update local membership cache with (granule id %d, node "
                    "id %d)",
                    update_info.granid(), update_info.node_id());
                rc = GTBL_UP;
            } 
            } else if (response.response_type() == SundialResponse::SUSPEND) {

            } else {
              assert(false);
            }
        }
        return rc;
    }


    RC AlbatrossUsrTxn::SendPrepareRequest(unordered_map<uint64_t, vector<YCSBRequest *>> * node_requests_map, std::map<uint32_t, RemoteNodeInfo *>& remote_nodes_involved) {
        auto thd_id = arboretum::Worker::GetThdId();
        // send prepare request to participants
        SundialRequest::NodeData * participant;
        for (auto it = node_requests_map->begin();
             it != node_requests_map->end(); it++) {
          uint64_t node_id = it->first;
          if (remote_nodes_involved.find(node_id) == remote_nodes_involved.end()) {
            remote_nodes_involved[node_id] = new RemoteNodeInfo;
            remote_nodes_involved[node_id]->state = TxnManager::State::RUNNING;
            remote_nodes_involved[node_id]->is_readonly = true;
          }
          SundialRequest &request = remote_nodes_involved[node_id]->request;
          SundialResponse &response = remote_nodes_involved[node_id]->response;
          request.Clear();
          response.Clear();
          request.set_txn_id(GetTxnId());
          request.set_request_type(SundialRequest::PREPARE_REQ);
          request.set_node_id(g_node_id);
          request.set_coord_id(g_node_id);
          request.set_thd_id(thd_id);

          // attach coordinator
          participant = request.add_nodes();
          participant->set_nid(g_node_id);
        //   // attach participants
        //   // TODO(Hippo): differentiate read-only case
        //   // if (!is_txn_read_only()) {
        //   for (auto itr = _remote_nodes_involved.begin();
        //        itr != _remote_nodes_involved.end(); itr++) {
        //     if (itr->second->is_readonly) continue;
        //     participant = request.add_nodes();
        //     participant->set_nid(it->first);
        //   }
        unordered_set<GID> granules;
        for (auto it2 = it->second.begin(); it2 != it->second.end(); it2++)
        {
            granules.insert(GetGranuleID((*it2)->key));
        }

        for (auto &gran: granules) {
            auto granule = request.add_involved_grans();
            granule->set_granule_id(gran);
        }

          for (auto it2 = it->second.begin(); it2 != it->second.end(); it2++)
          {
              if ((*it2)->ac_type == AccessType::UPDATE) {
                for (auto access : remote_tuple_accesses_) {
                    if (access.GetKeyUInt64() == ((*it2)->key)) {
                       SundialRequest::TupleData *tuple = request.add_tuple_data();
                       uint64_t tuple_size = access.GetDataSize();
                       tuple->set_key(access.GetKeyUInt64());
                       tuple->set_table_id(access.GetTableId());
                       tuple->set_size(tuple_size);
                       tuple->set_data(access.GetData(), tuple_size);
                    }
                }
              }
          }
          rpc_semaphore_->incr();
          rpc_client->sendRequestAsync(this, node_id, request, response);
        }
        return OK;
    }

    RC AlbatrossUsrTxn::ReceivePrepareResponse(unordered_map<uint64_t, vector<YCSBRequest *>> & node_requests_map, vector<YCSBRequest *>& remaining_reqs, std::map<uint32_t, RemoteNodeInfo *>& remote_nodes_involved) {
        rpc_semaphore_->wait();
        // clean up for
        RC rc = OK;
        // store data from remote read reponse
        for (auto it = remote_nodes_involved.begin();
             it != remote_nodes_involved.end(); it++) {
            uint64_t node_id = it->first;
            // remote_nodes_.push_back(node_id);
            SundialResponse &response = it->second->response;
            if (response.response_type() == SundialResponse::PREPARED_OK) {
                remote_nodes_.insert(node_id);
                vector<YCSBRequest *> reqs = node_requests_map[node_id];
                for (auto req: reqs) {
                   for (auto it = remaining_reqs.begin(); it != remaining_reqs.end(); ) {
                     if ((*it)->key == req->key) {
                         it = remaining_reqs.erase(it); 
                     } else {
                         ++it;
                     }
                   }
                }
            } else if (response.response_type() ==
                       SundialResponse::PREPARED_ABORT) {
            // _remote_nodes_involved[it->first]->state =
            //     TxnManager::State::ABORTED;
            // if aborted due to the outdated membership, update local
            // membership cache
            decision_ = ABORT;
            rc = ABORT;
            } else if (response.response_type() == SundialResponse::SUSPEND) {

            } else {
              assert(false);
            }
        }
        return decision_;
    }

    RC AlbatrossUsrTxn::SendCommitRequest(RC rc, unordered_map<uint64_t, vector<YCSBRequest *>> * node_requests_map, std::map<uint32_t, RemoteNodeInfo *>& remote_nodes_involved) {
        auto thd_id = arboretum::Worker::GetThdId();
        // send prepare request to participants
        SundialRequest::NodeData * participant;
        for (auto it = node_requests_map->begin();
             it != node_requests_map->end(); it++) {
          uint64_t node_id = it->first;
          if (remote_nodes_involved.find(node_id) == remote_nodes_involved.end()) {
            remote_nodes_involved[node_id] = new RemoteNodeInfo;
            remote_nodes_involved[node_id]->state = TxnManager::State::RUNNING;
            remote_nodes_involved[node_id]->is_readonly = true;
          }
          SundialRequest &request = remote_nodes_involved[node_id]->request;
          SundialResponse &response = remote_nodes_involved[node_id]->response;
          request.Clear();
          response.Clear();
          unordered_set<GID> granules;
          for (auto it2 = it->second.begin(); it2 != it->second.end(); it2++)
          {
              granules.insert(GetGranuleID((*it2)->key));
          }

          for (auto &gran: granules) {
              auto granule = request.add_involved_grans();
              granule->set_granule_id(gran);
          }

          request.set_txn_id(GetTxnId());
           SundialRequest::RequestType type = (rc == COMMIT)?
            SundialRequest::COMMIT_REQ : SundialRequest::ABORT_REQ;
          request.set_request_type( type );
          request.set_node_id(g_node_id);
          request.set_coord_id(g_node_id);
          request.set_thd_id(thd_id);
          rpc_semaphore_->incr();
          rpc_client->sendRequestAsync(this, node_id, request, response);
        }
        return OK;
    }

    RC AlbatrossUsrTxn::ReceiveCommitResponse(unordered_map<uint64_t, vector<YCSBRequest *>> & node_requests_map, vector<YCSBRequest *>& remaining_reqs, std::map<uint32_t, RemoteNodeInfo *>& remote_nodes_involved) {
        rpc_semaphore_->wait();
        // clean up for
        RC rc = OK;
        // store data from remote read reponse
        for (auto it = remote_nodes_involved.begin();
             it != remote_nodes_involved.end(); it++) {
            uint64_t node_id = it->first;
            // remote_nodes_.push_back(node_id);
            SundialResponse &response = it->second->response;
            if (response.response_type() == SundialResponse::ACK) {
                vector<YCSBRequest *> reqs = node_requests_map[node_id];
                for (auto req: reqs) {
                   for (auto it = remaining_reqs.begin(); it != remaining_reqs.end(); ) {
                     if ((*it)->key == req->key) {
                         it = remaining_reqs.erase(it); 
                     } else {
                         ++it;
                     }
                   }
                }
            } else if (response.response_type() == SundialResponse::SUSPEND) {

            } else {
              assert(false);
            }
        }
        return rc;
    }






// void AlbatrossUsrTxn::handle_prepare_resp(SundialResponse::ResponseType response,
//                                 uint32_t node_id) {
//     switch (response) {
//         case SundialResponse::PREPARED_OK:
//             break;
//         case SundialResponse::PREPARED_OK_RO:
//             break;
//         case SundialResponse::PREPARED_ABORT:
//             decision_ = ABORT;
//             break;
//         case SundialResponse::ACK:
//             // from leader/acceptor to coordinator in commit phase
//             break;
//         case SundialResponse::SUSPEND:
//             break;
//         default:
//             assert(false);
//     }
// }
}
