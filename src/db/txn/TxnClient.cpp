#include "db/ITxn.h"
#include "transport/rpc_client.h"
#include "TxnManager.h"
#include "OptionalGlobalData.h"
#include "db/AlbatrossMigrTxn.h"
#include "common/Worker.h"
#include "db/txn/TxnTable.h"

using sundial_rpc::SundialRequest;
using sundial_rpc::SundialResponse;
using sundial_rpc::SundialRPC;

using namespace std;
namespace arboretum
{
    /*
    * read data from remote nodes
    */
    RC
    ITxn::send_remote_package(unordered_map<uint64_t, vector<YCSBRequest *>> * node_requests_map, OID tbl_id)
    {
        auto thd_id = arboretum::Worker::GetThdId();
        for (auto it = node_requests_map->begin(); it != node_requests_map->end(); it++)
        {
            uint64_t node_id = it->first;
            if (node_id != g_node_id) {
            if (_remote_nodes_involved.find(node_id) == _remote_nodes_involved.end())
            {
                _remote_nodes_involved[node_id] = new RemoteNodeInfo;
                _remote_nodes_involved[node_id]->state = TxnManager::State::RUNNING;
                _remote_nodes_involved[node_id]->is_readonly = true;
            }
            SundialRequest &request = _remote_nodes_involved[node_id]->request;
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
                    _remote_nodes_involved[node_id]->is_readonly = false;
                    // TODO(hippo): figure out whether it is necessary to set_txn_read_write
                    // set_txn_read_write();
                }
            }
            }
        }

        // send out remote requests asynchronously
        for (auto it = _remote_nodes_involved.begin(); it != _remote_nodes_involved.end(); it++)
        {
            rpc_semaphore_->incr();
            rpc_client->sendRequestAsync(this, it->first, it->second->request,
                                         it->second->response);
        }

    //     rpc_semaphore_->wait();
    //     // clean up for 
    //     RC rc = OK;
    // // store data from remote read reponse 
    // for (auto it = _remote_nodes_involved.begin(); it != _remote_nodes_involved.end(); it ++) {
    //     uint64_t node_id = it->first;
    //     SundialResponse &response = it->second->response;
    //     if (response.response_type() == SundialResponse::RESP_OK) {
    //             for (int i = 0; i < response.tuple_data_size(); i++) {
    //                 // store remote read locally
    //                 int limit = 1;
    //                 auto tuple = response.tuple_data(i);
    //                 auto key = tuple.key();
    //                 //TODO(Hippo): replace table using real table from response info
    //                 Access access = { (AccessType)tuple.access_type(), SearchKey(key), db_->GetTable(tuple.table_id()), nullptr, limit, node_id};
    //                 // Access *access = &(*remote_tuple_accesses_.rbegin());
    //                 assert(node_id != g_node_id);
    //                 // access.home_node_id = node_id;
    //                 access.rows_ = nullptr;
    //                 auto sz = tuple.size();
    //                 auto write_copy = new WriteCopy(sz);
    //                 write_copy->Copy((void*)tuple.data().c_str());
    //                 access.data_ = write_copy;
    //                 remote_tuple_accesses_.push_back(access);
    //             }
    //     } else if (response.response_type() == SundialResponse::RESP_ABORT) {
    //             _remote_nodes_involved[it->first]->state =
    //                 TxnManager::State::ABORTED;
    //             rc = ABORT;
    //     } else {
    //             assert(false);
    //     }
    // }
        return OK;
    }

    RC ITxn::ReceiveRemoteResponse() {
        rpc_semaphore_->wait();
        // clean up for
        RC rc = OK;
        // store data from remote read reponse
        for (auto it = _remote_nodes_involved.begin();
             it != _remote_nodes_involved.end(); it++) {
            uint64_t node_id = it->first;
            SundialResponse &response = it->second->response;
            if (response.response_type() == SundialResponse::RESP_OK) {
            for (int i = 0; i < response.tuple_data_size(); i++) {
                // store remote read locally
                int limit = 1;
                auto tuple = response.tuple_data(i);
                auto key = tuple.key();
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
                // restore lock table and txn table 
                auto payload = response.albatross_payload(0);
                vector<ITxn *> * txn_tbl = AlbatrossMigrTxn::DeserializeTxnTbl(reinterpret_cast<const uint8_t*>(payload.txns().c_str()));
                unordered_map<OID, uint32_t> * lock_tbl = AlbatrossMigrTxn::DeserializeLockTbl(reinterpret_cast<const uint8_t*>(payload.locks().c_str()));
                for (auto &item: *lock_tbl) {
                   auto user_table = db_->GetTable(0);
                //    user_table->PutLock(item.first,  NEW(std::atomic<uint32_t>)(item.second));
                }
                for (ITxn *new_txn : *txn_tbl) {
                    if (new_txn->is_alb_related_txn_) {
                        ITxn *txn = txn_table->get_txn(new_txn->GetTxnId());
                        if (txn == nullptr) {
                            txn_table->add_txn(new_txn);
                        } else {
                            txn->MergeTxn(new_txn);
                        }
                    }
                }
                LOG_INFO("XXX albatross finish deser payload for %d txns and %d locks", txn_tbl->size(), lock_tbl->size());
            }

            // TODO: move this check to commit phase P1
            // TODO: optimization => only possible when it is migration
            // transaction
            if (response.cache_preheat_delta_size() > 0) {
                // store delta keys in current transaction and use it as a
                // filter mask when the migration transaction commit
                for (int j = 0; j < response.cache_preheat_delta_size(); ++j) {
                    auto delta = response.cache_preheat_delta(j);
                    LOG_INFO("Migration preheat delta for granule %d is of size %d", delta.granid(), delta.keys_size());
                    for (auto &delta_key : delta.keys()) {
                        cache_preheat_delta_.push_back(delta_key);
                    }
                }
            }
           
            if (response.alb_dalta_size() > 0) {
                for (int j = 0; j < response.alb_dalta_size(); ++j) {
                    auto delta = response.alb_dalta(j);
                    LOG_INFO("Migration complete delta for granule %d is of size %d", delta.granid(), delta.keys_size());
                    int i = 0;
                    for (auto &delta_key : delta.keys()) {
                        //hack: should insert key value into cache
                        auto value = delta.values(i).c_str();
                        i++;
                    }
                }
            }

            } else if (response.response_type() ==
                       SundialResponse::RESP_ABORT) {
            _remote_nodes_involved[it->first]->state =
                TxnManager::State::ABORTED;
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

void ITxn::handle_prepare_resp(SundialResponse::ResponseType response,
                                uint32_t node_id) {
    switch (response) {
        case SundialResponse::PREPARED_OK:
            // _remote_nodes_involved[node_id]->state = TxnManager::State::PREPARED;
            break;
        case SundialResponse::PREPARED_OK_RO:
            // _remote_nodes_involved[node_id]->state = TxnManager::State::COMMITTED;
            assert(_remote_nodes_involved[node_id]->is_readonly);
            break;
        case SundialResponse::PREPARED_ABORT:
            // _remote_nodes_involved[node_id]->state = TxnManager::State::ABORTED;
            decision_ = ABORT;
            break;
        case SundialResponse::ACK:
            // from leader/acceptor to coordinator in commit phase
            break;
        case SundialResponse::SUSPEND:
            break;
        default:
            assert(false);
    }
}
}
