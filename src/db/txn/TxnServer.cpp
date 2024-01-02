#include "ITxn.h"
#include "CCManager.h"
#include "OptionalGlobalData.h"
#include "transport/rpc_client.h"


using sundial_rpc::SundialRequest;
using sundial_rpc::SundialResponse;
using sundial_rpc::SundialRPC;

namespace arboretum {
// RPC Server
// ==========

RC ITxn::process_read_request(const SundialRequest* request,
                              SundialResponse* response) {
    OID tbl_id = request->tbl_id();
    // execute read requests
    auto table = db_->GetTable(tbl_id);
    auto isUserTable = table->IsUserTable(); 
    is_migr_txn_ = table->IsGranuleTable();
    if (is_migr_txn_) {
      M_ASSERT(table->IsSysTable() && tbl_id == 1, "XXX unexpected migr table %d", tbl_id);
      if (g_migr_albatross) {
        process_read_request_albatross_migr_txn(request, response);
      } else {
        process_read_request_migr_txn(request, response);
      }
    } else {
       M_ASSERT(table->IsUserTable() && tbl_id == 0, "XXX unexpected usr table %d", tbl_id);
       if (g_migr_albatross) {
         process_read_request_albatross_user_txn(request, response);
       } else {
         process_read_request_user_txn(request, response);
       }
    }
}


RC ITxn::process_read_request_user_txn(const SundialRequest* request,
                                       SundialResponse* response) {
    RC rc = OK;
    uint32_t num_tuples = request->read_requests_size();
    OID tbl_id = request->tbl_id();
    auto table = db_->GetTable(tbl_id);
    M_ASSERT(table->IsUserTable() && tbl_id == 0, "XXX unexpected usr table %d", tbl_id);
    bool has_wrong_member = false;
    std::tuple<uint64_t, uint64_t> memCorrection;
    // check whether the key's granule exist on this node and acquire
    // lock for the granule search from G_TBL to get the corresponding
    // node id for each request
    unordered_set<uint64_t> granule_set;
    for (uint32_t i = 0; i < num_tuples; i++) {
        auto key = request->read_requests(i).key();

        char* data;
        size_t size;
        auto g_table = db_->GetGranuleTable();
        auto granule_id = GetGranuleID(key);
        if (granule_set.find(granule_id) == granule_set.end()) {
            granule_set.insert(granule_id);
            rc = GetTuple(g_table, 0, SearchKey(granule_id), AccessType::READ,
                          data, size);
            if (rc == ABORT) {
                break;
            }
            auto g_schema = g_table->GetSchema();
            auto val = &(data[g_schema->GetFieldOffset(1)]);
            uint64_t home_node = *((uint64_t*)val);
            if (home_node != g_node_id) {
                // TODO(Hippo): remove this error assert and add correct
                // response_type for non-global sys txn impl
                memCorrection = make_tuple(granule_id, home_node);
                has_wrong_member = true;
                rc = ABORT;
                break;
            }
        }
    }

    // find outdated query for current node(due to migration)
    if (rc != ABORT) {
        for (uint32_t i = 0; i < num_tuples; i++) {
            // query on the target table
            auto key = request->read_requests(i).key();
            auto index_id = request->read_requests(i).index_id();
            auto ac_type = (AccessType)request->read_requests(i).access_type();
            auto lock_type = (LockType)request->read_requests(i).lock_type();
            char* data;
            size_t size;

            // if idx_id = 0, default is searching from primary index
            rc = GetTuple(table, 0, SearchKey(key), ac_type, data, size,
                          lock_type);
            if (rc == ABORT) {
                // LOG_INFO("Abort for key %s", to_string(req.key).c_str());
                LOG_INFO("XXX Abort for key %d with granule %d", key,
                         GetGranuleID(key));
                break;
            }
            // uint64_t table_id = row->get_table_id();
            SundialResponse::TupleData* tuple = response->add_tuple_data();
            // uint64_t tuple_size = row->get_tuple_size();
            tuple->set_key(key);
            tuple->set_table_id(tbl_id);
            tuple->set_size(size);
            tuple->set_access_type(ac_type);
            tuple->set_index_id(index_id);
            tuple->set_data(data, size);
        }
    } else {
        if (has_wrong_member) {
            LOG_INFO("Send query to the outdated node with id %d from node %d",
                     g_node_id, request->node_id());
            SundialResponse::MemberData* members = response->add_memb_data();
            members->set_granid(get<0>(memCorrection));
            members->set_node_id(get<1>(memCorrection));
        }
    }

    string status = "";
    if (rc == OK) {
        status = "OK";
    } else if (rc == ABORT) {
        status = "ABORT";
    } else {
        status = "OTHER";
    }

    // LOG_INFO("rpc finish process read_req for txn %d with status %s",
    // txn_id_, status.c_str());

    if (rc == ABORT) {
        // LOG_INFO("XXX rpc receive read req for txn %d response req with
        // status %s", txn_id_, status.c_str());
        cleanup(rc);
        response->set_response_type(SundialResponse::RESP_ABORT);
    } else {
        response->set_response_type(SundialResponse::RESP_OK);
    }
    return rc;
}

RC ITxn::process_read_request_migr_txn(const SundialRequest* request,
                                       SundialResponse* response) {
    RC rc = OK;
    uint32_t num_tuples = request->read_requests_size();
    OID tbl_id = request->tbl_id();
    auto table = db_->GetTable(tbl_id);
    M_ASSERT(table->IsSysTable() && tbl_id == 1, "XXX migr unexpected table %d", tbl_id);


    // find outdated query for current node(due to migration)
    for (uint32_t i = 0; i < num_tuples; i++) {
        // query on the target table
        auto key = request->read_requests(i).key();
        auto index_id = request->read_requests(i).index_id();
        auto ac_type = (AccessType)request->read_requests(i).access_type();
        auto lock_type = (LockType)request->read_requests(i).lock_type();
        char* data;
        size_t size;

        // if idx_id = 0, default is searching from primary index
        rc = GetTuple(table, 0, SearchKey(key), ac_type, data, size,
                      lock_type);
        if (rc == ABORT) {
            // LOG_INFO("Abort for key %s", to_string(req.key).c_str());
            LOG_INFO("XXX Abort for key %d with granule %d", key,
                     GetGranuleID(key));
            break;
        }

        // TODO: move replay wait and migr updates tracks reponse to commit
        // phase(P1)
        is_migr_txn_ = true;
        // string lsn_txn_start;
        // commit_lsn->Get(lsn_txn_start);
        string lsn_txn_start;
        // TODO: set default lsn "0" as a static variable
        granule_commit_tracker->getOrDefault(key, "0", lsn_txn_start);
        migr_critical_lsn_start_ = lsn_txn_start;
        // TODO(Hippo): need to verify transaction type is migration txn
        auto g_schema = table->GetSchema();
        auto val = &(data[g_schema->GetFieldOffset(1)]);
        uint64_t home_node = *((uint64_t*)val);
        if (g_node_id == home_node) {
            // get current commit lsn and wait for the replay catch up
            auto start = GetSystemClock();

            if ((!g_migr_nocritical_enable) &&
                (!g_migr_lock_early_release_enable)) {
                string cur_replayed_lsn;
                replay_lsn->Get(cur_replayed_lsn);
                std::cout << "XXX: Migration Target catchup lsn is "
                          << ConcurrentLSN::PrintStrFor(lsn_txn_start)
                          << ", Current replayed lsn is "
                          << ConcurrentLSN::PrintStrFor(cur_replayed_lsn)
                          << ", for granule " << key << std::endl;
                if (!replay_lsn->has_replayed(lsn_txn_start)) {
                    replay_lsn->WaitForCatchup(lsn_txn_start);
                }
                auto dur =  nano_to_ms(GetSystemClock() - start);
                if ((!g_migr_postheat_enable) && dur < 50) {
                    usleep(ARDB::RandUint64(400, 500)*1000);
                    dur = nano_to_ms(GetSystemClock() - start); 
                }

                cout << "XXX migr txn src node waits log replay "
                        "catchup for "
                     << dur << " ms for granule " << key
                     << endl;
            }

            if (g_migr_preheat_enable || g_migr_hybridheat_enable) {
                // auto start = GetSystemClock();
                auto granule_id = key;
                // size_t key_size =0;
                // add updates from migr_cache_preheat_tracker to
                // response and clear migr_cache_preheat_tracker after
                // replay waiting window, the value for granule_id in
                // migr_preheat_updates_tracker is immutable
                if (migr_preheat_updates_tracker->contains(granule_id)) {
                    unordered_set<uint64_t>* delta_keys;
                    migr_preheat_updates_tracker->get(granule_id,
                                                      delta_keys);
                    if (delta_keys->size() > 0) {
                        if (!g_migr_albatross && !g_migr_dynamast) {
                            // key_size = delta_keys->size();
                            SundialResponse::CachePreheatDelta* delta =
                                response->add_cache_preheat_delta();
                            delta->set_granid(granule_id);
                            for (auto key_it = delta_keys->begin();
                                 key_it != delta_keys->end(); ++key_it) {
                                delta->add_keys(*(key_it));
                            }
                            migr_preheat_updates_tracker->remove(granule_id);
                            delete delta_keys;
                        } else {
                            auto delta = response->add_alb_dalta();
                            delta->set_granid(granule_id);
                            for (auto key_it = delta_keys->begin();
                                 key_it != delta_keys->end(); ++key_it) {
                                delta->add_keys(*(key_it));
                                // set fake value
                                char* charArray = new char[1000];
                                std::fill_n(charArray, 1000, '0');
                                delta->add_values(charArray, 1000);
                                delete[] charArray;
                            }
                            migr_preheat_updates_tracker->remove(granule_id);
                            delete delta_keys;

                        }

                    }
                }
                // auto dur = GetSystemClock() - start;
                // cout<< "XXX migr txn src node preheat delta gen takes "
                // << nano_to_s(dur) << " seconds with size " << key_size
                // <<" for granule " << key << endl;
            }
        }

        // uint64_t table_id = row->get_table_id();
        SundialResponse::TupleData* tuple = response->add_tuple_data();
        // uint64_t tuple_size = row->get_tuple_size();
        tuple->set_key(key);
        tuple->set_table_id(tbl_id);
        tuple->set_size(size);
        tuple->set_access_type(ac_type);
        tuple->set_index_id(index_id);
        tuple->set_data(data, size);
    }

    // string status = "";
    // if (rc == OK) {
    //     status = "OK";
    // } else if (rc == ABORT) {
    //     status = "ABORT";
    // } else {
    //     status = "OTHER";
    // }

    // LOG_INFO("rpc finish process read_req for txn %d with status %s",
    // txn_id_, status.c_str());

    if (rc == ABORT) {
        // LOG_INFO("XXX rpc receive read req for txn %d response req with
        // status %s", txn_id_, status.c_str());
        cleanup(rc);
        response->set_response_type(SundialResponse::RESP_ABORT);
    } else {
        response->set_response_type(SundialResponse::RESP_OK);
    }
    return rc;
}





RC
ITxn::process_prepare_request(const SundialRequest* request,
    SundialResponse* response) {
    // LOG_INFO("rpc receive process prepare for txn %d ", txn_id_);
    // assert(_txn_state == RUNNING);
    RC rc = OK;
    uint32_t num_tuples = request->tuple_data_size();
    if (g_migr_albatross && !is_migr_txn_) {
    //    M_ASSERT(false, "find a proper way to get envolved granules in this txn");
        auto gran_num = request->involved_grans_size();
        for (int i = 0; i < gran_num; i++) {
            GID gran_id = request->involved_grans(i).granule_id();
            if (granule_activeworker_map.contains(gran_id)) {
                response->set_response_type(SundialResponse::SUSPEND);
                LOG_INFO("XXX albatross rpc prepare-request return suspend for granule %d", gran_id);
                rc = RC::SUSPEND;
                return rc;
            }

        }
    }

    // copy data to the write set.
    for (uint32_t i = 0; i < num_tuples; i++) {
        uint64_t key = request->tuple_data(i).key();
        uint64_t table_id = request->tuple_data(i).table_id();
        auto write_cp_data = get_data_from_buffer(key, table_id);
        write_cp_data->Copy((void *)request->tuple_data(i).data().c_str());
    }
    // set up all nodes involved (including sender, excluding self)
    // so that termination protocol will know where to find
    for (int i = 0; i < request->nodes_size(); i++) {
        uint64_t node_id = request->nodes(i).nid();
        if (node_id == g_node_id)
            continue;
        _remote_nodes_involved[node_id] = new RemoteNodeInfo;
        // prepare request ensure all the nodes attached are rw
        _remote_nodes_involved[node_id]->is_readonly = false;
    }

    // log vote along with transaction data
    string lsn;
    LogForP1(lsn);

    // log msg no matter it is readonly or not
    SundialResponse::ResponseType response_type = SundialResponse::PREPARED_OK;
    
    //TODO(Hippo): to support track _txn_state and enable remote read-only optimization 
    // if (num_tuples != 0) {
    //     // read-write
    //     _txn_state = PREPARED;
    // } else {
    //     // readonly remote nodes
    //     _txn_state = COMMITTED;
    //     // release lock (for pessimistic) and delete accesses
    //     _cc_manager->cleanup(COMMIT);
    //     response_type = SundialResponse::PREPARED_OK_RO;
    // }
    response->set_response_type( response_type);
    // LOG_INFO("rpc finish process prepare for txn %d ", txn_id_);
    return rc;
}



RC ITxn::process_decision_request(const SundialRequest* request,
                                 SundialResponse* response, RC rc) {
    // State status = (rc == COMMIT)? COMMITTED : ABORTED;
    // _txn_state = (rc == COMMIT)? COMMITTED : ABORTED;
   if (g_migr_albatross && !is_migr_txn_) {
    //    M_ASSERT(false, "find a proper way to get envolved granules in this txn");
        auto gran_num = request->involved_grans_size();
        for (int i = 0; i < gran_num; i++) {
            GID gran_id = request->involved_grans(i).granule_id();
            if (granule_activeworker_map.contains(gran_id)) {
                response->set_response_type(SundialResponse::SUSPEND);
                LOG_INFO("XXX albatross rpc decision-request return suspend for granule %d", gran_id);
                rc = RC::SUSPEND;
                return rc;
            }

        }
    }
    string status = "";
    if (rc == COMMIT) {
        status = "COMMIT";
    } else if (rc == ABORT) {
        status = "ABORT";
    } else {
       status = "OTHER";
    }
    string lsn;
    LogForP2(rc, lsn);
    // LOG_INFO("XXX rpc receive process decision for txn %d with status %s", txn_id_, status.c_str());
    cleanup(rc, lsn);
    // LOG_INFO("rpc finish process decision for txn %d with status %s", txn_id_, status.c_str());
    // _finish_time = get_sys_clock();
   
    // OPTIMIZATION: release locks as early as possible.
    // No need to wait for this log since it is optional (shared log
    // optimization)
    if (g_migr_lock_early_release_enable && IsMigrTxn()) {
        auto start = GetSystemClock();
        string cur_replayed_lsn;
        replay_lsn->Get(cur_replayed_lsn);
        std::cout << "XXX: Migration Target catchup lsn is "
                  << ConcurrentLSN::PrintStrFor(migr_critical_lsn_start_)
                  << ", Current replayed lsn is " 
                  << ConcurrentLSN::PrintStrFor(cur_replayed_lsn)
                //   << ", for granule " << key
                  << std::endl;
        if (!replay_lsn->has_replayed(migr_critical_lsn_start_)) {
            replay_lsn->WaitForCatchup(migr_critical_lsn_start_);
        }
        auto dur = nano_to_ms(GetSystemClock() - start);
        if ((!g_migr_postheat_enable) && dur < 50) {
                 usleep(ARDB::RandUint64(400, 500)*1000);
                 dur = nano_to_ms(GetSystemClock() - start); 
        }
        cout << "XXX migr txn src node waits log replay "
                "catchup for "
             << dur << " ms" 
            //  << nano_to_us(dur) << " us for granule " << key
             << endl;

    }
    response->set_response_type( SundialResponse::ACK );
    return rc;
}




}
