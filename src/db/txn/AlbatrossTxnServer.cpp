#include "ITxn.h"
#include "CCManager.h"
#include "OptionalGlobalData.h"
#include "transport/rpc_client.h"
#include "db/txn/TxnTable.h"
#include "db/AlbatrossUsrTxn.h"
#include "db/AlbatrossMigrTxn.h"

using sundial_rpc::SundialRequest;
using sundial_rpc::SundialResponse;
using sundial_rpc::SundialRPC;
namespace arboretum {

RC ITxn::process_read_request_albatross_user_txn(const SundialRequest* request,
                                       SundialResponse* response) {
    RC rc = OK;
    uint32_t num_tuples = request->read_requests_size();
    for (uint32_t i = 0; i < num_tuples; i++) {
      auto key = request->read_requests(i).key(); 
      auto granule_id = GetGranuleID(key);
      if (granule_activeworker_map.contains(granule_id)) {
        response->set_response_type(SundialResponse::SUSPEND);
        LOG_INFO("XXX albatross rpc read-request return suspend for granule %d", granule_id);
        rc = RC::SUSPEND;
        return rc;
      }
    }
    for (uint32_t i = 0; i < num_tuples; i++) {
      auto key = request->read_requests(i).key(); 
      auto granule_id = GetGranuleID(key);
      granule_involved_.insert(granule_id);
    }
    if (rc == RC::OK) {
       OID tbl_id = request->tbl_id();
       auto table = db_->GetTable(tbl_id);
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
              //  LOG_INFO("XXX Abort for key %d with granule %d", key,
              //           GetGranuleID(key));
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

       if (rc == ABORT) {
           // LOG_INFO("XXX rpc receive read req for txn %d response req with
           // status %s", txn_id_, status.c_str());
           cleanup(rc);
           response->set_response_type(SundialResponse::RESP_ABORT);
       } else {
           response->set_response_type(SundialResponse::RESP_OK);
       }
    }
    return rc;
}


RC ITxn::process_read_request_albatross_migr_txn(const SundialRequest *request,
                            SundialResponse *response) {
  // update suspend tracker for albatross
  uint32_t num_tuples = request->read_requests_size();
  M_ASSERT(request->read_requests_size() == 1, "unexpected tuple number %d", num_tuples);
  uint64_t granule_id = request->read_requests(0).key();
  std::atomic<uint32_t> * suspended_worker_num = NEW(std::atomic<uint32_t>)(0); 
  granule_activeworker_map.insert(granule_id, suspended_worker_num);
  // wait for the suspending complete
  uint32_t finish_mark = (g_num_rpc_server_threads >=32)? ~0u: ((1<< g_num_rpc_server_threads) -1); 
  LOG_INFO("XXX albatross wait for syspend for txn %d for granule %d", txn_id_, granule_id);
  auto starttime_chrono = std::chrono::high_resolution_clock::now();
  while (suspended_worker_num->load() != finish_mark) {};
  auto endtime_chrono = std::chrono::high_resolution_clock::now(); 
  auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(endtime_chrono - starttime_chrono).count();
  LOG_INFO("XXX albatross finish syspend for txn %d for granule %d with %d ms", txn_id_, granule_id, duration);
  //copy transaction states(lock table and txn table) to response
  vector<ITxn *> ongoing_txns;
  // auto txn_num = txn_table->get_size();
  // txn_table->get_txn_by_granule(granule_id, ongoing_txns); 
  vector<uint8_t> txn_buffer;
  std::unordered_map<OID, uint32_t> lock_tbl;
  auto txn_num = txn_table->serialize_all_txns(granule_id, &txn_buffer, lock_tbl);
  // txn_table->get_all_txns(ongoing_txns);
  // vector<uint8_t> txn_buffer;
  // AlbatrossMigrTxn::SerializeTxnTbl(&txn_buffer, ongoing_txns);
  // int tuple_num = 0;
  // for(ITxn * txn:ongoing_txns) {
  //   vector<Access> * accesses = txn->GetAccess();
  //   for (auto access: *accesses) {
  //     if (access.tbl_->IsUserTable() && GetGranuleID(access.key_.ToUInt64()) == granule_id) {
  //        auto tuple = reinterpret_cast<ITuple *>(access.rows_[0].Get());
  //        OID lock_key = tuple->GetTID();
  //        uint32_t lock_state = access.tbl_->GetLockState(tuple->GetTID()).load();
  //        lock_tbl[lock_key] = lock_state;
  //        tuple_num++;
  //     }
  //   }
  // }
  auto txn_buffer_size = txn_buffer.size();
  char* txn_data = reinterpret_cast<char*>(txn_buffer.data());
  SundialResponse::AlbatrossPayload* txns = response->add_albatross_payload();
  txns->set_txns(txn_data, txn_buffer_size);
  vector<uint8_t> lock_tbl_buffer;
  AlbatrossMigrTxn::SerializeLockTbl(&lock_tbl_buffer, lock_tbl);
  auto lock_tbl_buffer_size = lock_tbl_buffer.size();
  SundialResponse::AlbatrossPayload* lock_tbls = response->add_albatross_payload();
  char* lock_data = reinterpret_cast<char*>(lock_tbl_buffer.data());
  lock_tbls->set_locks(lock_data, lock_tbl_buffer_size);
  LOG_INFO("XXX albatross migr payload for granule %d is %d bytes: %d bytes of txns(%d), %d bytes of locks(%d)",granule_id, txn_buffer_size + lock_tbl_buffer_size, txn_buffer_size, txn_num, lock_tbl_buffer_size, lock_tbl.size());
  return process_read_request_migr_txn(request, response);
}



}