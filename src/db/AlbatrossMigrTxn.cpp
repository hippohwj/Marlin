#include "AlbatrossMigrTxn.h"
#include "remote/ILogStore.h"
#include "transport/rpc_client.h"
#include "common/GlobalData.h"
#include "buffer/ObjectBufferManager.h"
#include "IDataStore.h"
#include "common/Worker.h"
#include "CCManager.h"

namespace arboretum {


bool AlbatrossMigrTxn::IsMigrTxn() {
  return true;
}

void AlbatrossMigrTxn::SerializeLockTbl(vector<uint8_t> * buffer, unordered_map<OID, uint32_t>& lock_tbl) {
    uint32_t lock_tbl_size = lock_tbl.size(); 
    buffer->insert(buffer->end(), CHAR_PTR_CAST(&lock_tbl_size), CHAR_PTR_CAST(&lock_tbl_size) + sizeof(lock_tbl_size));
    for (auto& pair: lock_tbl) {
       OID key = pair.first;
       uint32_t value = pair.second;
       buffer->insert(buffer->end(), CHAR_PTR_CAST(&key), CHAR_PTR_CAST(&key) + sizeof(key));
       buffer->insert(buffer->end(), CHAR_PTR_CAST(&value), CHAR_PTR_CAST(&value) + sizeof(value));
    }
}

unordered_map<OID, uint32_t> * AlbatrossMigrTxn::DeserializeLockTbl(const uint8_t * buffer) {
    const uint8_t *offset = buffer;
    uint32_t lock_tbl_size;
    std::memcpy(&lock_tbl_size, offset, sizeof(lock_tbl_size));
    offset = offset + sizeof(lock_tbl_size);
    std::unordered_map<OID, uint32_t>* lockmap = new std::unordered_map<OID, uint32_t>();
    for (int i = 0; i < lock_tbl_size; i++) {
      OID key;
      std::memcpy(&key, offset, sizeof(key));
      offset = offset + sizeof(key);
      uint32_t value;
      std::memcpy(&value, offset, sizeof(value));
      offset = offset + sizeof(value);
      (*lockmap)[key] = value;
    }
    return lockmap;
}

void AlbatrossMigrTxn::SerializeTxnTbl(vector<uint8_t> * buffer, vector<ITxn *> & txns) {
    uint32_t txns_tbl_size = txns.size(); 
    buffer->insert(buffer->end(), CHAR_PTR_CAST(&txns_tbl_size), CHAR_PTR_CAST(&txns_tbl_size) + sizeof(txns_tbl_size));
    for (auto txn: txns) {
       txn->Serialize(buffer, 0);
    }
}

vector<ITxn *> * AlbatrossMigrTxn::DeserializeTxnTbl(const uint8_t * buffer) {
    const uint8_t *offset = buffer;
    uint32_t txn_tbl_size;
    std::memcpy(&txn_tbl_size, offset, sizeof(txn_tbl_size));
    offset = offset + sizeof(txn_tbl_size);
    LOG_INFO("XXX Deserialize Txn table with txn_num %d", txn_tbl_size);

    vector<ITxn *> * txns = new vector<ITxn *>();
    for (int i = 0; i < txn_tbl_size; i++) {
         ITxn * txn = new (MemoryAllocator::Alloc(sizeof(ITxn)))
                     ITxn(0, ardb);
        offset = txn->Deserialize(offset);
        txns->push_back(txn);
    }
    return txns;
}


RC AlbatrossMigrTxn::Execute(YCSBQuery *query) {
    critical_start = std::chrono::high_resolution_clock::now();
    auto thd_id = arboretum::Worker::GetThdId();
    query_ = query;
    RC rc = OK;
    // group requests by node id
    unordered_map<uint64_t, vector<YCSBRequest *>> node_requests_map;
    auto response_rpc_endtime_chrono = std::chrono::high_resolution_clock::now(); 
    auto starttime_chrono = std::chrono::high_resolution_clock::now();

    // partial migration txn
    for (uint32_t i = 0; i < query->req_cnt; i++) {
        YCSBRequest &req = query->requests[i];
        req.lock_type = LockType::NO_LOCK;
        
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
    auto node_req_endtime_chrono = std::chrono::high_resolution_clock::now(); 
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(node_req_endtime_chrono - starttime_chrono).count();
    LOG_INFO("XXX albatross finish migr GenNodeReqMap for txn %d with %d ms", txn_id_, duration);
  

    // auto start = GetSystemClock();

    // acquire locks on each participant
    rc = send_remote_package(&node_requests_map, query->tbl_id);
    LOG_INFO("XXX Albatross migr txn %d send read reqs for granule %d", txn_id_, query->requests[0].key);
    if (rc == ABORT || rc == FAIL) return rc;
    auto read_rpc_endtime_chrono = std::chrono::high_resolution_clock::now(); 
    duration = std::chrono::duration_cast<std::chrono::milliseconds>(read_rpc_endtime_chrono - node_req_endtime_chrono).count();
    LOG_INFO("XXX albatross finish migr SendReadReq for txn %d with %d ms", txn_id_, duration);

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
    LOG_INFO("XXX Albatross migr txn receive response for read req for granule %d", query->requests[0].key);
    response_rpc_endtime_chrono = std::chrono::high_resolution_clock::now(); 
    duration = std::chrono::duration_cast<std::chrono::milliseconds>(response_rpc_endtime_chrono - read_rpc_endtime_chrono).count();
    LOG_INFO("XXX albatross finish migr ReadReqResponse for txn %d with %d ms", txn_id_, duration);



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

    auto loca_exec_chrono = std::chrono::high_resolution_clock::now(); 
    duration = std::chrono::duration_cast<std::chrono::milliseconds>(loca_exec_chrono - response_rpc_endtime_chrono).count();
    LOG_INFO("XXX albatross finish migr LocalExec for txn %d with %d ms", txn_id_, duration);

    return rc;
}


}