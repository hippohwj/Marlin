//
// Created by Zhihan Guo on 4/1/23.
//

#ifndef ARBORETUM_SRC_DB_ITXN_H_
#define ARBORETUM_SRC_DB_ITXN_H_

// #include "common/Common.h"
#include "common/GlobalData.h"
#include "common/semaphore_sync.h"
#include "db/ARDB.h"
#include "db/ITxn.h"
#include "local/ITable.h"
#include "local/ITuple.h"
#include "sundial.grpc.pb.h"
#include "sundial.pb.h"
#include "txn/TxnManager.h"
#include "ycsb/YCSBWorkload.h"
#include <vector>


using sundial_rpc::SundialRequest;
using sundial_rpc::SundialResponse;
using sundial_rpc::SundialRPC;

using namespace std;

namespace arboretum {

struct WriteCopy {
    explicit WriteCopy(size_t sz) : sz_(sz) {
        data_ = static_cast<char *>(MemoryAllocator::Alloc(sz));
    };
    ~WriteCopy() { DEALLOC(data_); };
    void Copy(void *src) const { memcpy(data_, src, sz_); };
    char *data_{nullptr};  // copy-on-write
    size_t sz_{0};
};

struct Access {
    AccessType ac_{READ};
    SearchKey key_{};
    ITable *tbl_{nullptr};
    SharedPtr *rows_{nullptr};
    int num_rows_{0};
    bool need_release_lock{true};
    uint32_t node_id_{0};
    WriteCopy *data_{nullptr};
    bool is_migrated_{false};

    uint64_t GetKeyUInt64() { return key_.ToUInt64(); };

    OID GetTableId() { return tbl_->GetTableId(); };

    size_t GetDataSize() { return data_->sz_; };

    char *GetData() { return data_->data_; }

    void Serialize(vector<uint8_t> * buffer) {
        uint32_t ac_type = static_cast<uint32_t>(ac_);
        buffer->insert(buffer->end(), CHAR_PTR_CAST(&ac_type), CHAR_PTR_CAST(&ac_type) + sizeof(ac_type));
        uint64_t key = key_.ToUInt64();
        buffer->insert(buffer->end(), CHAR_PTR_CAST(&key), CHAR_PTR_CAST(&key) + sizeof(key));
        OID tbl_id = GetTableId();
        M_ASSERT(tbl_id == 0, "XXX Txn Ser unexpected: access tbl_id is %d", tbl_id);
        buffer->insert(buffer->end(), CHAR_PTR_CAST(&tbl_id), CHAR_PTR_CAST(&tbl_id) + sizeof(tbl_id));

        if (ac_ == UPDATE) {
           size_t tuple_sz = tbl_->GetTotalTupleSize();
           buffer->insert(buffer->end(), CHAR_PTR_CAST(&tuple_sz), CHAR_PTR_CAST(&tuple_sz) + sizeof(tuple_sz)); 
           ITuple * tuple =  reinterpret_cast<ITuple *>(rows_[0].Get());
           M_ASSERT(tbl_id == tuple->GetTableId(), "access tbl_id %d is not equal to tuple tbl_id %d", tbl_id, tuple->GetTableId());
           char * tuple_data = reinterpret_cast<char *>(tuple);
           buffer->insert(buffer->end(), tuple_data, tuple_data + tuple_sz); 
           size_t data_sz = GetDataSize();
           buffer->insert(buffer->end(), CHAR_PTR_CAST(&data_sz), CHAR_PTR_CAST(&data_sz) + sizeof(data_sz)); 
           char * data = GetData();
           buffer->insert(buffer->end(), data, data + data_sz); 
         } else {
           size_t tuple_sz = tbl_->GetTotalTupleSize();
           buffer->insert(buffer->end(), CHAR_PTR_CAST(&tuple_sz), CHAR_PTR_CAST(&tuple_sz) + sizeof(tuple_sz)); 
           ITuple * tuple =  reinterpret_cast<ITuple *>(rows_[0].Get());
           char * data = reinterpret_cast<char *>(tuple);
           buffer->insert(buffer->end(), data, data + tuple_sz); 
         }
    }
    
    const uint8_t * Deserialize(const uint8_t * buffer, ARDB *db) {
        const uint8_t * offset = buffer; 
        uint32_t ac_type;
        std::memcpy(&ac_type, offset, sizeof(ac_type));
        ac_ = static_cast<AccessType>(ac_type);
        offset = offset + sizeof(ac_type);
        uint64_t key;
        std::memcpy(&key, offset, sizeof(key));
        offset = offset + sizeof(key);
        key_ = SearchKey(key);
        OID tbl_id;
        std::memcpy(&tbl_id, offset, sizeof(tbl_id));
        offset = offset + sizeof(tbl_id);
        M_ASSERT(tbl_id == 0, "XXX Txn Deser unexpected: access tbl_id is %d", tbl_id);
        //Hippo: fix this hack
        // tbl_ = db_->GetTable(0);
        tbl_ = db->GetTable(0);
        // auto tuple_sz = table->GetTotalTupleSize();
        if (ac_ == UPDATE) {
           auto tuples = NEW_SZ(SharedPtr, 1);
           size_t tuple_sz;
           std::memcpy(&tuple_sz, offset, sizeof(tuple_sz));
           offset = offset + sizeof(tuple_sz);
           auto tuple = new (MemoryAllocator::Alloc(tuple_sz)) ITuple(0, 0);
           tuple->SetFromLoaded(const_cast<char*>(reinterpret_cast<const char*>(offset)), tuple_sz);
           tuples[0].Init(tuple);
           M_ASSERT(tuple->GetTableId() == tbl_id, "access tbl_id %d is not equal to tuple tbl_id %d", tbl_id, tuple->GetTableId());

           offset = offset + tuple_sz; 
           rows_ = tuples;

           size_t data_sz;
           std::memcpy(&data_sz, offset, sizeof(data_sz));
           offset = offset + sizeof(data_sz);
           auto write_copy = new WriteCopy(data_sz);
           write_copy->Copy((void *)offset);
           data_ = write_copy;
           offset = offset + data_sz;
        } else {
           auto tuples = NEW_SZ(SharedPtr, 1);
           size_t tuple_sz;
           std::memcpy(&tuple_sz, offset, sizeof(tuple_sz));
           offset = offset + sizeof(tuple_sz);
        //    char* data;
        //    std::memcpy(&data, offset, tuple_sz);
        //    offset = offset + tuple_sz; 
           auto tuple = new (MemoryAllocator::Alloc(tuple_sz)) ITuple(0, 0);
           tuple->SetFromLoaded(const_cast<char*>(reinterpret_cast<const char*>(offset)), tuple_sz);
           tuples[0].Init(tuple);
           offset = offset + tuple_sz; 
           rows_ = tuples; 
        }
        num_rows_ = 1;
        is_migrated_ = true;
        need_release_lock = false;
        return offset;
    }
};

class ITxn {
   public:
    static uint32_t KeyToNode(uint64_t key) {
         return (uint32_t)(key/g_num_rows_per_node); 
    };

    ITxn(OID txn_id, ARDB *db);
    OID GetTxnId() const { return txn_id_; };

    // Transaction-related methods
    // ========================

    virtual RC Execute(YCSBQuery *query);
    virtual RC Commit(RC rc);
    RC Abort();

    // Execution-related methods
    void InsertTuple(ITable *&p_table, char *&data, size_t sz);
    virtual bool IsMigrTxn();





  protected:
    RC GetTuple(ITable *p_table, OID idx_id, SearchKey key, AccessType ac,
                char *&data, size_t &size, LockType lock_type=LockType::NO_WAIT, bool delta_access=false);
    RC GetTupleOrInsert(ITable *p_table, OID idx_id, SearchKey key, AccessType ac_type,
                  char *&data, size_t &sz, LockType lock_type,  char * preload_tuple);
    size_t GenDataForP1Log(string& data_str);
    size_t GenDataAndGranulesForP1Log(string& data_str, unordered_set<uint64_t> * granules);
    RC Log(string &data, uint8_t status_new, string &lsn);
    RC Log(string &data, uint8_t status_new, string &lsn, unordered_set<uint64_t> * granules);
    RC LogForP1(string& lsn);
    RC LogForP2(RC rc, string& lsn);
    virtual RC CommitP1();
    virtual RC CommitP2(RC rc);


   public:
    void Serialize(vector<uint8_t> * buffer, OID granule_id);
    const uint8_t * Deserialize(const uint8_t * buffer);
    vector<Access> * GetAccess() {
       return &tuple_accesses_; 
    }
    // only for albatross
    void MergeTxn(ITxn * txn);

    bool ContainsGranule(GID granuleid);
    // RPC related methods for Distributed transactions
    // ========================
    // cliend-side
    RC send_remote_package(unordered_map<uint64_t, vector<YCSBRequest *>> * node_requests_map, OID tbl_id);
    RC ReceiveRemoteResponse();
    // virtual void handle_prepare_resp(SundialResponse::ResponseType response,
    //                          uint32_t node_id);
    void handle_prepare_resp(SundialResponse::ResponseType response,
                             uint32_t node_id);

    // server-side
    RC process_read_request(const SundialRequest *request,
                            SundialResponse *response);
    RC process_read_request_user_txn(const SundialRequest *request,
                            SundialResponse *response);
    RC process_read_request_albatross_user_txn(const SundialRequest *request,
                            SundialResponse *response);
    RC process_read_request_migr_txn(const SundialRequest *request,
                            SundialResponse *response);
    RC process_read_request_albatross_migr_txn(const SundialRequest *request,
                            SundialResponse *response);
    RC process_prepare_request(const SundialRequest *request,
                               SundialResponse *response);
    RC process_decision_request(const SundialRequest* request,
                                 SundialResponse* response, RC rc);

   SemaphoreSync *rpc_semaphore_;
   SemaphoreSync *log_semaphore_;


   protected:
    // Transaction Local Buffer Access methods
    // ========================
    WriteCopy *get_data_from_buffer(uint64_t key, uint32_t table_id);

    // States Cleanup
    // ========================
    void cleanup(RC rc, string &lsn);
    void cleanup(RC rc);
    static void FinishAccess(SharedPtr &ptr, bool dirty = false);
    OID txn_id_{0};
    // txn local buffer
    vector<Access> tuple_accesses_;
    vector<Access> preheat_delta_accesses_;
    vector<Access> remote_tuple_accesses_;
    std::unordered_set<GID> granule_involved_; 

    // 2pc-related
    volatile RC decision_;
    string migr_critical_lsn_start_;

    bool is_migr_txn_{false};
    bool is_alb_related_txn_{false};
    // rpc related structure
    struct RemoteNodeInfo {
        volatile TxnManager::State state;
        bool is_readonly;
        // At any point in time, a remote node has at most 1 request and 1
        // response.
        SundialRequest request;
        SundialResponse response;
    };

    std::map<uint32_t, RemoteNodeInfo *> _remote_nodes_involved;
    //TODO: move to migration txn
    vector<uint64_t> cache_preheat_delta_;


    ARDB *db_;
};
}  // namespace arboretum

#endif  // ARBORETUM_SRC_DB_ITXN_H_
