#include <cmath>
#include "YCSBScaleinWorkload.h"
#include "LoadBalanceWorkload.h"
#include "common/BenchWorker.h"
#include "ITxn.h"
#include "common/GlobalData.h"
#include "common/OptionalGlobalData.h"
#include "db/txn/TxnTable.h"

namespace arboretum {

YCSBScaleinWorkload::YCSBScaleinWorkload(ARDB *db, YCSBConfig *config) : YCSBWorkload(db, config) {
}

YCSBQuery *YCSBScaleinWorkload::GenQuery() {
    auto query = NEW(YCSBQuery);
    query->tbl_id = tables_[config_->GetTblName()];
    query->req_cnt = config_->num_req_per_query_;
    query->requests = NEW_SZ(YCSBRequest, query->req_cnt);
    query->is_all_remote_readonly = false;
    query->q_type = QueryType::REGULAR;
    size_t _request_cnt = 0;
    M_ASSERT(config_->num_req_per_query_ <= 64,
             "Change the following constant if num_req_per_query_ > 64");
    uint64_t all_keys[64];
    uint64_t table_size = config_->num_rows_;
    
    uint64_t max_key = ((uint64_t)config_->num_dataload_nodes_) * config_->num_rows_ - 1;
    size_t granule_num_per_node = GetGranuleID(config_->num_rows_ - 1) + 1;
    uint64_t max_granule_id = GetGranuleID(max_key);

    size_t granule_num = 1;
    // bool isSingleGranuleTxn = true;
    bool isSingleGranuleTxn = (g_single_granule_txn)? true: (ARDB::RandDouble() < 0.8);
    if (isSingleGranuleTxn) {
        granule_num = 1;
    } else {
        granule_num = 2;
    }
    
    uint64_t granule_ids[granule_num];
    int count = 0;
    while (count < granule_num) {
        uint64_t granule_id = ARDB::RandUint64(0, max_granule_id);
        bool exist = false;
        for (uint32_t j = 0; j < count; j++)
            if (granule_ids[j] == granule_id) exist = true;
        if (!exist) {
            granule_ids[count] = granule_id;
            count++;
        }
    }

    while (_request_cnt < config_->num_req_per_query_) {
        YCSBRequest &req = query->requests[_request_cnt];

        size_t granule_idx = (isSingleGranuleTxn)? 0: ARDB::RandUint64(0, granule_num - 1);
        uint64_t primary_key = g_granule_size_mb*1024 * granule_ids[granule_idx] + ARDB::RandUint64(0, g_granule_size_mb*1024 -1); 

        double r = ARDB::RandDouble();
        req.ac_type = (r < config_->read_perc_) ? READ : UPDATE;
        req.key = primary_key;
        req.value = 0;
        // remove duplicates
        bool exist = false;
        for (uint32_t i = 0; i < _request_cnt; i++)
            if (all_keys[i] == req.key) exist = true;
        if (!exist) all_keys[_request_cnt++] = req.key;
    }

    return query;
}

// YCSBQuery *YCSBScaleinWorkload::GenQuery() {
//     auto query = NEW(YCSBQuery);
//     query->tbl_id = tables_[config_->GetTblName()];
//     query->req_cnt = config_->num_req_per_query_;
//     query->requests = NEW_SZ(YCSBRequest, query->req_cnt);
//     query->is_all_remote_readonly = false;
//     query->q_type = QueryType::REGULAR;
//     size_t _request_cnt = 0;
//     M_ASSERT(config_->num_req_per_query_ <= 64,
//              "Change the following constant if num_req_per_query_ > 64");
//     uint64_t all_keys[64];
//     uint64_t table_size = config_->num_rows_;
    
//     uint64_t max_key = ((uint64_t)config_->num_dataload_nodes_) * config_->num_rows_ - 1;
//     size_t granule_num_per_node = GetGranuleID(config_->num_rows_ - 1) + 1;
//     uint64_t max_granule_id = GetGranuleID(max_key);
//     size_t granule_num = 2;
//     // size_t granule_num = 3;
//     uint64_t granule_ids[granule_num];
//     for (int i = 0; i< granule_num; i++) {
//         granule_ids[i] = ARDB::RandUint64(0, max_granule_id);
//     }

//     while (_request_cnt < config_->num_req_per_query_) {
//         YCSBRequest &req = query->requests[_request_cnt];

//         // uint32_t node_id;
//         // // node_id = (g_node_id +
//         // //            ARDB::RandUint64(1, config_->num_dataload_nodes_ - 1)) %
//         // //           config_->num_dataload_nodes_;
//         // node_id = ARDB::RandUint64(0, config_->num_dataload_nodes_ - 1);
//         // M_ASSERT(node_id != g_node_id, "Wrong remote query node id");
//         // uint64_t row_id = (config_->zipf_theta_ == 0)
//         //                       ? ARDB::RandUint64(0, table_size - 1)
//         //                       : zipf(table_size - 1, config_->zipf_theta_);

//         size_t granule_idx = ARDB::RandUint64(0, granule_num - 1);
//         uint64_t primary_key = g_granule_size_mb*1024 * granule_ids[granule_idx] + ARDB::RandUint64(0, g_granule_size_mb*1024 -1); 

//         // uint64_t primary_key = config_->num_rows_ * node_id + row_id;
//         double r = ARDB::RandDouble();
//         req.ac_type = (r < config_->read_perc_) ? READ : UPDATE;
//         req.key = primary_key;
//         req.value = 0;
//         // remove duplicates
//         bool exist = false;
//         for (uint32_t i = 0; i < _request_cnt; i++)
//             if (all_keys[i] == req.key) exist = true;
//         if (!exist) all_keys[_request_cnt++] = req.key;
//     }

//     return query;
// }

} // namespace