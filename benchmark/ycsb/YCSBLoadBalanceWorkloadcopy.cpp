// #include <cmath>
// #include "YCSBLoadBalanceWorkload.h"
// #include "LoadBalanceWorkload.h"
// #include "common/BenchWorker.h"
// #include "ITxn.h"
// #include "common/GlobalData.h"
// #include "common/OptionalGlobalData.h"
// #include "db/txn/TxnTable.h"

// namespace arboretum {

// YCSBLoadBalanceWorkload::YCSBLoadBalanceWorkload(ARDB *db, YCSBConfig *config) : YCSBWorkload(db, config) {
// }

// YCSBQuery *YCSBLoadBalanceWorkload::GenQuery() {
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

//     size_t granule_num = 1;
//     // bool isSingleGranuleTxn = true;
//     bool isSingleGranuleTxn = (g_single_granule_txn)? true: (ARDB::RandDouble() < 0.8);
//     if (isSingleGranuleTxn) {
//         granule_num = 1;
//     } else {
//         granule_num = 2;
//     }

//     uint64_t max_key = ((uint64_t)config_->num_dataload_nodes_) * config_->num_rows_ - 1;
//     // bool is_skew = ARDB::RandDouble() < 0.8; 
//     size_t granule_num_per_node = GetGranuleID(config_->num_rows_ - 1) + 1;
//     uint64_t max_granule_id = GetGranuleID(max_key);
//     uint64_t skew_granule_min = GetGranuleID(config_->scale_node_id_ * config_->num_rows_);
//     uint64_t skew_granule = ARDB::RandUint64( skew_granule_min, skew_granule_min + granule_num_per_node - 1);
//     uint64_t granule_ids[granule_num];
//     granule_ids[0] = skew_granule;
//     if (granule_num > 1) {
//        granule_ids[1] = ARDB::RandUint64(0, max_granule_id); 
//        while(granule_ids[1] >= skew_granule_min && granule_ids[1] <= skew_granule_min + granule_num_per_node - 1) {
//             granule_ids[1] = ARDB::RandUint64(0, max_granule_id); 
//        }
//     }

//     while (_request_cnt < config_->num_req_per_query_) {
//         YCSBRequest &req = query->requests[_request_cnt];
//         uint64_t granule_id  = (granule_num == 1)? granule_ids[0]:granule_ids[ARDB::RandUint64(granule_num)];
//         uint64_t primary_key = g_granule_size_mb*1024 * granule_id + ARDB::RandUint64(0, g_granule_size_mb*1024 -1); 

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

// } // namespace