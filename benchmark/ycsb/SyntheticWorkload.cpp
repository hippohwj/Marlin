
#include "SyntheticWorkload.h"
#include "ITxn.h"
#include "MigrationTxn.h"
#include "PostheatTxn.h"
#include "common/GlobalData.h"
#include "common/OptionalGlobalData.h"
#include "ycsb/YCSBConfig.h"
#include "db/txn/TxnTable.h"

namespace arboretum {

// single node migration
// uint32_t SyntheticWorkload::granule_num_per_node = 32 * 3;

SyntheticWorkload::SyntheticWorkload(ARDB *db, YCSBConfig *config) : ScaleoutWorkload(db, config) {
    granule_num_per_node = config_->num_rows_ / (g_granule_size_mb * 1024);
}

void SyntheticWorkload::Init() {
  LOG_INFO("Synthetic Init...")
  string tbl_name = config_->GetTblName();
  std::ifstream in("configs/"+ tbl_name);
  InitSchema(in);
  LoadSysTBL();
  GenLockTable();
//   if (config_->migrate_cache_enable_) {
//      LoadData(tbl_name);
//   }
  GenQueries();
}

void SyntheticWorkload::InitComplement() {
  LOG_INFO("Synthetic Complement Init...")
  string tbl_name = config_->GetTblName();
  GenLockTable();
//   if (config_->migrate_cache_enable_) {
//      LoadData(tbl_name);
//   }
  GenQueries();
}

void SyntheticWorkload::LoadData(string& tbl_name) {
  char data[schemas_[tbl_name]->GetTupleSz()];
  // strcpy(&data[schemas_[0]->GetFieldOffset(1)], "init");
//   uint64_t rows_per_node = config_-> num_rows_;

  // load data/cache
  for (auto gid: granules_to_migr) {
       auto start_key = g_granule_size_mb*1024 * gid;
       for (int i = 0; i < g_granule_size_mb*1024; i++) {
          uint64_t primary_key = start_key + i;
          // set primary key
          schemas_[tbl_name]->SetPrimaryKey(static_cast<char *>(data), primary_key);
          schemas_[tbl_name]->SetNumericField(1, data, primary_key);
          db_->InsertTuple(tables_[tbl_name], primary_key, data, schemas_[tbl_name]->GetTupleSz());
       }
    
  }

//   if (g_storage_data_loaded && g_check_preloaded_data) {
//     // TODO(Hippo): generate granule id properly
//     M_ASSERT((uint64_t)db_->CheckStorageSize() == config_->num_rows_,
//              "Size of data inserted does not match!");
//   }
  LOG_INFO("Finished loading YCSB workload and checked durability");
}


void SyntheticWorkload::GenLockTable() {
    auto user_table = db_->GetTable(0);
    int migr_gran_num = 32;
    int server_num = g_num_nodes - 1;
    int src_node_id = (g_node_id - 1 + server_num) % server_num;
    uint64_t start_gran_id = granule_num_per_node * src_node_id;
    for (int i = 0;i < migr_gran_num; i++) {
        granules_to_migr.push_back(start_gran_id + i);
    }
    for (auto gid: granules_to_migr) {
       auto start_key = g_granule_size_mb*1024 * gid;
       for (int i = 0; i < g_granule_size_mb*1024; i++) {
            // user_table->Putputstart_key + i] = NEW(std::atomic<uint32_t>)(0);
            user_table->PutLock(start_key + i,  NEW(std::atomic<uint32_t>)(0));
       }
    }
}

void SyntheticWorkload::GenQueries() {
    LOG_INFO("Scalein Workload Generate Queries...")
    // auto tbl_id = db_->GetGranuleTable()->GetTableId();
    auto g_tbl_name = db_->GetGranuleTable()->GetTableName();
    auto tbl_id = db_->GetGranuleTableID(); 
    cout << "XXX: gen query for table " << g_tbl_name << ", with tbl_id " << tbl_id << endl;
    auto query_tbl_name = config_->GetTblName();
    auto post_heat_tbl_id = tables_[config_->GetTblName()];
    // M_ASSERT(g_migr_threads_num <= granule_num_per_node, "maximum migr thread
    // num should be %d, current is %d", granule_num_per_node,
    // g_migr_threads_num);
    LockType lock_type =
        (g_migr_wait_die_enable) ? LockType::WAIT_DIE : LockType::NO_WAIT;
    int migr_gran_num_per_node = 32;
    int migr_gran_num_per_thd = migr_gran_num_per_node / g_migr_threads_num;
    M_ASSERT((migr_gran_num_per_node % g_migr_threads_num) == 0,
             "number of granules per node (%d) to be migrated has to be "
             "distributed in threads (%d) evenly",
             migr_gran_num_per_node, g_migr_threads_num);
    cout << "XXX: " << granules_to_migr.size() << " granules to migr to node  " << g_node_id << " is ( ";
    for (auto gid: granules_to_migr) {
        cout << gid << " ";
    }
    cout << " )" << endl;


    for (int i = 0; i < g_migr_threads_num; i++) {
        vector<YCSBQuery *> *queries = new vector<YCSBQuery *>();
        for (int j = 0; j < migr_gran_num_per_thd; j++) {
            int start_idx = i * migr_gran_num_per_thd;
            auto granule_id = granules_to_migr[start_idx + j];
            auto query = NEW(YCSBQuery);
            query->tbl_id = tbl_id;
            query->req_cnt = 1;
            query->q_type = QueryType::MIGR;
            query->requests = NEW_SZ(YCSBRequest, query->req_cnt);
            query->is_all_remote_readonly = false;
            // query to migrate
            query->requests[0].key = granule_id;
            query->requests[0].value = g_node_id;
            query->requests[0].ac_type = AccessType::UPDATE;
            query->requests[0].lock_type = lock_type;
            queries->push_back(query);
            if (g_migr_postheat_enable) {
                // post-heat query
                auto query = NEW(YCSBQuery);
                query->tbl_id = post_heat_tbl_id;
                query->req_cnt = 1;
                query->q_type = QueryType::GRANULE_SCAN;
                query->requests = NEW_SZ(YCSBRequest, query->req_cnt);
                query->is_all_remote_readonly = true;
                query->requests[0].key = granule_id;
                query->requests[0].value = g_node_id;
                query->requests[0].ac_type = AccessType::READ;
                query->requests[0].lock_type = lock_type;
                queries->push_back(query);
            }
            all_queries_.insert(std::make_pair(i, queries));
        }
    }

    // printout queries
    for (auto it = all_queries_.begin(); it != all_queries_.end(); ++it) {
        auto queries = it->second;
        // queries->resize(1);
        cout << "thread_id: " << it->first << " get queries( ";
        for (auto query = queries->begin(); query != queries->end(); ++query) {
            cout << (*query)->requests[0].key << " ";
        }
        cout << " )" << endl;
    }
}

}