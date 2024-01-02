
#include "RampUpScaleoutWorkload.h"
#include "ITxn.h"
#include "MigrationTxn.h"
#include "PostheatTxn.h"
#include "common/GlobalData.h"
#include "common/OptionalGlobalData.h"
#include "ycsb/YCSBConfig.h"
#include "transport/rpc_client.h"
#include "db/txn/TxnTable.h"

namespace arboretum {

// single node migration
uint32_t RampUpScaleoutWorkload::granule_num_in_cluster = 32 * 12;

RampUpScaleoutWorkload::RampUpScaleoutWorkload(ARDB *db, YCSBConfig *config) : ScaleoutWorkload(db, config) {
  int max_clients_per_node = 64;
  if (g_rampup_proactive_enable) {
    //proactively migrate
    time_before_migr_ = ((g_node_id * max_clients_per_node - (max_clients_per_node/2)) * g_rampup_inteval_ms)/1000.0;
    time_after_migr_ = (g_node_id == 3)? config_->time_after_migration_: 0;
  } else {
    // reactively migrate
      time_before_migr_ = ((g_node_id * max_clients_per_node) * g_rampup_inteval_ms) / 1000.0;
      time_after_migr_ = (g_node_id == 3)? config_->time_after_migration_: 0;
  }

}

void RampUpScaleoutWorkload::GenGranulesToMigrate() {
  M_ASSERT(g_node_id > 0 && g_node_id < 4, "Don't support migrate on current node %d", g_node_id);
  int migr_granule_num_per_node = (granule_num_in_cluster/(g_node_id + 1))/ g_node_id; 
      for (int nodeid = 0; nodeid < g_node_id; nodeid++) {
        for (uint64_t i = 0; i < migr_granule_num_per_node; i++) {
            GranulesToMigrate.push_back(node_granules_map[nodeid]->at(i));
        }
  }
}

void RampUpScaleoutWorkload::Init() {
  LOG_INFO("Scaleout Init...")
  string tbl_name = config_->GetTblName();
  std::ifstream in("configs/"+ tbl_name);
  InitSchema(in);
  LoadSysTBL();
  if (config_->migrate_cache_enable_) {
     LoadData(tbl_name);
  }
  GenGranulesToMigrate();
  GenLockTable();
  GenQueries();
}

void RampUpScaleoutWorkload::InitComplement() {
  LOG_INFO("LoadBalance Complement Init...")
  M_ASSERT(config_->migrate_cache_enable_ == 0, "only support migration_cache_enable_ = 0");
  GenGranulesToMigrate();
  GenLockTable();
  GenQueries();
}



void RampUpScaleoutWorkload::GenLockTable() {
    auto user_table = db_->GetTable(0);
    for (auto gid: GranulesToMigrate) {
       auto start_key = g_granule_size_mb*1024 * gid;
       for (int i = 0; i < g_granule_size_mb*1024; i++) {
            // user_table->Putputstart_key + i] = NEW(std::atomic<uint32_t>)(0);
            user_table->PutLock(start_key + i,  NEW(std::atomic<uint32_t>)(0));
       }
    }
}

void RampUpScaleoutWorkload::GenQueries() {
  LOG_INFO("RampUpScaleout Workload Generate Queries...")
  auto g_tbl_name = db_->GetGranuleTable()->GetTableName();
  auto tbl_id = db_->GetGranuleTableID(); 
  auto post_heat_tbl_id = tables_[config_->GetTblName()];
  // M_ASSERT(g_migr_threads_num <= granule_num_per_node, "maximum migr thread num should be %d, current is %d", granule_num_per_node, g_migr_threads_num);
  LockType lock_type =
      (g_migr_wait_die_enable) ? LockType::WAIT_DIE : LockType::NO_WAIT;
  uint32_t granule_num_per_node_per_thd = GranulesToMigrate.size() / g_migr_threads_num;

  for (int i = 0; i < g_migr_threads_num; i++) {
      vector<YCSBQuery *> *queries = new vector<YCSBQuery *>();
      for (int z = 0; z < granule_num_per_node_per_thd; z++) {
          uint32_t start_idx = granule_num_per_node_per_thd * i;
          auto granule_id = GranulesToMigrate[z + start_idx];
          auto query = NEW(YCSBQuery);
          query->tbl_id = tbl_id;
          query->req_cnt = 1;
          query->q_type = QueryType::MIGR;
          query->requests = NEW_SZ(YCSBRequest, query->req_cnt);
          query->is_all_remote_readonly = false;
          // query to migrate
          // query->requests[0].key = GranulesToMigrate[i];
          query->requests[0].key = granule_id;
          query->requests[0].value = g_node_id;
          query->requests[0].ac_type = AccessType::UPDATE;
          query->requests[0].lock_type = lock_type;
          granule_tbl_cache->get(granule_id, query->requests[0].src_node);
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
      }

      all_queries_.insert(std::make_pair(i, queries));
  }

  // printout queries
  for (auto it = all_queries_.begin(); it != all_queries_.end(); ++it) {
    auto queries = it->second;
    cout<< "thread_id: " << it->first << " get queries( ";
    for(auto query = queries->begin(); query != queries->end(); ++query) {
      cout << (*query)->requests[0].key << " ";

    } 
    cout << " )"<< endl;
  }
}

}