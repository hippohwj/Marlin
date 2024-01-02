//
// Created by Zhihan Guo on 3/30/23.
//

#include "ARDB.h"
#include "ITxn.h"
#include "MigrationTxn.h"
#include "DynamastMigrTxn.h"
#include "AlbatrossMigrTxn.h"
#include "AlbatrossUsrTxn.h"
#include "PostheatTxn.h"
#include "local/ITable.h"
#include "remote/IDataStore.h"
#include "common/Worker.h"
#include "buffer/ObjectBufferManager.h"
#include "common/GlobalData.h"
#include "remote/ILogStore.h"

using boost::property_tree::ptree;

namespace arboretum {
__thread drand48_data ARDB::rand_buffer_;

ARDB::ARDB() {
  CalculateCPUFreq();
  // init classes
  LOG_INFO("Start ARDB instance.")
  g_data_store = NEW(IDataStore);
  g_log_store = NEW(ILogStore);
  g_syslog_store = NEW(ILogStore)(true);
  if (g_buf_type == OBJBUF) {
    g_buf_mgr = NEW(ObjectBufferManager)(this);
  } else if (g_buf_type == PGBUF) {
  }
}

void ARDB::LoadConfig(int argc, char * argv[]) {
  // load configuration
  if (argc > 1) {
    memcpy(g_config_fname, argv[1], 100);
  }
  ptree root;
  LOG_INFO("Loading DB Config from file: %s", g_config_fname);
  read_json(g_config_fname, root);
  auto config_name = root.get<std::string>("config_name");
  LOG_DEBUG("Loading DB Config: %s", config_name.c_str());
  for (ptree::value_type &item : root.get_child("db_config")){
    // if (item.first == "g_cluster_members") {
    //   LOG_INFO("g_cluster_members size %u",item.second.size());
    //   // g_cluster_members = new char*[item.second.size()];
    //   u_int8_t count = 0;
    //   for (const auto& value : item.second) {
    //       string member= value.second.get_value<std::string>(); 
    //       LOG_INFO("g_cluster_members mem: %s",member.c_str());
    //       g_cluster_members.push_back(member);
    //       // g_cluster_members[count] = member;
    //       count++;
    //   }

    //   continue;
    // }
    DB_CONFIGS(IF_GLOBAL_CONFIG2, IF_GLOBAL_CONFIG3,
               IF_GLOBAL_CONFIG4, IF_GLOBAL_CONFIG5)
    if (item.first == "g_out_fname") {
      std::strcpy(g_out_fname, item.second.get_value<std::string>().c_str());
      continue;
    } else if (item.first == "azure_tbl_conn_str") {
      std::strcpy(azure_tbl_conn_str, item.second.get_value<std::string>().c_str());
    } else if (item.first == "azure_log_conn_str") {
      std::strcpy(azure_log_conn_str, item.second.get_value<std::string>().c_str());
    } else if (item.first == "azure_cosmos_conn_str") {
      std::strcpy(azure_cosmos_conn_str, item.second.get_value<std::string>().c_str());
    }else if (item.first ==  "g_tbl_name") {
      std::strcpy(g_tbl_name, item.second.get_value<std::string>().c_str());
    } 
  }
  if (g_save_output) {
    cout << "hello print" << endl;
    g_out_file.open(g_out_fname, std::ios_base::app);
    g_out_file << "{";
  }
  DB_CONFIGS(PRINT_DB_CONFIG2, PRINT_DB_CONFIG3, PRINT_DB_CONFIG4, PRINT_DB_CONFIG5)
}

OID ARDB::CreateTable(std::string tbl_name, ISchema * schema) {
  OID tbl_id = table_cnt_++;
  auto table = NEW(ITable)(std::move(tbl_name), tbl_id, schema);
  if (table->IsGranuleTable()) {
    g_tbl_id_ = tbl_id;
  } else if (table->IsNodeTable()) {
    n_tbl_id_ = tbl_id;
  } else if (table->IsUserTable()) {
    u_tbl_id_ = tbl_id;
  }
  tables_.push_back(table);
  return tbl_id;
}

RC ARDB::InsertTuple(OID tbl, OID tupleid, char *data, size_t sz, ITxn * txn) {
  // insert tuple in the table, table will insert it into every index.
  if (!txn) {
    // data loading
    tables_[tbl]->InsertTuple(tupleid, data, sz);
  } else {
    txn->InsertTuple(tables_[tbl], data, sz);
  }
  return RC::OK;
}


//TODO: change interface to delete by key instead of tuple
void ARDB::IndexDelete(ITable * tbl, OID tupleid, char *data, size_t sz, ITxn * txn) {
  // insert tuple in the table, table will insert it into every index.
  if (!txn) {
    tbl->IndexDelete(tupleid, data, sz);
  } else {
    M_ASSERT(false, "not supported");
  }
}

void ARDB::BatchInsert(OID tbl, OID partition,  unordered_map<uint64_t, string>&map) {
  // g_data_store->AsyncBatchWrite(tables_[tbl]->GetTableName(), partition, &map);
  tables_[tbl]->BatchInsert(partition, &map);
}
void ARDB::WaitForAsyncBatchLoading(int cnt) {
  g_data_store->WaitForAsync(cnt);
}

int64_t ARDB::CheckStorageSize() {
  return g_data_store->CheckSize();
}

ITxn *ARDB::StartTxn(TxnType xtype) {
  Worker::IncrMaxThdTxnId();
  // txn_id format:
  //     | unique number | worker_thread_id | node_id |
            // uint64_t txn_id = max_txn_id ++;
            // txn_id = txn_id * g_num_worker_threads + _thd_id;
            // txn_id = txn_id * g_num_nodes + g_node_id;
  // auto txn_id = g_num_worker_threads * Worker::GetMaxThdTxnId() + Worker::GetThdId();

  ITxn * txn;
  if (xtype == TxnType::USER) {
    auto txn_id = g_num_worker_threads * Worker::GetMaxThdTxnId() + Worker::GetThdId();
    txn_id = txn_id * g_num_nodes + g_node_id;
    if (!g_migr_albatross) {
      txn = new (MemoryAllocator::Alloc(sizeof(ITxn))) ITxn(txn_id, this);
    } else {
      txn = new (MemoryAllocator::Alloc(sizeof(AlbatrossUsrTxn))) AlbatrossUsrTxn(txn_id, this);
    }
  } else if (xtype == TxnType::MIGRATION) {
    auto txn_id = g_migr_threads_num * Worker::GetMaxThdTxnId() + Worker::GetThdId();
    txn_id = txn_id * g_num_nodes + g_node_id;
    txn = new (MemoryAllocator::Alloc(sizeof(MigrationTxn))) MigrationTxn(txn_id, this);
    // txn = new (MemoryAllocator::Alloc(sizeof(MigrationTxn))) MigrationTxn(numeric_limits<uint32_t>::max() - Worker::GetMaxThdTxnId(), this);
  } else if (xtype == TxnType::POSTHEAT) {
    auto txn_id = g_migr_threads_num * Worker::GetMaxThdTxnId() + Worker::GetThdId();
    txn_id = txn_id * g_num_nodes + g_node_id;
    txn = new (MemoryAllocator::Alloc(sizeof(PostheatTxn))) PostheatTxn(txn_id, this);
  } else if (xtype == TxnType::DYNAMAST) {
    auto txn_id = g_migr_threads_num * Worker::GetMaxThdTxnId() + Worker::GetThdId();
    txn_id = txn_id * g_num_nodes + g_node_id;
    txn = new (MemoryAllocator::Alloc(sizeof(DynamastMigrTxn))) DynamastMigrTxn(txn_id, this);
  } else if (xtype == TxnType::ALBATROSS) {
    auto txn_id = g_migr_threads_num * Worker::GetMaxThdTxnId() + Worker::GetThdId();
    txn_id = txn_id * g_num_nodes + g_node_id;
    txn = new (MemoryAllocator::Alloc(sizeof(AlbatrossMigrTxn))) AlbatrossMigrTxn(txn_id, this);
  } else {
    M_ASSERT(false, "only support user and migration txn now");
  }
  return txn;
}

double ARDB::RandDouble() {
  double r;
  drand48_r(&rand_buffer_, &r);
  return r;
}

uint64_t
ARDB::RandUint64()
{
    int64_t rint64 = 0;
    lrand48_r(&rand_buffer_, &rint64);
    return rint64;
}

uint64_t
ARDB::RandUint64(uint64_t max)
{
    return RandUint64() % max;
}

//included
uint64_t
ARDB::RandUint64(uint64_t min, uint64_t max)
{
    return min + RandUint64(max - min + 1);
}


OID ARDB::CreateIndex(OID tbl_id, OID col, IndexType tpe) {
  return tables_[tbl_id]->CreateIndex(col, tpe);
}

bool ARDB::CheckBufferWarmedUp() {
  if (g_buf_type == NOBUF) {
    return true;
  } else if (g_buf_type == OBJBUF) {
    return ((ObjectBufferManager *) g_buf_mgr)->IsWarmedUp();
  }
  return false;
}

} // arboretum