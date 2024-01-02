//
// Created by Zhihan Guo on 3/31/23.
//

#ifndef ARBORETUM_SRC_COMMON_GLOBALDATA_H_
#define ARBORETUM_SRC_COMMON_GLOBALDATA_H_

#include "Types.h"
#include <mm_malloc.h>
#include <condition_variable>

using namespace std;

namespace arboretum {

#define DECL_GLOBAL_CONFIG2(tpe, var) extern tpe var;
#define DECL_GLOBAL_CONFIG3(tpe, var, val) DECL_GLOBAL_CONFIG2(tpe, var)
#define DECL_GLOBAL_CONFIG4(tpe, var, val, f) DECL_GLOBAL_CONFIG2(tpe, var)
#define DECL_GLOBAL_CONFIG5(tpe, var, val, f1, f2) DECL_GLOBAL_CONFIG2(tpe, var)
#define DEFN_GLOBAL_CONFIG2(tpe, var) tpe var;
#define DEFN_GLOBAL_CONFIG3(tpe, var, val) tpe var = val;
#define DEFN_GLOBAL_CONFIG4(tpe, var, val, f) DEFN_GLOBAL_CONFIG3(tpe, var, val)
#define DEFN_GLOBAL_CONFIG5(tpe, var, val, f1, f2) DEFN_GLOBAL_CONFIG3(tpe, var, val)
#define IF_GLOBAL_CONFIG2(tpe, var) ;
#define IF_GLOBAL_CONFIG3(tpe, var, val) \
if (item.first == #var) { (var) = item.second.get_value<tpe>(); continue; }
#define IF_GLOBAL_CONFIG4(tpe, var, val, f) IF_GLOBAL_CONFIG3(tpe, var, val)
#define IF_GLOBAL_CONFIG5(tpe, var, val, f1, f2)          \
if (item.first == #var) {                                 \
  auto str = item.second.get_value<std::string>();        \
  (var) = f2(str); continue;                              \
}
#define PRINT_DB_CONFIG2(tpe, name) ;
#define PRINT_DB_CONFIG3(tpe, name, val) { \
std::cout << #name << ": " << (name) << std::endl; \
if (g_save_output) g_out_file << "\"" << #name << "\"" << ": " << (name) << ", "; }
#define PRINT_DB_CONFIG4(tpe, name, val, func) { \
std::cout << #name << ": " << func(name) << std::endl; \
if (g_save_output) g_out_file << "\"" << #name << "\"" << ": \"" \
<< func(name) << "\", "; }
#define PRINT_DB_CONFIG5(tpe, name, val, f1, f2) \
PRINT_DB_CONFIG4(tpe, name, val, f1)

// pre-declaration
class BufferManager;
class IDataStore;
class ILogStore;
class SundialRPCClient;
class SundialRPCServerImpl;
class TxnTable;
class ARDB;
class ClusterManager;


// Configuration
// ====================
extern char g_config_fname[100];
extern char ifconfig_file[];
extern std::atomic<int> gc_log_size;
extern volatile uint32_t dur_migr;

// Cluster
// ====================
extern ClusterManager *  cluster_manager;
extern vector<string> g_cluster_members;
extern vector<vector<uint64_t> *> node_granules_map;

// Dynamast States
extern std::mutex mm_mutex;
extern int mm_count;
extern std::condition_variable migr_mark;
extern std::mutex an_mutex;
extern int an_count;
extern std::condition_variable active_thread;


// DB
// ====================
extern ARDB * ardb;


// RPC
// ====================
extern SundialRPCClient *  rpc_client;
extern SundialRPCServerImpl * rpc_server;

// TXN
// ====================
extern TxnTable *       txn_table;

// Index
// ====================
#define DB_INDEX_CONFIG(x, y, z, a)                         \
a(IndexType, g_index_type, IndexType::REMOTE,               \
IndexTypeToString, StringToIndexType)                       \
y(size_t, g_idx_btree_fanout, 18)                           \
y(double, g_idx_btree_split_ratio, 0.9)

// Local Storage
// ====================
#define AR_PAGE_SIZE 8192
#define DB_LOCAL_STORE_CONFIG(x, y, z, a)                   \
x(BufferManager *, g_buf_mgr)                               \
a(BufferType, g_buf_type, NOBUF,                            \
BufferTypeToString, StringToBufferType)                     \
y(size_t, g_total_buf_sz, 128 * 1024 * 1024)

// Remote Storage
// ====================
#define DB_REMOTE_STORE_CONFIG(x, y, z, a)                 \
y(uint64_t, g_granule_id, 0)                               \
x(IDataStore *, g_data_store)                              \
x(ILogStore *, g_log_store)                                \
x(ILogStore *, g_syslog_store)                                \
y(uint64_t, g_scale_node_id, 3)         \
z(bool, g_storage_data_loaded, true, BoolToString)         \
z(bool, g_check_preloaded_data, false, BoolToString)
extern char g_data_store_host[30];
extern size_t g_data_store_port;
extern char g_log_store_host[30];
extern size_t g_log_store_port;
extern char g_syslog_store_host[30];
extern size_t g_syslog_store_port;
extern char g_storage_pwd[30];
extern size_t g_granule_num_per_node;
extern char azure_tbl_conn_str[500];
extern char azure_log_conn_str[500];
extern char azure_cosmos_conn_str[500];
extern char g_tbl_name[30];




#define DB_SYSTEM_CONFIG(x, y, z, a)                       \
y(size_t, g_num_worker_threads, 1)                         \
y(size_t, g_num_nodes, 3)                         \
y(size_t, g_num_azure_tbl_client, 1)                         \
y(size_t, g_load_data_gb_st, 0)                         \
y(size_t, g_client_node_id, 4)                         \
y(size_t, g_remote_req_retries, 3)                       \
y(size_t, g_num_rpc_server_threads, 8)                         \
y(size_t, g_migr_threads_num, 1)                         \
y(size_t, g_migr_conc_lag_ms, 0)                         \
y(size_t, g_migr_sleep_ms, 0)                         \
y(size_t, g_sys_log_id, 1000)                         \
y(size_t, g_ring_buffer_size, 1000000)                         \
y(size_t, g_replay_flush_threshold, 100)                         \
y(size_t, g_replay_fetch_batch_mb, 48)                         \
y(size_t, g_replay_backcontrol_size, 150)                         \
y(size_t, g_groupcommit_wait_us, 10000)                    \
y(size_t, g_gc_lognum_bound, 25)                    \
y(size_t, g_gc_size_timeout_ms, 1000)                    \
y(size_t, g_granule_size_mb, 128)                    \
y(size_t, g_num_rows_per_node, 0)                             \
y(size_t, g_rampup_inteval_ms, 1000)                             \
y(size_t, g_rampup_on_premise_size, 0)                             \
y(size_t, g_rampup_duration_s, 300)                             \
y(size_t, g_on_premise_duration_s, 40)                             \
y(double, g_cpu_freq, 1.0)                                 \
y(double, g_read_percent, 1.0)                             \
z(bool, g_is_migr_node, false, BoolToString)               \
z(bool, g_single_granule_txn, false, BoolToString)               \
z(bool, g_on_premise_enable, false, BoolToString)               \
z(bool, g_replay_cosmos_enable, false, BoolToString)               \
z(bool, g_loadbalance_enable, false, BoolToString)               \
z(bool, g_scaleout_enable, false, BoolToString)               \
z(bool, g_synthetic_enable, false, BoolToString)               \
z(bool, g_rampup_scaleout_enable, false, BoolToString)               \
z(bool, g_rampup_proactive_enable, false, BoolToString)               \
z(bool, g_scalein_enable, false, BoolToString)               \
z(bool, g_scalein_mimic_ft_enable, false, BoolToString)               \
z(bool, g_faulttolerance_enable, false, BoolToString)               \
z(bool, g_replay_enable, true, BoolToString)               \
z(bool, g_replay_producer_enable, true, BoolToString)               \
z(bool, g_replay_from_buffer_enable, false, BoolToString)               \
z(bool, g_partial_migr_enable, false, BoolToString)               \
z(bool, g_migr_dynamast, false, BoolToString)               \
z(bool, g_migr_albatross, false, BoolToString)               \
z(bool, g_migr_preheat_enable, false, BoolToString)               \
z(bool, g_migr_postheat_enable, false, BoolToString)               \
z(bool, g_migr_hybridheat_enable, false, BoolToString)               \
z(bool, g_migr_lock_early_release_enable, false, BoolToString)               \
z(bool, g_migr_nocritical_enable, false, BoolToString)               \
z(bool, g_migr_wait_die_enable, false, BoolToString)               \
z(bool, g_migr_interweave_enable, false, BoolToString)               \
z(bool, g_groupcommit_enable, true, BoolToString)          \
z(bool, g_groupcommit_size_based, true, BoolToString)          \
z(bool, g_grpc_server_pool_enable, false, BoolToString)          \
z(bool, g_replay_parallel_flush_enable, true, BoolToString)          \
z(bool, g_replay_backcontrol_enable, true, BoolToString)          \
z(bool, g_cache_preheat_mask_enable, true, BoolToString)          \
z(bool, g_gc_future, true, BoolToString)          \
z(bool, g_terminate_exec, false, BoolToString)             \
z(bool, g_warmup_finished, false, BoolToString)            \
z(bool, g_enable_logging, true, BoolToString)              \
z(bool, g_force_write, false, BoolToString)                \
y(OID, g_node_id, 0) z(bool, g_save_output, false, BoolToString)


extern std::ofstream g_out_file;
extern char g_out_fname[100];

extern const std::string GRANULE_TABLE_NAME;
extern const std::string NODE_TABLE_NAME;

// Definition of the global constant string

#define DB_CONFIGS(x, y, z, a)     \
DB_INDEX_CONFIG(x, y, z, a)        \
DB_LOCAL_STORE_CONFIG(x, y, z, a)  \
DB_REMOTE_STORE_CONFIG(x, y, z, a) \
DB_SYSTEM_CONFIG(x, y, z, a)
DB_CONFIGS(DECL_GLOBAL_CONFIG2, DECL_GLOBAL_CONFIG3,
           DECL_GLOBAL_CONFIG4, DECL_GLOBAL_CONFIG5)



} // arboretum

#endif //ARBORETUM_SRC_COMMON_GLOBALDATA_H_
