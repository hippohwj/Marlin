//
// Created by Zhihan Guo on 3/31/23.
//

#include "GlobalData.h"
#include "Common.h"
#include "transport/rpc_client.h"
#include "transport/rpc_server.h"

namespace arboretum {

char g_config_fname[100] = "configs/sample.cfg";

char ifconfig_file[80] = "ifconfig.txt";


char g_data_store_host[30] = "127.0.0.1";
size_t g_data_store_port = 6379;
char g_log_store_host[30] = "127.0.0.1";
size_t g_log_store_port = 6380;
char g_syslog_store_host[30] ;
size_t g_syslog_store_port = 6381;
size_t g_granule_num_per_node = 8;

char g_storage_pwd[30] = "sundial-dev";
char g_tbl_name[30] = "M_TBL";


char azure_tbl_conn_str[500] = "";
char azure_log_conn_str[500] = "";
char azure_cosmos_conn_str[500] = "";



std::ofstream g_out_file;
char g_out_fname[100] = "sample.out";

const string GRANULE_TABLE_NAME= "G_TBL";
const string NODE_TABLE_NAME= "N_TBL";


std::mutex mm_mutex;
int mm_count = 0;
std::condition_variable migr_mark;
std::mutex an_mutex;
int an_count = 0;
std::condition_variable active_thread;
volatile uint32_t dur_migr = 0;


ClusterManager *  cluster_manager;
// char g_cluster_members[3][30];
vector<string> g_cluster_members;
vector<vector<uint64_t> *> node_granules_map;
std::atomic<int> gc_log_size{1};


ARDB * ardb;

SundialRPCClient *  rpc_client;
SundialRPCServerImpl * rpc_server;

TxnTable *      txn_table;




DB_CONFIGS(DEFN_GLOBAL_CONFIG2, DEFN_GLOBAL_CONFIG3,
           DEFN_GLOBAL_CONFIG4, DEFN_GLOBAL_CONFIG5)


} // arboretum