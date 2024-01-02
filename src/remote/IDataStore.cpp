#include "IDataStore.h"
#include <future>
#include <thread>

namespace arboretum {

IDataStore::IDataStore() {
finished_async_ = new std::atomic<int>(0);
#if REMOTE_STORAGE_TYPE == REMOTE_STORAGE_REDIS
  client_ = new RedisClient * [g_num_nodes];
  uint32_t node_id = 0;
    while (node_id < g_num_nodes) {
        client_[node_id]= NEW(RedisClient)(const_cast<char*>(g_cluster_members[node_id].c_str()), g_data_store_port);
        LOG_INFO("[DataStore] init data store client to - %s", g_cluster_members[node_id].c_str());
        node_id++;
    }
#elif REMOTE_STORAGE_TYPE == REMOTE_STORAGE_AZURE  
  M_ASSERT(azure_tbl_conn_str != "",  "need to initialize azure_tbl_conn_str");
  client_ = new AzureTableClient * [g_num_azure_tbl_client];
  for (int i = 0; i< g_num_azure_tbl_client; i++) {
      client_[i] = NEW(AzureTableClient)(string(azure_tbl_conn_str), finished_async_);
      client_[i]->CreateTableIfNotExist(g_tbl_name);
  }
  // client_ = NEW(AzureTableClient)(string(azure_tbl_conn_str));
  // client_->CreateTableIfNotExist(g_tbl_name);
  LOG_INFO("[DataStore] init data store client to azure table [%s] with connection %s", g_tbl_name, azure_tbl_conn_str);
#endif
}


IDataStore::IDataStore(const string& conn) {
  // "DefaultEndpointsProtocol=https;AccountName=autoscale2023cosmos;AccountKey=TI8NvFGpuDfBA6T3AHxDetGBb1L60W4ItwBOqvBWUL2QyuQ6k8PqASRXzG0lLCCtEN7pWih2fJlgACDbeoMlNA==;TableEndpoint=https://autoscale2023cosmos.table.cosmos.azure.com:443/;"

finished_async_ = new std::atomic<int>(0);
#if REMOTE_STORAGE_TYPE == REMOTE_STORAGE_REDIS
  client_ = new RedisClient * [g_num_nodes];
  uint32_t node_id = 0;
    while (node_id < g_num_nodes) {
        client_[node_id]= NEW(RedisClient)(const_cast<char*>(g_cluster_members[node_id].c_str()), g_data_store_port);
        LOG_INFO("[DataStore] init data store client to - %s", g_cluster_members[node_id].c_str());
        node_id++;
    }
#elif REMOTE_STORAGE_TYPE == REMOTE_STORAGE_AZURE  
  // M_ASSERT(azure_tbl_conn_str != "",  "need to initialize azure_tbl_conn_str");
  client_ = new AzureTableClient * [g_num_azure_tbl_client];
  for (int i = 0; i< g_num_azure_tbl_client; i++) {
      // client_[i] = NEW(AzureTableClient)(string(azure_tbl_conn_str), finished_async_);
      client_[i] = NEW(AzureTableClient)(conn, finished_async_);
      client_[i]->CreateTableIfNotExist(g_tbl_name);
  }
  // client_ = NEW(AzureTableClient)(string(azure_tbl_conn_str));
  // client_->CreateTableIfNotExist(g_tbl_name);
  LOG_INFO("[DataStore] init data store client to azure table [%s] with connection %s", g_tbl_name, conn);
#endif
}



void IDataStore::Read(const std::string &tbl_name, uint64_t granule_id, const std::string& key, std::string &data) {
#if REMOTE_STORAGE_TYPE == REMOTE_STORAGE_REDIS
    //TODO(hippo): using system table to locate granule id for the key
    auto cid = GetDataStoreNodeByGranuleID(granule_id);
    client_[cid]->LoadSync(tbl_name + "-" + std::to_string(granule_id), key, data);
#elif REMOTE_STORAGE_TYPE == REMOTE_STORAGE_AZURE 
    //TODO(hippo): using system table to locate granule id for the key
    int cid = granule_id % g_num_azure_tbl_client; 
    client_[cid]->LoadSync(tbl_name, std::to_string(granule_id), key, data);
#endif
}

// used in data loading
void IDataStore::Write(const std::string& tbl_name, uint64_t granule_id, const std::string& key, char *data, size_t sz) {
#if REMOTE_STORAGE_TYPE == REMOTE_STORAGE_REDIS
  auto cid = GetDataStoreNodeByGranuleID(granule_id);
  client_[cid]->StoreSync(tbl_name + "-" + std::to_string(granule_id), key, data, sz);
#elif REMOTE_STORAGE_TYPE == REMOTE_STORAGE_AZURE 
  int cid = granule_id % g_num_azure_tbl_client; 
  client_[cid]->StoreSync(tbl_name, std::to_string(granule_id), key, data, sz);
#endif
}


// used in log replay when g_replay_parallel_flush_enable is off
void IDataStore::BatchWrite(const std::string& tbl_name, uint64_t granule_id, unordered_map<uint64_t, string> *kvs) {
  #if REMOTE_STORAGE_TYPE == REMOTE_STORAGE_REDIS
  auto cid = GetDataStoreNodeByGranuleID(granule_id);
  client_[cid]->store_sync_data(tbl_name + "-" + std::to_string(granule_id), kvs);
  #elif REMOTE_STORAGE_TYPE == REMOTE_STORAGE_AZURE 
  int cid = granule_id % g_num_azure_tbl_client; 
  client_[cid]->BatchStoreSync(tbl_name, std::to_string(granule_id), kvs);
  #endif
}

// used in log replay when g_replay_parallel_flush_enable is on
#if REMOTE_STORAGE_TYPE == REMOTE_STORAGE_REDIS

void IDataStore::AsyncBatchWrite(
    const std::string &tbl_name, uint64_t granule_id,
    unordered_map<uint64_t, string> *kvs,
    const cpp_redis::reply_callback_t &batch_write_callback) {
  auto cid = GetDataStoreNodeByGranuleID(granule_id);
  client_[cid]->AsyncBatchKVStore(tbl_name + "-" + std::to_string(granule_id),
                                  kvs, batch_write_callback);
#elif REMOTE_STORAGE_TYPE == REMOTE_STORAGE_AZURE
void IDataStore::AsyncBatchWrite(const std::string &tbl_name,
                                 uint64_t granule_id,
                                 unordered_map<uint64_t, string> *kvs) {
  // client_->AsyncBatchKVStore(tbl_name + "-" + std::to_string(granule_id),
  // kvs, batch_write_callback);
  int cid = granule_id % g_num_azure_tbl_client; 
  if (!g_replay_cosmos_enable) {
    client_[cid]->BatchStoreAsync(tbl_name, std::to_string(granule_id), kvs);
  } else {
    std::future<void> future = std::async(std::launch::async, &AzureTableClient::BatchStoreSync, client_[cid], tbl_name, std::to_string(granule_id), kvs);
    // client_[cid]->BatchStoreSync(tbl_name, std::to_string(granule_id), kvs);
  }

}

void IDataStore::WaitForAsync(size_t cnt) {
    while (finished_async_->load() < cnt) {};
    finished_async_->store(0);
}


#endif

  void IDataStore::WriteAndRead(const std::string &key, char *data, size_t sz,
                                const std::string &load_key,
                                std::string &load_data) {
#if REMOTE_STORAGE_TYPE == REMOTE_STORAGE_REDIS
  client_[g_node_id]->StoreAndLoadSync(g_granule_id, key, data, sz, load_key, load_data);
#elif REMOTE_STORAGE_TYPE == REMOTE_STORAGE_AZURE 
  M_ASSERT(false, "won't support WriteAndRead(cache writeback) for azure");
#endif
}

int64_t IDataStore::CheckSize() {
#if REMOTE_STORAGE_TYPE == REMOTE_STORAGE_REDIS
  return client_[g_node_id]->CheckSize(g_granule_id);
#elif REMOTE_STORAGE_TYPE == REMOTE_STORAGE_AZURE 
  M_ASSERT(false, "CheckSize is to be supported");
#endif
}

  //   // void store_scan(const std::string &hset_name, uint32_t batch_size, function<string(cpp_redis::reply &reply)> scan_callback);
void IDataStore::granuleScan(const std::string &tbl_name, uint64_t granule_id, uint32_t page_size, function<void(uint64_t key, char * preloaded_data)> scan_callback) {
#if REMOTE_STORAGE_TYPE == REMOTE_STORAGE_REDIS
    auto cid = GetDataStoreNodeByGranuleID(granule_id);
    M_ASSERT(false, "to support callback function (scan_callback) with parameter uint64_t key");
    client_[cid]->storeScan(
        tbl_name + "-" + std::to_string(granule_id), page_size,
        [&scan_callback](cpp_redis::reply &reply) {
            if (reply.is_error()) {
                M_ASSERT(false, "Redis receives invalid reply: %s ",
                         reply.error().c_str());
            }
            if (reply.is_null()) {
                M_ASSERT(
                    false,
                    "Redis receives null reply");
            }
            vector<cpp_redis::reply> items = reply.as_array();
            assert(items.size() == 2);
            string cursor = items[0].as_string();
            vector<cpp_redis::reply> data = items[1].as_array();
           
            uint32_t idx = 0;
            for (auto it = data.begin(); it != data.end(); ++it) {
                if (idx & 1) {
                scan_callback(const_cast<char *>((*(it)).as_string().c_str()));
                }
                idx++;
            }
            // LOG_INFO("cache preheat scan gets %d items, cursor is %s",
                    //  idx / 2, cursor.c_str());
            return cursor;
        });
#elif REMOTE_STORAGE_TYPE == REMOTE_STORAGE_AZURE 
  // RC ScanSync(const std::string &tbl_name_input, std::string partition,
  //             function<void(string& key, string& preloaded_data)> scan_callback) {
    int cid = granule_id % g_num_azure_tbl_client; 
    client_[cid]->ScanSync(tbl_name, to_string(granule_id), scan_callback);
#endif
} 


} // arboretum