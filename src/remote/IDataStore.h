//
// Created by Zhihan Guo on 3/30/23.
//

#ifndef ARBORETUM_DISTRIBUTED_SRC_TRANSPORT_DATASTORE_H_
#define ARBORETUM_DISTRIBUTED_SRC_TRANSPORT_DATASTORE_H_

#include "common/Common.h"
#include "RedisClient.h"
#include "AzureTableClient.h"

namespace arboretum {

class IDataStore {
 public:
  IDataStore();
  IDataStore(const string& conn);
  

  AzureTableClient ** GetClients(){
    return client_;
  }
  //Azure support
  //used in query
  
  void Read(const std::string &tbl_name, uint64_t granule_id,const std::string& key, std::string &data);
  
  // used in migration preheat
  void granuleScan(const std::string &tbl_name, uint64_t granule_id, uint32_t page_size, function<void(uint64_t key, char * preloaded_data)> scan_callback); 

  // used in log replay when g_replay_parallel_flush_enable is off
  void BatchWrite(const std::string& tbl_name, uint64_t granule_id, unordered_map<uint64_t, string> *kvs);

  // used in log replay when g_replay_parallel_flush_enable is on
  #if REMOTE_STORAGE_TYPE == REMOTE_STORAGE_REDIS
    void AsyncBatchWrite(const std::string& tbl_name, uint64_t granule_id, unordered_map<uint64_t, string> *kvs, const cpp_redis::reply_callback_t &batch_write_callback);
  #elif REMOTE_STORAGE_TYPE == REMOTE_STORAGE_AZURE 
    void AsyncBatchWrite(const std::string& tbl_name, uint64_t granule_id, unordered_map<uint64_t, string> *kvs);
    void WaitForAsync(size_t cnt);
  #endif


  //Azure support for data loading
  void Write(const std::string &tbl_name, uint64_t granule_id, const std::string& key, char * data, size_t sz);

  // void WriteAndRead(uint64_t granule_id, const std::string &key, char *data, size_t sz, const std::string &load_key, std::string &load_data);

  // only used in cache writeback(no longer needed for log replay)
  void WriteAndRead(const std::string &key, char *data, size_t sz, const std::string &load_key, std::string &load_data);
  
  
  int64_t CheckSize();
  

 private:
   std::atomic<int> * finished_async_;
#if REMOTE_STORAGE_TYPE == REMOTE_STORAGE_REDIS
  RedisClient ** client_;
#elif REMOTE_STORAGE_TYPE == REMOTE_STORAGE_AZURE 
  AzureTableClient ** client_;
#endif
};

} // arboretum

#endif //ARBORETUM_DISTRIBUTED_SRC_TRANSPORT_DATASTORE_H_
