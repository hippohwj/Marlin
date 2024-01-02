#ifndef ARBORETUM_SRC_REMOTE_REDISDATASTORE_H_
#define ARBORETUM_SRC_REMOTE_REDISDATASTORE_H_
#include "Common.h"
// #if REMOTE_STORAGE_TYPE == REMOTE_STORAGE_REDIS


#include <cpp_redis/cpp_redis>
#include "log/LogEntry.h"
#include <functional>

namespace arboretum {

class RedisClient {
 public:
  RedisClient(char * host, size_t port);

  // data storage
  RC StoreSync(const std::string &hset_name, const std::string &key, char *value, size_t sz);
  void BatchStoreSync(uint64_t i, std::multimap<std::string, std::string> map);
  void LoadSync(const std::string &hset_name, const std::string & key, std::string &basic_string);
  void StoreAndLoadSync(uint64_t granule_id, const std::string &key, char *value, size_t sz,
                        const std::string &load_key, std::string &load_data);
  int64_t CheckSize(uint64_t granule_id);
  void LogSync(std::string &stream, OID txn_id, std::multimap<std::string, std::string> &log);
  void read_logs_next_batch_sync(uint64_t node_id, string read_start, uint16_t batch_size,
                                       const cpp_redis::reply_callback_t &reply_callback);
  void store_sync_data(const std::string &hset_name, unordered_map<uint64_t, string> *kvs);
  // used for log replay
  void AsyncBatchKVStore(const std::string &hset_name, unordered_map<uint64_t, string> *kvs, const cpp_redis::reply_callback_t &reply_callback);

  void storeScan(const std::string &hset_name, uint32_t batch_size, function<string(cpp_redis::reply &reply)> scan_callback);
  // void log_sync_data(uint64_t node_id, uint64_t txn_id, int status,
  //                          string &data);

  void logDataAsync(uint64_t node_id, uint64_t txn_id, int status,
                           std::string &data, const cpp_redis::reply_callback_t &reply_callback);

  void log_sync_data(uint64_t node_id, uint64_t txn_id, int status,
                           std::string &data, string& lsn);
  void log_sync_data(uint64_t node_id, uint64_t txn_id, int status,
                     std::string &data, const cpp_redis::reply_callback_t &reply_callback);
  void GroupLogSync(uint64_t node_id, vector<LogEntry *> * logs, string& lsn);

 private:
  cpp_redis::client *client_;
  bool tls_{false};
};
}

// #endif // REMOTE_STORAGE_TYPE == REMOTE_STORAGE_REDIS

#endif //ARBORETUM_SRC_REMOTE_REDISDATASTORE_H_
