#ifndef ARBORETUM_SRC_REMOTE_ILOGSTORE_H_
#define ARBORETUM_SRC_REMOTE_ILOGSTORE_H_

#include "common/Common.h"
#include "RedisClient.h"
#include "AzureLogClient.h"
#include "db/log/LogEntry.h"
#include "common/OptionalGlobalData.h"
#include <azure/storage/blobs.hpp>



namespace arboretum {



class ILogStore {
 public:
  ILogStore(bool isSysLog=false);
  void Log(uint64_t node_id, LogEntry* logEntry);

  void GroupLog(uint64_t node_id, vector<LogEntry *> * logs, string& lsn);


  #if REMOTE_STORAGE_TYPE == REMOTE_STORAGE_REDIS

  // TODO: unify this interface for redis and azure
  void AsyncLog(uint64_t node_id, LogEntry* logEntry, const cpp_redis::reply_callback_t &reply_callback);

  void Log(uint64_t node_id, uint64_t txn_id, int status, string &data, const cpp_redis::reply_callback_t &reply_callback);

  void BatchRead(uint64_t node_id, string read_start, uint16_t batch_size, const cpp_redis::reply_callback_t &reply_callback);

  #elif REMOTE_STORAGE_TYPE == REMOTE_STORAGE_AZURE 

  future<RC> AsyncLog(uint64_t node_id, LogEntry* logEntry);

  void BatchRead(vector<LogEntry *> * log_entries);

  void BatchRead(function<void(LogEntry *log_entry)> read_callback);
  #endif



 private:
#if REMOTE_STORAGE_TYPE == REMOTE_STORAGE_REDIS
  RedisClient * client_;
#elif REMOTE_STORAGE_TYPE == REMOTE_STORAGE_AZURE 
  AzureLogClient * client_;
  LogLSN cur_lsn_;
  vector<uint8_t> batch_read_buffer_;

#endif
};

}



#endif //ARBORETUM_SRC_REMOTE_ILOGSTORE_H_