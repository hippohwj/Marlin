#ifndef ARBORETUM_SRC_DB_LOG_LOGREPLAYTHREAD_H_
#define ARBORETUM_SRC_DB_LOG_LOGREPLAYTHREAD_H_

// #include <cpp_redis/core/reply.hpp>
// #include "system/thread.h"
// #include "utils/helper.h"
#include "LogEntry.h"
#include "Common.h"
#include "remote/RedisClient.h"
#include "remote/ILogStore.h"
#include "remote/IDataStore.h"
#include "db/ARDB.h"
#include "common/semaphore_sync.h"
#include "common/BlockingQueue.h"


namespace arboretum {
// Log Replay
#define LOG_REPLAY_BATCH_SIZE           600


/**
 * TxnTracker tracks ongoing transactions for log replay service.
 */
class TxnTracker {
 public:
  TxnTracker() = default;
  ~TxnTracker() = default;

  uint32_t GetSize() {
    return txn_table_.size();
  }

  void ToString(string &str) {
    std::ostringstream ss;
    ss << "TxnTracker: \n";

    for(auto& it: txn_table_) {
      string tmp;
      it.second->to_string(tmp);
      ss << tmp << "\n";
    }
    str = ss.str();
  }


  bool IsEmpty() { return txn_table_.empty(); }
  /**
   * Should append info to txn tracker or flush data to the data store
   * @param entry
   * @return true => should append
   *         false => should remove info from tracker and flush data to the data store
   */
  tuple<bool, LogEntry *> AppendOrFlush(LogEntry* entry);

 private:
   std::map<uint64_t, LogEntry *> txn_table_;
};

class LogReplayThread {
 public:
  // LogReplayThread(uint64_t thd_id, uint64_t node_id);
  // LogReplayThread();
  LogReplayThread(ARDB *db);

  RC run();

  // static void parse_reply(cpp_redis::reply &reply, LogEntry &logEntry);

  #if REMOTE_STORAGE_TYPE == REMOTE_STORAGE_REDIS
  static void parse_reply(cpp_redis::reply &reply, vector<LogEntry *> * logs, string& lsn);
  // #elif REMOTE_STORAGE_TYPE == REMOTE_STORAGE_AZURE
  // static void parse_reply(vector<uint8_t> * buffer, vector<LogEntry *> * logs, string& lsn);
  #endif

  void TrackTxn(LogEntry &entry) { assert(false); };

 private:
  ARDB *db_{nullptr};
  uint64_t node_id;
  TxnTracker *txnTracker;
  ILogStore *replay_logstore;
  IDataStore *replay_datastore;
  SemaphoreSync *log_semaphore_;
};

class LogReplayProducer {
  public:
    LogReplayProducer(ARDB *db, ILogStore *replay_logstore, BlockingQueue<void *> * queue);
    RC run();

  private:
    ARDB *db_{nullptr};
    ILogStore *replay_logstore;
    BlockingQueue<void *> * logs_;
};

}




#endif //ARBORETUM_SRC_DB_LOG_LOGREPLAYTHREAD_H_