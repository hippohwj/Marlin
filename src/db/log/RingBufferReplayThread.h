#ifndef ARBORETUM_SRC_DB_LOG_RINGBUFFERREPLAYTHREAD_H_
#define ARBORETUM_SRC_DB_LOG_RINGBUFFERREPLAYTHREAD_H_

// #include <cpp_redis/core/reply.hpp>
// #include "system/thread.h"
// #include "utils/helper.h"
#include "LogEntry.h"
#include "LogReplayThread.h"
#include "Common.h"
#include "remote/RedisClient.h"
#include "remote/ILogStore.h"
#include "remote/IDataStore.h"
#include "db/ARDB.h"
#include "common/semaphore_sync.h"
#include "common/BlockingQueue.h"


namespace arboretum {

class RingBufferReplayThread {
 public:
  RingBufferReplayThread(ARDB *db);

  RC run();

  void TrackTxn(LogEntry &entry) { assert(false); };

 private:
  ARDB *db_{nullptr};
  uint64_t node_id;
  TxnTracker *txnTracker;
  ILogStore *replay_logstore;
  IDataStore *replay_datastore;
  SemaphoreSync *log_semaphore_;
};

}
#endif //ARBORETUM_SRC_DB_LOG_RINGBUFFERREPLAYTHREAD_H_