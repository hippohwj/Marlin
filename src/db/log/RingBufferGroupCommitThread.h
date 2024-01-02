#ifndef ARBORETUM_SRC_DB_LOG_RINGBUFFERGROUPCOMMITTHREAD_H_
#define ARBORETUM_SRC_DB_LOG_RINGBUFFERROUPCOMMITTHREAD_H_

// #include "Common.h"
#include "remote/RedisClient.h"

namespace arboretum {

class RingBufferGroupCommitThread {
 public:
  RingBufferGroupCommitThread();

  RC run();
};

}


#endif //ARBORETUM_SRC_DB_LOG_RINGBUFFERGROUPCOMMITTHREAD_H_