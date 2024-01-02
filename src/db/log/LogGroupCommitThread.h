#ifndef ARBORETUM_SRC_DB_LOG_LOGGROUPCOMMITTHREAD_H_
#define ARBORETUM_SRC_DB_LOG_LOGGROUPCOMMITTHREAD_H_

// #include "Common.h"
#include "remote/RedisClient.h"

namespace arboretum {

class GroupCommitThread {
 public:
  GroupCommitThread();

  RC run();

 private:
  uint64_t node_id;
};

}


#endif //ARBORETUM_SRC_DB_LOG_LOGGROUPCOMMITTHREAD_H_