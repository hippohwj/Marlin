#ifndef ARBORETUM_SRC_COMMON_LOCKS_SEMAPHORE_SYNC_H_
#define ARBORETUM_SRC_COMMON_LOCKS_SEMAPHORE_SYNC_H_

#include "Types.h"

namespace arboretum {

class SemaphoreSync {
public:
    SemaphoreSync();
    uint32_t          incr();
    uint32_t          decr();
    void              wait();
    void              reset();
private:
    uint32_t          _semaphore;
    pthread_cond_t *  _cond;
    pthread_mutex_t * _mutex;
};

}
#endif //ARBORETUM_SRC_COMMON_LOCKS_SEMAPHORE_SYNC_H_

