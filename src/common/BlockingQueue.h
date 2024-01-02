#ifndef ARBORETUM_SRC_COMMON_BLOCKINGQUEUE_H_
#define ARBORETUM_SRC_COMMON_BLOCKINGQUEUE_H_


#include <mutex>
#include <condition_variable>
#include <deque>

using namespace std;

namespace arboretum {

template <typename T>
class BlockingQueue
{
private:
    std::mutex              d_mutex;
    std::condition_variable d_condition;
    std::deque<T>           d_queue;
public:
    BlockingQueue() {}
    void Push(T value); 
    T Pop();
};

}

#endif //ARBORETUM_SRC_COMMON_CONCURRENTHASHMAP_H_