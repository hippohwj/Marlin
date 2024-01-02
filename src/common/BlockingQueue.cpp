#include "BlockingQueue.h"

namespace arboretum {

template <typename T>
void BlockingQueue<T>::Push(T value) {
    {
        std::unique_lock<std::mutex> lock(this->d_mutex);
        d_queue.push_front(value);
    }
    this->d_condition.notify_one();
}

template <typename T>
T BlockingQueue<T>::Pop() {
    std::unique_lock<std::mutex> lock(this->d_mutex);
    this->d_condition.wait(lock, [=] { return !this->d_queue.empty(); });
    T rc(std::move(this->d_queue.back()));
    this->d_queue.pop_back();
    return rc;
}

// template class BlockingQueue<SundialRPCServerImpl::CallData *>;
template class BlockingQueue<void *>;


}  // namespace arboretum