#include "GroupLogBuffer.h"

namespace arboretum {

void GroupLogBuffer::PutLog(LogEntry * log_entry) {
    buffer_.push_back(log_entry);
}

bool GroupLogBuffer::isEmpty() {
    return buffer_.empty();
}

size_t GroupLogBuffer::BufferSize() {
    return buffer_.size();
}

void GroupLogBuffer::Clear() {
    return buffer_.clear();
}

vector<LogEntry *> * GroupLogBuffer::GetBuffer(){
    return &buffer_;
} 


void GroupLogBuffer::AcquireLock() {
    lock_.lock();
}

void GroupLogBuffer::ReleaseLock() {
    lock_.unlock();
} 

uint32_t GroupLogBuffer::GetEpoch() {
    return epoch_.load();
}

void GroupLogBuffer::IncreaseEpoch() {
    epoch_++;
}

}



// #include <iostream>
// #include <vector>
// #include <thread>
// #include <mutex>
// #include <condition_variable>

// template <typename T>
// class RingBuffer {
// public:
//     RingBuffer(int size) : buffer_(size), size_(size), readIdx_(0), writeIdx_(0) {}

//     void Write(const T& data) {
//         std::unique_lock<std::mutex> lock(mutex_);
//         fullCondition_.wait(lock, [this] { return !IsFull(); });

//         buffer_[writeIdx_] = data;
//         writeIdx_ = (writeIdx_ + 1) % size_;

//         lock.unlock();
//         emptyCondition_.notify_one();
//     }

//     T Read() {
//         std::unique_lock<std::mutex> lock(mutex_);
//         emptyCondition_.wait(lock, [this] { return !IsEmpty(); });

//         T data = buffer_[readIdx_];
//         readIdx_ = (readIdx_ + 1) % size_;

//         lock.unlock();
//         fullCondition_.notify_one();

//         return data;
//     }

// private:
//     bool IsFull() const {
//         return ((writeIdx_ + 1) % size_) == readIdx_;
//     }

//     bool IsEmpty() const {
//         return readIdx_ == writeIdx_;
//     }

//     std::vector<T> buffer_;
//     int size_;
//     int readIdx_;
//     int writeIdx_;

//     std::mutex mutex_;
//     std::condition_variable fullCondition_;
//     std::condition_variable emptyCondition_;
// };

// // Example usage
// int main() {
//     RingBuffer<int> ringBuffer(10);

//     // Producer thread
//     std::thread producer([&]() {
//         for (int i = 0; i < 20; ++i) {
//             ringBuffer.Write(i);
//             std::this_thread::sleep_for(std::chrono::milliseconds(100));
//         }
//     });

//     // Consumer thread
//     std::thread consumer([&]() {
//         for (int i = 0; i < 20; ++i) {
//             int data = ringBuffer.Read();
//             std::cout << "Consumed: " << data << std::endl;
//             std::this_thread::sleep_for(std::chrono::milliseconds(200));
//         }
//     });

//     producer.join();
//     consumer.join();

//     return 0;
// }
