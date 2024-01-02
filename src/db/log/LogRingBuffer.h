#ifndef ARBORETUM_SRC_DB_LOG_LOGRINGBUFFER_H_
#define ARBORETUM_SRC_DB_LOG_LOGRINGBUFFER_H_

#include "common/Common.h"
#include "log/LogEntry.h"
// #include <iostream>
// #include <vector>
#include <thread>
#include <mutex>
#include <condition_variable>

using namespace std::chrono_literals;


namespace arboretum {

template <typename T>
class LogRingBuffer {
   public:
    LogRingBuffer(int size)
        : buffer_(size), size_(size), replayIdx_(0), writeIdx_(0), commitIdx_(0){}

    void Write(const T& data) {
        std::unique_lock<std::mutex> lock(mutex_);
        canWriteCondition_.wait(lock, [this] { return CanWrite(); });
        buffer_[writeIdx_] = data;
        writeIdx_ = (writeIdx_ + 1) % size_;
        lock.unlock();
        canCommitCondition_.notify_one();
    }

    T Replay() {
        return NonBlockingReplay();
    }

    T BlockingReplay() {
        std::unique_lock<std::mutex> lock(mutex_);
        // cout << "XXX enter blocking replay: replayIdx_ is " << replayIdx_ << "; commitIdx_ is " << commitIdx_ << endl;
        canReplayCondition_.wait(lock, [this] { return CanReplay(); });
        T data = buffer_[replayIdx_];
        replayIdx_ = (replayIdx_ + 1) % size_;
        lock.unlock();

        if (g_replay_backcontrol_enable) {
            commitBackOffCondition_.notify_one();
        }
        canWriteCondition_.notify_one();
        return data;
    }

    T NonBlockingReplay() {
        std::unique_lock<std::mutex> lock(mutex_);
        if (CanReplay()) {
            T data = buffer_[replayIdx_];
            replayIdx_ = (replayIdx_ + 1) % size_;
            lock.unlock();
            if (g_replay_backcontrol_enable) {
                commitBackOffCondition_.notify_one();
            }
            canWriteCondition_.notify_one();
            return data;
        } else {
            lock.unlock();
            return nullptr;
        }
    }

    bool CommitSnapshot(vector<T>* snapshot_buffer) {
        int commitIdx_snapshot;
        int writeIdx_snapshot;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            commitIdx_snapshot = commitIdx_;
            writeIdx_snapshot = writeIdx_;
            // TODO: move this copy out of critical lock section and optimize
            // with std::copy
            for (int i = commitIdx_snapshot; i != writeIdx_snapshot;
                 i = ((i + 1) % size_)) {
                snapshot_buffer->push_back(buffer_[i]);
            }
        }

        // //TODO: optimize with std::copy
        // for (size_t i = commitIdx_snapshot; i != writeIdx_snapshot; i =
        // ((i+1) % size_)) {
        //   snapshot_buffer->push_back(buffer_[i]);
        // }
    }


    void CommitSnapshotForSize(vector<T>* snapshot_buffer, int commit_size, size_t timeout_ms) {
        std::unique_lock<std::mutex> lock(mutex_);
        // canCommitCondition_.wait_for(
        //     lock,timeout_ms*1ms,[this, &commit_size] { return CanCommit(commit_size); });
        // auto start = std::chrono::high_resolution_clock::now(); 
        bool is_finish = canCommitCondition_.wait_for(
            lock,std::chrono::milliseconds(timeout_ms),[this, &commit_size] { return CanCommit(commit_size); });
        // lock.unlock();
        // std::unique_lock<std::mutex> lock1(mutex_);
        // if (g_replay_backcontrol_enable) {
        // //   commitBackOffCondition_.wait(
        // //     lock1,[this, &commit_size] { return !BackControlFullByCommit(commit_size); });
        //     bool is_finish = commitBackOffCondition_.wait_for(
        //     lock,std::chrono::milliseconds(timeout_ms*2),[this, &commit_size] { return !BackControlFullByCommit(commit_size);});
        // } 
      
        // auto end = std::chrono::high_resolution_clock::now(); 

      
        // canCommitCondition_.wait(
        //     lock, [this, &commit_size] { return CanCommit(commit_size); });
        int commitIdx_snapshot;
        int writeIdx_snapshot;
        commitIdx_snapshot = commitIdx_;
        writeIdx_snapshot = writeIdx_;
        lock.unlock();
        for (int i = commitIdx_snapshot; i != writeIdx_snapshot;
             i = ((i + 1) % size_)) {
            snapshot_buffer->push_back(buffer_[i]);
        }
        // if (!is_finish) {
        //     cout<< "XXX Group Commit Timeout ("<< timeout_ms << " ms), buffer size is " << snapshot_buffer->size() << endl;
        // } else {
        //     cout<< "XXX Group Commit Normal, buffer size is " << snapshot_buffer->size() << ", duration ms is " << std::chrono::duration_cast<std::chrono::milliseconds>(
        //                 end - start).count() << endl;
        // }
    }

    // void Commit(int commit_size) {
    //     std::unique_lock<std::mutex> lock(mutex_);
    //     canCommitCondition_.wait(
    //         lock, [this, &commit_size] { return CanCommit(commit_size); });
    //     commitIdx_ = (commitIdx_ + commit_size) % size_;
    //     // cout << "XXX CommitIdx_ is " << commitIdx_ << ", replayIdx_ is " << replayIdx_ <<  ", after commit" << endl;
    //     lock.unlock();

    //     canReplayCondition_.notify_one();
    // }

    void Commit(int commit_size) {
      {
        std::lock_guard<std::mutex> lock(mutex_);        
        commitIdx_ = (commitIdx_ + commit_size) % size_;
      }
      canReplayCondition_.notify_one();
    }

    bool CommitAll() {
      std::lock_guard<std::mutex> lock(mutex_);
      return IsCommitEmptyForSize(1);
    }

    size_t SizeToCommit() {
      //TODO: check correctness
      std::lock_guard<std::mutex> lock(mutex_);
      return (writeIdx_ - commitIdx_);
    }

    // size_t SizeToReplay() {

    // }


   private:

    bool CanWrite() const { return !IsFull(); }
    // bool CanWrite() const { return (g_replay_backcontrol_enable)? (!BackControlFull()):(!IsFull()); }
    bool CanReplay() const { return !IsReplayEmpty(); }
    bool CanCommit(int commit_size) const { 
        int gap = (writeIdx_ >= commitIdx_)?(writeIdx_ - commitIdx_) : (size_ - commitIdx_ + writeIdx_); 
        return gap >= commit_size;
    }

    // bool CanCommit(int commit_size) const { return !IsCommitEmptyForSize(commit_size); }
    // bool CanCommit(int commit_size) const { 
    //     int gap = (writeIdx_ >= commitIdx_)?(writeIdx_ - commitIdx_) : (size_ - commitIdx_ + writeIdx_); 
    //     return (g_replay_backcontrol_enable)? (gap >=commit_size && !BackControlFullByCommit(commit_size)):gap >= commit_size;
    // }
    // bool CanCommit(int commit_size) const { 
    //     int relative_commit_idx = (commitIdx_ >= replayIdx_)? (commitIdx_ - replayIdx_) : (size_ - replayIdx_ + commitIdx_); 
    //     int relative_write_idx = (writeIdx_ >= replayIdx_)? (writeIdx_ - replayIdx_) : (size_ - replayIdx_ + writeIdx_);
    //     return relative_commit_idx + commit_size <= relative_write_idx;
    // }
    // bool CanCommit() const { return !IsCommitEmpty(); }

    bool BackControlFull() const {return  ((writeIdx_ + size_ - replayIdx_) % size_) >= g_replay_backcontrol_size; }
    bool BackControlFullByCommit(int commit_size) const {return  ((commitIdx_ + commit_size + size_ - replayIdx_) % size_) >= g_replay_backcontrol_size; }

    bool IsFull() const { return ((writeIdx_ + 1) % size_) == replayIdx_; }

    //TODO: check correctness
    bool IsCommitEmptyForSize(int commit_size) const {return ((commitIdx_ + commit_size - 1) % size_) == writeIdx_; }

    bool IsReplayEmpty() const { return replayIdx_ == commitIdx_;}

    std::vector<T> buffer_;
    int size_;
    int replayIdx_;
    int writeIdx_;
    int commitIdx_;

    std::mutex mutex_;
    std::condition_variable canWriteCondition_;
    std::condition_variable canCommitCondition_;
    std::condition_variable commitBackOffCondition_;
    std::condition_variable canReplayCondition_;
};

    template class LogRingBuffer<LogEntry *>;

}  // namespace arboretum

#endif //ARBORETUM_SRC_DB_LOG_LOGRINGBUFFER_H_
