//
// Created by Zhihan Guo on 4/4/23.
//

#ifndef ARBORETUM_SRC_COMMON_RWLOCK_H_
#define ARBORETUM_SRC_COMMON_RWLOCK_H_

#include <shared_mutex>
#include <atomic>

namespace arboretum {

struct RWLock {
  std::shared_timed_mutex latch;
  std::atomic<uint32_t> status{0}; // 1 bit for ex lock; the others for ref cnt
  char pad_[64 - sizeof(std::shared_timed_mutex) - sizeof(std::atomic<uint32_t>)]; // padding to avoid false sharing
  bool TryLockEX() {
    if (latch.try_lock()) {
      status = 1;
      return true;
    }
    return false;
  };
  void LockEX() {
    latch.lock();
    status = 1;
  }
  void LockSH() {
    latch.lock_shared();
    status.fetch_add(1 << 1);
  }
  void Lock(bool exclusive) {
    if (exclusive)
      LockEX();
    else
      LockSH();
  }
  bool IsEXLocked() const { return status == 1; };
  bool IsLocked() const { return status > 0; };
  void Unlock() {
    if (IsEXLocked()) {
      status = 0;
      latch.unlock();
    } else if (IsLocked()) {
      status.fetch_sub(1 << 1);
      latch.unlock_shared();
    }
  }
};

struct UpgradableRWLock {
  std::shared_timed_mutex latch;
  std::atomic<uint32_t> status{0}; // 1 bit for ex lock; the others for ref cnt
  char pad_[64 - sizeof(std::shared_timed_mutex) - sizeof(std::atomic<uint32_t>)]; // padding to avoid false sharing
  bool TryLockEX() {
    if (latch.try_lock()) {
      status = 1;
      return true;
    }
    return false;
  };
  void LockEX() {
    while(status != 0) continue;
    latch.lock();
    status = 1;
  }
  void LockSH() {
    while(status == 1) continue;
    latch.lock_shared();
    status.fetch_add(1 << 1);
  }
  void Lock(bool exclusive) {
    if (exclusive)
      LockEX();
    else
      LockSH();
  }
  bool IsEXLocked() const { return status == 1; };
  bool IsLocked() const { return status > 0; };
  void Unlock() {
    if (IsEXLocked()) {
      status = 0;
      latch.unlock();
    } else if (IsLocked()) {
      status.fetch_sub(1 << 1);
      latch.unlock_shared();
    }
  }
  void UpgradeLock() {
    // keep trying change the status to ex locked when there is no other readers (ref_cnt = 1)
    uint32_t expected = 1 << 1;
    while (status.compare_exchange_strong(expected, 1)) {
      expected = 1 << 1;
    }
    latch.unlock();
    latch.lock();
  }

};

}

#endif //ARBORETUM_SRC_COMMON_RWLOCK_H_
