#include "common/Common.h"
#include "CCManager.h"

namespace arboretum {

RC CCManager::AcquireLock(std::atomic<uint32_t> &state,
                          arboretum::AccessType type, LockType lock_type) {
    if (lock_type == LockType::NO_WAIT) {
        // NO_WAIT impl.
        uint32_t unlocked = 0;
        if (type == UPDATE) {
            return state.compare_exchange_strong(unlocked, WriteLockMask) ?
            OK: ABORT;
        }
        // Acquire read lock
        auto current = state.load(std::memory_order_acquire);
        if (current & WriteMask) {
            return ABORT;  // write locked
        }
        uint32_t rd_locked = current + ReadInc;
        // not write locked nor write waiting
        while (!state.compare_exchange_strong(current, rd_locked)) {
            if (current & WriteMask) return ABORT;
            rd_locked = current + ReadInc;
        }
        return OK;
    } else if (lock_type == LockType::WAIT_DIE) {
        return WaitUntilAcquireLock(state, type);
    } else if (lock_type == LockType::NO_LOCK) {
      return OK;
    } else {
        M_ASSERT(false,
                 "Unsupported lock type, currently only support NO_WAIT and "
                 "WAIT_DIE");
    }
}

RC CCManager::WaitUntilAcquireLock(std::atomic<uint32_t> &state,
                                     arboretum::AccessType type) {
  if (type == UPDATE) {
    uint32_t unlocked = 0;
    bool is_first_wait = false;
    while (true) {
        unlocked = (is_first_wait)? WriteWaitMask: 0;
        if (state.compare_exchange_strong(unlocked, WriteLockMask,
                                          std::memory_order_acquire)) {
            // Successfully acquired the write lock
            break;
        }
        if (!is_first_wait && ((unlocked & WriteMask) == 0)) {
            auto pre_state = state.fetch_or(WriteWaitMask, std::memory_order_acquire);
            // if ((pre_state & WriteWaitMask) == 0) {
            if ((pre_state & WriteMask) == 0) {
              // first thread to wait
              is_first_wait = true;
            }
        } else {
          std::this_thread::yield(); 
        }
    }
    return OK;
  } else {
    // Acquire read lock
    uint32_t current = state.load(std::memory_order_acquire);
    while (true) {
        if ((current & WriteMask) == 0) {
          // if current is read lock
            if (state.compare_exchange_strong(current, current + ReadInc,
                                              std::memory_order_acquire)) {
                break;
            }
        } else {
          // if current is write lock or write waiting state
            current = state.load(std::memory_order_acquire);
        }
    }
    return OK;
  }
}

void CCManager::ReleaseLock(std::atomic<uint32_t> &state, arboretum::AccessType type) {
  if (type == READ) {
    uint32_t prev = state.fetch_sub(ReadInc,  std::memory_order_release);
    M_ASSERT(prev > 0, "prev should be bigger than 0, which is %d", prev);
  } else {
    state.store(0,  std::memory_order_release);
  }
}

RC CCManager::TryEXLock(std::atomic<uint32_t> &state) {
  if (state > 0) return ABORT;
  uint32_t unlocked = 0;
  uint32_t wr_locked = 1;
  return state.compare_exchange_strong(unlocked, wr_locked) ? OK : ABORT;
}


} // arboretum