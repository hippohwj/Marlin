//
// Created by Zhihan Guo on 4/1/23.
//

#ifndef ARBORETUM_SRC_DB_CCMANAGER_H_
#define ARBORETUM_SRC_DB_CCMANAGER_H_

#include <Types.h>
namespace arboretum {
class CCManager {
 public:
  static RC TryEXLock(std::atomic<uint32_t> &state);
  static RC AcquireLock(std::atomic<uint32_t> &state, arboretum::AccessType type, LockType lock_type=LockType::NO_WAIT);
  static RC WaitUntilAcquireLock(std::atomic<uint32_t> &state, arboretum::AccessType type);
  static void ReleaseLock(std::atomic<uint32_t> &state, arboretum::AccessType type);
  
  static const uint32_t WriteLockMask = 1; // LSB used to indicate write lock
  static const uint32_t WriteWaitMask = 1 << 1; // indicate write wait
  static const uint32_t WriteMask = WriteLockMask | WriteWaitMask; // indicate locked by write or waited by write
  static const uint32_t ReadInc= 1 << 2; //  indicate read count progress unit


};
}

#endif //ARBORETUM_SRC_DB_CCMANAGER_H_
