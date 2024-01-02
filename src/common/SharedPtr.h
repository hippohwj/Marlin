//
// Created by Zhihan Guo on 4/6/23.
//

#ifndef ARBORETUM_SRC_COMMON_SHAREDPTR_H_
#define ARBORETUM_SRC_COMMON_SHAREDPTR_H_

#include "GlobalData.h"

namespace arboretum {

class SharedPtr {
 public:
  SharedPtr() {
    ref_cnt_ = NEW(std::atomic<uint64_t>)(1);
  }
  void Init(void * ptr = nullptr) {
    ptr_ = ptr;
    // (*ref_cnt_) = 1;
    ref_cnt_ = NEW(std::atomic<uint64_t>)(1);
  }

  void InitFrom(SharedPtr& sp) {
    ptr_ = sp.ptr_;
    ref_cnt_ = sp.ref_cnt_;
    (*ref_cnt_)++;
  }

  ~SharedPtr() {
    Free();
  }

  bool Free() {
    auto cnt = (*ref_cnt_).fetch_sub(1);
    if (cnt == 1) {
      DEALLOC(ref_cnt_);
      DEALLOC(ptr_);
      return true;
    }
    return false;
  }

  void * Get() { return ptr_; };

  template<typename T>
  T& operator*() { return *(reinterpret_cast<T *>(ptr_)); };

  template<typename T>
  T* operator->() { return ptr_; };

 private:
  std::atomic<uint64_t> * ref_cnt_;
  void* ptr_{nullptr};

};

} // arboretum

#endif //ARBORETUM_SRC_COMMON_SHAREDPTR_H_
