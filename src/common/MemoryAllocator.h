//
// Created by Zhihan Guo on 3/31/23.
//

#ifndef ARBORETUM_SRC_COMMON_MEMORYALLOCATOR_H_
#define ARBORETUM_SRC_COMMON_MEMORYALLOCATOR_H_

#include <jemalloc/jemalloc.h>

namespace arboretum {
class MemoryAllocator {
 public:
  static void *Alloc(size_t size, size_t alignment = 0) {
    if (alignment == 0) {
      return malloc(size);
    }
    return aligned_alloc(alignment, size);
  };
  static void Dealloc(void *ptr) { free(ptr); };
};
}

#endif //ARBORETUM_SRC_COMMON_MEMORYALLOCATOR_H_
