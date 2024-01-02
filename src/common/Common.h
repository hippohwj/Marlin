//
// Created by Zhihan Guo on 3/31/23.
//

#ifndef ARBORETUM_SRC_COMMON_COMMON_H_
#define ARBORETUM_SRC_COMMON_COMMON_H_

#include <algorithm>
#include <cstdlib>
#include <functional>
#include <iostream>
#include <thread>
#include <unistd.h>
#include <map>
#include <string>
#include <unordered_set>
#include "GlobalData.h"
#include "SharedPtr.h"
#include "Types.h"

using namespace std;
namespace arboretum {

//TODO(Hippo): move to another file
#define COMPILER_BARRIER asm volatile("" ::: "memory");

// returns true if cas is successful
#define ATOM_CAS(dest, oldval, newval) \
    __sync_bool_compare_and_swap(&(dest), oldval, newval)

// on draco, each pause takes approximately 3.7 ns.
#define PAUSE __asm__ ( "pause;" );

#define CHAR_PTR_CAST(ptr) reinterpret_cast<char*>(ptr)

#define ATOM_ADD_FETCH(dest, value) \
    __sync_add_and_fetch(&(dest), value)

inline float nano_to_s(uint64_t nano_duration) {
    return static_cast<float>(nano_duration) / 1000000000;
}

inline float nano_to_ms(uint64_t nano_duration) {
    return static_cast<float>(nano_duration) / 1000000;
}
inline float nano_to_us(uint64_t nano_duration) {
    return static_cast<float>(nano_duration) / 1000;
}

inline uint64_t GetSystemClock() {
  // in nanosecond
#if defined(__i386__)
  uint64_t ret;
  __asm__ __volatile__("rdtsc" : "=A" (ret));
  return ret;
#elif defined(__x86_64__)
  unsigned hi, lo;
  __asm__ __volatile__ ("rdtsc" : "=a"(lo), "=d"(hi));
  uint64_t ret = ((uint64_t) lo) | (((uint64_t) hi) << 32);
  // ret = (uint64_t) ((double) ret / g_cpu_freq); // nano second
  ret = (uint64_t) ((double) ret / 2.19); // nano second
  return ret;
#else
  LOG_ERROR("Instruction set architecture is not supported yet.");
#endif
}

inline void CalculateCPUFreq() {
  // measure CPU Freqency
  auto tp = NEW(timespec);
  clock_gettime(CLOCK_REALTIME, tp);
  uint64_t start_t = tp->tv_sec * 1000000000 + tp->tv_nsec;
  auto starttime = GetSystemClock();
  sleep(1);
  auto endtime = GetSystemClock();
  clock_gettime(CLOCK_REALTIME, tp);
  auto end_t = tp->tv_sec * 1000000000 + tp->tv_nsec;
  auto runtime = end_t - start_t;
  // g_cpu_freq = 1.0 * (endtime - starttime) * g_cpu_freq / runtime;
  // LOG_INFO("CPU freqency is %.2f", g_cpu_freq);

}

inline uint64_t GetGranuleID(uint64_t key) {
  return key/(g_granule_size_mb*1024);
}

inline uint64_t GetDataStoreNodeID(uint64_t key) {
  return GetGranuleID(key) / g_granule_num_per_node;
}

inline uint64_t GetDataStoreNodeByGranuleID(uint64_t gran_id) {
  return gran_id / g_granule_num_per_node;
}

}

#endif //ARBORETUM_SRC_COMMON_COMMON_H_
