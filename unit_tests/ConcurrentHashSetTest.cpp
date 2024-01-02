//
// Created by Zhihan Guo on 4/15/23.
//

#include <thread>
#include <algorithm>
#include "gtest/gtest.h"

#include "common/ConcurrentHashSet.h"
#include "db/ARDB.h"

using namespace arboretum;


void InsertMapTask(ConcurrentHashSet<uint64_t> * shared_map) {
  for (int i = 0; i < 100; i++) {
    vector<uint64_t> batch;
    for (int j = 0; j < 200; j++) {
      batch.push_back(i * 200 + j);
    }
    shared_map->insertBatch(&batch);
    // usleep(100);
  }
  // LOG_DEBUG("thd %d release ex lock", tid);
}

void AccessMapTask(ConcurrentHashSet<uint64_t> *shared_map) {
  // LOG_DEBUG("thd %d release sh latch", tid);
  for (int i = 0; i < 100000; i++) {
      uint32_t value = ARDB::RandUint64(100*200*2);
      if (shared_map->contains(value)) {
          usleep(100);
          shared_map->remove(value);
      }
  }
}

TEST(ConcurrentHashSetTest, MixTest) {
  ConcurrentHashSet<uint64_t> *shared_map = new ConcurrentHashSet<uint64_t>();
  std::vector<std::thread> threads;
  InsertMapTask(shared_map);

  for (int i = 0; i < 10; i++) {
    if (i == 0) {
    threads.emplace_back(InsertMapTask, shared_map);
    }
    threads.emplace_back(AccessMapTask, shared_map);
  }
  std::for_each(threads.begin(),threads.end(), std::mem_fn(&std::thread::join));

  EXPECT_EQ(shared_map->getLockValue(), 0);
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  ::testing::GTEST_FLAG(filter) = "ConcurrentHashSetTest.*";
  return RUN_ALL_TESTS();
}