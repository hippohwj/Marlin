//
// Created by Zhihan Guo on 4/15/23.
//

#include <thread>
#include <algorithm>
#include "gtest/gtest.h"

#include "db/CCManager.h"

using namespace arboretum;

// void ExclusiveLatchTask(std::atomic<uint32_t> * test_latch, int * counter, int tid) {
//   while (CCManager::AcquireLock(*test_latch, UPDATE) != OK) continue;
//   LOG_DEBUG("thd %d hold ex lock", tid);
//   for (int i = 0; i < 100000; i++) {
//     (*counter)++;
//   }
//   LOG_DEBUG("thd %d release ex lock", tid);
//   CCManager::ReleaseLock(*test_latch, UPDATE);
// }

// void SharedLatchTask(std::atomic<uint32_t> * test_latch, const int * counter, int tid) {
//   while (CCManager::AcquireLock(*test_latch, READ) != OK) continue;
//   LOG_DEBUG("thd %d hold sh latch", tid);
//   int total;
//   for (int i = 0; i < 100000; i++) {
//     total += *counter;
//   }
//   LOG_DEBUG("thd %d release sh latch", tid);
//   CCManager::ReleaseLock(*test_latch, READ);
// }

void ExclusiveLatchTask(std::atomic<uint32_t> *test_latch,
                        std::atomic<bool> *stop_sign, int tid) {
    while (!stop_sign->load()) {
        CCManager::AcquireLock(*test_latch, UPDATE, LockType::WAIT_DIE);
        // while ( CCManager::AcquireLock(*test_latch, UPDATE, LockType::WAIT_DIE) != OK) continue;
        // while ( CCManager::AcquireLock(*test_latch, UPDATE, LockType::NO_WAIT) != OK) continue;
        // LOG_DEBUG("thd %d acquire write lock ", tid);
        // LOG_DEBUG("thd %d release write lock ", tid);
        std::cout << "thd " << tid << " acquire write lock "<< std::endl;
        std::cout << "thd " << tid << " release write lock "<< std::endl;
        CCManager::ReleaseLock(*test_latch, UPDATE);
    }
}

void SharedLatchTask(std::atomic<uint32_t> * test_latch, std::atomic<bool> * stop_sign, int tid) {
  bool isRead = true;
  while (!stop_sign->load()) {
    // CCManager::AcquireLock(*test_latch, READ, LockType::NO_WAIT);
    auto access_type = (isRead)? READ:UPDATE;
    while (CCManager::AcquireLock(*test_latch, access_type, LockType::NO_WAIT) != OK) continue;
    // LOG_DEBUG("thd %d acquire read lock ", tid);
    // LOG_DEBUG("thd %d release read lock ", tid);
    std::cout << "thd " << tid << " acquire read lock "<< std::endl;
    std::cout << "thd " << tid << " release read lock "<< std::endl;
    CCManager::ReleaseLock(*test_latch, access_type);
    isRead = !isRead;
  } 

}



TEST(CCTest, MixTest) {
  std::atomic<uint32_t> test_lock{0};
  std::atomic<bool> stop_sign{false};
  // int counter = 0;
  std::vector<std::thread> threads;
  for (int i = 0; i < 32; i++) {
    if (i==9 || i==6) {
      threads.emplace_back(ExclusiveLatchTask, &test_lock, &stop_sign, i);
    } else {
      threads.emplace_back(SharedLatchTask, &test_lock, &stop_sign, i);
    }
  }

  sleep(10);
  stop_sign.store(true);
  std::for_each(threads.begin(),threads.end(), std::mem_fn(&std::thread::join));
  EXPECT_EQ(test_lock.load(), 0);
}


// TEST(CCTest, MixTest) {
//   std::atomic<uint32_t> test_lock{0};
//   int counter = 0;
//   std::vector<std::thread> threads;
//   for (int i = 0; i < 10; i++) {
//     threads.emplace_back(SharedLatchTask, &test_lock, &counter, i);
//     threads.emplace_back(ExclusiveLatchTask, &test_lock, &counter, i);

//   }
//   std::for_each(threads.begin(),threads.end(), std::mem_fn(&std::thread::join));
//   EXPECT_EQ(test_lock.load(), 0);
// }



// TEST(CCTest, WriteTest) {
//   std::atomic<uint32_t> test_lock{0};
//   int counter = 0;
//   std::vector<std::thread> threads;
//   for (int i = 0; i < 10; i++) {
//     threads.emplace_back(ExclusiveLatchTask, &test_lock, &counter, i);
//   }
//   std::for_each(threads.begin(),threads.end(), std::mem_fn(&std::thread::join));
//   EXPECT_EQ(counter, 100000 * 10);
// }

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  ::testing::GTEST_FLAG(filter) = "CCTest.*";
  return RUN_ALL_TESTS();
}