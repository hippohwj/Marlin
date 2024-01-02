//
// Created by Zhihan Guo on 4/17/23.
//

#ifndef ARBORETUM_BENCHMARK_YCSB_H_
#define ARBORETUM_BENCHMARK_YCSB_H_

#include "ycsb/YCSBConfig.h"
#include "ycsb/YCSBWorkload.h"
#include "common/BenchWorker.h"
#include "db/log/LogReplayThread.h"
#include "db/log/LogGroupCommitThread.h"
#include "db/ARDB.h"

namespace arboretum {
namespace ycsb {

// #define PRINT_BENCH_CONFIG(tpe, name, val) { \
// std::cout << #name << ": " << config->name << std::endl; }
// #define PRINT_DB_CONFIG4(tpe, name, val, func) { \
// std::cout << #name << ": " << func(name) << std::endl; }
// #define PRINT_DB_CONFIG3(tpe, name, val) { \
// std::cout << #name << ": " << (name) << std::endl; }
// #define PRINT_DB_CONFIG2(tpe, name) ;


void * start_replay_thread(void * thread) {
  ((LogReplayThread *)thread)->run();
  return nullptr;
}

void * start_gc_thread(void * thread) {
  ((GroupCommitThread *)thread)->run();
  return nullptr;
}

void ycsb(ARDB *db, YCSBConfig *config) {
  auto workload = YCSBWorkload(db, config);
  if (g_index_type != arboretum::REMOTE){
     g_storage_data_loaded = true;
     g_index_type = arboretum::BTREE;
     g_buf_type = arboretum::OBJBUF;
     std::vector<std::thread> threads;
     BenchWorker workers[config->num_workers_];
     auto *pthread_log_replay = new pthread_t;
     auto *pthread_groupcommit = new pthread_t;

     if (g_replay_enable) {
     // launch log replay thread
     pthread_create(pthread_log_replay, nullptr, start_replay_thread,
                    (void *) new LogReplayThread(db));
     LOG_INFO("Log Replay Thread Started");
     }

     if (g_groupcommit_enable) {
      // launch groupcommit thread
         pthread_create(pthread_groupcommit, nullptr, start_gc_thread,
                        (void *) new GroupCommitThread);
         LOG_INFO("Group Commit Thread Started");
     }

    // start worker threads
     for (int i = 0; i < config->num_workers_; i++) {
       workers[i].worker_id_ = i;
       threads.emplace_back(YCSBWorkload::Execute, &workload, &workers[i]);
     }

     
     // while (!g_warmup_finished) {
     while (true) {
       sleep(config->warmup_time_);
       if (arboretum::ARDB::CheckBufferWarmedUp()) {
         g_warmup_finished = true;
         LOG_INFO("Finished warming up");
         break;
       }
     }
     sleep(config->runtime_);
     g_terminate_exec = true;
     std::for_each(threads.begin(),threads.end(), std::mem_fn(&std::thread::join));
     // NOTE: must print db stats later than bench stats due to format json
     BenchWorker::PrintStats(config->num_workers_);
     
     if (g_groupcommit_enable) {
        LOG_INFO("Wait for group commit thread to finish...")
        pthread_join(*pthread_groupcommit, nullptr);
     }

     if (g_replay_enable) {
       LOG_INFO("Wait for replay thread to finish...")
       pthread_join(*pthread_log_replay, nullptr);
     }
     LOG_INFO("All threads finished")
  }
  Worker::PrintStats();
}


} // ycsb
} // arboretum

#endif //ARBORETUM_BENCHMARK_YCSB_H_
