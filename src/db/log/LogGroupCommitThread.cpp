#include "LogGroupCommitThread.h"
#include "OptionalGlobalData.h"
#include "GlobalData.h"
#include "remote/ILogStore.h"
#include "Worker.h"
#include <algorithm>
#include "db/ClusterManager.h"

namespace arboretum {

  GroupCommitThread::GroupCommitThread() {
     gc_buffer = NEW(GroupLogBuffer);
     commit_lsn = new ConcurrentLSN();
  }

  RC GroupCommitThread::run() {
        bool sync_end = false;
        while (!sync_end) {
          // sync_end = are_all_worker_done();
          sync_end = cluster_manager->all_worker_done();

          gc_buffer->AcquireLock();
          if (!gc_buffer->isEmpty()) {
            auto start_time = GetSystemClock();
            string flush_lsn; 
            vector<LogEntry *> * internal_buffer = gc_buffer->GetBuffer();
            vector<LogEntry *> buffer(internal_buffer->size());
            std::copy(internal_buffer->begin(), internal_buffer->end(), buffer.begin());
            gc_buffer->Clear();
            gc_buffer->ReleaseLock();
            g_log_store->GroupLog(g_node_id, &buffer, flush_lsn);
            // update global newest commit lsn
            commit_lsn->Set(flush_lsn);
            // unordered_map<uint64_t, string> granule_tracker(buffer.size());
            unordered_map<uint64_t, string> granule_tracker;
            // cout << "XXX group commit lsn for granules[ ";
            for (vector<LogEntry *>::iterator it = buffer.begin();
                 it < buffer.end(); it++) {
                unordered_set<uint64_t>* granules = (*it)->GetGranules();
                for (auto git = granules->begin(); git!= granules->end();git++) {
                    auto granule_id = *git;
                    granule_tracker[granule_id] = flush_lsn;
                    // cout << granule_id << ", ";
                }
            }
            // cout << " ] is " << ConcurrentLSN::PrintStrFor(flush_lsn) << endl;
            granule_commit_tracker->InsertBatch(&granule_tracker);
            // cout << "granule_commit_tracker is "<<granule_commit_tracker->PrintStr() << endl; 
            size_t commit_size = buffer.size();
            // LOG_INFO("GroupCommit Success,commit size is %u, update to LSN(%s), granules num %d", commit_size, ConcurrentLSN::PrintStrFor(flush_lsn).c_str(), granule_tracker.size());
            if (g_warmup_finished) {
               Worker::dbstats_.int_stats_.gc_log_entry_count_ += commit_size;
               Worker::dbstats_.int_stats_.gc_log_flush_count_ += 1;
            }

            if (g_gc_future) {
              // update lsn to future 
              //update lsn for each log entry and delete log entries
              for(vector<LogEntry *>::iterator it = buffer.begin(); it < buffer.end(); it++){
                   (*it)->SetLSNPromise(flush_lsn);
              }
              // buffer.clear();
            } else {
              // set lsn to log_entry and update epoch 
              //update lsn for each log entry and delete log entries
              for(vector<LogEntry *>::iterator it = buffer.begin(); it < buffer.end(); it++){
                   (*it)->SetLSN(flush_lsn);
              }
              // buffer->clear();
              gc_buffer->IncreaseEpoch();
            }

            if (g_warmup_finished) {
              Worker::dbstats_.int_stats_.gc_log_time_ns_ += GetSystemClock() - start_time;
            }
            usleep(g_groupcommit_wait_us);
          } else {
            // LOG_INFO("XXX Empty Buffer for Group Commit");
            gc_buffer->ReleaseLock(); 
            usleep(g_groupcommit_wait_us * 2);
          }
        }

       M_ASSERT(gc_buffer->isEmpty(), "group commit buffer is not empty, current size is %d", gc_buffer->BufferSize());
       cluster_manager->receive_groupcommit_finished();
       // summarize stats
       arboretum::Worker::latch_.lock();
       Worker::SumStats();
       arboretum::Worker::latch_.unlock();
       LOG_INFO("Group Commit Thread finished!");
  }
}