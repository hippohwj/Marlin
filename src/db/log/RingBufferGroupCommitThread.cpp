#include "RingBufferGroupCommitThread.h"
#include "OptionalGlobalData.h"
#include "GlobalData.h"
#include "remote/ILogStore.h"
#include "Worker.h"
#include <algorithm>
#include "db/ClusterManager.h"

namespace arboretum {

  RingBufferGroupCommitThread::RingBufferGroupCommitThread() {
     commit_lsn = new ConcurrentLSN();
  }

  RC RingBufferGroupCommitThread::run() {
        if (g_groupcommit_size_based) {
            bool sync_end = false;
            bool gc_warm_up = (g_rampup_scaleout_enable)? false: true;
            // int commit_bound = g_gc_lognum_bound; 
            int commit_bound = (g_rampup_scaleout_enable)? gc_log_size.load(): g_gc_lognum_bound; 
            // int timeout_ms = g_gc_size_timeout_ms;
            int timeout_ms = (g_rampup_scaleout_enable)? 10: g_gc_size_timeout_ms;
            int divisor = (g_rampup_scaleout_enable && g_rampup_on_premise_size > 0)? g_rampup_on_premise_size: 1;

            while (!sync_end) {
                sync_end = cluster_manager->all_worker_done();
                vector<LogEntry *> buffer;
                if (!gc_warm_up) {
                    // commit_bound = gc_log_size.load();
                    // if (commit_bound >= g_gc_lognum_bound){
                    //     commit_bound = g_gc_lognum_bound; 
                    //     gc_warm_up = true;
                    // }
                    int log_size =  gc_log_size.load()/divisor;
                    if (log_size >= g_gc_lognum_bound){
                        commit_bound = g_gc_lognum_bound; 
                        // timeout_ms = g_gc_size_timeout_ms;
                        gc_warm_up = true;
                    } else {
                        // commit_bound = (log_size > 1)? (log_size/2):1;
                        commit_bound = log_size;
                    }
                } 
                gc_ring_buffer->CommitSnapshotForSize(
                    &buffer, commit_bound, timeout_ms);
                if (!buffer.empty()) {
                    auto start_time = GetSystemClock();
                    string flush_lsn;
                    g_log_store->GroupLog(g_node_id, &buffer, flush_lsn);
                    for (vector<LogEntry *>::iterator it = buffer.begin();
                         it < buffer.end(); it++) {
                        (*it)->SetLSN(flush_lsn);
                    }
                    size_t commit_size = buffer.size();
                    gc_ring_buffer->Commit(commit_size);
                    // update global newest commit lsn
                    commit_lsn->Set(flush_lsn);
                    cout << "XXX Group Commit to lsn: " << ConcurrentLSN::PrintStrFor(flush_lsn) << "takes " << nano_to_ms(GetSystemClock() - start_time)<< " ms"<< endl;
                    if (g_warmup_finished) {
                        Worker::dbstats_.int_stats_.gc_log_entry_count_ +=
                            commit_size;
                        Worker::dbstats_.int_stats_.gc_log_flush_count_ += 1;
                    }
                    M_ASSERT(g_gc_future,
                             "only support g_gc_future when "
                             "g_replay_from_buffer_enable is on");

                    // update lsn to future
                    // update lsn for each log entry and delete log entries
                    for (vector<LogEntry *>::iterator it = buffer.begin();
                         it < buffer.end(); it++) {
                        //   (*it)->SetLSNPromise(flush_lsn);
                        (*it)->SetPromise();
                    }
                    // buffer.clear();

                    if (g_warmup_finished) {
                        Worker::dbstats_.int_stats_.gc_log_time_ns_ +=
                            GetSystemClock() - start_time;
                    }
                }
            }

        } else {
        bool sync_end = false;
        while (!sync_end) {
          // sync_end = are_all_worker_done();
          sync_end = cluster_manager->all_worker_done();
          vector<LogEntry *> buffer;
          gc_ring_buffer->CommitSnapshot(&buffer);
          if (!buffer.empty()) {
              auto start_time = GetSystemClock();
              string flush_lsn;
              g_log_store->GroupLog(g_node_id, &buffer, flush_lsn);
           
              for (vector<LogEntry *>::iterator it = buffer.begin();
                   it < buffer.end(); it++) {
                  (*it)->SetLSN(flush_lsn);
              }

              size_t commit_size = buffer.size();
              gc_ring_buffer->Commit(commit_size);
              // update global newest commit lsn
              commit_lsn->Set(flush_lsn);
              if (g_warmup_finished) {
                  Worker::dbstats_.int_stats_.gc_log_entry_count_ +=
                      commit_size;
                  Worker::dbstats_.int_stats_.gc_log_flush_count_ += 1;
              }
              M_ASSERT(g_gc_future,
                       "only support g_gc_future when "
                       "g_replay_from_buffer_enable is on");

              // update lsn to future
              // update lsn for each log entry and delete log entries
              for (vector<LogEntry *>::iterator it = buffer.begin();
                   it < buffer.end(); it++) {
                //   (*it)->SetLSNPromise(flush_lsn);
                  (*it)->SetPromise();
              }
              // buffer.clear();

              if (g_warmup_finished) {
                  Worker::dbstats_.int_stats_.gc_log_time_ns_ +=
                      GetSystemClock() - start_time;
              }

              usleep(g_groupcommit_wait_us);
          } else {
              usleep(g_groupcommit_wait_us * 2);
          }
        }
      }
        M_ASSERT(gc_ring_buffer->CommitAll(), "group commit buffer is not empty, current size is %d waiting for commit", gc_ring_buffer->SizeToCommit());
       // add a marker for last entry
       gc_ring_buffer->Write(LogEntry::NewFakeEntry());
       gc_ring_buffer->Commit(1);
       cluster_manager->receive_groupcommit_finished();
       // summarize stats
       arboretum::Worker::latch_.lock();
       Worker::SumStats();
       arboretum::Worker::latch_.unlock();
       LOG_INFO("Ring Buffer Group Commit Thread finished!");
  }
}