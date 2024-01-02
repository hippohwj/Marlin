//
// Created by Zhihan Guo on 3/31/23.
//

#ifndef ARBORETUM_SRC_COMMON_WORKER_H_
#define ARBORETUM_SRC_COMMON_WORKER_H_

#include "Common.h"
#include <algorithm>


namespace arboretum {

#define DB_INT_STATS(x) x(accesses_, 0, db) x(misses_, 0, db) x(evict_count_, 0, db) x(log_count_, 0, db) x(txn_count_, 0, db)  x(remote_req_count_, 0, db) x(local_req_count_, 0, db)  x(log_size_, 0, db) x(miss_load_count_, 0, db) x(evict_flush_count_, 0, db)  x(gc_log_entry_count_, 0, db) x(gc_log_flush_count_, 0, db)
#define DB_TIME_STATS(x) x(cc_time_, 0, db) x(idx_time_, 0, db) x(evict_time_, 0, db) x(log_time_, 0, db) x(txn_time_, 0, db) x(evict_flush_load_time_, 0, db) x(miss_load_time_, 0, db) x(idx_search_time_, 0, db) x(gc_log_time_, 0, db) x(txn_exe_time_, 0, db)  x(txn_commit_time_, 0, db) 

#define DECL_INT_STATS(var, val, prefix) uint64_t var{val};
#define RESET_INT_STATS(var, val, prefix) var = val;
#define SUM_INT_STATS(var, val, prefix) \
prefix##sum_stats_.int_stats_.var += prefix##stats_.int_stats_.var; 

#define PRINT_INT_STATS(var, val, prefix) \
std::cout << "Total " << #var << ": " << prefix##sum_stats_.int_stats_.var     \
<< std::endl;                                                                  \
if (g_save_output) { g_out_file << "\"" << #var << "\": " <<                             \
prefix##sum_stats_.int_stats_.var << ", "; }

#define DECL_TIME_STATS(var, val, prefix) uint64_t var##ns_{val}; double sum_##var##ms_{val};
#define RESET_TIME_STATS(var, val, prefix) var##ns_ = val; sum_##var##ms_ = val;
#define SUM_TIME_STATS(var, val, prefix) \
prefix##sum_stats_.int_stats_.sum_##var##ms_ += prefix##stats_.int_stats_.var##ns_ / 1000000.0;
#define PRINT_TIME_STATS_MS(var, val, prefix)                 \
std::cout << "Total " << #var << " (in ms): " <<              \
prefix##sum_stats_.int_stats_.sum_##var##ms_ << std::endl;    \
if (g_save_output) { g_out_file << "\"" << #var << "ms\": " \
<< prefix##sum_stats_.int_stats_.sum_##var##ms_ << ", "; }

struct DBStats {
  struct IntStats {
    DB_INT_STATS(DECL_INT_STATS)
    DB_TIME_STATS(DECL_TIME_STATS)
    void Reset() {
      DB_INT_STATS(RESET_INT_STATS)
      DB_TIME_STATS(RESET_TIME_STATS)
    };
  };
  struct VecStats {
    void Reset() {};
  };
  IntStats int_stats_{};
  VecStats vec_stats_{};
  void Reset() {
    int_stats_.Reset();
    vec_stats_.Reset();
  }
};

class Worker {
 public:
  static vector<uint64_t> throughput_per_sec;
  static vector<uint64_t> abort_per_sec;
  static atomic<uint64_t> commit_count_;
  static atomic<uint64_t> abort_count_;
  static vector<float> user_txn_latency_accum;
  static vector<float> migr_txn_latency_accum;
  static vector<double> migr_critical_latency;

  static vector<double> migrate_starts;
  static vector<double> migrate_critical_starts;
  static vector<double> migrate_critical_ends;
  static vector<double> migrate_ends;

  static OID GetThdId() { return thd_id_; };
  static size_t GetMaxThdTxnId() { return max_txn_id_; };

  static void SetThdId(OID i) { thd_id_ = i; };
  static void SetMaxThdTxnId(OID i) { max_txn_id_ = i; };
  static void IncrMaxThdTxnId() { max_txn_id_++; };

  static void SumStats() {
    DB_INT_STATS(SUM_INT_STATS)
    DB_TIME_STATS(SUM_TIME_STATS)
  }

  static void CalcRealTimeThroughput() {
   if (throughput_per_sec.size() >= 2) {
     for (size_t i = throughput_per_sec.size() - 1; i >= 1; i--) {
        throughput_per_sec[i] = throughput_per_sec[i] - throughput_per_sec[i-1]; 
     }
   }
   if (abort_per_sec.size() >= 2) {
     for (size_t i = abort_per_sec.size() - 1; i >= 1; i--) {
        abort_per_sec[i] = abort_per_sec[i] - abort_per_sec[i-1]; 
     }
   }
     cout << endl;
  }

  static void PrintStats() {
    LOG_INFO("Print DB Stats:");
    DB_TIME_STATS(PRINT_TIME_STATS_MS)
    DB_INT_STATS(PRINT_INT_STATS)

    bool is_client_node = (g_node_id == g_client_node_id);
    bool is_migr_node = false;
    if (g_loadbalance_enable) {
      is_migr_node = (!is_client_node) && (g_node_id > g_scale_node_id);
    } else if (g_scaleout_enable) {
      is_migr_node = g_node_id == g_scale_node_id; 
    } else if (g_scalein_enable) {
      is_migr_node = (g_node_id < g_scale_node_id); 
    } else if (g_faulttolerance_enable) {
      is_migr_node = (g_node_id == g_scale_node_id + 1); 
    } else if (g_rampup_scaleout_enable) {
      is_migr_node = (g_node_id != 0) && (!is_client_node); 
    } else if (g_synthetic_enable) {
      is_migr_node = !is_client_node;
    }


   //  if (g_node_id != g_scale_node_id) {
    if (is_client_node) {
        CalcRealTimeThroughput();
        std::ostringstream oss;
        oss << "[";
        for (size_t i = 0; i < throughput_per_sec.size(); i++) {
            if (i != throughput_per_sec.size() - 1) {
                oss << throughput_per_sec[i] << ", ";
            } else {
                oss << throughput_per_sec[i];
            }
        }
        oss << "]";
        std::cout << "real_time_throughput: " << oss.str() << endl;

        std::ostringstream oss_abort;
        oss_abort << "[";
        for (size_t i = 0; i < abort_per_sec.size(); i++) {
            if (i != abort_per_sec.size() - 1) {
                oss_abort << abort_per_sec[i] << ", ";
            } else {
                oss_abort << abort_per_sec[i];
            }
        }
        oss_abort << "]";
        std::cout << "real_time_abort: " << oss_abort.str() << endl;
    }

    auto sum_accesses = dbsum_stats_.int_stats_.accesses_;
    auto sum_misses = dbsum_stats_.int_stats_.misses_;
    auto avg_evict_latency = (dbsum_stats_.int_stats_.evict_count_ == 0)? 0: dbsum_stats_.int_stats_.sum_evict_time_ms_ * 1000.0 / dbsum_stats_.int_stats_.evict_count_; 
    auto avg_evict_locate_latency_per_txn = (dbsum_stats_.int_stats_.txn_count_== 0)? 0: dbsum_stats_.int_stats_.sum_evict_time_ms_ * 1000.0 / dbsum_stats_.int_stats_.txn_count_; 
    auto avg_evict_flush_load_latency = (dbsum_stats_.int_stats_.evict_count_ == 0)? 0: dbsum_stats_.int_stats_.sum_evict_flush_load_time_ms_ * 1000.0 / dbsum_stats_.int_stats_.evict_count_; 
    auto avg_evict_load_latency_per_txn =  dbsum_stats_.int_stats_.sum_evict_flush_load_time_ms_ * 1000.0 / dbsum_stats_.int_stats_.txn_count_; 
    auto avg_miss_load_latency = (dbsum_stats_.int_stats_.miss_load_count_== 0)? 0 : dbsum_stats_.int_stats_.sum_miss_load_time_ms_ * 1000.0 / dbsum_stats_.int_stats_.miss_load_count_; 
    auto avg_index_search_latency = dbsum_stats_.int_stats_.sum_idx_search_time_ms_ * 1000.0 /  dbsum_stats_.int_stats_.txn_count_;
    auto avg_exe_latency = dbsum_stats_.int_stats_.sum_txn_exe_time_ms_ * 1000.0 /  dbsum_stats_.int_stats_.txn_count_;
    auto avg_commit_latency = dbsum_stats_.int_stats_.sum_txn_commit_time_ms_ * 1000.0 /  dbsum_stats_.int_stats_.txn_count_;
    auto avg_log_latency = dbsum_stats_.int_stats_.sum_log_time_ms_ * 1000.0 / dbsum_stats_.int_stats_.log_count_; 
    auto avg_log_latency_per_txn = dbsum_stats_.int_stats_.sum_log_time_ms_ * 1000.0 / dbsum_stats_.int_stats_.txn_count_; 
    auto avg_log_size = dbsum_stats_.int_stats_.log_size_ * 1.0 / (1024 * dbsum_stats_.int_stats_.log_count_) ; 
    auto avg_txn_latency = dbsum_stats_.int_stats_.sum_txn_time_ms_ * 1000.0 / dbsum_stats_.int_stats_.txn_count_; 
    auto avg_gc_entry_count = (dbsum_stats_.int_stats_.gc_log_flush_count_  == 0)? 0: dbsum_stats_.int_stats_.gc_log_entry_count_ / dbsum_stats_.int_stats_.gc_log_flush_count_;
    auto avg_gc_log_latency = (dbsum_stats_.int_stats_.gc_log_flush_count_  == 0)? 0: dbsum_stats_.int_stats_.sum_gc_log_time_ms_ * 1000.0 / dbsum_stats_.int_stats_.gc_log_flush_count_; 

    auto remote_req_rate = (dbsum_stats_.int_stats_.remote_req_count_ + dbsum_stats_.int_stats_.local_req_count_ == 0)? 0: dbsum_stats_.int_stats_.remote_req_count_ * 1.0/ (dbsum_stats_.int_stats_.remote_req_count_ + dbsum_stats_.int_stats_.local_req_count_);

    std::cout << "Avg evict latency: " << avg_evict_latency << " us " << std::endl; 
    std::cout << "Avg evict flush/load latency: " << avg_evict_flush_load_latency << " us " << std::endl; 
    std::cout << "Avg load latency: " << avg_miss_load_latency << " us " << std::endl; 

    if (g_groupcommit_enable) {
      std::cout << "Avg group commit entry count: " << avg_gc_entry_count << std::endl; 
      std::cout << "Avg group commit latency: " << avg_gc_log_latency << " us " << std::endl; 
    }

    LOG_INFO("Print Txn Latency Breakdown:");
    std::cout << "Avg index search latency: " << avg_index_search_latency << " us " << std::endl; 
    std::cout << "Avg evict-locate latency: " << avg_evict_locate_latency_per_txn << " us " << std::endl; 
    std::cout << "Avg evict/load latency: " << avg_evict_load_latency_per_txn << " us " << std::endl; 
    std::cout << "Avg log latency: " << avg_log_latency_per_txn << " us " << std::endl; 
    std::cout << "Avg txn exec latency: " << avg_exe_latency << " us " << std::endl; 
    std::cout << "Avg txn commit latency: " << avg_commit_latency << " us " << std::endl; 
    std::cout << "Avg txn latency: " << avg_txn_latency << " us " << std::endl; 
    

    std::cout << "Avg log size: " << avg_log_size << " KB " << std::endl; 
    auto hit_rate = (sum_accesses == 0)? 0.0: (sum_accesses - sum_misses) * 1.0 / sum_accesses;
    std::cout << "Hit rate: " << hit_rate << std::endl;
    if (g_save_output) {
      if (is_client_node) {
          std::ostringstream oss;
          oss << "[";
          for (size_t i = 0; i < throughput_per_sec.size(); i++) {
             if (i != throughput_per_sec.size() - 1) {
                oss << throughput_per_sec[i] << ", ";
             } else {
                oss << throughput_per_sec[i];
             }

          }
          oss << "]";
          g_out_file << "\"real_time_throughput\": " << oss.str() << ", ";
    std::ostringstream oss_abort;
        oss_abort << "[";
        for (size_t i = 0; i < abort_per_sec.size(); i++) {
            if (i != abort_per_sec.size() - 1) {
                oss_abort << abort_per_sec[i] << ", ";
            } else {
                oss_abort << abort_per_sec[i];
            }
        }
        oss_abort << "]";
       g_out_file << "\"real_time_abort\": " << oss_abort.str() << ", ";
         if (g_synthetic_enable) {
            std::ostringstream latency_oss;
            latency_oss << "[";
            for (size_t i = 0; i < user_txn_latency_accum.size(); i++) {
               if (i != user_txn_latency_accum.size() - 1) {
                  latency_oss << user_txn_latency_accum[i] << ", ";
               } else {
                  latency_oss << user_txn_latency_accum[i];
               }
            }
            latency_oss << "]";
            g_out_file << "\"user_txn_latencies\": " << latency_oss.str() << ", ";
         }



          std::sort(user_txn_latency_accum.begin(), user_txn_latency_accum.end());

          // Calculate min and max
          float min = user_txn_latency_accum.front();
          float max = user_txn_latency_accum.back();

          // Calculate quartiles
          size_t n = user_txn_latency_accum.size();
          size_t lowerIndex = n / 4;
          size_t upperIndex = 3 * n / 4;
          float lowerQuartile = (n % 4 == 0) ? (user_txn_latency_accum[lowerIndex - 1] + user_txn_latency_accum[lowerIndex]) / 2.0f : user_txn_latency_accum[lowerIndex];
          float upperQuartile = (n % 4 == 0) ? (user_txn_latency_accum[upperIndex - 1] + user_txn_latency_accum[upperIndex]) / 2.0f : user_txn_latency_accum[upperIndex];

          // Calculate mean
          float sum = 0.0f;
          for (float value : user_txn_latency_accum) {
              sum += value;
          }
          float mean = sum / n;
        
          g_out_file << "\"user_txn_latency_min_ms\": " << min << ", ";
          std::cout << "user_txn_latency_min_ms: " << min << std::endl;
          g_out_file << "\"user_txn_latency_max_ms\": " << max << ", ";
          std::cout << "user_txn_latency_max_ms: " << max << std::endl;
          g_out_file << "\"user_txn_latency_upper_quartile_ms\": " << upperQuartile << ", ";
          std::cout << "user_txn_latency_upper_quartile_ms: " << upperQuartile << std::endl;
          g_out_file << "\"user_txn_latency_lower_quartile_ms\": " << lowerQuartile << ", ";
          std::cout << "user_txn_latency_lower_quartile_ms: " << lowerQuartile << std::endl;
          g_out_file << "\"user_txn_latency_mean_ms\": " << mean << ", ";
          std::cout << "user_txn_latency_mean_ms: " << mean << std::endl;

      }


      if (is_migr_node) {
          std::ostringstream oss_latency;
          oss_latency << "[";
          for (size_t i = 0; i < migr_txn_latency_accum.size(); i++) {
             if (i != migr_txn_latency_accum.size() - 1) {
                oss_latency << migr_txn_latency_accum[i] << ", ";
             } else {
                oss_latency << migr_txn_latency_accum[i];
             }

          }
          oss_latency << "]";
          g_out_file << "\"migration_latency_ms\": " << oss_latency.str() << ", ";
          std::cout << "migration_latency_ms: " <<  oss_latency.str() << std::endl;


          std::ostringstream oss;
          oss << "[";
          for (size_t i = 0; i < migrate_starts.size(); i++) {
             if (i != migrate_starts.size() - 1) {
                oss << migrate_starts[i] << ", ";
             } else {
                oss << migrate_starts[i];
             }

          }
          oss << "]";
          g_out_file << "\"migration_starts\": " << oss.str() << ", ";

          std::ostringstream oss_critical;
          oss_critical << "[";
          for (size_t i = 0; i < migrate_critical_starts.size(); i++) {
             if (i != migrate_critical_starts.size() - 1) {
                oss_critical << migrate_critical_starts[i] << ", ";
             } else {
                oss_critical << migrate_critical_starts[i];
             }

          }
          oss_critical << "]";
          g_out_file << "\"migration_critical_starts\": " << oss_critical.str() << ", ";


          std::ostringstream oss_critical_ends;
          oss_critical_ends << "[";
          for (size_t i = 0; i < migrate_critical_ends.size(); i++) {
             if (i != migrate_critical_ends.size() - 1) {
                oss_critical_ends << migrate_critical_ends[i] << ", ";
             } else {
                oss_critical_ends << migrate_critical_ends[i];
             }

          }
          oss_critical_ends << "]";
          g_out_file << "\"migration_critical_ends\": " << oss_critical_ends.str() << ", ";


         std::ostringstream oss_critical_latency;
          oss_critical_latency << "[";
          for (size_t i = 0; i < migr_critical_latency.size(); i++) {
             if (i != migr_critical_latency.size() - 1) {
                oss_critical_latency << migr_critical_latency[i] << ", ";
             } else {
                oss_critical_latency << migr_critical_latency[i];
             }

          }
          oss_critical_latency << "]";
         std::cout << "migration_critical_latency_ms: " <<  oss_critical_latency.str() << std::endl;
         g_out_file << "\"migration_critical_latency_ms\": " << oss_critical_latency.str() << ", ";




          
          std::ostringstream oss_ends;
          oss_ends << "[";
          for (size_t i = 0; i < migrate_ends.size(); i++) {
             if (i != migrate_ends.size() - 1) {
                oss_ends << migrate_ends[i] << ", ";
             } else {
                oss_ends << migrate_ends[i];
             }

          }
          oss_ends << "]";
          g_out_file << "\"migration_ends\": " << oss_ends.str() << ", ";

      }

      g_out_file << "\"gc_avg_entry_count_\": " << avg_gc_entry_count << ", ";
      g_out_file << "\"gc_avg_latency_us\": " << avg_gc_log_latency << ", ";

      g_out_file << "\"txn_avg_latency_us\": " << avg_txn_latency << ", ";
      g_out_file << "\"txn_avg_idx_search_us\": " << avg_index_search_latency << ", ";
      g_out_file << "\"txn_avg_evict_locate_us\": " << avg_evict_locate_latency_per_txn << ", ";
      g_out_file << "\"txn_avg_evict_load_us\": " << avg_evict_load_latency_per_txn << ", ";
      // g_out_file << "\"txn_avg_load_us\": " << avg_evict_load_latency_per_txn << "} " << std::endl;
      g_out_file << "\"txn_avg_log_us\": " << avg_log_latency_per_txn<< ", ";


      g_out_file << "\"remote_req_count\": " << dbsum_stats_.int_stats_.remote_req_count_ << ", ";
      g_out_file << "\"local_req_count\": " << dbsum_stats_.int_stats_.local_req_count_ << ", ";
      g_out_file << "\"remote_req_rate\": " << remote_req_rate << ", ";

      g_out_file << "\"hit_rate_\": " << hit_rate << "} " << std::endl;
    }

    // if (g_save_output) {
    //   g_out_file << "\"avg_commit_latency_us\": " << avg_latency << ", ";
    //   g_out_file << "\"avg_per_worker_runtime_sec\": " << avg_runtime << ", ";
    //   g_out_file << "\"throughput_txn_per_sec\": " << throughput << ", ";
    // }

  }

  static __thread DBStats dbstats_;
  static DBStats dbsum_stats_;
  static mutex latch_;




 private:
  static __thread OID thd_id_;
  static __thread OID max_txn_id_;

};
}
#endif //ARBORETUM_SRC_COMMON_WORKER_H_
