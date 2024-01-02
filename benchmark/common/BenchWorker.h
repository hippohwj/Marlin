//
// Created by Zhihan Guo on 3/31/23.
//

#ifndef ARBORETUM_BENCHMARK_BENCHWORKER_H_
#define ARBORETUM_BENCHMARK_BENCHWORKER_H_

#include "Common.h"
#include "Worker.h"

namespace arboretum {

#define BENCH_INT_STATS(x) x(commit_cnt_, 0, bench) x(abort_cnt_, 0, bench)
#define BENCH_TIME_STATS(x) x(thd_runtime_, 0, bench)
#define BENCH_VEC_STATS(x) x(commit_latency_, 0, bench)
#define SUM_VEC_STATS(stat, val, prefix)                                   \
  std::vector<uint64_t> &vec = prefix##stats_.vec_stats_.stat##ns_;        \
  prefix##sum_stats_.vec_stats_.stat##ns_.insert(                          \
  prefix##sum_stats_.vec_stats_.stat##ns_.end(), vec.begin(), vec.end());  \
  uint64_t sum_ns = 0;                                                     \
  for (auto num : vec) sum_ns += num;                                      \
  prefix##sum_stats_.vec_stats_.stat##us_sum += sum_ns / 1000.0;

#define PRINT_VEC_STATS(var, val, prefix) \
auto & vec = prefix##sum_stats_.vec_stats_.var##ns_; \
std::sort(vec.begin(), vec.end());        \
std::cout << #var << " ( 0% in us): "   \
<< vec[0] / 1000.0 << std::endl;       \
std::cout << #var << " (50% in us): "   \
<< vec[vec.size() * 0.50] / 1000 << std::endl;       \
std::cout << #var << " (99% in us): "                \
<< vec[vec.size() * 0.99] / 1000 << std::endl;       \
if (g_save_output) {                                 \
  g_out_file << "\"" << #var << "perc0_us\": " << vec[0] / 1000.0 << ", "; \
  g_out_file << "\"" << #var << "perc50_us\": "      \
  << vec[vec.size() * 0.50] / 1000.0 << ", ";        \
  g_out_file << "\"" << #var << "perc99_us\": "      \
  << vec[vec.size() * 0.99] / 1000.0 << ", ";        \
}


struct BenchStats : DBStats {
  struct BenchIntStats : DBStats::IntStats {
    BENCH_INT_STATS(DECL_INT_STATS)
    BENCH_TIME_STATS(DECL_TIME_STATS)
  };
  struct BenchVecStats : DBStats::VecStats {
    std::vector<uint64_t> commit_latency_ns_{};
    uint64_t commit_latency_us_sum{};
  };
  BenchIntStats int_stats_{};
  BenchVecStats vec_stats_{};
};

class BenchWorker : public Worker {
 public:
  BenchWorker();

  void SumStats() {
    BENCH_INT_STATS(SUM_INT_STATS)
    BENCH_TIME_STATS(SUM_TIME_STATS)
    BENCH_VEC_STATS(SUM_VEC_STATS)
  }

  static void PrintStats(uint64_t num_worker_threads) {
    LOG_INFO("Print Bench Stats:");
    BENCH_TIME_STATS(PRINT_TIME_STATS_MS)
    BENCH_INT_STATS(PRINT_INT_STATS)
    // BENCH_VEC_STATS(PRINT_VEC_STATS)
    // auto avg_latency = benchsum_stats_.vec_stats_.commit_latency_us_sum * 1.0 /
    // benchsum_stats_.int_stats_.commit_cnt_;
    // std::printf("Average commit latency (us): %.2f\n", avg_latency);
    auto avg_runtime = benchsum_stats_.int_stats_.sum_thd_runtime_ms_ / 1000.0 / num_worker_threads;
    std::printf("Average runtime (sec) of each worker: %.2f\n", avg_runtime);
    auto throughput = benchsum_stats_.int_stats_.commit_cnt_ / avg_runtime;
    auto throughput1 =  Worker::commit_count_.load() / avg_runtime;
    float abort_ratio = benchsum_stats_.int_stats_.abort_cnt_ * 1.0 / (benchsum_stats_.int_stats_.commit_cnt_ + benchsum_stats_.int_stats_.abort_cnt_); 
    // auto throughput1 = benchsum_stats_.int_stats_.txn_count_ / avg_runtime;
    std::printf("Throughput1 (txn/sec): %.2f\n", throughput);
    std::printf("Throughput2 (txn/sec): %.2f\n", throughput1);
    std::printf("Abort Ratio: %.2f\n", abort_ratio);
    if (g_save_output) {
    //   g_out_file << "\"avg_commit_latency_us\": " << avg_latency << ", ";
      g_out_file << "\"avg_per_worker_runtime_sec\": " << avg_runtime << ", ";
      g_out_file << "\"throughput_txn_per_sec\": " << throughput << ", ";
      g_out_file << "\"abort_ratio\": " << abort_ratio << ", ";
    }
  }

  OID worker_id_{};
  BenchStats benchstats_;
  static BenchStats benchsum_stats_;
};

} // arbotretum

#endif //ARBORETUM_BENCHMARK_BENCHWORKER_H_
