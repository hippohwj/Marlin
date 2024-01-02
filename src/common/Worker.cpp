//
// Created by Zhihan Guo on 3/31/23.
//

#include "Worker.h"

namespace arboretum {

__thread OID Worker::thd_id_;
__thread OID Worker::max_txn_id_;
__thread DBStats Worker::dbstats_;
 

DBStats Worker::dbsum_stats_;
std::mutex Worker::latch_;
atomic<uint64_t> Worker::commit_count_{0};
atomic<uint64_t> Worker::abort_count_{0};
vector<uint64_t> Worker::throughput_per_sec;
vector<uint64_t> Worker::abort_per_sec;
vector<float> Worker::user_txn_latency_accum;
vector<float> Worker::migr_txn_latency_accum;
vector<double> Worker::migr_critical_latency;
vector<double> Worker::migrate_starts;
vector<double> Worker::migrate_critical_starts;
vector<double> Worker::migrate_critical_ends;
vector<double> Worker::migrate_ends;

}


