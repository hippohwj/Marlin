
#include "OptionalGlobalData.h"

namespace arboretum {

// Workload
// ====================
volatile uint32_t       _num_worker_done=0;

// Log Replay
// ====================
ConcurrentLSN * replay_lsn;


// Group Commit
// ====================
GroupLogBuffer * gc_buffer;
LogRingBuffer<LogEntry *> * gc_ring_buffer;
ConcurrentLSN * commit_lsn;
condition_variable commit_lsn_cv;
mutex commit_lsn_mtx;

// Client/Load balancer cache
ConcurrentHashMap<uint64_t, uint64_t> * granule_tbl_cache;

ConcurrentHashMap<uint64_t, string> * granule_commit_tracker;

// cache preheat updates tracker for src nodes
ConcurrentHashMap<uint64_t, std::unordered_set<uint64_t> *> * migr_preheat_updates_tracker;

// active worker suspend tracker for albatross
ConcurrentHashMap<uint64_t, std::atomic<uint32_t>* > granule_activeworker_map; 



// cache preheat delta mask for cache in the dest node
ConcurrentHashSet<uint64_t> * cache_preheat_mask;




void worker_thread_done()
{
    ATOM_ADD_FETCH(_num_worker_done, 1);
}

//TODO(hippo): compare with real number of workthreads  
bool are_all_worker_done() {
    return _num_worker_done >= g_num_worker_threads;
}
}