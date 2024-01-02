#ifndef ARBORETUM_SRC_COMMON_OPTIONALGLOBALDATA_H_
#define ARBORETUM_SRC_COMMON_OPTIONALGLOBALDATA_H_

#include "log/LogSeqNum.h"
#include "log/GroupLogBuffer.h"
#include "log/LogRingBuffer.h"
#include "common/ConcurrentHashMap.h"
#include "common/ConcurrentHashSet.h"
#include <string>
#include <mutex>
#include <condition_variable>
#include <unordered_set>


namespace arboretum {

class ConcurrentLSN;
class GroupLogBuffer;

void worker_thread_done();
bool are_all_worker_done();

extern ConcurrentLSN * replay_lsn;
extern GroupLogBuffer * gc_buffer;
extern LogRingBuffer<LogEntry *> * gc_ring_buffer;
extern ConcurrentLSN * commit_lsn;
extern condition_variable commit_lsn_cv;
extern mutex commit_lsn_mtx;

extern ConcurrentHashMap<uint64_t, uint64_t> * granule_tbl_cache;
extern ConcurrentHashMap<uint64_t, string> * granule_commit_tracker;

extern ConcurrentHashMap<uint64_t, std::unordered_set<uint64_t>* >  * migr_preheat_updates_tracker;

// active worker suspend tracker for albatross
extern ConcurrentHashMap<uint64_t, std::atomic<uint32_t>* > granule_activeworker_map; 

extern ConcurrentHashSet<uint64_t> * cache_preheat_mask;


extern volatile uint32_t _num_sync_received;

}

#endif //ARBORETUM_SRC_COMMON_OPTIONALGLOBALDATA_H_