#ifndef ARBORETUM_SRC_DB_LOG_LOGTAILINDEX_H_
#define ARBORETUM_SRC_DB_LOG_LOGTAILINDEX_H_

#include <unordered_map>
#include <queue>
#include "LogSeqNum.h"
#include "TailIndexEntry.h"

using namespace std;
namespace arboretum {
// Log Tail Index Capacity in K
#define LOG_TAIL_INDEX_CAPACITY_K       4000

class LogTailIndex {
public:
    LogTailIndex();

    ~LogTailIndex();

    void insert(LogSeqNum *lsn_new, vector<uint64_t> *keys);

/**
 * get MEL(Minimum Equivalent Replay-LSN)
 * @param key
 * @param logSeqNum
 */
    void get_MEL(uint64_t key, LogSeqNum *logSeqNum);

private:
    unordered_map<uint64_t, TailIndexEntry *> tail_index;
    priority_queue<TailIndexEntry *> lsn_entries_pq;
    //TODO(hippo): change it to read/write lock
    pthread_mutex_t _lock;

    void lock();

    void release();
};

}
#endif // ARBORETUM_SRC_DB_LOG_LOGTAILINDEX_H_
