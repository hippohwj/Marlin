#include "LogTailIndex.h"

namespace arboretum {

LogTailIndex::LogTailIndex() {
    pthread_mutex_init(&_lock, NULL);
}

LogTailIndex::~LogTailIndex() {
    while (!lsn_entries_pq.empty()) {
        delete lsn_entries_pq.top();
        lsn_entries_pq.pop();
    }
    tail_index.clear();
}

void LogTailIndex::insert(LogSeqNum *lsn_new, vector<uint64_t> *keys) {
    lock();
    // check capacity
    while (lsn_entries_pq.size() + keys->size() > LOG_TAIL_INDEX_CAPACITY_K) {
        LogSeqNum *top_entry = new LogSeqNum((LogSeqNum &) lsn_entries_pq.top());
        // remote all items with the least LSN
        while (*(lsn_entries_pq.top()->get_lsn()) == *top_entry) {
            uint64_t cur_key = lsn_entries_pq.top()->get_key();
            lsn_entries_pq.pop();
            tail_index.erase(cur_key);
        }
    }
    // insert new entries into log tail index
    for (auto &key: *keys) {
        TailIndexEntry *entry = new TailIndexEntry(key, lsn_new);
        lsn_entries_pq.push(entry);
        tail_index[key] = entry;
    }
    release();
}

/**
 * get MEL(Minimum Equivalent Replay-LSN)
 * @param key
 * @param logSeqNum
 */
void LogTailIndex::get_MEL(uint64_t key, LogSeqNum *logSeqNum) {
    lock();
    auto iter = tail_index.find(key);
    if (iter != tail_index.end()) {
        // tail index hit
        string lsn_str = "";
        iter->second->get_lsn()->get_lsn_str(lsn_str);
        logSeqNum->set_lsn(lsn_str);
    } else if (!lsn_entries_pq.empty()) {
        //tail index miss, find the smallest lsn in the tail index
        string lsn_str = "";
        lsn_entries_pq.top()->get_lsn()->get_lsn_str(lsn_str);
        logSeqNum->set_lsn(lsn_str);
    }
    release();
}

void LogTailIndex::lock() {
    pthread_mutex_lock(&_lock);
}


void LogTailIndex::release() {
    pthread_mutex_unlock(&_lock);
}

}