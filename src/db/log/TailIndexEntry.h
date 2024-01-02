#ifndef ARBORETUM_SRC_DB_LOG_TAILINDEXENTRY_H_
#define ARBORETUM_SRC_DB_LOG_TAILINDEXENTRY_H_

#include "LogSeqNum.h"

namespace arboretum {
class TailIndexEntry{
public:
    TailIndexEntry(uint64_t key, LogSeqNum* lsn_new);

    ~TailIndexEntry();

    uint64_t get_key();
    LogSeqNum* get_lsn();

private:
    uint64_t _key;
    LogSeqNum* _lsn;
};
}
#endif // ARBORETUM_SRC_DB_LOG_TAILINDEXENTRY_H_

