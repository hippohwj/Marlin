#include "TailIndexEntry.h"

namespace arboretum {

TailIndexEntry::TailIndexEntry(uint64_t key, LogSeqNum *lsn_new) {
    _key = key;
    _lsn = new LogSeqNum(*lsn_new);
}

TailIndexEntry::~TailIndexEntry() {
    delete &_lsn;
}

uint64_t TailIndexEntry::get_key() {
    return _key;
}

LogSeqNum* TailIndexEntry::get_lsn() {
    return _lsn;
}

}