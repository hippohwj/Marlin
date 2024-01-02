#ifndef ARBORETUM_SRC_DB_LOG_LOGSEQNUM_H_
#define ARBORETUM_SRC_DB_LOG_LOGSEQNUM_H_

#include <string>

// #include "system/global.h"
// #include "config.h"

#include <condition_variable>
#include <mutex>

namespace arboretum {

using namespace std;

class LogSeqNum {
   private:
    string _lsn = "0";

   public:
    LogSeqNum();

    LogSeqNum(string &lsn);

    LogSeqNum(LogSeqNum *lsn);

    ~LogSeqNum();

    bool operator==(const LogSeqNum &otherLSN) const {
        return _lsn == otherLSN._lsn;
    }

    void set_lsn(string &new_lsn);

    bool has_init();

    void get_lsn_str(string &lsn);

    void print();

    void copy(LogSeqNum *lsn);
};

// class ConcurrentLSN {
// public:
//     ConcurrentLSN();

//     ~ConcurrentLSN();

//     void set_lsn(string new_lsn);

//     bool has_init();

//     void get_lsn(string &lsn);

//     bool has_replayed(string &target_lsn);

//     void print();

// private:
//     //TODO(hippo): change it to read/write lock
//     pthread_mutex_t _lock;
//     LogSeqNum *_lsn = new LogSeqNum();

//     void lock();

//     void release();

// };
// }

class ConcurrentLSN {
   public:
    ConcurrentLSN();

    ~ConcurrentLSN();

    void Set(string new_lsn);

    void SetAndNotify(string new_lsn);

    void Get(string &lsn);

    bool has_replayed(string &target_lsn);

    void WaitForCatchup(string &target_lsn);

    bool IsFresherThan(string &lsn);


    void print();

    string PrintStr();

    static string PrintStrFor(string& input_lsn);

   private:
    // TODO(hippo): change it to read/write lock
    mutex lsn_mtx_;
    string lsn_ = "0";
    condition_variable cv;

    // not thread-safe
    static bool HasInit(string &target_lsn);

    // void lock();

    // void release();
};
}  // namespace arboretum

#endif //ARBORETUM_SRC_DB_LOG_LOGSEQNUM_H_

