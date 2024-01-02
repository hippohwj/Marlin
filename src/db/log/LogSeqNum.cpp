#include "LogSeqNum.h"
#include "Common.h"
#include <iostream>
#include "common/Message.h" 


using namespace std;

namespace arboretum {


LogSeqNum::LogSeqNum() = default;

LogSeqNum::LogSeqNum(LogSeqNum *lsn) {
    lsn->get_lsn_str(_lsn);
}

LogSeqNum::LogSeqNum(string &lsn) {
    _lsn = lsn;
}

LogSeqNum::~LogSeqNum() {
    //TODO(hippo): add destruction
}

bool LogSeqNum::has_init() {
    return _lsn != "0";
}

void LogSeqNum::set_lsn(string &new_lsn) {
    _lsn = new_lsn;
}

void LogSeqNum::get_lsn_str(string &lsn) {
    lsn = _lsn;
}

void LogSeqNum::print() {
    cout << "LogSeqNum is " << _lsn << endl;
}

///////////////ConcurrentLSN///////////////////
bool ConcurrentLSN::IsFresherThan(string& target_lsn) {
    std::lock_guard<std::mutex> lock(lsn_mtx_);  
    uint64_t target_lsn_int = std::stoull(target_lsn);
    uint64_t cur_lsn_int = std::stoull(lsn_);
    return target_lsn_int < cur_lsn_int; 
}


ConcurrentLSN::ConcurrentLSN() {

}

ConcurrentLSN::~ConcurrentLSN() {
    //TODO(hippo): add destruction
}

bool ConcurrentLSN::has_replayed(string &target_lsn) {
    std::lock_guard<std::mutex> lock(lsn_mtx_);  
    #if REMOTE_STORAGE_TYPE == REMOTE_STORAGE_REDIS
       return target_lsn.compare(lsn_) <=0 || target_lsn == "0";
    #elif REMOTE_STORAGE_TYPE == REMOTE_STORAGE_AZURE
       uint64_t target_lsn_int = std::stoull(target_lsn);
       uint64_t cur_lsn_int = std::stoull(lsn_);
       return  target_lsn_int <= cur_lsn_int || target_lsn_int == 0; 
    #endif
}
// not thread-safe
bool ConcurrentLSN::HasInit(string &target_lsn) {
    uint64_t cur_lsn_int = std::stoull(target_lsn);
    return cur_lsn_int != 0;
}


void ConcurrentLSN::Get(string &lsn) {
    std::lock_guard<std::mutex> lock(lsn_mtx_);  
    lsn = lsn_;
}

void ConcurrentLSN::Set(string new_lsn) {
    std::lock_guard<std::mutex> lock(lsn_mtx_);
    lsn_ = new_lsn;
}

void ConcurrentLSN::SetAndNotify(string new_lsn) {
    {
        std::lock_guard<std::mutex> lock(lsn_mtx_);
        lsn_ = new_lsn;
    }
    cv.notify_all();
}

void ConcurrentLSN::WaitForCatchup(string &target_lsn) {
    unique_lock<mutex> lock(lsn_mtx_);
    #if REMOTE_STORAGE_TYPE == REMOTE_STORAGE_REDIS
    cv.wait(lock, [target_lsn, this] { return target_lsn.compare(lsn_) <=0; });
    #elif REMOTE_STORAGE_TYPE == REMOTE_STORAGE_AZURE
    cv.wait(lock, [target_lsn, this] { 
        uint64_t target_lsn_int = std::stoull(target_lsn);
        uint64_t cur_lsn_int = std::stoull(lsn_);
        return target_lsn_int <= cur_lsn_int; 
    });
    #endif
    lock.unlock();
}

void ConcurrentLSN::print() {
    std::lock_guard<std::mutex> lock(lsn_mtx_);  
    cout << "LogSeqNum is " << lsn_ << endl;
}


//   string GetLSNStr() {
//      uint64_t lsn = (((uint64_t)blob_idx_) << (64-8)) | (uint64_t)offset_;
//      return to_string(lsn);
//   }
  
//   string PrintStr() {
//     return "blob id: " + to_string(blob_idx_) + ", offset: " + to_string(offset_);
//   }


//only for azure LSN
string ConcurrentLSN::PrintStr() {
    std::lock_guard<std::mutex> lock(lsn_mtx_);  
    uint64_t lsn_num = std::stoull(lsn_);
    uint64_t blob_id = lsn_num >> (64-8);
    uint64_t offset = (lsn_num << 8) >> 8;
    return "blob id: " + to_string(blob_id) + ", offset: " + to_string(offset);
}

string ConcurrentLSN::PrintStrFor(string& input_lsn) {
    uint64_t lsn_num = std::stoull(input_lsn);
    uint64_t blob_id = lsn_num >> (64-8);
    uint64_t offset = (lsn_num << 8) >> 8;
    return "blob id: " + to_string(blob_id) + ", offset: " + to_string(offset);
}


}