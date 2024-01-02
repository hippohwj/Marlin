#ifndef ARBORETUM_SRC_DB_LOG_LOG_ENTRY_H_
#define ARBORETUM_SRC_DB_LOG_LOG_ENTRY_H_

// #include "system/global.h"
// #include "system/txn/txn.h"
// #include "system/cc_manager.h"
#include "txn/TxnManager.h"
#include "Common.h"
#include <future>


namespace arboretum {
using namespace std;

class LogEntry {
 public:
  static LogEntry * NewFakeEntry();

  // LogEntry() : lsn_{"0"},status_{TxnManager::State::RUNNING}, txn_id_{0} {};
  // LogEntry() : status_{TxnManager::State::RUNNING}, txn_id_{0} {};

  LogEntry(string lsn, string data, uint8_t status_new, uint64_t txn_id_new);

  LogEntry(string data, uint8_t status_new, uint64_t txn_id_new);

  bool IsCommit() const { return status_ == TxnManager::State::COMMITTED; }
  bool IsPrepare() const { return status_ == TxnManager::State::PREPARED; }
  bool IsAbort() const { return status_ == TxnManager::State::ABORTED; }
  bool IsFakeEntry() const { return status_ == UINT8_MAX;}

  //TODO(Hippo): eliminate the case for data_ == ""
  bool HasData() { return !data_.empty() && (data_ != ""); }
  string GetLSN() { return lsn_; }
  void GetLSN(string& out_lsn) { out_lsn = lsn_; }
  string &GetData() { return data_; }
  uint64_t GetTxnId() const { return txn_id_; }
  uint8_t GetStatus() { return status_; }
  void SetLSN(string& lsn);
  void SetGranules(unordered_set<uint64_t>*  granules);
  unordered_set<uint64_t>* GetGranules();
  future<bool> GetLSNFuture();
  void SetLSNPromise(string& lsn);
  void SetPromise();

  #if REMOTE_STORAGE_TYPE == REMOTE_STORAGE_AZURE
  void Serialize(vector<uint8_t> * buffer);
  uint32_t GetSerializedLen();
  static LogEntry * Deserialize(uint8_t * buffer);
  static bool HasLogEntry(uint8_t * buffer, size_t buffer_size);
  #endif
  

  static void DeserializeLogData(string &data, unordered_map<uint64_t, unordered_map<uint64_t, string>> *kv);

  void to_string(string &str);
  void print();

 private:
  string lsn_= "0";
  string data_;
  uint8_t status_;
  uint64_t txn_id_;
  unordered_set<uint64_t> granules_;

  // Promise for gc 
  promise<bool> lsn_promise_;
};
}

#endif //ARBORETUM_SRC_DB_LOG_LOG_ENTRY_H_
