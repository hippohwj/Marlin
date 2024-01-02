//
// Created by Zhihan Guo on 3/31/23.
//

#ifndef ARBORETUM_SRC_LOCAL_ITUPLE_H_
#define ARBORETUM_SRC_LOCAL_ITUPLE_H_

#include "common/Common.h"
#include "ISchema.h"

namespace arboretum {
class ITuple {

 public:
  ITuple() = default;;
  explicit ITuple(OID tbl_id, OID tid) : tbl_id_(tbl_id), tid_(tid) {};
  void SetFromLoaded(char * loaded_data, size_t sz) {
    auto loaded = reinterpret_cast<ITuple *> (loaded_data);
    tbl_id_ = loaded->GetTableId(); tid_ = loaded->GetTID();
    SetData(loaded->GetData(), sz); };
    // SetData(loaded_data, sz); };
  void SetData(char * src, size_t sz);
  char * GetData() { return (char *) this + sizeof(ITuple); };
  void SetTID(OID tid) { tid_ = tid; };
  OID GetTID() const { return tid_; };
  OID GetTableId() const { return tbl_id_; };
  void GetField(ISchema * schema, OID col, SearchKey &key);
  void GetPrimaryKey(ISchema * schema, SearchKey &key);

  // LSN related operations
  void SetLSN(string &new_lsn);
  bool HasLSNInit();
  void GetLSNStr(string &lsn);

  std::string GetPrimaryStorageKey(ISchema * schema) {
    SearchKey key;
    GetPrimaryKey(schema, key);
    return std::to_string(tbl_id_) + "-" + key.ToString();
  }
  static std::string GenStorageKey(OID tbl_id, SearchKey& key) {
    return std::to_string(tbl_id) + "-" + key.ToString();
  }

  // buffer helper
  std::atomic<uint32_t> buf_state_{0};
  
  ITuple * clock_next_{nullptr};

 private:
  OID tbl_id_{0};
  // SearchKey key_{}; // TODO: unused but kept since data is pre-loaded
  OID tid_{0}; // identifier used in the lock table
  string _lsn = "0"; // record the lsn for the "dirty" tuple

 public:
  // TODO: for now must kept it at this pos since data is pre-loaded
  std::mutex buf_latch_{};
};

}

#endif //ARBORETUM_SRC_LOCAL_ITUPLE_H_
