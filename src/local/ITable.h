//
// Created by Zhihan Guo on 3/30/23.
//

#ifndef ARBORETUM_SRC_LOCAL_ITABLE_H_
#define ARBORETUM_SRC_LOCAL_ITABLE_H_


#include <utility>

#include "common/Common.h"
#include "ITuple.h"


namespace arboretum {

class IIndex;
class ITable {

 public:
  ITable(std::string tbl_name, OID tbl_id, ISchema * schema) :
  tbl_name_(std::move(tbl_name)), tbl_id_(tbl_id), schema_(schema) {};
  bool IsSysTable();
  bool IsUserTable();
  bool IsGranuleTable();
  bool IsNodeTable();
  OID CreateIndex(OID col, IndexType tpe);
  RC InsertTuple(OID tupleid, char *data, size_t sz);
  RC InsertTuple(OID tupleid, char *data, size_t sz, bool update_lock_tbl);
  RC InsertPreheatTuple(char * preloaded_data);

  void BatchInsert(OID partition, unordered_map<uint64_t, std::string> * map);


  void IndexInsert(ITuple * p_tuple);

  SharedPtr* IndexSearch(SearchKey key, OID idx_id, int limit=1);
  SharedPtr * IndexSearchOrFill(SearchKey key, OID idx_id, int limit, char * preheated_tuple);
  void Delete(SearchKey key);
  void IndexDelete(ITuple *tuple);
  void IndexDelete(OID tupleid, char *data, size_t sz, bool update_lock_tbl = false);

  inline ISchema *GetSchema() { return schema_; };
  inline OID GetTableId() const { return tbl_id_; };
  inline string GetTableName() { return tbl_name_; };

  inline size_t GetTotalTupleSize() {
    return schema_->tuple_sz_ + sizeof(ITuple);
  }; // including size of metadata
  inline size_t GetTupleDataSize() { return schema_->tuple_sz_; };
  IIndex * GetIndex(size_t idx_id);
  // std::atomic<uint32_t>& GetLockState(OID tid) { return *lock_tbl_[tid]; };
  std::atomic<uint32_t>& GetLockState(OID tid) { return *lock_tbl_[tid]; };
  std::atomic<uint32_t>* GetLockStatePtr(OID tid) { return lock_tbl_[tid]; };

  void PutLock(OID tid, std::atomic<uint32_t>* lock) {
    lock_tbl_[tid] = lock;
  };

 private:
  std::string tbl_name_;
  OID tbl_id_{0};
  std::atomic<size_t> row_cnt_{0};
  std::vector<IIndex *> indexes_;
  ISchema * schema_;
  // std::vector<std::atomic<uint32_t>*> lock_tbl_; // each lock: 1-bit EX lock, 31-bit ref cnt
  std::unordered_map<OID, std::atomic<uint32_t>*> lock_tbl_; // each lock: 1-bit EX lock, 31-bit ref cnt
};

} // arboretum

#endif //ARBORETUM_SRC_LOCAL_ITABLE_H_
