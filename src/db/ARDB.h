//
// Created by Zhihan Guo on 3/30/23.
//

#ifndef ARBORETUM_DISTRIBUTED_SRC_DB_DB_H_
#define ARBORETUM_DISTRIBUTED_SRC_DB_DB_H_

#include "common/Common.h"
#include "local/ITable.h"

namespace arboretum {

class ITable;
class ISchema;
class ITxn;

enum TxnType { USER, MIGRATION, POSTHEAT,  ADDNODE, DELETENODE, DYNAMAST, ALBATROSS};


class ARDB {

 public:
  ARDB();

  OID CreateTable(std::string tbl_name, ISchema * schema); // will use pkey as index
  OID CreateIndex(OID tbl_id, OID col, IndexType tpe);

  RC InsertTuple(OID tbl, OID tupleid, char * data, size_t sz, ITxn * txn = nullptr);
  void IndexDelete(ITable * tbl, OID tupleid, char *data, size_t sz, ITxn * txn = nullptr);
  
  // IRow * GetRow(OID tbl_id, SearchKey key);
  // RC SetRow();
  // GetRows();
  // CreateSecondaryIndex(table, column);
  // ITxn * StartTxn(TxnType type = TxnType::USER);
  ITxn *StartTxn(TxnType xtype = TxnType::USER);

  ITable *GetTable(OID i) { return tables_[i]; };
  ITable *GetTableByName(string &tbl_name) {
      std::vector<ITable *>::iterator it = std::find_if(
          begin(tables_), end(tables_),
          [tbl_name](ITable *tbl) { return tbl->GetTableName() == tbl_name; });
      if (it != end(tables_)) {
          return *it;
      } else {
          M_ASSERT(false, "Cannot find table %s", tbl_name);
      }
  }

  ITable *GetGranuleTable() { return tables_[g_tbl_id_]; };
  ITable *GetNodeTable() { return tables_[n_tbl_id_]; };
  ITable *GetUserTable() { return tables_[u_tbl_id_]; };
  size_t GetGranuleTableID() {return g_tbl_id_;};
  bool IsSysTable(OID tbl_id) { return tbl_id == g_tbl_id_ || tbl_id == n_tbl_id_;}
  void BatchInsert(OID tbl, OID partition,  unordered_map<uint64_t, string> &map);
  void WaitForAsyncBatchLoading(int cnt);

  static void LoadConfig(int i, char **p_string);
  static bool CheckBufferWarmedUp();
  int64_t CheckStorageSize();
  static double RandDouble();
  static uint64_t RandUint64();
  static uint64_t RandUint64(uint64_t max);
  static uint64_t RandUint64(uint64_t min, uint64_t max);

  static void InitRand(uint32_t thd_id) { srand48_r(thd_id, &rand_buffer_); }

 private:
  size_t table_cnt_{0};
  std::vector<ITable *> tables_;
  size_t g_tbl_id_{1}; 
  size_t n_tbl_id_{2};
  OID u_tbl_id_{0};

  static __thread drand48_data rand_buffer_;
};

} // arboretum


#endif //ARBORETUM_DISTRIBUTED_SRC_DB_DB_H_
