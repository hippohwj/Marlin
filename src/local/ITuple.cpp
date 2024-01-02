//
// Created by Zhihan Guo on 3/31/23.
//

#include "ITuple.h"

namespace arboretum {

void ITuple::SetData(char *src, size_t sz) {
  memcpy(GetData(), src, sz);
}

void ITuple::GetField(ISchema * schema, OID col, SearchKey &key) {
  auto val = &(GetData()[schema->GetFieldOffset(col)]);
  switch (schema->GetFieldType(col)) {
    case DataType::BIGINT:
      key = SearchKey(*((int64_t *) val), DataType::BIGINT);
      break;
    case DataType::INTEGER:
      key = SearchKey(*((int *) val), DataType::INTEGER);
      break;
    default:
      LOG_ERROR("Not support FLOAT8 as search key yet");
  }
}

void ITuple::GetPrimaryKey(ISchema *schema, SearchKey &key) {
  // TODO: for now assume non-composite pkeys
  auto col = schema->GetPKeyColIds()[0];
  GetField(schema, col, key);
}

bool ITuple::HasLSNInit() {
    return _lsn != "0";
}

void ITuple::SetLSN(string &new_lsn) {
    _lsn = string(new_lsn);
}

void ITuple::GetLSNStr(string &lsn) {
    lsn = _lsn;
}

}