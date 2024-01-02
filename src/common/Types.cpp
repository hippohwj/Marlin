//
// Created by Zhihan Guo on 3/31/23.
//
#include "Types.h"


namespace arboretum {
DataType StringToDataType(std::string &s) {
  if (s == "string") {
    return DataType::CHAR;
  } else if (s == "int64_t") {
    return DataType::BIGINT;
  } else if (s == "double") {
    return DataType::FLOAT8;
  }
  LOG_ERROR("type %s not supported", s.c_str());
}

BufferType StringToBufferType(std::string &s) {
  if (s == "NOBUF") {
    return BufferType::NOBUF;
  } else if (s == "OBJBUF") {
    return BufferType::OBJBUF;
  } else if (s == "PGBUF") {
    return BufferType::PGBUF;
  }
  LOG_ERROR("type %s not supported", s.c_str());
}

IndexType StringToIndexType(std::string &s) {
  if (s == "REMOTE") {
    return IndexType::REMOTE;
  } else if (s == "BTREE") {
    return IndexType::BTREE;
  }
  LOG_ERROR("type %s not supported", s.c_str());
}

std::string BufferTypeToString(BufferType tpe) {
  switch (tpe) {
    case BufferType::OBJBUF:
      return "OBJBUF";
    case NOBUF:
      return "NOBUF";
    case PGBUF:
      return "PGBUF";
  }
}

std::string IndexTypeToString(IndexType tpe) {
  switch (tpe) {
    case IndexType::BTREE:
      return "BTREE";
    case IndexType::REMOTE:
      return "REMOTE";
  }
}

std::string BoolToString(bool tpe) {
  if (tpe) {
    return "True";
  }
  return "False";
}


}