//
// Created by Zhihan Guo on 3/30/23.
//

#ifndef ARBORETUM_DISTRIBUTED_SRC_COMMON_TYPES_H_
#define ARBORETUM_DISTRIBUTED_SRC_COMMON_TYPES_H_


#include <atomic>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>
#include <cstdint>
#include <cstring>
#include <cassert>
#include <map>
#include <numeric>
#include <vector>
#include <queue>
#include <set>
#include <sstream>
#include <string>
#include <unordered_map>
#include "Message.h"
#include "MemoryAllocator.h"
#include "RWLock.h"
#include "CircularLinkedList.h"

namespace arboretum {

// type definitions
typedef uint32_t OID; // Object ID
typedef uint64_t GID; // Granule ID


// enumerations
enum RC { OK, COMMIT, ERROR, ABORT, FAIL, GTBL_UP, SUSPEND};
// enum RC {RCOK, COMMIT, ABORT, WAIT, LOCAL_MISS, SPECULATE, ERROR, FINISH, FAIL};
enum IndexType { REMOTE, BTREE };
enum AccessType { READ, UPDATE, INSERT, DELETE, SCAN };
enum QueryType {REGULAR, MIGR, GRANULE_SCAN};
enum LockType { NO_WAIT, WAIT_DIE, WOUND_WAIT, NO_LOCK};
enum BufferType { NOBUF, PGBUF, OBJBUF };
enum DataType { CHAR, VARCHAR, INTEGER, BIGINT, FLOAT8 };
DataType StringToDataType(std::string &s);
BufferType StringToBufferType(std::string &s);
IndexType StringToIndexType(std::string &s);
std::string BufferTypeToString(BufferType tpe);
std::string IndexTypeToString(IndexType tpe);
std::string BoolToString(bool tpe);

// macros
#define REMOTE_STORAGE_REDIS 1
#define REMOTE_STORAGE_AZURE 2
#define REMOTE_STORAGE_FAKE 10
#define REMOTE_STORAGE_TYPE REMOTE_STORAGE_AZURE
#define NEW(tpe) new (MemoryAllocator::Alloc(sizeof(tpe))) tpe
#define NEW_SZ(tpe, sz) new (MemoryAllocator::Alloc(sizeof(tpe) * (sz))) tpe[sz]
#define DEALLOC(ptr) MemoryAllocator::Dealloc(ptr)
#define DELETE(ptr, tpe) (ptr)->~tpe()
#define NANO_TO_US(t) t / 1000.0
#define NANO_TO_MS(t) t / 1000000.0
#define NANO_TO_S(t)  t / 1000000000.0

// structures
struct SearchKey {
  #define ARBORETUM_KEY_SIZE 21
  char data_[ARBORETUM_KEY_SIZE]{};
  // TODO(zhihan): right now double the size for faster uint64 comparison,
  //  and convert 64 bit int to base10 string (21 bytes)
  //  should convert to base64 string
  uint64_t numeric_{0};
  DataType type_{DataType::BIGINT};
  SearchKey() = default;;
  explicit SearchKey(uint64_t key) : type_(DataType::BIGINT) {
    numeric_ = key;
  };
  explicit SearchKey(uint64_t key, DataType type) : type_(type) {
    numeric_ = key;
  };
  explicit SearchKey(std::string & s) : type_(DataType::CHAR) {
    if (s.size() > ARBORETUM_KEY_SIZE) LOG_ERROR("Key size exceed limits! ");
    std::strcpy(data_, s.c_str());
    numeric_ = strtol(data_, nullptr, 10);
  };
  explicit SearchKey(char * s, size_t sz) : type_(DataType::CHAR) {
    if (sz > ARBORETUM_KEY_SIZE) LOG_ERROR("Key size exceed limits! ");
    std::strcpy(data_, s);
    numeric_ = strtol(data_, nullptr, 10);
  };
  std::string ToString() const { return std::to_string(numeric_); };
  uint64_t ToUInt64() const { return numeric_; };
  bool operator==(const SearchKey &tag) const {
    if (type_ != DataType::VARCHAR)
      return ToUInt64() == tag.ToUInt64();
    else
      return data_ == tag.data_;
  };
  bool operator!=(const SearchKey &tag) const {
    if (type_ != DataType::VARCHAR)
      return ToUInt64() != tag.ToUInt64();
    else
      return data_ != tag.data_;
  };
  bool operator<(const SearchKey &tag) const {
    if (type_ != DataType::VARCHAR)
      return ToUInt64() < tag.ToUInt64();
    else
      return data_ < tag.data_;
  }
  bool operator>(const SearchKey &tag) const {
    if (type_ != DataType::VARCHAR)
      return ToUInt64() > tag.ToUInt64();
    else
      return data_ > tag.data_;
  }
  bool operator<=(const SearchKey &tag) const {
    if (type_ != DataType::VARCHAR)
      return ToUInt64() <= tag.ToUInt64();
    else
      return data_ <= tag.data_;
  }
  struct SearchKeyHash {
    size_t operator()(const SearchKey& k) const
    {
      if (k.type_ != DataType::VARCHAR) {
        return std::hash<uint64_t>()(k.ToUInt64());
      } else {
        return std::hash<std::string>()(k.ToString());
      }
    }
  };
};

// -------------------------
// | Item Identifier (TID) |
// -------------------------

struct ItemTag {
  uint64_t ptr_;
  ItemTag() : ptr_(0) {};
  bool IsNull() const { return ptr_ == 0; };
  explicit ItemTag(uint64_t val) { ptr_ = val; };
  uint64_t ToUInt64() const { return ptr_; };
  std::string ToString() const { return std::to_string(ptr_); };
  bool operator==(const ItemTag &tag) const {
    return ptr_ == tag.ptr_;
  };
  bool operator<(const ItemTag &tag) const {
    return ptr_ < tag.ptr_;
  };
  bool operator>(const ItemTag &tag) const {
    return ptr_ > tag.ptr_;
  };
  bool operator!=(const ItemTag &tag) const {
    return ptr_ != tag.ptr_;
  };
};

} // arboretum

#endif //ARBORETUM_DISTRIBUTED_SRC_COMMON_TYPES_H_
