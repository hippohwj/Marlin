//
// Created by Zhihan Guo on 4/3/23.
//

#ifndef ARBORETUM_SRC_DB_BUFFER_OBJECTBUFFERMANAGER_H_
#define ARBORETUM_SRC_DB_BUFFER_OBJECTBUFFERMANAGER_H_

#include "BufferManager.h"
#include "Common.h"
#include "ITable.h"
#include "common/GlobalData.h"

namespace arboretum {

class ARDB;

class ObjectBufferManager : public BufferManager {
 public:
  explicit ObjectBufferManager(ARDB *db);
  ITuple * AllocTuple(size_t tuple_sz, OID tbl_id, OID tid);
  static ITuple *AccessTuple(SharedPtr &ptr);
  ITuple * LoadFromStorage(OID tbl_id, SearchKey key);
  ITuple * LoadFromPreloadData(OID tbl_id, char * pre_loaded_data);
  ITuple * Load(OID tbl_id, SearchKey* key, char * pre_loaded_data);
  void Evict(ITuple *tuple);


  static void FinishAccessTuple(SharedPtr &ptr,  bool dirty=false);
  static void FinishAccessTuple(ITuple *tuple, bool dirty);
  static void ClockPrint(ITuple * evict_hand, ITable * table, ITuple * tuple);
  static void ClockHandPrint(ITuple * evict_hand, ITable * table);
  static void ClockAllPrint(ITuple * evict_hand, ITable * table);

  bool ShouldSkip(ITuple * cur_hand);

  size_t GetBufferSize() const { return num_slots_; };
  bool IsWarmedUp() const {
    bool warmed = allocated_ >= 0.999 * num_slots_;
    LOG_DEBUG("checking warm up status = %d (allocated_ = %lu, total = %u)",
              warmed, allocated_.load(), num_slots_);
    return warmed;
  };

 private:
  static inline bool CheckFlag(uint32_t state, uint64_t flag) { return (state & BUF_FLAG_MASK) & flag; }
  static inline uint32_t ClearFlag(uint32_t state, uint64_t flag) { state &= ~flag; return state; }
  static inline uint32_t SetFlag(uint32_t state, uint64_t flag) { state |= flag;
    return state;}
  static inline uint32_t GetPinCount(uint32_t state) { return BUF_STATE_GET_REFCOUNT(state); }
  static inline uint32_t GetUsageCount(uint32_t state) { return BUF_STATE_GET_USAGECOUNT(state); }
  static inline bool IsPinned(uint32_t state) { return GetPinCount(state) > 0; }
  static inline void Pin(ITuple * row);
  static inline void UnPin(ITuple * row);

 private:
  void EvictAndLoad(const std::string& storage_key, ITuple *tuple, bool load = false);
  void EvictAndFill(ITuple *tuple, char * preloaded_data, const std::string& storage_key, bool load);
  void EvictAndPreload(ITuple *tuple, char * preloaded_data);

  ARDB *db_{nullptr};
  ITuple *evict_hand_{nullptr};
  ITuple *evict_tail_{nullptr};
  size_t num_slots_{0};
  std::atomic<size_t> allocated_{0};
  std::mutex latch_;
};

}

#endif //ARBORETUM_SRC_DB_BUFFER_OBJECTBUFFERMANAGER_H_
