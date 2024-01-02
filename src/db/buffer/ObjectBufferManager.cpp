//
// Created by Zhihan Guo on 4/3/23.
//

#include "ObjectBufferManager.h"
#include "local/ITable.h"
#include "db/ARDB.h"
#include "remote/IDataStore.h"
#include "common/Worker.h"
#include "common/GlobalData.h"
#include "common/OptionalGlobalData.h"
#include "log/LogSeqNum.h"


namespace arboretum {

ObjectBufferManager::ObjectBufferManager(ARDB *db) : db_(db) {
  num_slots_ = g_total_buf_sz / 1000; // an estimate for now
  // evict_end_threshold_ is used for AllocTuple caused eviction only
  // on-demand eviction only evict till need
  LOG_INFO("Initiated Object Buffer with %zu slots", num_slots_);
}

ITuple *ObjectBufferManager::AccessTuple(SharedPtr &ptr) {
  // called after lock permission is granted.
  auto tuple = reinterpret_cast<ITuple *>(ptr.Get());
  if (!tuple) {
    LOG_ERROR("AccessTuple: invalid addr");
  }
  tuple->buf_latch_.lock();
  Pin(tuple);
  tuple->buf_latch_.unlock();
  return tuple;
}

void ObjectBufferManager::FinishAccessTuple(SharedPtr &ptr, bool dirty) {
  auto tuple = reinterpret_cast<ITuple *>(ptr.Get());
  FinishAccessTuple(tuple, dirty);
  ptr.Free();
}

void ObjectBufferManager::FinishAccessTuple(ITuple * tuple, bool dirty) {
  assert(tuple);
  tuple->buf_latch_.lock();
  //TODO(hippo): remove this hack way of getting primary key
  // auto val = &(tuple->GetData()[0]);
  // auto key = *((int64_t *) val);
  // LOG_INFO("Write dirty tuple for key(%s) with lsn (%s)", to_string(key).c_str(), lsn.c_str());
  UnPin(tuple);
  if (dirty)
    tuple->buf_state_ = SetFlag(tuple->buf_state_, BM_DIRTY);
  tuple->buf_latch_.unlock();
}

void ObjectBufferManager::ClockPrint(ITuple * evict_hand, ITable * table, ITuple * tuple) {
  if (!evict_hand) {
    SearchKey idx_key;
    tuple->GetPrimaryKey(table->GetSchema(), idx_key);
    LOG_INFO("clock insert tuple as the new hand for key (%lu)", idx_key.ToUInt64());
  } else {
    SearchKey idx_key;
    tuple->GetPrimaryKey(table->GetSchema(), idx_key);
    SearchKey hand_key;
    evict_hand->GetPrimaryKey(table->GetSchema(), hand_key);
    LOG_INFO("clock insert tuple for key (%lu), current hand is for key (%lu)", idx_key.ToUInt64(), hand_key.ToUInt64());
  }

}

void ObjectBufferManager::ClockHandPrint(ITuple * evict_hand, ITable * table) {
    SearchKey hand_key;
    evict_hand->GetPrimaryKey(table->GetSchema(), hand_key);
    LOG_INFO("clock HandPrint currrent hand is for key (%lu)", hand_key.ToUInt64());
}

void ObjectBufferManager::ClockAllPrint(ITuple * evict_hand, ITable * table) {

    auto item = evict_hand->clock_next_;
    // auto count= 0;
    // while (item != evict_hand && count < 20)
    auto should_print=false;
    while (item != evict_hand)
    {
      SearchKey key;
      item->GetPrimaryKey(table->GetSchema(), key);
      if (key.ToUInt64() == 1) {
        should_print=true;
      }
      // LOG_INFO("next key is (%lu)", key.ToUInt64());
      item = item->clock_next_;
      // count++;
    }

    if (should_print) {
          LOG_INFO("clock start");
    SearchKey hand_key;
    evict_hand->GetPrimaryKey(table->GetSchema(), hand_key);
    LOG_INFO("currrent hand is for key (%lu)", hand_key.ToUInt64());
       item = evict_hand->clock_next_;
       while (item != evict_hand)
       {
          SearchKey key;
          item->GetPrimaryKey(table->GetSchema(), key);
          LOG_INFO("next key is (%lu)", key.ToUInt64());
          item = item->clock_next_;
          // count++;
       }
           LOG_INFO("clock end");
    }
}

ITuple * ObjectBufferManager::AllocTuple(size_t tuple_sz, OID tbl_id, OID tid) {
  auto tuple = new (MemoryAllocator::Alloc(tuple_sz)) ITuple(tbl_id, tid);
  auto buf_sz = allocated_.fetch_add(1);

  if (buf_sz > num_slots_) {
    EvictAndLoad("", tuple, false);
  } else {
    latch_.lock();
    Pin(tuple);
    auto table = db_->GetTable(tbl_id);
    ClockInsert(evict_hand_, evict_tail_, tuple);
    latch_.unlock();
  }
  return tuple;
}

ITuple *ObjectBufferManager::LoadFromStorage(OID tbl_id, SearchKey key) {
  Load(tbl_id, &key, nullptr);
}
ITuple * ObjectBufferManager::LoadFromPreloadData(OID tbl_id, char * pre_loaded_data) {
  Load(tbl_id, nullptr, pre_loaded_data);
}

ITuple *ObjectBufferManager::Load(OID tbl_id, SearchKey* key, char * pre_loaded_data) {
  // will delete tuple from index if evicted.
  auto table = db_->GetTable(tbl_id);
  auto tuple_sz = table->GetTotalTupleSize();
  auto tuple = new (MemoryAllocator::Alloc(tuple_sz)) ITuple(tbl_id, 0);
  // auto storage_key = ITuple::GenStorageKey(tbl_id, key);
  

  auto storage_key = (key)? to_string(key->ToUInt64()): "";
  auto buf_sz = allocated_.fetch_add(1);
  // std::cout << "load from storage allocated : " << buf_sz << std::endl;
  if (buf_sz > num_slots_) {
    if (pre_loaded_data) {
      // std::cout << "XXX Evict and Load => Search key is : " << key->ToUInt64() << std::endl;
      EvictAndPreload(tuple, pre_loaded_data);
    } else {
      EvictAndLoad(storage_key, tuple, true);
    }
  } else {
    if (pre_loaded_data) {
      tuple->SetFromLoaded(pre_loaded_data, table->GetTupleDataSize());
      // SearchKey idx_key;
      // tuple->GetPrimaryKey(table->GetSchema(), idx_key);
      // cout<< "XXX load tuple from pre_loaded data with key " << idx_key.ToUInt64() << endl;
    } else {
      std::string data;
      auto starttime = GetSystemClock();
      uint64_t granule_id = GetGranuleID(key->ToUInt64());
      g_data_store->Read(table->GetTableName(), granule_id, storage_key, data);
      auto endtime = GetSystemClock();
      if (g_warmup_finished) {
          // Worker::dbstats_.int_stats_.log_time_ns_ += endtime - starttime;
          Worker::dbstats_.int_stats_.miss_load_count_ += 1;
          Worker::dbstats_.int_stats_.miss_load_time_ns_ += endtime - starttime;
      }

      // don't copy everything, only copy things you need
      tuple->SetFromLoaded(const_cast<char *>(data.c_str()),
                           table->GetTupleDataSize());
    }

    // LOG_INFO("Newly loaded tuple of key %s with lsn %s", key.ToString().c_str(), tuple->GetLSNStr().c_str());
    // string tuple_lsn;
    // tuple->GetLSNStr(tuple_lsn);
    // std::cout << "Newly loaded tuple of key " << key.ToString() << " with lsn " << tuple_lsn << std::endl; 
    // M_ASSERT(key == pkey, "Loaded data (pkey) does not match");
    // no need to hold individual latch since no one can access it now
    Pin(tuple);
    UnPin(tuple); // to just increase usage count to avoid being evicted.
    // add to buffer
    latch_.lock();
    // TODO: does not support composite key yet
    ClockInsert(evict_hand_, evict_tail_, tuple);
    latch_.unlock();
  }

  if (key) {
    // TODO: will delete. fix pre-loaded polluted data
    SearchKey pkey;
    tuple->GetPrimaryKey(table->GetSchema(), pkey);
    if ((*key) != pkey) {
      std::cout <<"data pollution in storage, key is " << key->ToUInt64() << ", pkey is " << pkey.ToUInt64() << std::endl;
      LOG_ERROR("data pollution in storage, key is %d, pkey is %d!", key->ToUInt64(), pkey.ToUInt64());
      table->GetSchema()->SetPrimaryKey(tuple->GetData(), key->ToUInt64());
      tuple->SetTID(key->ToUInt64());
      tuple->buf_state_ = SetFlag(tuple->buf_state_, BM_DIRTY);
    }
  }
  return tuple;
}

// if latched or replay lag back, skip; otherwise, try write lock.
bool ObjectBufferManager::ShouldSkip(ITuple * cur_hand) {
  //TODO(Hippo): avoid add systemtable item into clock
  if (db_->IsSysTable(cur_hand->GetTableId())) {
    return true;
  }
  if (IsPinned(cur_hand->buf_state_)) {
     return true;
  } 

  if (g_replay_enable) {
        string lsn;
        cur_hand->GetLSNStr(lsn); 
        bool is_log_dirty = cur_hand->HasLSNInit() && !replay_lsn->has_replayed(lsn);
        if (is_log_dirty) {
           // a hack way to get primary key
          auto val = &(cur_hand->GetData()[0]);
          auto key = *((int64_t *) val);
          LOG_INFO("Avoid eviction for key (%s) with lsn(%s), because it hasn't been replayed yet", to_string(key).c_str(), lsn.c_str());
        }
        return is_log_dirty;
  }
}

void ObjectBufferManager::Evict(ITuple *tuple) {
  //  EvictAndFill(tuple, nullptr, storage_key, load);
    auto table = db_->GetTable(tuple->GetTableId());
    // latch_.lock();
    // ClockRemoveHand(tuple, evict_tail_, allocated_);
    // latch_.unlock();
    table->IndexDelete(tuple); // will unlock and free
}

void ObjectBufferManager::EvictAndPreload(ITuple *tuple, char * preloaded_data) {
     EvictAndFill(tuple, preloaded_data, "", true);
}

void ObjectBufferManager::EvictAndLoad(const std::string& storage_key,
                                       ITuple *tuple, bool load) {
   EvictAndFill(tuple, nullptr, storage_key, load);
}




void ObjectBufferManager::EvictAndFill(ITuple *tuple, char * preloaded_data, const std::string& storage_key, bool load) {
  latch_.lock();
  auto start = evict_hand_;
  auto rounds = 0;
  auto starttime = GetSystemClock();

  while(true) {
    if (!evict_hand_ || allocated_ == 0) {
      LOG_ERROR("Run out of buffer, increase buffer size!");
    }
    auto table = db_->GetTable(evict_hand_->GetTableId());
    // if (!IsPinned(evict_hand_->buf_state_) && evict_hand_->buf_latch_.try_lock()) {
    if (!ShouldSkip(evict_hand_) && evict_hand_->buf_latch_.try_lock()) {
      // already held tuple latch.
      if (GetUsageCount(evict_hand_->buf_state_) != 0) {
        evict_hand_->buf_state_ -= BUF_USAGECOUNT_ONE;
        evict_hand_->buf_latch_.unlock();
        ClockAdvance(evict_hand_, evict_tail_);
        continue;
      }
      // try to evict the tuple
      // write to storage if dirty and load expected in the same roundtrip
      bool flush = CheckFlag(evict_hand_->buf_state_, BM_DIRTY);
      auto flush_tuple = evict_hand_;
      auto flush_size = table->GetTotalTupleSize();
      // auto flush_key = flush_tuple->GetPrimaryStorageKey(table->GetSchema());
      SearchKey flush_key;
      flush_tuple->GetPrimaryKey(table->GetSchema(), flush_key);
      // add tuple to buffer and remove evict_hand_ from buffer
      ClockRemoveHand(evict_hand_, evict_tail_, allocated_);
      Pin(tuple);
      UnPin(tuple);
      ClockInsert(evict_hand_, evict_tail_, tuple);
      latch_.unlock();
      auto endtime = GetSystemClock();
      std::string data;
      auto flush_start_time = GetSystemClock();
      if (flush) {
        M_ASSERT(false, "need to support correct store/fetch pattern using table name + granule id");
        assert(!g_replay_enable);
        // need to flush, then flush and load
        if (load) {
          g_data_store->WriteAndRead(flush_key.ToString(), reinterpret_cast<char *>(flush_tuple), flush_size,
                                     storage_key, data);
          tuple->SetFromLoaded(const_cast<char *>(data.c_str()), table->GetTupleDataSize());
          // memcpy(tuple, data.c_str(), data.size());
        } else {
          g_data_store->Write(db_->GetTable(flush_tuple->GetTableId())->GetTableName(), g_granule_id, flush_key.ToString(), reinterpret_cast<char *>(flush_tuple), flush_size);
        }
        flush_tuple->buf_latch_.unlock();
        table->IndexDelete(flush_tuple); // will unlock and free
        if (g_warmup_finished) {
           Worker::dbstats_.int_stats_.evict_flush_count_ += 1;
        }
        // LOG_DEBUG("Flushed tuple %s and load tuple %s", flush_key.ToString().c_str(), storage_key.c_str());
      } else if (load) {
      // if (load) {
        // no need to flush, just load
        flush_tuple->buf_latch_.unlock();
        if (preloaded_data) {
          tuple->SetFromLoaded(preloaded_data, table->GetTupleDataSize());
        } else {
          uint64_t granule_id = GetGranuleID(std::stoull(storage_key));
          g_data_store->Read(table->GetTableName(), granule_id, storage_key, data);
          tuple->SetFromLoaded(const_cast<char *>(data.c_str()), table->GetTupleDataSize());
          // SearchKey pkey;
          // tuple->GetPrimaryKey(table->GetSchema(), pkey);
          // M_ASSERT(tuple->GetTID() == pkey.ToUInt64(), "Loaded data (tid) does not match");
          // memcpy(tuple, data.c_str(), data.size());
          // LOG_DEBUG("Evicted tuple %s and load tuple %s", flush_key.ToString().c_str(), storage_key.c_str());
        }

      } else {
        flush_tuple->buf_latch_.unlock();
        table->IndexDelete(flush_tuple); // will unlock and free
        // LOG_DEBUG("Evicted tuple %s", flush_key.ToString().c_str());
      }
      auto flush_end_time = GetSystemClock();

     if (g_warmup_finished) {
        // Worker::dbstats_.int_stats_.log_time_ns_ += endtime - starttime;
        Worker::dbstats_.int_stats_.evict_count_ += 1;
        Worker::dbstats_.int_stats_.evict_time_ns_ += endtime - starttime;
        Worker::dbstats_.int_stats_.evict_flush_load_time_ns_ += flush_end_time - flush_start_time;
     }

      return;
    } else {
//       LOG_DEBUG("Skip pinned row (state = %u, pin count = %u, is_pinned = %d)",
//                 evict_hand_->buf_state_.load(),
//                 GetPinCount(evict_hand_->buf_state_.load()),
//                 IsPinned(evict_hand_->buf_state_.load()));
      //  LOG_DEBUG("Clock advance because the current hand is lateched");
       ClockAdvance(evict_hand_, evict_tail_);
       if (evict_hand_ == start) {
         rounds++;
         if (rounds >= BM_MAX_USAGE_COUNT) {
           LOG_ERROR("All pinned!")
         }
       }
    }
  }
}

void ObjectBufferManager::Pin(ITuple *row) {
  row->buf_state_ += BUF_REFCOUNT_ONE;
  // update usage count (weight)
  if (GetUsageCount(row->buf_state_) < BM_MAX_USAGE_COUNT) {
    row->buf_state_ += BUF_USAGECOUNT_ONE;
  }
}

void ObjectBufferManager::UnPin(ITuple *row) {
  row->buf_state_ -= BUF_REFCOUNT_ONE;
}

} // arboretum