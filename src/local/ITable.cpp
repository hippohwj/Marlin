//
// Created by Zhihan Guo on 3/30/23.
//

#include "ITable.h"
#include "ITuple.h"
#include "db/access/IndexBTreeObject.h"
#include "db/buffer/ObjectBufferManager.h"
#include "remote/IDataStore.h"
#include "common/Worker.h"
#include "common/OptionalGlobalData.h"
#include "ARDB.h"


namespace arboretum {

OID ITable::CreateIndex(OID col, IndexType tpe) {
  switch (tpe) {
    case IndexType::BTREE:
      if (g_buf_type == OBJBUF) {
        auto index = NEW(IndexBTreeObject);
        indexes_.push_back(index);
        index->AddCoveringCol(col);
        return index->GetIndexId();
      } else {
        LOG_ERROR("not supported page buffer for btree yet.");
      }
      break;
    case REMOTE:
      break;
  }
  // for only support non-composite key
  return 0;
}

bool ITable::IsSysTable() {
  return tbl_name_ == GRANULE_TABLE_NAME || tbl_name_ == NODE_TABLE_NAME; 
}

bool ITable::IsUserTable() {
  return !IsSysTable(); 
}


bool ITable::IsGranuleTable(){
  return tbl_name_ ==  GRANULE_TABLE_NAME;
}

bool ITable::IsNodeTable() {
  return tbl_name_ == NODE_TABLE_NAME;
}

IIndex * ITable::GetIndex(size_t idx_id) {
  // auto len = indexes_.size();
  return indexes_[idx_id];
}

RC ITable::InsertTuple(OID tupleid, char *data, size_t sz, bool update_lock_tbl) {
  auto row_cnt = row_cnt_.fetch_add(1);
  auto tid = tupleid;
  if (!g_storage_data_loaded) {
    if (g_buf_type == NOBUF) {
      auto tuple = new (MemoryAllocator::Alloc(GetTotalTupleSize()))
          ITuple(tbl_id_, tid);
      tuple->SetData(data, sz);
      IndexInsert(tuple);
    } else if (g_buf_type == OBJBUF) {
      if (row_cnt <= ((ObjectBufferManager *) g_buf_mgr)->GetBufferSize()) {
        auto tuple = ((ObjectBufferManager *) g_buf_mgr)->AllocTuple(
            GetTotalTupleSize(), tbl_id_, tid);
        tuple->SetData(data, sz);
        string fake_lsn="0";
        tuple->SetLSN(fake_lsn);
        // LOG_INFO("Index insert for tuple ");
        IndexInsert(tuple);
        // need to unpin
        // arboretum::ObjectBufferManager::FinishAccessTuple(tuple, true);
        // ObjectBufferManager::FinishAccessTuple(tuple, fake_lsn, true);
        //TODO(hippo): optimize insert tuple into better warm up phase 
        double r = ARDB::RandDouble();
        bool is_dirty = (g_replay_enable)? false: r < (1 - g_read_percent);
        ObjectBufferManager::FinishAccessTuple(tuple, is_dirty);
      }
    } else {
      // keep inserting in the latest page without holding latches.
      // if full, allocate a new page and finish accessing this one
      // once all done. send a message to close last one.
    }
  }
  if (update_lock_tbl) {
      // TODO: not thread safe. for future insert request after loading, need to
      //  protect it with latches
      // insert into lock table
      lock_tbl_[tid] = NEW(std::atomic<uint32_t>)(0);
  }
  return OK;
}

RC ITable::InsertTuple(OID tupleid, char *data, size_t sz) {
  InsertTuple(tupleid, data, sz, true);
}

void ITable::BatchInsert(OID partition, unordered_map<uint64_t, std::string> * map) {
    g_data_store->AsyncBatchWrite(GetTableName(), partition, map);
}


void ITable::IndexInsert(ITuple * p_tuple) {
  // insert into index
  if (g_index_type == IndexType::REMOTE) {
      SearchKey pkey;
      p_tuple->GetPrimaryKey(schema_, pkey);
      //TODO(Hippo): find a better way to transfer key to granule id for current table
      uint64_t granule_id = GetGranuleID(pkey.ToUInt64());
      if (!IsSysTable()) {
         g_data_store->Write(tbl_name_, granule_id, pkey.ToString(),
                          reinterpret_cast<char *>(p_tuple),
                          GetTotalTupleSize());
      }

  } else if (g_index_type == IndexType::BTREE) {
    for (auto & index : indexes_) {
      // generate search key for corresponding index
      SearchKey idx_key;
      // TODO: does not support composite key yet
      p_tuple->GetField(schema_, *index->GetCols().begin(), idx_key);
      // LOG_INFO("insert into index for insert txn for key (%lu)", idx_key.ToUInt64());
      ((IndexBTreeObject *) index)->Insert(idx_key, p_tuple);
    }
  }
}

RC ITable::InsertPreheatTuple(char * preloaded_data) {
  M_ASSERT(g_index_type == IndexType::BTREE,
           "only support btree index for cache preheat");
  for (auto &index : indexes_) {
    auto tuple = ((ObjectBufferManager *)g_buf_mgr)
                     ->LoadFromPreloadData(tbl_id_, preloaded_data);
    //get the primary key from the tuple
    SearchKey idx_key;
    tuple->GetField(schema_, *index->GetCols().begin(), idx_key);
    ((IndexBTreeObject *)index)->Insert(idx_key, tuple);
  }
  return OK;
}

SharedPtr *
ITable::IndexSearch(SearchKey key, OID idx_id, int limit) {
   return IndexSearchOrFill(key, idx_id, limit, nullptr);
}

SharedPtr *
ITable::IndexSearchOrFill(SearchKey key, OID idx_id, int limit, char * preload_tuple) {
  if (g_index_type == IndexType::REMOTE) {
    M_ASSERT(limit == 1, "Currently only support limit = 1");
    auto tuples = NEW_SZ(SharedPtr, limit);
    std::string data;
    uint64_t granule_id = GetGranuleID(key.ToUInt64());
    g_data_store->Read(tbl_name_, granule_id, key.ToString(), data);
    auto tuple = NEW_SZ(ITuple, data.size());
    memcpy(tuple, data.c_str(), data.size());
    M_ASSERT(tuple->GetTableId() == tbl_id_, "Loaded data does not match!");
    SearchKey pkey;
    tuple->GetPrimaryKey(schema_, pkey);
    M_ASSERT(pkey == key, "Loaded data does not match!");
    // M_ASSERT(tuple->GetTID() == key.ToUInt64(), "Loaded data does not match!");
    tuples[0].Init(tuple);
    return tuples;
  } else if (g_index_type == IndexType::BTREE) {
    if (g_buf_type == OBJBUF) {
      auto index = (IndexBTreeObject *) indexes_[idx_id];
      auto key_value = key.ToUInt64(); 
      // TODO: remove this hard code
      // if (IsUserTable() && (g_node_id == 3) && cache_preheat_mask->contains(key_value)) {
      //   // cache miss! need to load from storage
      //   LOG_INFO("[Cache Preheat Delta]cache miss and load into index for key (%lu)", key_value);
      //   auto tuple =
      //       ((ObjectBufferManager *)g_buf_mgr)->LoadFromStorage(tbl_id_, key);
      //   auto tags = index->Insert(key, tuple, true);
      //   cache_preheat_mask->remove(key_value);
      //   if (g_warmup_finished) {
      //       Worker::dbstats_.int_stats_.misses_++;
      //       Worker::dbstats_.int_stats_.accesses_++;
      //   }
      //   return tags;
      // } else {
        auto search_start = GetSystemClock();
        auto tags = index->Search(key);
        auto search_end = GetSystemClock();
        if (g_warmup_finished) {
            // Worker::dbstats_.int_stats_.log_time_ns_ += endtime - starttime;
            Worker::dbstats_.int_stats_.idx_search_time_ns_ +=
                search_end - search_start;
        }

        if (!tags) {
            if (preload_tuple) {
                // cache miss! need to load from storage
                auto tuple = ((ObjectBufferManager *)g_buf_mgr)
                                 ->LoadFromPreloadData(tbl_id_, preload_tuple);
                tags = index->Insert(key, tuple, true);
            } else {
                // cache miss! need to load from storage
                LOG_INFO("cache miss and load into index for key (%lu)",
                         key.ToUInt64());
                auto tuple = ((ObjectBufferManager *)g_buf_mgr)->LoadFromStorage(tbl_id_, key);
                tags = index->Insert(key, tuple, true);
                if (g_warmup_finished) {
                    Worker::dbstats_.int_stats_.misses_++;
                    // if (g_node_id == g_scale_node_id) {
                    //   cout << "XXX: cache miss ++ and cur miss stats is " <<
                    //   Worker::dbstats_.int_stats_.misses_ << endl;
                    // }
                }
            }
        }
        if (g_warmup_finished) {
            Worker::dbstats_.int_stats_.accesses_++;
            // if (g_node_id == g_scale_node_id) {
            //  cout << "XXX: cache access ++ and cur access stats is " <<
            //  Worker::dbstats_.int_stats_.accesses_<< endl;
            // }
        }
        return tags;
      // }

    } else {
      LOG_DEBUG("not supported");
    }
  }
  return nullptr;
}

// TODO: change the interface to delete by key
void ITable::IndexDelete(OID tupleid, char *data, size_t sz,
                         bool update_lock_tbl) {
  // auto tuple = ((ObjectBufferManager *)g_buf_mgr)
  //                  ->AllocTuple(GetTotalTupleSize(), tbl_id_, tupleid);
  auto tuple = new (MemoryAllocator::Alloc(GetTotalTupleSize())) ITuple(tbl_id_, tupleid);
  tuple->SetData(data, sz);
  string fake_lsn = "0";
  tuple->SetLSN(fake_lsn);
  IndexDelete(tuple);
  DEALLOC(tuple);
}


void ITable::Delete(SearchKey key) {
  auto tuples = IndexSearch(key, 0, 1);
  // table->IndexDelete(reinterpret_cast<ITuple *>(tuples[0].Get()));
  auto tuple = reinterpret_cast<ITuple *>(tuples[0].Get()); 
  M_ASSERT(tuple != nullptr, "XXX cannot find tuple for with key %d", key.ToUInt64());
  ((ObjectBufferManager *)g_buf_mgr)->Evict(tuple);
}

void ITable::IndexDelete(ITuple *p_tuple) {
  // delete from all indexes
  if (g_index_type == IndexType::REMOTE) {
    LOG_ERROR("should not happen");
  } else if (g_index_type == IndexType::BTREE) {
    size_t index_id = 0;
    auto myid = this_thread::get_id();
      stringstream ss;
      ss << myid;
      string mystring = ss.str();
    for (auto &index : indexes_) {
      // generate search key for corresponding index
      SearchKey idx_key;
      // TODO: does not support composite key yet
      p_tuple->GetField(schema_, *index->GetCols().begin(), idx_key);
  
      LOG_INFO(
          "index delete for key (%lu) with tuple id (%lu) and granule_id(%lu, "
          "%lu) with idx id %d in thread %s",
          idx_key.ToUInt64(), p_tuple->GetTID(),
          GetGranuleID(idx_key.ToUInt64()), GetGranuleID(p_tuple->GetTID()),
          index_id, mystring.c_str());
      ((IndexBTreeObject *)index)->Delete(idx_key, p_tuple);
      index_id++;
    }
  }
}

} // arboretum