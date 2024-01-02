#include "ILogStore.h"
#include "common/Worker.h"

using namespace Azure::Storage::Blobs;
namespace arboretum {

ILogStore::ILogStore(bool isSysLog) {
#if REMOTE_STORAGE_TYPE == REMOTE_STORAGE_REDIS
    if (!isSysLog) {
        client_ = NEW(RedisClient)(g_log_store_host, g_log_store_port);
    } else {
        client_ = NEW(RedisClient)(g_syslog_store_host, g_syslog_store_port);
    }
#elif REMOTE_STORAGE_TYPE == REMOTE_STORAGE_AZURE
    M_ASSERT(azure_log_conn_str != "",  "need to initialize azure_log_conn_str");

    if (!isSysLog) {
        client_ = NEW(AzureLogClient)(azure_log_conn_str, g_node_id);
    } else {
        client_ = NEW(AzureLogClient)(azure_log_conn_str, g_sys_log_id);
    }
#endif
}


void ILogStore::Log(uint64_t node_id, LogEntry* logEntry) {
  if (g_groupcommit_enable) {
    if (g_gc_future) {
      // use future and block wait to reduce cpu consumption
      if (g_replay_from_buffer_enable) {
        auto lsn_future = logEntry->GetLSNFuture();
        gc_ring_buffer->Write(logEntry);
        // block wait for group commit
        lsn_future.get();
      } else {
        gc_buffer->AcquireLock();
        auto lsn_future = logEntry->GetLSNFuture();
        gc_buffer->PutLog(logEntry);
        gc_buffer->ReleaseLock();
        // block wait for group commit
        lsn_future.get();
      }
    } else {
      // use epoch and busy wait
        gc_buffer->AcquireLock();
        auto epoch = gc_buffer->GetEpoch();
        gc_buffer->PutLog(logEntry);
        gc_buffer->ReleaseLock();
        //busy wait for group commit
        while (epoch >= gc_buffer->GetEpoch()) {};
    }
  } else {
    #if REMOTE_STORAGE_TYPE == REMOTE_STORAGE_REDIS
    string log_lsn;
    client_->log_sync_data(node_id, logEntry->GetTxnId(), logEntry->GetStatus(), logEntry->GetData(), log_lsn);
    logEntry->SetLSN(log_lsn);
    #elif REMOTE_STORAGE_TYPE == REMOTE_STORAGE_AZURE 
    M_ASSERT(false, "azure log store only support group commit, please use group commit");
    #endif
  }
}

void ILogStore::GroupLog(uint64_t node_id, vector<LogEntry *> *logs,
                         string &lsn) {
#if REMOTE_STORAGE_TYPE == REMOTE_STORAGE_REDIS
  client_->GroupLogSync(node_id, logs, lsn);
#elif REMOTE_STORAGE_TYPE == REMOTE_STORAGE_AZURE
  auto start_time = GetSystemClock();

  vector<uint8_t> buffer;
  for (vector<LogEntry *>::iterator it = logs->begin(); it < logs->end();
       it++) {
    LogEntry *entry = *it;
    entry->Serialize(&buffer);
  }
  bool append_finish = false;
  while (!append_finish) {
    auto res = client_->GroupLogSync(cur_lsn_.blob_idx_, &buffer,lsn);
    append_finish = res.append_res;
    if (res.blob_st == BlobStatus::COMPLETE) {
      cur_lsn_.blob_idx_ ++;
    }
  }
#endif
}

// TODO: implement the remainning methods for azure

#if REMOTE_STORAGE_TYPE == REMOTE_STORAGE_REDIS

void ILogStore::AsyncLog(uint64_t node_id, LogEntry* logEntry, const cpp_redis::reply_callback_t &reply_callback) {
      client_->logDataAsync(node_id, logEntry->GetTxnId(), logEntry->GetStatus(), logEntry->GetData(), reply_callback);
}


void ILogStore::Log(uint64_t node_id, uint64_t txn_id, int status, string &data, const cpp_redis::reply_callback_t &reply_callback) {
  client_->log_sync_data(node_id, txn_id, status, data, reply_callback);
}

//TODO(Hippo): remove callback
void ILogStore::BatchRead(uint64_t node_id, string read_start, uint16_t batch_size,
                                       const cpp_redis::reply_callback_t &reply_callback) {
       client_->read_logs_next_batch_sync(node_id,read_start, batch_size, reply_callback);

}

#elif REMOTE_STORAGE_TYPE == REMOTE_STORAGE_AZURE 
future<RC> ILogStore::AsyncLog(uint64_t node_id, LogEntry* logEntry) {

}

//TODO(Hippo): remove callback
void ILogStore::BatchRead(vector<LogEntry *> *log_entries) {
  // TODO: set batch_size as global variable
  uint32_t batch_size = g_replay_fetch_batch_mb * 1024 * 1024;
  auto res = client_->ReadNextBatch(&cur_lsn_, batch_size, &batch_read_buffer_);
  if (res.read_size > 0) {
    while (
        LogEntry::HasLogEntry(batch_read_buffer_.data() + cur_lsn_.offset_,
                              batch_read_buffer_.size() - cur_lsn_.offset_)) {
      LogEntry *log_entry =
          LogEntry::Deserialize(batch_read_buffer_.data() + cur_lsn_.offset_);
      string cur_lsn_str = cur_lsn_.GetLSNStr();
      log_entry->SetLSN(cur_lsn_str);
      log_entries->push_back(log_entry);
      cur_lsn_.offset_ += log_entry->GetSerializedLen();
    }
    // LOG_INFO("XXX: Log replay batch read %d log entries", log_entries->size());
  } else {
    // res.read_size == 0 can only happens when there is no entry in log to read or current blob has been fetched completely
     M_ASSERT(
        cur_lsn_.offset_ == batch_read_buffer_.size(),
        "unexpected case for batchread: curlsn offset is %d, buffer size is %d",
        cur_lsn_.offset_, batch_read_buffer_.size());
  }

  if (res.st == BlobStatus::COMPLETE) {
    M_ASSERT(
        cur_lsn_.offset_ == batch_read_buffer_.size(),
        "unexpected case for batchread: curlsn offset is %d, buffer size is %d",
        cur_lsn_.offset_, batch_read_buffer_.size());
    cur_lsn_.blob_idx_++;
    cur_lsn_.offset_ = 0;
    batch_read_buffer_.clear();
  }
}

void ILogStore::BatchRead(function<void(LogEntry *log_entry)> read_callback) {
  // TODO: set batch_size as global variable
  uint32_t batch_size = g_replay_fetch_batch_mb * 1024 * 1024;
  auto res = client_->ReadNextBatch(&cur_lsn_, batch_size, &batch_read_buffer_);
  if (res.read_size > 0) {
    while (
        LogEntry::HasLogEntry(batch_read_buffer_.data() + cur_lsn_.offset_,
                              batch_read_buffer_.size() - cur_lsn_.offset_)) {
      LogEntry *log_entry =
          LogEntry::Deserialize(batch_read_buffer_.data() + cur_lsn_.offset_);
      string cur_lsn_str = cur_lsn_.GetLSNStr();
      log_entry->SetLSN(cur_lsn_str);
      read_callback(log_entry);
      cur_lsn_.offset_ += log_entry->GetSerializedLen();
    }
    // LOG_INFO("XXX: Log replay batch read %d log entries", log_entries->size());
  } else {
    // res.read_size == 0 can only happens when there is no entry in log to read or current blob has been fetched completely
     M_ASSERT(
        cur_lsn_.offset_ == batch_read_buffer_.size(),
        "unexpected case for batchread: curlsn offset is %d, buffer size is %d",
        cur_lsn_.offset_, batch_read_buffer_.size());
  }

  if (res.st == BlobStatus::COMPLETE) {
    M_ASSERT(
        cur_lsn_.offset_ == batch_read_buffer_.size(),
        "unexpected case for batchread: curlsn offset is %d, buffer size is %d",
        cur_lsn_.offset_, batch_read_buffer_.size());
    cur_lsn_.blob_idx_++;
    cur_lsn_.offset_ = 0;
    batch_read_buffer_.clear();
  }
}


#endif



}