#ifndef ARBORETUM_SRC_REMOTE_AZURELOGCLIENT_H_
#define ARBORETUM_SRC_REMOTE_AZURELOGCLIENT_H_
#if REMOTE_STORAGE_TYPE == REMOTE_STORAGE_AZURE

#include "Common.h"
#include <azure/storage/blobs.hpp>
#include "log/LogEntry.h"
#include <functional>

using namespace Azure::Storage::Blobs;
namespace arboretum {

enum BlobStatus { NORMAL, COMPLETE};

struct LogLSN {
  uint32_t blob_idx_{1};
  int64_t offset_{0};

  string GetLSNStr() {
     uint64_t lsn = (((uint64_t)blob_idx_) << (64-8)) | (uint64_t)offset_;
     return to_string(lsn);
  }
  
  string PrintStr() {
    return "blob id: " + to_string(blob_idx_) + ", offset: " + to_string(offset_);
  }

};

struct ReadResult {
  BlobStatus st;
  uint32_t read_size;
};

struct AppendResult {
  bool append_res;
  BlobStatus blob_st;
};

class AzureLogClient {
 public:
  static const char* AZURE_STORAGE_CONNECTION_STRING;
  AzureLogClient(const std::string& connectionString, uint64_t node_id);

  // used in log replay, can only be called in a single thread.
  ReadResult ReadNextBatch(LogLSN * log_lsn, uint32_t batch_size, vector<uint8_t> * buffer);

  future<RC> logDataAsync(uint64_t node_id, LogEntry* logEntry);

  // void GroupLogSync(vector<LogEntry *> * logs, string& lsn);

  AppendResult GroupLogSync(uint32_t blob_idx, vector<uint8_t> *log_buffer, string& lsn);

 private:
  string GetBlobName(uint32_t blob_id);

  static const std::string blob_prefix_ ;
  static const std::string block_count_exceeds_msg_;
  static const int32_t blocks_max_num_per_blob_ ;
  static const std::string invalid_range_msg_;
  // static string GetLSN(uint32_t blob_idx_, uint32_t append_block_num);

  // uint32_t blob_idx_{0};
  // int64_t next_read_offset_{0};
  // BlobContainerClient container_client_;
  shared_ptr<BlobContainerClient> container_client_; 
  // AppendBlobClient blob_client_;
  // shared_ptr<AppendBlobClient> blob_client_;
  unordered_map<uint32_t,  shared_ptr<AppendBlobClient> > blob_clients_;

};
}

#endif // REMOTE_STORAGE_TYPE == REMOTE_STORAGE_AZURE

#endif //ARBORETUM_SRC_REMOTE_AZURELOGCLIENT_H_
