#include "AzureLogClient.h"
#include "Common.h"
#include "GlobalData.h"
#include "OptionalGlobalData.h"
#include "db/log/LogSeqNum.h"
#include "common/Message.h" 
#include <functional>


#if REMOTE_STORAGE_TYPE == REMOTE_STORAGE_AZURE
using namespace Azure::Storage::Blobs;
using namespace std;
namespace arboretum {

const int32_t AzureLogClient::blocks_max_num_per_blob_ = 50000;
const std::string AzureLogClient::blob_prefix_ = "log";
const std::string AzureLogClient::block_count_exceeds_msg_ = "BlockCountExceedsLimit";
const std::string AzureLogClient::invalid_range_msg_ = "InvalidPageRange";
const char* AzureLogClient::AZURE_STORAGE_CONNECTION_STRING = "AZURE_STORAGE_CONNECTION_STRING";


string AzureLogClient::GetBlobName(uint32_t blob_id){
  return blob_prefix_ + to_string(blob_id);
}

// string AzureLogClient::GetLSN(uint32_t blob_idx_, uint32_t append_block_num) {
//   return to_string(blob_idx_* AzureLogClient::blocks_max_num_per_blob_ + append_block_num);
// }

AzureLogClient::AzureLogClient(const std::string& connectionString, uint64_t node_id){
  const std::string containerName = "log-node-" + to_string(node_id);
  string blob_name = GetBlobName(1);
  LOG_INFO("Connecting to Azure Log Store at %s : %s : %s", connectionString.c_str(), containerName.c_str(), blob_name.c_str());

  BlobClientOptions clientOptions = BlobClientOptions();
  container_client_ = std::make_shared<BlobContainerClient>(BlobContainerClient::CreateFromConnectionString(connectionString, containerName, clientOptions));
  container_client_->CreateIfNotExists();
  blob_clients_.insert(make_pair(0, std::make_shared<AppendBlobClient>(container_client_->GetAppendBlobClient(blob_name))));
  LOG_INFO("Successfully connected to Azure Log Store(container: %s; blob: %s)", containerName.c_str(), blob_name.c_str());
}

// call from a single thread
// void AzureLogClient::GroupLogSync(vector<LogEntry *> * logs, string& lsn) {
//   vector<uint8_t> buffer;
//   for (vector<LogEntry *>::iterator it = logs->begin(); it < logs->end();
//        it++) {
//       LogEntry *entry = *it;
//       entry->Serialize(&buffer);
//   }
//   bool append_finish = false;
//   while (!append_finish) {
//       try {
//           auto blockStream =
//               Azure::Core::IO::MemoryBodyStream(buffer.data(), buffer.size());
//           Azure::Response<Models::AppendBlockResult> res =
//               blob_client_->AppendBlock(blockStream);
//           M_ASSERT(
//               (res.RawResponse.get()->GetStatusCode() ==
//                Azure::Core::Http::HttpStatusCode::Created),
//               "unexpected status code %d",
//               static_cast<uint32_t>(res.RawResponse.get()->GetStatusCode()));
//           lsn = GetLSN(blob_idx_, res.Value.CommittedBlockCount);
//           append_finish = true;
//       } catch (const Azure::Core::RequestFailedException &e) {
//           if (e.ErrorCode == AzureLogClient::block_count_exceeds_msg_) {
//               blob_idx_++;
//               blob_client_ =  std::make_shared<AppendBlobClient>(container_client_->GetAppendBlobClient(GetBlobName()));
//           } else {
//               LOG_ERROR("Encounter error: %s", e.Message.c_str());
//               M_ASSERT(false, "Unexpected Exception When appending: %s",
//                        e.ErrorCode.c_str());
//           }
//       }
//   }
// }

// call from a single thread
AppendResult AzureLogClient::GroupLogSync(uint32_t blob_idx,
                                         vector<uint8_t>* log_buffer,
                                         string& lsn) {
  shared_ptr<AppendBlobClient> client;
  if (blob_clients_.find(blob_idx) != blob_clients_.end()) {
      client = blob_clients_[blob_idx];
  } else {
      client = std::make_shared<AppendBlobClient>(
          container_client_->GetAppendBlobClient(GetBlobName(blob_idx)));
      blob_clients_.insert(make_pair(blob_idx, client));
  }
  if (log_buffer->size() == 0) {
      BlobStatus st =  BlobStatus::NORMAL;
      AppendResult append_res = {true, st};
      return append_res;
  }
  try {
    //   cout<<"XXX append buffer size " << log_buffer->size() << endl;
      auto blockStream = Azure::Core::IO::MemoryBodyStream(log_buffer->data(),
                                                           log_buffer->size());
      Azure::Response<Models::AppendBlockResult> res =
          client->AppendBlock(blockStream);
      M_ASSERT((res.RawResponse.get()->GetStatusCode() ==
                Azure::Core::Http::HttpStatusCode::Created),
               "unexpected status code %d",
               static_cast<uint32_t>(res.RawResponse.get()->GetStatusCode()));
      LogLSN log_lsn = {blob_idx, res.Value.AppendOffset};
      lsn = log_lsn.GetLSNStr();
      BlobStatus st = (res.Value.CommittedBlockCount <  AzureLogClient::blocks_max_num_per_blob_)? BlobStatus::NORMAL: BlobStatus::COMPLETE;
      AppendResult append_res = {true, st};
      return append_res;
  } catch (const Azure::Core::RequestFailedException& e) {
      if (e.ErrorCode == AzureLogClient::block_count_exceeds_msg_) {
          AppendResult append_res = {false, BlobStatus::COMPLETE};
          return append_res;
      } else {
          LOG_ERROR("Encounter error: %s", e.Message.c_str());
          M_ASSERT(false, "Unexpected Exception When appending: %s",
                   e.ErrorCode.c_str());
      }
  } catch (const std::exception &e) {
      std::cout << "Error: " << e.what() << std::endl;
      LOG_ERROR("failed to append block.");
  }
}

// call from multiple threads
future<RC> AzureLogClient::logDataAsync(uint64_t node_id, LogEntry* logEntry) {
    std::async(std::launch::async, 
         [logEntry, this]() {
            vector<uint8_t> buffer;
            logEntry->Serialize(&buffer);
            M_ASSERT(false, "TODO: finish this method");
            return OK;
        }
    );

}

// used in log replay, can only be called in a single thread.
ReadResult AzureLogClient::ReadNextBatch(LogLSN * log_lsn, uint32_t batch_size, vector<uint8_t> * buffer) {
     shared_ptr<AppendBlobClient> client;
     uint32_t blob_idx = log_lsn->blob_idx_;
     if (blob_clients_.find(blob_idx) != blob_clients_.end()) {
      client = blob_clients_[blob_idx];
     } else {
      client = std::make_shared<AppendBlobClient>(
          container_client_->GetAppendBlobClient(GetBlobName(blob_idx)));
      blob_clients_.insert(make_pair(blob_idx, client));
     }
    string read_lsn = log_lsn->GetLSNStr();
    size_t wait_count = 0;
    bool can_read = commit_lsn->IsFresherThan(read_lsn);
    while (!can_read && wait_count < 3) {
        // sleep 500ms to let the log append
        // if (wait_count > 10) {
        //     LOG_INFO("ReadNextBatch waits for more than 5 secs, cur read lsn is (%s), cur commit lsn is (%s)", log_lsn->PrintStr().c_str(), commit_lsn->PrintStr().c_str());
        // }
        usleep(500000);
        wait_count++;
        can_read = commit_lsn->IsFresherThan(read_lsn);
    }

    if (!can_read) {
        // log replay already catches up the newest log commit
        // LOG_INFO("XXX: Log replay catches up the newest log commit, read_lsn is (%s)", log_lsn->PrintStr().c_str())
        ReadResult res = {BlobStatus::NORMAL,0};
        return res;
    }

    try {
      Azure::Storage::Blobs::DownloadBlobToOptions options;
      options.Range = {log_lsn->offset_, batch_size};

    //   options.Range.Value().Offset = log_lsn->offset_;
    //   options.Range.Value().Length = batch_size;
      //   options.TransferOptions.InitialChunkSize = 10;
      //   options.TransferOptions.ChunkSize = 10;
      //   options.TransferOptions.Concurrency = 5;
      vector<uint8_t> i_buffer(batch_size);
      Azure::Response<Models::DownloadBlobToResult> res = client->DownloadTo(i_buffer.data(), i_buffer.size(), options);
      M_ASSERT((res.RawResponse.get()->GetStatusCode() ==
                Azure::Core::Http::HttpStatusCode::Created) || (res.RawResponse.get()->GetStatusCode() ==
                Azure::Core::Http::HttpStatusCode::PartialContent),
               "unexpected status code %d",
               static_cast<uint32_t>(res.RawResponse.get()->GetStatusCode()));
      M_ASSERT(res.Value.ContentRange.Offset == log_lsn->offset_, "response offset %ld doesn't match request offset %ld", res.Value.ContentRange.Offset, log_lsn->offset_);
      uint32_t content_len = res.Value.ContentRange.Length.Value();
      if (content_len > 0) {
          buffer->insert(buffer->end(), i_buffer.data(),
                         i_buffer.data() + content_len);
          ReadResult res = {BlobStatus::NORMAL,content_len};
        //   LOG_INFO("XXX: Log replay read batch reads %d bytes for read lsn(%s)", content_len, log_lsn->PrintStr().c_str())
          return res;
      } else {
         M_ASSERT(false, "Unexpected response for ReadNextBatch: read zero bytes from %s", log_lsn->PrintStr().c_str());
      }

    } catch (const Azure::Core::RequestFailedException &e) {
      // TODO: deal with the exceptions
      if (e.ErrorCode == AzureLogClient::invalid_range_msg_) {
          // reach end of the blob
          //TODO: remove redundant props check
          auto props = client->GetProperties().Value;
          M_ASSERT((props.BlobSize == log_lsn->offset_) && (props.CommittedBlockCount.Value() == AzureLogClient::blocks_max_num_per_blob_), 
          "Unexpected properties when encounter invalid range for batch read, blob size is %d, commited blobs num is %d, batch read lsn is %s", props.BlobSize, props.CommittedBlockCount.Value(), log_lsn->PrintStr().c_str());
          LOG_INFO("Encounter invalid range while ReadNextBatch for range: %s %s", e.Message.c_str(), e.ErrorCode.c_str());
          ReadResult res = {BlobStatus::COMPLETE, 0};
          return res;
      } else {
          LOG_ERROR("Encounter error: %s", e.Message.c_str());
          M_ASSERT(false, "Unexpected Exception When appending: %s",
                   e.ErrorCode.c_str());
      }
    }
}



}

#endif // REMOTE_STORAGE_TYPE == REMOTE_STORAGE_AZURE