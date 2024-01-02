// #include <utility>

#include "LogEntry.h"
#include "Common.h"
#include "local/ITuple.h"
#include <cstring>

using namespace std;
namespace arboretum {
    
LogEntry::LogEntry(string lsn_new, string data_new, uint8_t status_new, uint64_t txn_id_new) {
    lsn_ = std::move(lsn_new);
    data_ = std::move(data_new);
    status_ = status_new;
    txn_id_ = txn_id_new;
}

LogEntry::LogEntry(string data_new, uint8_t status_new, uint64_t txn_id_new) {
    lsn_ = "0";
    data_ = std::move(data_new);
    status_ = status_new;
    txn_id_ = txn_id_new;
}

LogEntry * LogEntry::NewFakeEntry() {
    return NEW(LogEntry)("", UINT8_MAX, 0); 
}

future<bool> LogEntry::GetLSNFuture() {
  return lsn_promise_.get_future();
}

void LogEntry::SetLSN(string& lsn) {
    lsn_ = lsn;
}

void LogEntry::SetGranules(unordered_set<uint64_t>* granules) {
    granules_.insert(granules->begin(), granules->end());
}

unordered_set<uint64_t>* LogEntry::GetGranules() {
    return &granules_;
}


void LogEntry::SetLSNPromise(string& lsn){
    lsn_= lsn;
    lsn_promise_.set_value(true);
}

void LogEntry::SetPromise(){
    lsn_promise_.set_value(true);
}

void LogEntry::to_string(string &str) {
    std::ostringstream ss;
    ss << "LogEntry( LSN:" << lsn_ << "; TXN_ID:" << txn_id_ << "; Status:" << (int)status_ << "; Data:";
    // generate hex string for data
    if (data_.empty()) {
        ss << "NULL";
    } else {
        ss << data_;
    }
    ss << " )";
    str = ss.str();
}


void LogEntry::print() {
    string a;
    to_string(a);
    cout << a << endl;
}

#if REMOTE_STORAGE_TYPE == REMOTE_STORAGE_AZURE
uint32_t LogEntry::GetSerializedLen() {
    size_t data_len = data_.length();
    return sizeof(status_) + sizeof(txn_id_) + data_len + sizeof(data_len);
}


void LogEntry::Serialize(vector<uint8_t> * buffer) {
   const char * data = data_.c_str();
   size_t data_len = data_.length();
   size_t log_len = sizeof(status_) + sizeof(txn_id_) + data_len;
//    LOG_INFO("XXX: LOG length is %d, log status is %d, data length is %d", log_len, status_, data_.length());
   buffer->insert(buffer->end(), CHAR_PTR_CAST(&log_len), CHAR_PTR_CAST(&log_len) + sizeof(log_len));
   buffer->insert(buffer->end(), CHAR_PTR_CAST(&status_), CHAR_PTR_CAST(&status_) + sizeof(status_));
   buffer->insert(buffer->end(),CHAR_PTR_CAST(&txn_id_), CHAR_PTR_CAST(&txn_id_) + sizeof(txn_id_));
   if (data_len > 0) {
      buffer->insert(buffer->end(),data, data + data_len);
   }
}

bool LogEntry::HasLogEntry(uint8_t * buffer, size_t buffer_size) {
    if (buffer_size >= sizeof(size_t)) {
        uint8_t *offset = buffer;
        size_t data_len;
        std::memcpy(&data_len, offset, sizeof(data_len));
        return buffer_size >= data_len + sizeof(data_len);
    } else {
        return false;
    }
}

LogEntry * LogEntry::Deserialize(uint8_t * buffer) {
    uint8_t *offset = buffer;
    size_t log_len;
    std::memcpy(&log_len, offset, sizeof(log_len));
    offset = offset + sizeof(log_len);
    uint8_t status;
    std::memcpy(&status, offset, sizeof(status));
    offset = offset + sizeof(status);
    uint64_t txn_id;
    std::memcpy(&txn_id, offset, sizeof(txn_id));
    offset = offset + sizeof(txn_id);
    size_t data_len = log_len - sizeof(status) - sizeof(txn_id);
    if (data_len > 0) {
        string value(offset, offset + data_len);
        auto entry = NEW(LogEntry)(value, status, txn_id);
        return entry;
    } else {
        auto entry = NEW(LogEntry)("", status, txn_id);
        return entry;
    }
}
#endif


void LogEntry::DeserializeLogData(string &data, unordered_map<uint64_t, unordered_map<uint64_t, string>> *kvs) {
    char *data_c = const_cast<char *>(data.c_str());
    uint16_t wr_num;
    char *offset = data_c;
    std::memcpy(&wr_num, offset, sizeof(wr_num));
    offset = data_c + sizeof(uint16_t);
    for (int i = 0; i < wr_num; i++) {
        uint64_t key;
        std::memcpy(&key, offset, sizeof(uint64_t));
        offset = offset + sizeof(uint64_t);
        size_t value_size;
        std::memcpy(&value_size, offset, sizeof(size_t));
        offset = offset + sizeof(size_t);
        string value(offset, offset + value_size);

        auto loaded = reinterpret_cast<ITuple *> (offset);
        auto tbl_id = loaded->GetTableId();
        auto granule_id = GetGranuleID(key);
        //compose unique granule id 
        uint64_t granule_uid = ((uint64_t)tbl_id)<<32 | granule_id; 
        auto it = kvs->find(granule_uid);
        if (it != kvs->end()) {
          // granule exists in history map
          //TODO(Hippo): use emplace to avoid copy
          (it->second)[key] = value;
        } else {
            unordered_map<uint64_t, string> map;
            map[key] = value;
            kvs->emplace(granule_uid, std::move(map));
        }
        // (*kv)[key] = value;
        offset = offset + value_size;
    }
}
}