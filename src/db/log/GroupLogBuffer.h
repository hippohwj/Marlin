#ifndef ARBORETUM_SRC_DB_LOG_GROUPLOGBUFFER_H_
#define ARBORETUM_SRC_DB_LOG_GROUPLOGBUFFER_H_

#include "common/Common.h"
#include "log/LogEntry.h"


namespace arboretum {
// class LogEntry;

 class GroupLogBuffer {
 public: 
      GroupLogBuffer() {};
      // ~ConcurrentLSN() {};

      bool isEmpty();
      size_t BufferSize();
      void Clear();
      void PutLog(LogEntry* log_entry);

      void AcquireLock();
      void ReleaseLock();

      // vector<LogEntry *> &GetBuffer(); 
      vector<LogEntry *> * GetBuffer();
      
      uint32_t GetEpoch();
      void IncreaseEpoch();

 
 private:
   std::mutex lock_;
   std::vector<LogEntry *> buffer_;
   std::atomic<uint32_t> epoch_{0};

 };

}


#endif //ARBORETUM_SRC_DB_LOG_GROUPLOGBUFFER_H_