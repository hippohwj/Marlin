#ifndef ARBORETUM_SRC_DB_CLUSTERMANAGER_H_
#define ARBORETUM_SRC_DB_CLUSTERMANAGER_H_

#include "common/Types.h"

namespace arboretum {
// Cluster Manager maintains the information of the entire cluster and shared by
// all the threads.
class ClusterManager {
   public:
    ClusterManager();
    void                    receive_sync_request();
    void receive_groupcommit_finished();
    void receive_cur_node_finished();
    void receive_migr_thread_finished();
    uint32_t num_sync_requests_received() { return _num_sync_received; }
    bool start_sync_done(bool isNewNode=false); 
    bool end_sync_done(bool isNewNode=false); 
    bool end_sync_done(uint32_t target_signal_num);
    bool is_groupcommit_done();
    bool all_worker_done();
    bool MigrationDone(uint8_t migr_thread_num);
    volatile uint32_t migr_thread_finished_;
 


   private:
    uint32_t _num_finished_worker_threads;
    volatile uint32_t _num_sync_received;
    volatile uint32_t _group_commit_finished;
    volatile uint32_t _cur_node_finished;
    // volatile uint32_t migr_thread_finished_;
};

}  // namespace arboretum
#endif  // ARBORETUM_SRC_DB_CLUSTERMANAGER_H_