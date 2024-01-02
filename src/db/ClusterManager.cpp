#include "ClusterManager.h"
#include "common/Common.h"

namespace arboretum {
ClusterManager::ClusterManager() {
    _num_finished_worker_threads = 0;
    _num_sync_received = 0;
    _group_commit_finished = 0;
    _cur_node_finished = 0;
    migr_thread_finished_ = 0;
}

void ClusterManager::receive_sync_request() {
    ATOM_ADD_FETCH(_num_sync_received, 1);
}

void ClusterManager::receive_groupcommit_finished() {
    ATOM_ADD_FETCH(_group_commit_finished, 1);
}

void ClusterManager::receive_cur_node_finished() {
    ATOM_ADD_FETCH(_cur_node_finished, 1);
}


void ClusterManager::receive_migr_thread_finished() {
    ATOM_ADD_FETCH(migr_thread_finished_, 1);
}




bool ClusterManager::start_sync_done(bool isNewNode) {
    if (!isNewNode) {
       return cluster_manager->num_sync_requests_received() >= g_num_nodes - 1;
    } else {
       return cluster_manager->num_sync_requests_received() >= g_num_nodes;
    }
}
bool ClusterManager::end_sync_done(bool isNewNode) {
    if (!isNewNode) {
       return cluster_manager->num_sync_requests_received() >= (g_num_nodes - 1) * 2;
    } else {
       return cluster_manager->num_sync_requests_received() >= g_num_nodes * 2;
    }
}

bool ClusterManager::end_sync_done(uint32_t target_signal_num) {
    return cluster_manager->num_sync_requests_received() >= target_signal_num;
}


bool ClusterManager::MigrationDone(uint8_t migr_thread_num) {
   return migr_thread_finished_ >= migr_thread_num;
}



bool ClusterManager::all_worker_done() {
   return end_sync_done(false) && _cur_node_finished >= 1;
}

bool ClusterManager::is_groupcommit_done() {
   return  _group_commit_finished >= 1;
}

}  // namespace arboretum