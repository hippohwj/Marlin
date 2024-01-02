#include "common/BenchWorker.h"
#include "common/Worker.h"
#include "common/GlobalData.h"
#include "db/ARDB.h"
#include "db/ClusterManager.h"
#include "db/log/LogGroupCommitThread.h"
#include "db/log/RingBufferGroupCommitThread.h"
#include "db/log/LogReplayThread.h"
#include "db/log/RingBufferReplayThread.h"
#include "db/txn/TxnTable.h"
#include "transport/rpc_client.h"
#include "transport/rpc_server.h"
#include "ycsb/YCSBConfig.h"
#include "ycsb/YCSBWorkload.h"
#include "ycsb/YCSBScaleinWorkload.h"
#include "ycsb/YCSBLoadBalanceWorkload.h"
#include "ycsb/ScaleinWorkload.h"
#include "ycsb/SyntheticWorkload.h"
#include "ycsb/RampUpScaleoutWorkload.h"
#include "ycsb/LoadBalanceWorkload.h"
#include "ycsb/FaultToleranceWorkload.h"

using namespace arboretum;

void *start_rpc_server(void *input) {
    rpc_server->run();
    return nullptr;
}

void *start_replay_thread(void *thread) {
    ((LogReplayThread *)thread)->run();
    return nullptr;
}

void *start_gc_thread(void *thread) {
    ((GroupCommitThread *)thread)->run();
    return nullptr;
}

void *start_ring_buffer_replay_thread(void *thread) {
    ((RingBufferReplayThread *)thread)->run();
    return nullptr;
}

void *start_ring_buffer_gc_thread(void *thread) {
    ((RingBufferGroupCommitThread *)thread)->run();
    return nullptr;
}

int main(int argc, char *argv[]) {
    M_ASSERT(argc == 3, "number of args for ComputeService has to be 2");
    g_node_id = std::stoi(argv[2]);
    cout << "g_node_id is " << g_node_id << endl;

    ARDB::LoadConfig(argc, argv);
    auto config =
        new (MemoryAllocator::Alloc(sizeof(YCSBConfig))) YCSBConfig(argc, argv);
    //   M_ASSERT( (!g_loadbalance_enable) && (!g_scaleout_enable) && ( (!g_scalein_enable) || (!g_faulttolerance_enable)), "only one switcher can be true");
        g_read_percent = config->read_perc_;
        // TODO(Hippo): remove this variable when systable is ready
        g_num_rows_per_node = config->num_rows_;
        g_num_worker_threads = config->num_workers_;
        g_granule_num_per_node = GetGranuleID(config->num_rows_ - 1) + 1;
        LOG_INFO("XXX Granule number per node is %d", g_granule_num_per_node);

        granule_tbl_cache = new ConcurrentHashMap<uint64_t, uint64_t>();
        granule_commit_tracker = new ConcurrentHashMap<uint64_t, string>();
        migr_preheat_updates_tracker =
            new ConcurrentHashMap<uint64_t, std::unordered_set<uint64_t> *>();
        cache_preheat_mask = new ConcurrentHashSet<uint64_t>();


        // TODO(Hippo): pass args from system config settings
        if (g_index_type != arboretum::REMOTE) {
            g_cluster_members.push_back("10.0.0.4");
            g_cluster_members.push_back("10.0.0.5");
            g_cluster_members.push_back("10.0.0.6");
            g_cluster_members.push_back("10.0.0.7");
            g_cluster_members.push_back("10.0.0.8");
        } else {
            g_cluster_members.push_back("10.0.0.4");
            g_cluster_members.push_back("10.0.0.5");
            g_cluster_members.push_back("10.0.0.6");
        }

        g_num_nodes = g_cluster_members.size();
        // g_client_node_id = g_num_nodes - 1;
        uint8_t migr_node_num = 0;
        uint32_t migr_sig_num = 0;
        strcpy(g_syslog_store_host, "10.1.0.4");
        auto db = new (MemoryAllocator::Alloc(sizeof(ARDB))) ARDB;
        ardb = db;
        if (!g_rampup_scaleout_enable) {
            gc_log_size.store(g_gc_lognum_bound);
        }

        // start rpc services
        g_scale_node_id = config->scale_node_id_;
        // TODO: remove this hack-- use isScaleNode to denote the skew node
        bool isClientNode = g_node_id == g_client_node_id; 
        bool isServerNode = !isClientNode; 

        if (g_scalein_enable) {
           g_is_migr_node = isServerNode && (g_node_id != g_scale_node_id);
           migr_node_num = g_num_nodes - 2;
        } else if (g_faulttolerance_enable) {
           g_is_migr_node = g_node_id == g_scale_node_id + 1;
           migr_node_num = 1;
        } else if (g_scaleout_enable) {
           g_is_migr_node = g_node_id == g_scale_node_id;
           migr_node_num = 1;
        } else if (g_loadbalance_enable) {
           g_is_migr_node = isServerNode && (g_node_id != g_scale_node_id);
           migr_node_num = g_num_nodes - 2;
        } else if (g_rampup_scaleout_enable) {
           if (g_rampup_on_premise_size > 0) {
              g_is_migr_node = false;
              migr_node_num = 0;
           } else {
              g_is_migr_node = isServerNode && g_node_id != 0;
              migr_node_num = g_num_nodes - 2;
           }
        } else if (g_synthetic_enable) {
            if (g_on_premise_enable) {
               g_is_migr_node = false;
               migr_node_num = 0;
            } else {
               g_is_migr_node = isServerNode;
               migr_node_num = g_num_nodes - 1;
            }
        }

        migr_sig_num = g_migr_threads_num * migr_node_num; 

        // initialize global variables
        cluster_manager = new ClusterManager();
        txn_table = new TxnTable();

        uint32_t main_thd_id = 0;

        string host_url = g_cluster_members[g_node_id] + ":50051";
        LOG_INFO("[Service] host url %s", host_url.c_str());
        rpc_server = new SundialRPCServerImpl(host_url, db);
        auto *pthread_rpc = new pthread_t;
        pthread_create(pthread_rpc, nullptr, start_rpc_server, nullptr);

        // sleep to wait all compute nodes to start services.
        sleep(g_num_nodes * 2);
        // sleep(g_num_nodes);

        // init rpc client
        LOG_INFO("[Service] start to initialize client");
        rpc_client = new SundialRPCClient();

        LOG_INFO("[Service] start loading data");

        Workload *workload;
        Workload *migr_workload;
        if (g_index_type != arboretum::REMOTE) {
            // cache heatup for queries
            if (g_scalein_enable || g_scaleout_enable || g_rampup_scaleout_enable ||
                g_faulttolerance_enable || g_synthetic_enable) {
                workload = new YCSBScaleinWorkload(db, config);
            } else if (g_loadbalance_enable) {
                workload = new YCSBLoadBalanceWorkload(db, config);
            }

            if (g_is_migr_node) {
                if (g_scalein_enable) {
                    ScaleinWorkload *scalein_workload =
                        new ScaleinWorkload(db, config);
                    scalein_workload->InitComplement();
                    migr_workload = scalein_workload;
                } else if (g_faulttolerance_enable) {
                    FaultToleranceWorkload *ft_workload =
                        new FaultToleranceWorkload(db, config);
                    ft_workload->InitComplement();
                    migr_workload = ft_workload;
                } else if (g_scaleout_enable) {
                    ScaleoutWorkload *scaleout_workload =
                        new ScaleoutWorkload(db, config);
                    scaleout_workload->InitComplement();
                    migr_workload = scaleout_workload;
                } else if (g_rampup_scaleout_enable) {
                    RampUpScaleoutWorkload *scaleout_workload =
                        new RampUpScaleoutWorkload(db, config);
                    scaleout_workload->InitComplement();
                    migr_workload = scaleout_workload;
                } else if (g_loadbalance_enable) {
                    LoadBalanceWorkload *lb_workload =
                        new LoadBalanceWorkload(db, config);
                    lb_workload->InitComplement();
                    migr_workload = lb_workload;
                }  else if (g_synthetic_enable) {
                    SyntheticWorkload * s_workload = new SyntheticWorkload(db, config);
                    s_workload->InitComplement();
                    migr_workload = s_workload;
                    if (config->migrate_cache_enable_) {
                        string tbl_name =config->GetTblName(); 
                        workload->LoadDataForMigr(tbl_name, s_workload->GenGranulesToMigr());
                    }
                }
            }
        } else {
            // load data
            workload = new YCSBWorkload(db, config);
        }

        // a hack: only load data when index type is REMOTE
        if (g_index_type != arboretum::REMOTE) {
            uint64_t starttime;
            uint64_t endtime;

            // start related threads (group commit, log replay)
            auto *pthread_log_replay = new pthread_t;
            auto *pthread_groupcommit = new pthread_t;
            std::vector<std::thread> threads;
            size_t thread_num = config->num_workers_;
            size_t migr_thd_num = g_migr_threads_num;

            BenchWorker workers[thread_num + migr_thd_num];

            if (isServerNode) {
                // Server node
                if (g_replay_from_buffer_enable) {
                    gc_ring_buffer =
                        new LogRingBuffer<LogEntry *>(g_ring_buffer_size);
                    LOG_INFO("Ring buffer size is %d", g_ring_buffer_size);
                }

                if (g_replay_enable) {
                    if (g_replay_from_buffer_enable) {
                        pthread_create(pthread_log_replay, nullptr,
                                       start_ring_buffer_replay_thread,
                                       (void *)new RingBufferReplayThread(db));
                    } else {
                        pthread_create(pthread_log_replay, nullptr,
                                       start_replay_thread,
                                       (void *)new LogReplayThread(db));
                    }
                    LOG_INFO("Log Replay Thread Started");
                }

                if (g_groupcommit_enable) {
                    if (g_replay_from_buffer_enable) {
                        pthread_create(pthread_groupcommit, nullptr,
                                       start_ring_buffer_gc_thread,
                                       (void *)new RingBufferGroupCommitThread);

                    } else {
                        pthread_create(pthread_groupcommit, nullptr,
                                       start_gc_thread,
                                       (void *)new GroupCommitThread);
                    }
                    LOG_INFO("Group Commit Thread Started");
                }

                // if (g_replay_enable || g_groupcommit_enable) {
                //     // wait group commit thread and log replay thread to
                //     start sleep(3);
                // }

                // start worker threads and run workloads
                LOG_INFO("DB Server started");

                // sync check that all nodes in the cluster are ready to run
                // workloads
                LOG_INFO("[Service] Synchronization starts");
                // Notify other nodes that the current node has finished
                // initialization
                for (uint32_t i = 0; i < g_num_nodes; i++) {
                    if (i == g_node_id) continue;
                    SundialRequest request;
                    SundialResponse response;
                    request.set_request_type(SundialRequest::SYS_REQ);
                    request.set_node_id(g_node_id);
                    request.set_thd_id(main_thd_id);
                    rpc_client->sendRequest(i, request, response);
                }
                // Can start only if all other nodes have also finished
                // initialization
                while (!cluster_manager->start_sync_done(false)) {
                    usleep(100);
                }
                LOG_INFO("[Service] Synchronization done");

                // TODO(Hippo): remove this hack and enable warm up
                g_warmup_finished = true;
                if (g_is_migr_node) {
                    // launch migration threads
                    for (size_t i = 0; i < migr_thd_num; i++) {
                        workers[thread_num + i].worker_id_ = thread_num + i + 1;
                        threads.emplace_back(ScaleoutWorkload::Execute,
                                             migr_workload,
                                             &workers[thread_num + i]);
                    }
                }

                LOG_INFO("[Service] wait for migration workload to finish");

                if (g_rampup_scaleout_enable) {
                    if (g_rampup_on_premise_size == 0) {
                        while (!cluster_manager->MigrationDone(migr_thd_num *
                                                       migr_node_num)) {
                        std::this_thread::sleep_for(std::chrono::seconds(1));
                        gc_log_size.fetch_add(1);
                       }
                    } else {
                        int timer = 0;
                        while (timer < g_rampup_duration_s) {
                          std::this_thread::sleep_for(std::chrono::seconds(1));
                          timer++;
                          gc_log_size.fetch_add(1);
                        }

                    }

                } else {
                    if (migr_sig_num != 0) {
                        while (!cluster_manager->MigrationDone(migr_thd_num *
                                                           migr_node_num)) {
                            std::this_thread::sleep_for(std::chrono::seconds(1));
                        }
                    } else {
                       M_ASSERT(g_on_premise_enable == true, "unexpected case");
                       int timer = 0;
                       while (timer < g_on_premise_duration_s) {
                         std::this_thread::sleep_for(std::chrono::seconds(1));
                         timer++;
                       }
                    }
                }

                g_terminate_exec = true;

                std::for_each(threads.begin(), threads.end(),
                              std::mem_fn(&std::thread::join));

                cluster_manager->receive_cur_node_finished();

                // sync wait all nodes to stop and summarize stats
                LOG_INFO("[Service] End synchronization starts");
                SundialRequest request;
                SundialResponse response;
                request.set_request_type(SundialRequest::SYS_REQ);
                // only end sync req will add legal node id
                request.set_node_id(g_node_id);
                request.set_thd_id(main_thd_id);

                // Notify other nodes the completion of the current node.
                for (uint32_t i = 0; i < g_num_nodes; i++) {
                    if (i == g_node_id) continue;
                    starttime = GetSystemClock();
                    rpc_client->sendRequest(i, request, response);
                    endtime = GetSystemClock() - starttime;
                    // INC_FLOAT_STATS(time_rpc, endtime);
                    LOG_INFO("[Service] network roundtrip to node %u: %u us", i,
                             endtime / 1000);
                }

                while (!cluster_manager->end_sync_done(false)) usleep(100);
                LOG_INFO("[Service] End synchronization ends");

                if (g_groupcommit_enable) {
                    LOG_INFO("Wait for group commit thread to finish...")
                    pthread_join(*pthread_groupcommit, nullptr);
                }

                if (g_replay_enable) {
                    LOG_INFO("Wait for replay thread to finish...")
                    gc_ring_buffer->Write(LogEntry::NewFakeEntry());
                    gc_ring_buffer->Commit(1);
                    pthread_join(*pthread_log_replay, nullptr);
                }

            } else {
                // client node
                {
                  std::lock_guard<std::mutex> lock(an_mutex);        
                  an_count = thread_num; 
                }
              
                // sync check that all nodes in the cluster are ready to run
                // workloads
                LOG_INFO("[Service] Synchronization starts");
                // Notify other nodes that the current node has finished
                // initialization
                for (uint32_t i = 0; i < g_num_nodes; i++) {
                    if (i == g_node_id) continue;
                    SundialRequest request;
                    SundialResponse response;
                    request.set_request_type(SundialRequest::SYS_REQ);
                    request.set_node_id(g_node_id);
                    request.set_thd_id(main_thd_id);
                    rpc_client->sendRequest(i, request, response);
                }
                // Can start only if all other nodes have also finished
                // initialization
                while (!cluster_manager->start_sync_done(false)) {
                    usleep(100);
                }
                LOG_INFO("[Service] Synchronization done");

                g_warmup_finished = true;
                // client node launches client threads
                for (size_t i = 0; i < thread_num; i++) {
                    workers[i].worker_id_ = i + 1;
                    threads.emplace_back(YCSBWorkload::Execute, workload,
                                         &workers[i]);
                }

                LOG_INFO("[Service] wait for migration workload to finish");
                
                if (g_rampup_scaleout_enable &&
                    (g_rampup_on_premise_size > 0)) {
                    int timer = 0;
                    while (timer < g_rampup_duration_s) {
                        std::this_thread::sleep_for(std::chrono::seconds(1));
                        // count++;
                        Worker::abort_per_sec.push_back(Worker::abort_count_.load());
                        Worker::throughput_per_sec.push_back(
                            Worker::commit_count_.load());
                        timer++;
                    }
                } else {
                    if (migr_sig_num != 0) {
                        int sec = 0;
                        bool migr_start = false;
                        while (sec < config->time_before_migration_) {
                            std::this_thread::sleep_for(std::chrono::seconds(1));
                            Worker::abort_per_sec.push_back(Worker::abort_count_.load());
                                  Worker::throughput_per_sec.push_back(
                            Worker::commit_count_.load());
                            sec++;
                        }
                        ATOM_ADD_FETCH(dur_migr, 1);

                        while (!cluster_manager->MigrationDone(migr_sig_num)) {
                            std::this_thread::sleep_for(std::chrono::seconds(1));
                            Worker::abort_per_sec.push_back(Worker::abort_count_.load());
                                  Worker::throughput_per_sec.push_back(
                            Worker::commit_count_.load());
                        }
                        ATOM_ADD_FETCH(dur_migr, 1);
                        for (int i = 0; i< config->time_after_migration_; i++) {
                            std::this_thread::sleep_for(std::chrono::seconds(1));
                            Worker::abort_per_sec.push_back(Worker::abort_count_.load());
                                  Worker::throughput_per_sec.push_back(
                            Worker::commit_count_.load());
                        }

                    } else {
                       M_ASSERT(g_on_premise_enable == true, "unexpected case");
                       ATOM_ADD_FETCH(dur_migr, 1);
                       int timer = 0;
                       while (timer < g_on_premise_duration_s) {
                         std::this_thread::sleep_for(std::chrono::seconds(1));
                         Worker::abort_per_sec.push_back(Worker::abort_count_.load());
                               Worker::throughput_per_sec.push_back(
                            Worker::commit_count_.load());
                         timer++;
                       }
                    }
                    
                }

                // sleep(config->runtime_);
                g_terminate_exec = true;

                std::for_each(threads.begin(), threads.end(),
                              std::mem_fn(&std::thread::join));

                cluster_manager->receive_cur_node_finished();
                // sync wait all nodes to stop and summarize stats
                LOG_INFO("[Service] End synchronization starts");
                SundialRequest request;
                SundialResponse response;
                request.set_request_type(SundialRequest::SYS_REQ);
                // only end sync req will add legal node id
                request.set_node_id(g_node_id);
                request.set_thd_id(main_thd_id);

                // Notify other nodes the completion of the current node.
                for (uint32_t i = 0; i < g_num_nodes; i++) {
                    if (i == g_node_id) continue;
                    starttime = GetSystemClock();
                    rpc_client->sendRequest(i, request, response);
                    endtime = GetSystemClock() - starttime;
                    // INC_FLOAT_STATS(time_rpc, endtime);
                    LOG_INFO("[Service] network roundtrip to node %u: %u us", i,
                             endtime / 1000);
                }

                while (!cluster_manager->end_sync_done(false)) usleep(100);
                LOG_INFO("[Service] End synchronization ends");
            }

            pthread_join(*pthread_rpc, nullptr);

            LOG_INFO("All threads finished")
            // NOTE: must print db stats later than bench stats due to format
            // json
            BenchWorker::PrintStats(thread_num);
            Worker::PrintStats();
        }
        return 0;
}