#include <map>
// #include "system/manager.h"
// #include "config.h"
// #include "transport/redis_client.h"
// #include "log_replay_thread.h"
// #include "utils/helper.h"
// #include "system/global.h"
#include "GlobalData.h"
#include "LogEntry.h"
#include "RingBufferReplayThread.h"
#include "RedisClient.h"
// #include "GlobalData.h"
#include <sstream>

#include "LogSeqNum.h"
#include "common/OptionalGlobalData.h"
#include "db/ClusterManager.h"
#include "local/ITable.h"
#include "remote/IDataStore.h"
#include "remote/ILogStore.h"

namespace arboretum {

// LogReplayThread::LogReplayThread() {
RingBufferReplayThread::RingBufferReplayThread(ARDB *db) {
    txnTracker = new TxnTracker();
    replay_lsn = new ConcurrentLSN();
    log_semaphore_ = new SemaphoreSync();
    replay_logstore = NEW(ILogStore);
    if (!g_replay_cosmos_enable) {
        replay_datastore = NEW(IDataStore);
    } else {
        const string conn = string(azure_cosmos_conn_str);
        replay_datastore = NEW(IDataStore)(conn);
    }
    db_ = db;
}

#if REMOTE_STORAGE_TYPE == REMOTE_STORAGE_AZURE
RC RingBufferReplayThread::run() {
    LOG_INFO("---------------Log Replay Start to Run-------------------");
    // pulling from log (as reply) and write to data store
    bool sync_end = false;
    bool empty_reply = false;
    uint64_t replay_read_valid_log_num = 0;
    uint64_t replay_loop_num = 0;
    uint64_t replay_flush_num = 0;
    uint64_t replay_total_flush_entry_num = 0;
    uint64_t accum_replay_read_log_total_time = 0;
    uint64_t accum_replay_read_entry_num = 0;
    uint64_t accum_replay_read_valid_log_total_time = 0;
    uint64_t accum_flush_to_datastore_total_time = 0;
    uint64_t replay_time = 0;
    uint64_t replay_start = GetSystemClock();
    unordered_map<uint64_t, unordered_map<uint64_t, string>> kvs;
    auto clients = replay_datastore->GetClients();

    string last_entry_lsn = "0";
    vector<uint8_t> log_bytes_buffer;
    // size_t azure_batch_op_limit = 99;
    size_t azure_batch_op_limit = 100;
    std::unordered_map<uint64_t, std::string> currentSmallMap;
    M_ASSERT(g_replay_producer_enable,
             "g_replay_producer_enable has to be enabled");
    LOG_INFO("Log Replay Consumer Started");
    // current thread plays as a consumer
    LogEntry *log_entry = gc_ring_buffer->BlockingReplay();
    bool finish = log_entry->IsFakeEntry();
    uint32_t map_size = 0;
    uint64_t batch_st_time = GetSystemClock();
    size_t batch_fill_entry_num = 0;
    while (!finish) {
        if (log_entry) {
            batch_fill_entry_num ++;
            last_entry_lsn = log_entry->GetLSN();
            tuple<bool, LogEntry *> res = txnTracker->AppendOrFlush(log_entry);
            if (!get<0>(res)) {
                auto legal_entry = get<1>(res);
                M_ASSERT(legal_entry->IsPrepare(),
                         "should only get prepared log entry to parse");
                LogEntry::DeserializeLogData(legal_entry->GetData(), &kvs);
                delete legal_entry;
            }
            map_size = 0;
            for (auto &it : kvs) {
                map_size += it.second.size();
            }
        }

        if ((log_entry == nullptr) || (map_size >= g_replay_flush_threshold)) {
            uint64_t flush_datastore_start = GetSystemClock();
            LOG_INFO("[Replay]: buffer batch fill takes %.2f ms, log entry consumed %d, mapsize %d, isnullptr %d", nano_to_ms(flush_datastore_start - batch_st_time), batch_fill_entry_num, map_size, (log_entry==nullptr)? 1:0);
            size_t async_cnt = 0;
            vector<std::future<void>> futures;
            // flush kvs to data store and delete elements in kvs
            for (auto &it : kvs) {
                uint64_t granule_uid = it.first;
                auto tbl_id = granule_uid >> 32;
                auto tbl = db_->GetTable(tbl_id);
                if (!tbl->IsSysTable()) {
                    auto tbl_name = tbl->GetTableName();
                    uint64_t granule_id = (granule_uid << 32) >> 32;
                    unordered_map<uint64_t, string> *rows = &(it.second);
                    if (rows->size() <= azure_batch_op_limit) {
                        if (g_replay_parallel_flush_enable) {
                            async_cnt++;
                            // cout << "XXX replay granule id " << granule_id << endl;
                            cout << "XXX [Replay] async replay for granule " << granule_id << ", async_cnt " << async_cnt << ", rows "<< rows->size() << endl;
                            // replay_datastore->AsyncBatchWrite(tbl_name,
                            //                                   granule_id, rows);
                            futures.push_back(std::async(std::launch::async, &AzureTableClient::BatchStoreSync, clients[granule_id % g_num_azure_tbl_client], tbl_name, std::to_string(granule_id), rows));
                                
                        } else {
                            replay_datastore->BatchWrite(tbl_name, granule_id,
                                                         rows);
                        }

                    } else {
                        for (const auto &pair : *rows) {
                            // Check if the current small map is full
                            if (currentSmallMap.size() >=
                                azure_batch_op_limit) {
                                if (g_replay_parallel_flush_enable) {
                                    async_cnt++;
                                    // cout << "XXX replay granule id " << granule_id << endl;
                                    cout << "XXX [Replay] async replay for granule " << granule_id << ", async_cnt " << async_cnt << ", rows" << currentSmallMap.size() << endl;
                                    // replay_datastore->AsyncBatchWrite(
                                    //     tbl_name, granule_id, &currentSmallMap);
                                    futures.push_back(std::async(std::launch::async, &AzureTableClient::BatchStoreSync, clients[granule_id % g_num_azure_tbl_client], tbl_name, std::to_string(granule_id), rows));

                                } else {
                                    replay_datastore->BatchWrite(
                                        tbl_name, granule_id, &currentSmallMap);
                                }
                                currentSmallMap.clear();
                            }
                            currentSmallMap[pair.first] = pair.second;
                        }
                        if (currentSmallMap.size() > 0) {
                            if (g_replay_parallel_flush_enable) {
                                async_cnt++;
                                cout << "XXX [Replay] async replay for granule " << granule_id << ", async_cnt " << async_cnt << ", rows" << currentSmallMap.size() << endl;
                                // replay_datastore->AsyncBatchWrite(
                                //     tbl_name, granule_id, &currentSmallMap);
                                futures.push_back(std::async(std::launch::async, &AzureTableClient::BatchStoreSync, clients[granule_id % g_num_azure_tbl_client], tbl_name, std::to_string(granule_id), rows));

                            } else {
                                replay_datastore->BatchWrite(
                                    tbl_name, granule_id, &currentSmallMap);
                            }
                            currentSmallMap.clear();
                        }
                    }

                    // add updates to migr_preheat_updates_tracker
                    if ((g_migr_preheat_enable || g_migr_hybridheat_enable) &&
                        migr_preheat_updates_tracker->contains(granule_id)) {
                        unordered_set<uint64_t> *updates;
                        migr_preheat_updates_tracker->get(granule_id, updates);
                        for (auto kv_it = rows->begin(); kv_it != rows->end();
                             ++kv_it) {
                            updates->insert(kv_it->first);
                        }
                    }
                }
            }
            uint64_t send_req_end = GetSystemClock(); 
            if (g_replay_parallel_flush_enable && (async_cnt > 0)) {
                cout << "XXX [Replay] wait for asycn flush with count " << async_cnt << endl;
                replay_datastore->WaitForAsync(async_cnt);
                // LOG_INFO(
                //     "[Replay]: async batch insert finishe with %d async "
                //     "calls",
                //     async_cnt);
            }
            // replay_lsn->SetAndNotify(last_entry_lsn);
            // LOG_INFO("[Replay] Update Current LSN to %s After An
            // Effective Batch Replay", last_entry_lsn.c_str());
            replay_lsn->SetAndNotify(last_entry_lsn);
            uint64_t flush_end_st = GetSystemClock(); 
            batch_st_time = flush_end_st;
            batch_fill_entry_num = 0;
            uint64_t dur = flush_end_st - flush_datastore_start; 
            accum_flush_to_datastore_total_time += dur;
            LOG_INFO("[Replay]: Logrepaly update replay lsn to (%s) takes %.2f ms(%.2f ms sends, %.2f ms waits)",
            ConcurrentLSN::PrintStrFor(last_entry_lsn).c_str(), nano_to_ms(dur), nano_to_ms(send_req_end-flush_datastore_start), nano_to_ms(flush_end_st - send_req_end));
            kvs.clear();
            futures.clear();
            replay_total_flush_entry_num += map_size;
            replay_flush_num += 1;
        } 

        if (log_entry == nullptr) {
            log_entry = gc_ring_buffer->BlockingReplay();
            string lsn_str = log_entry->GetLSN(); 
            cout << "XXX blocking replay to lsn:" <<ConcurrentLSN::PrintStrFor(lsn_str)<< endl;
            finish = log_entry->IsFakeEntry();
        } else {
        //     replay_lsn->SetAndNotify(last_entry_lsn);
        //        LOG_INFO("XXX[Replay]: Logrepaly update replay lsn to (%s)",
        //     ConcurrentLSN::PrintStrFor(last_entry_lsn).c_str());
        //    log_entry = gc_ring_buffer->BlockingReplay();
           log_entry = gc_ring_buffer->Replay();
           if (log_entry) {
               finish = log_entry->IsFakeEntry();
           }
        }

    }

    // do final flush after all workers stop
    map_size = kvs.size();
    if (map_size > 0) {
        uint64_t flush_datastore_start = GetSystemClock();
        size_t async_cnt = 0;
        // flush kvs to data store and delete elements in kvs
        for (auto &it : kvs) {
            auto granule_uid = it.first;
            auto tbl_id = granule_uid >> 32;
            auto tbl = db_->GetTable(tbl_id);
            // no need to replay systable into data store
            if (!tbl->IsSysTable()) {
                auto tbl_name = tbl->GetTableName();
                auto granule_id = (granule_uid << 32) >> 32;
                unordered_map<uint64_t, string> *rows = &(it.second);
                if (rows->size() <= azure_batch_op_limit) {
                    if (g_replay_parallel_flush_enable) {
                        async_cnt++;
                        replay_datastore->AsyncBatchWrite(tbl_name, granule_id,
                                                          rows);
                    } else {
                        replay_datastore->BatchWrite(tbl_name, granule_id,
                                                     rows);
                    }

                } else {
                    for (const auto &pair : *rows) {
                        // Check if the current small map is full
                        if (currentSmallMap.size() >= azure_batch_op_limit) {
                            if (g_replay_parallel_flush_enable) {
                                async_cnt++;
                                replay_datastore->AsyncBatchWrite(
                                    tbl_name, granule_id, &currentSmallMap);
                            } else {
                                replay_datastore->BatchWrite(
                                    tbl_name, granule_id, &currentSmallMap);
                            }
                            currentSmallMap.clear();
                        }
                        currentSmallMap[pair.first] = pair.second;
                    }
                    if (currentSmallMap.size() > 0) {
                        if (g_replay_parallel_flush_enable) {
                            async_cnt++;
                            replay_datastore->AsyncBatchWrite(
                                tbl_name, granule_id, &currentSmallMap);
                        } else {
                            replay_datastore->BatchWrite(tbl_name, granule_id,
                                                         &currentSmallMap);
                        }
                        currentSmallMap.clear();
                    }
                }

                // if (g_replay_parallel_flush_enable) {
                //     async_cnt ++;
                //     replay_datastore->AsyncBatchWrite(tbl_name,
                //     granule_id, rows);
                // } else {
                //     replay_datastore->BatchWrite(tbl_name, granule_id,
                //     rows);
                // }
                // add updates to migr_preheat_updates_tracker
                if ((g_migr_preheat_enable || g_migr_hybridheat_enable) &&
                    migr_preheat_updates_tracker->contains(granule_id)) {
                    unordered_set<uint64_t> *updates;
                    migr_preheat_updates_tracker->get(granule_id, updates);
                    for (auto kv_it = rows->begin(); kv_it != rows->end();
                         ++kv_it) {
                        updates->insert(kv_it->first);
                    }
                }
            }
        }
        if (g_replay_parallel_flush_enable && (async_cnt > 0)) {
            //    log_semaphore_->wait();
            replay_datastore->WaitForAsync(async_cnt);
        }
        replay_lsn->SetAndNotify(last_entry_lsn);
        LOG_INFO(
            "[Replay] Last Flush: Update Current LSN to %s After "
            "An Effective Batch Replay",
            ConcurrentLSN::PrintStrFor(last_entry_lsn).c_str());
        kvs.clear();
        accum_flush_to_datastore_total_time +=
            GetSystemClock() - flush_datastore_start;
        replay_total_flush_entry_num += map_size;
        replay_flush_num += 1;
    }

    replay_time += GetSystemClock() - replay_start;
    if (!txnTracker->IsEmpty()) {
        string log_content;
        txnTracker->ToString(log_content);
        std::cout << "txnTracker is, size of leftover elements is "
                  << txnTracker->GetSize() << ", content is " << log_content
                  << std::endl;
        if (!g_migr_albatross) {
        M_ASSERT(txnTracker->IsEmpty(),
                 "txnTracker is not empty at the end of the replay service, "
                 "size of leftover elements is %d, content is %s\n",
                 txnTracker->GetSize(), log_content.c_str());
        } 

    }

    auto replay_avg_entry_num_per_flush =
        replay_total_flush_entry_num * 1.0 / replay_flush_num;
    auto replay_avg_flush_latency_us_per_flush =
        nano_to_us(accum_flush_to_datastore_total_time) / replay_flush_num;
    auto replay_avg_flush_latency_us_per_entry =
        nano_to_us(accum_flush_to_datastore_total_time) /
        replay_total_flush_entry_num;
    // auto replay_avg_read_latency_us_per_entry =
    //     nano_to_us(accum_replay_read_log_total_time) /
    //     accum_replay_read_entry_num;

    LOG_INFO("---------------Replay stats-------------------");
    LOG_INFO("[REPLAY] replay_total_flush_num: %d", replay_flush_num);
    LOG_INFO("[REPLAY] replay_total_flush_entry_num: %lu",
             replay_total_flush_entry_num);
    LOG_INFO("[REPLAY] replay_total_flush_latency_us: %.2f",
             nano_to_us(accum_flush_to_datastore_total_time));

    LOG_INFO("[REPLAY] replay_avg_entry_num_per_flush: %.2f",
             replay_avg_entry_num_per_flush);
    LOG_INFO("[REPLAY] replay_avg_flush_latency_us_per_flush: %.2f",
             replay_avg_flush_latency_us_per_flush);
    LOG_INFO("[REPLAY] replay_avg_flush_latency_us_per_entry: %.2f ",
             replay_avg_flush_latency_us_per_entry);
    // LOG_INFO("[REPLAY] replay_total_read_entry_num: %lu",
    //          accum_replay_read_entry_num);
    // LOG_INFO("[REPLAY] replay_avg_read_latency_us_per_entry_us: %.2f",
    //          replay_avg_read_latency_us_per_entry);
    LOG_INFO("---------------Replay stats-------------------");

    if (g_save_output) {
        g_out_file << "\"replay_total_flush_entry_num\": "
                   << replay_total_flush_entry_num << ", ";
        g_out_file << "\"replay_avg_entry_num_per_flush\": "
                   << g_replay_flush_threshold << ", ";
        g_out_file << "\"replay_avg_flush_latency_us_per_flush\": "
                   << replay_avg_flush_latency_us_per_flush << ", ";
        g_out_file << "\"replay_avg_flush_latency_us_per_entry\": "
                   << replay_avg_flush_latency_us_per_entry << ", ";
        // g_out_file << "\"replay_total_read_entry_num\": "
        //            << accum_replay_read_entry_num << ", ";
        // g_out_file << "\"replay_avg_read_latency_us_per_entry\": "
        //            << replay_avg_read_latency_us_per_entry << ", ";
    }

    return OK;
}

#endif

}  // namespace arboretum
