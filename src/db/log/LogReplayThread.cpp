#include <map>
// #include "system/manager.h"
// #include "config.h"
// #include "transport/redis_client.h"
// #include "log_replay_thread.h"
// #include "utils/helper.h"
// #include "system/global.h"
#include "GlobalData.h"
#include "LogEntry.h"
#include "LogReplayThread.h"
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
void *start_producer_thread(void *thread) {
    ((LogReplayProducer *)thread)->run();
    return nullptr;
}

LogReplayProducer::LogReplayProducer(ARDB *db, ILogStore *new_replay_logstore, BlockingQueue<void *> * queue) {
    db_ = db;
    replay_logstore = new_replay_logstore;
    logs_ = queue;
}


tuple<bool, LogEntry *> TxnTracker::AppendOrFlush(LogEntry *entry) {
    // TODO(hippo): deal with abort decision log
    if (entry->IsCommit()) {
        if (!entry->HasData()) {
            // reach this branch for 2PC of multi-partition txn.
            // get data and remove txn info from txn tracker and flush data to
            // the data store
            auto it = txn_table_.find(entry->GetTxnId());
            if (it != txn_table_.end()) {
                LogEntry *data_entry = it->second;
                txn_table_.erase(it);
                return make_tuple(false, data_entry);
            } else {
                string log_content;
                entry->to_string(log_content);
                if (!g_migr_albatross) {
                    M_ASSERT(false,
                         "Unexpected Case: encounter a commit log carrying no "
                         "data with no history prepare log for the same txn, "
                         "the log content is: %s\n",
                         log_content.c_str());

                } else {
                  LOG_INFO(
                         "Unexpected Case: encounter a commit log carrying no "
                         "data with no history prepare log for the same txn, "
                         "the log content is: %s\n",
                         log_content.c_str());

                }
   
            }
        } else {
            // reach this branch for single partition commit
            // flush to data store
            // TODO(Hippo): support single partition commit later
            M_ASSERT(false, "no impl for single partition commit");
            return make_tuple(false, entry);
        }
    } else if (entry->IsPrepare()) {
        M_ASSERT(entry->HasData(), "prepare log entry should carry data");
        // track this txn
        // LOG_INFO("track entry with txn id: %u, with status %llu",
        // entry->GetTxnId(), entry->GetStatus());
        txn_table_[entry->GetTxnId()] = entry;
        return make_tuple(true, nullptr);
    } else {
        if (entry->IsAbort()) {
            // LOG_INFO("encounter log entry for ABORT");
            return make_tuple(true, nullptr);
        } else {
            string log_content;
            entry->to_string(log_content);
            M_ASSERT(
                false,
                "unknown log entry(expect prepare/commit/abort log entry) %s",
                log_content.c_str());
        }
    }
}

// LogReplayThread::LogReplayThread() {
LogReplayThread::LogReplayThread(ARDB *db) {
    txnTracker = new TxnTracker();
    replay_lsn = new ConcurrentLSN();
    log_semaphore_ = new SemaphoreSync();
    replay_logstore = NEW(ILogStore);
    replay_datastore = NEW(IDataStore);
    db_ = db;
}

#if REMOTE_STORAGE_TYPE == REMOTE_STORAGE_REDIS
void LogReplayThread::parse_reply(cpp_redis::reply &reply,
                                  vector<LogEntry *> *logs, string &lsn) {
    vector<cpp_redis::reply> items = reply.as_array();
    assert(items.size() == 2);
    lsn = items[0].as_string();
    vector<cpp_redis::reply> data = items[1].as_array();
    // if log carry data, data.size is supposed to be 4; otherwise, it is 2
    // for now, only support four fields for one txn
    // TODO(Hippo): support 2PC logs
    vector<cpp_redis::reply>::iterator it = data.begin();
    while (it < data.end()) {
        string data_field_name = (*(it)).as_string();
        it++;
        string user_data_str = (*(it)).as_string();
        it++;
        string status_field_name = (*(it)).as_string();
        it++;
        uint8_t status = static_cast<uint8_t>(std::stoul((*(it)).as_string()));
        it++;
        // logEntry = LogEntry(lsn, user_data_str, status, 0);
        // logs.push_back(NEW(LogEntry)(lsn, user_data_str, status, 0));
        std::stringstream test(status_field_name);
        std::string segment;
        while (std::getline(test, segment, '-')) {
        }
        uint64_t txn_id = std::stoul(segment);
        // LOG_INFO("parse log entry: data field name %s, status_field_name %s,
        // status %hhu, txnid %lu", data_field_name.c_str(),
        // status_field_name.c_str(), status, txn_id)
        logs->push_back(new LogEntry(lsn, user_data_str, status, txn_id));
    }
    // assert(data.size() == 4 || data.size() == 2);
    // if (data.size() == 4) {
    //     string user_data_str = data[1].as_string();
    //     uint8_t status = std::stoi(data[3].as_string());
    //     logEntry = LogEntry(lsn, user_data_str, status, 0);
    // } else {
    //     uint8_t status = std::stoi(data[1].as_string());
    //     logEntry = LogEntry(lsn, "", status, 0);
    // }
}


RC LogReplayThread::run() {
    LOG_INFO("---------------Log Replay Start to Run-------------------");
    // pulling from log (as reply) and write to redis data store
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
    // TODO(hippo): use the real node id
    //    while (!(empty_reply && sync_end)) {
    //     // sync_end = glob_manager->are_all_worker_done();
    //     sync_end = are_all_worker_done();
    //     empty_reply = true;
    //     sleep(2);
    //    }
    unordered_map<uint64_t, unordered_map<uint64_t, string>> kvs;

    string last_entry_lsn = "0";
    while (!(empty_reply && sync_end)) {
        // sync_end = are_all_worker_done();
        // sync_end = cluster_manager->end_sync_done();
        sync_end = cluster_manager->is_groupcommit_done();
        vector<cpp_redis::reply> replies;
        // usleep(1000);
        uint64_t read_start = GetSystemClock();
        replay_logstore->BatchRead(
            g_node_id, last_entry_lsn, LOG_REPLAY_BATCH_SIZE,
            [&replay_read_valid_log_num, &replies, &empty_reply,
             this](cpp_redis::reply &reply) {
                if (reply.is_null()) {
                    empty_reply = true;
                    return OK;
                } else {
                    empty_reply = false;
                    replay_read_valid_log_num++;
                    replies = reply.as_array()[0].as_array();
                }
                return OK;
            });
        uint64_t read_end = GetSystemClock();
        accum_replay_read_log_total_time += read_end - read_start;

        if (!empty_reply) {
            accum_replay_read_valid_log_total_time += read_end - read_start;
            // replies[0] is the stream name
            // replies[1] is the array of log items
            assert(!replies.empty());
            replies = replies[1].as_array();
            assert(!replies.empty());
            // TODO(Hippo): optimize => insert a batch of log entries to data
            // store in one call
            for (cpp_redis::reply entry : replies) {
                // LogEntry *log_entry = new LogEntry();
                vector<LogEntry *> log_entries;
                parse_reply(entry, &log_entries, last_entry_lsn);
                accum_replay_read_entry_num += log_entries.size();
                for (auto it = log_entries.begin(); it < log_entries.end();
                     it++) {
                    auto log_entry = *(it);
                    tuple<bool, LogEntry *> res =
                        txnTracker->AppendOrFlush(log_entry);
                    if (!get<0>(res)) {
                        auto legal_entry = get<1>(res);
                        M_ASSERT(legal_entry->IsPrepare(),
                                 "should only get prepared log entry to parse");
                        LogEntry::DeserializeLogData(legal_entry->GetData(),
                                                     &kvs);
                        delete legal_entry;
                    }
                }
                uint32_t map_size = 0;
                for (auto &it : kvs) {
                    map_size += it.second.size();
                }

                // g_replay_parallel_flush_enable

                if (map_size >= g_replay_flush_threshold) {
                    uint64_t flush_datastore_start = GetSystemClock();
                    // flush kvs to data store and delete elements in kvs
                    for (auto &it : kvs) {
                        uint64_t granule_uid = it.first;
                        auto tbl_id = granule_uid >> 32;
                        auto tbl = db_->GetTable(tbl_id);
                        if (!tbl->IsSysTable()) {
                            auto tbl_name = tbl->GetTableName();
                            uint64_t granule_id = (granule_uid << 32) >> 32;
                            unordered_map<uint64_t, string> *rows =
                                &(it.second);
                            if (g_replay_parallel_flush_enable) {
                            log_semaphore_->incr();
                            replay_datastore->AsyncBatchWrite(
                                tbl_name, granule_id, rows,
                                [this](cpp_redis::reply &reply) {
                                    if (reply.is_error() || reply.is_null()) {
                                        M_ASSERT(false,
                                                 "[Replay] unexpected reply "
                                                 "when flush to datastore: %s",
                                                 reply.as_string().c_str());
                                        log_semaphore_->decr();
                                    } else {
                                        log_semaphore_->decr();
                                    }
                                    // TODO(Hippo): properly set up lsn for the
                                    // entry
                                    //   log_entry->GetLSN(lsn);
                                });
                            } else {
                                replay_datastore->BatchWrite(tbl_name, granule_id, rows); 
                            }
                            // add updates to migr_preheat_updates_tracker
                            if ((g_migr_preheat_enable || g_migr_hybridheat_enable) &&
                                migr_preheat_updates_tracker->contains(
                                    granule_id)) {
                                unordered_set<uint64_t> *updates;
                                migr_preheat_updates_tracker->get(granule_id,
                                                                  updates);
                                for (auto kv_it = rows->begin();
                                     kv_it != rows->end(); ++kv_it) {
                                    updates->insert(kv_it->first);
                                }
                            }
                        }
                    }
                    if (g_replay_parallel_flush_enable) {
                        log_semaphore_->wait();
                    }
                    // replay_lsn->SetAndNotify(last_entry_lsn);
                    // LOG_INFO("[Replay] Update Current LSN to %s After An
                    // Effective Batch Replay", last_entry_lsn.c_str());
                    kvs.clear();
                    accum_flush_to_datastore_total_time +=
                        GetSystemClock() - flush_datastore_start;
                    replay_total_flush_entry_num += map_size;
                    replay_flush_num += 1;
                }
                replay_lsn->SetAndNotify(last_entry_lsn);
                // //TODO(Hippo): optimize => insert a batch of log entries to
                // data store in one call tuple<bool, LogEntry *> res =
                // txnTracker->AppendOrFlush(
                //         log_entry);
                // if (!get<0>(res)) {
                //     // flush to the data store
                //     map <uint64_t, string> kvs;
                //     LogEntry *cur_entry = get<1>(res);
                // LogEntry::DeserializeLogData(cur_entry->GetData(), &kvs);
                //     if (!kvs.empty()) {
                //         uint64_t flush_datastore_start = GetSystemClock();
                //         redis_c->store_sync_data(fake_node_id, &kvs);
                //         accum_flush_to_datastore_total_time +=
                //         GetSystemClock() - flush_datastore_start;
                //         replay_flush_to_datastore_num++;
                //     }
                //     last_entry_lsn = log_entry->GetLSN();
                //     // replay_lsn->set_lsn(log_entry->GetLSN());
                //     // LOG_INFO("[Replay] Update Current LSN to %s After An
                //     Effective Batch Replay", log_entry->GetLSN().c_str());
                //     delete cur_entry;
                // }
            }

        } else {
            //    LOG_INFO("---------------Replay Empty
            //    Batch-------------------"); sleep(1);
        }
        replay_loop_num++;
    }
    // do final flush after all workers stop
    uint32_t map_size = kvs.size();
    if (map_size > 0) {
        uint64_t flush_datastore_start = GetSystemClock();
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
                if (g_replay_parallel_flush_enable) {
                log_semaphore_->incr();
                replay_datastore->AsyncBatchWrite(
                    tbl_name, granule_id, rows,
                    [this](cpp_redis::reply &reply) {
                        if (reply.is_error() || reply.is_null()) {
                            M_ASSERT(false,
                                     "[Replay] unexpected reply "
                                     "when flush to datastore: %s",
                                     reply.as_string().c_str());
                            log_semaphore_->decr();
                        } else {
                            log_semaphore_->decr();
                        }
                        // TODO(Hippo): properly set up lsn for the
                        // entry
                        //   log_entry->GetLSN(lsn);
                    });
                } else {
                    replay_datastore->BatchWrite(tbl_name, granule_id, rows);
                }
                // add updates to migr_preheat_updates_tracker
                if ( (g_migr_preheat_enable || g_migr_hybridheat_enable) &&
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
        if (g_replay_parallel_flush_enable) {
        log_semaphore_->wait();
        }
        replay_lsn->SetAndNotify(last_entry_lsn);
        LOG_INFO(
            "[Replay] Last Flush: Update Current LSN to %s After "
            "An Effective Batch Replay",
            last_entry_lsn.c_str());
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
        M_ASSERT(txnTracker->IsEmpty(), "txnTracker is not empty at the end of the replay service, size of leftover elements is %d, content is %s\n", txnTracker->GetSize(), log_content.c_str());
    }

    auto replay_avg_entry_num_per_flush = replay_total_flush_entry_num * 1.0/replay_flush_num;
    auto replay_avg_flush_latency_us_per_flush =
        nano_to_us(accum_flush_to_datastore_total_time) / replay_flush_num;
    auto replay_avg_flush_latency_us_per_entry =
        nano_to_us(accum_flush_to_datastore_total_time) /
        replay_total_flush_entry_num;
    auto replay_avg_read_latency_us_per_entry =
        nano_to_us(accum_replay_read_log_total_time) /
        accum_replay_read_entry_num;

    LOG_INFO("---------------Replay stats-------------------");
    LOG_INFO("[REPLAY] replay_total_flush_entry_num: %lu",
             replay_total_flush_entry_num);
    LOG_INFO("[REPLAY] replay_avg_entry_num_per_flush: %lu",
             replay_avg_entry_num_per_flush);
    LOG_INFO("[REPLAY] replay_avg_flush_latency_us_per_flush: %.2f",
             replay_avg_flush_latency_us_per_flush);
    LOG_INFO("[REPLAY] replay_avg_flush_latency_us_per_entry: %.2f ",
             replay_avg_flush_latency_us_per_entry);
    LOG_INFO("[REPLAY] replay_total_read_entry_num: %lu",
             accum_replay_read_entry_num);
    LOG_INFO("[REPLAY] replay_avg_read_latency_us_per_entry: %.2f",
             replay_avg_read_latency_us_per_entry);
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
        g_out_file << "\"replay_total_read_entry_num\": "
                   << accum_replay_read_entry_num << ", ";
        g_out_file << "\"replay_avg_read_latency_us_per_entry\": "
                   << replay_avg_read_latency_us_per_entry << ", ";
    }

    return OK;
}
#elif REMOTE_STORAGE_TYPE == REMOTE_STORAGE_AZURE
RC LogReplayProducer::run() {
   LOG_INFO("---------------Log Replay Producer Start to Run-------------------");
   bool sync_end = false;
   bool empty_reply = false;
   uint64_t accum_replay_read_log_total_time = 0;
   uint64_t accum_replay_read_entry_num = 0;
   uint64_t accum_replay_read_count = 0;
   
   while (!(empty_reply && sync_end)) {
        sync_end = cluster_manager->is_groupcommit_done();
        // usleep(1000);
        uint64_t read_start = GetSystemClock();
        size_t entry_count = 0;
        replay_logstore->BatchRead([this, &entry_count](LogEntry* log_entry){
            logs_->Push(log_entry);
            entry_count++;
        });
        accum_replay_read_count++;
        empty_reply = (entry_count == 0);
        accum_replay_read_entry_num += entry_count;
        uint64_t read_end = GetSystemClock();
        accum_replay_read_log_total_time += read_end - read_start;
   }

   // add a marker for last entry
   logs_->Push(LogEntry::NewFakeEntry());
   LOG_INFO("--------------- Log Replay Producer Stats -------------------");
   LOG_INFO("[REPLAY] replay_total_read_count: %lu",
             accum_replay_read_count);
   LOG_INFO("[REPLAY] replay_total_read_entry_num: %lu",
             accum_replay_read_entry_num);
   LOG_INFO("[REPLAY] replay_total_read_latency_us: %.2f", nano_to_us(accum_replay_read_log_total_time));
   LOG_INFO("[REPLAY] replay_avg_batchread_latency_us: %.2f", nano_to_us(accum_replay_read_log_total_time)/accum_replay_read_count);
   LOG_INFO("--------------- Log Replay Producer Stats -------------------");

    if (g_save_output) {
        g_out_file << "\"replay_total_read_count\": "
                   << accum_replay_read_count << ", ";
        g_out_file << "\"replay_total_read_entry_num\": "
                   << accum_replay_read_entry_num << ", ";
        g_out_file << "\"replay_total_read_latency_us\": "
                   << nano_to_us(accum_replay_read_log_total_time) << ", ";
        g_out_file << "\"replay_avg_batchread_latency_us\": "
                   << nano_to_us(accum_replay_read_log_total_time)/accum_replay_read_count << ", ";
    }
}


RC LogReplayThread::run() {
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
    // TODO(hippo): use the real node id
    //    while (!(empty_reply && sync_end)) {
    //     // sync_end = glob_manager->are_all_worker_done();
    //     sync_end = are_all_worker_done();
    //     empty_reply = true;
    //     sleep(2);
    //    }
    unordered_map<uint64_t, unordered_map<uint64_t, string>> kvs;

    string last_entry_lsn = "0";
    vector<uint8_t> log_bytes_buffer;
    // size_t azure_batch_op_limit = 99;
    size_t azure_batch_op_limit = 100;
    std::unordered_map<uint64_t, std::string> currentSmallMap;
    if (g_replay_producer_enable) {
        // start producer thread
        BlockingQueue<void *> *logs_queue = new BlockingQueue<void *>();
        //   queue->Push((void *)this);
        auto *producer = new pthread_t;
        pthread_create(
            producer, nullptr, start_producer_thread,
            (void *)new LogReplayProducer(db_, replay_logstore, logs_queue));
        LOG_INFO("Log Replay Consumer Started");
        // current thread plays as a consumer
        LogEntry *log_entry = static_cast<LogEntry *>(logs_queue->Pop());
        bool finish = log_entry->IsFakeEntry();
        while (!finish) {
            last_entry_lsn = log_entry->GetLSN();
            tuple<bool, LogEntry *> res = txnTracker->AppendOrFlush(log_entry);
            if (!get<0>(res)) {
                auto legal_entry = get<1>(res);
                M_ASSERT(legal_entry->IsPrepare(),
                         "should only get prepared log entry to parse");
                LogEntry::DeserializeLogData(legal_entry->GetData(), &kvs);
                delete legal_entry;
            }
            uint32_t map_size = 0;
            for (auto &it : kvs) {
                map_size += it.second.size();
            }
            if (map_size >= g_replay_flush_threshold) {
                uint64_t flush_datastore_start = GetSystemClock();
                size_t async_cnt = 0;
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
                                replay_datastore->AsyncBatchWrite(
                                    tbl_name, granule_id, rows);
                            } else {
                                replay_datastore->BatchWrite(tbl_name,
                                                             granule_id, rows);
                            }

                        } else {
                            for (const auto &pair : *rows) {
                                // Check if the current small map is full
                                if (currentSmallMap.size() >=
                                    azure_batch_op_limit) {
                                    if (g_replay_parallel_flush_enable) {
                                        async_cnt++;
                                        replay_datastore->AsyncBatchWrite(
                                            tbl_name, granule_id,
                                            &currentSmallMap);
                                    } else {
                                        replay_datastore->BatchWrite(
                                            tbl_name, granule_id,
                                            &currentSmallMap);
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
                                    replay_datastore->BatchWrite(
                                        tbl_name, granule_id, &currentSmallMap);
                                }
                                currentSmallMap.clear();
                            }
                        }

                        // add updates to migr_preheat_updates_tracker
                        if ((g_migr_preheat_enable ||
                             g_migr_hybridheat_enable) &&
                            migr_preheat_updates_tracker->contains(
                                granule_id)) {
                            unordered_set<uint64_t> *updates;
                            migr_preheat_updates_tracker->get(granule_id,
                                                              updates);
                            for (auto kv_it = rows->begin();
                                 kv_it != rows->end(); ++kv_it) {
                                updates->insert(kv_it->first);
                            }
                        }
                    }
                }
                if (g_replay_parallel_flush_enable) {
                    replay_datastore->WaitForAsync(async_cnt);
                    LOG_INFO(
                        "[Replay]: async batch insert finishe with %d async "
                        "calls",
                        async_cnt);
                    // log_semaphore_->wait();
                }
                // replay_lsn->SetAndNotify(last_entry_lsn);
                // LOG_INFO("[Replay] Update Current LSN to %s After An
                // Effective Batch Replay", last_entry_lsn.c_str());
                // replay_lsn->SetAndNotify(last_entry_lsn);
                // LOG_INFO("XXX[Replay]: Logrepaly update replay lsn to (%s)",
                // ConcurrentLSN::PrintStrFor(last_entry_lsn).c_str());
                kvs.clear();
                accum_flush_to_datastore_total_time +=
                    GetSystemClock() - flush_datastore_start;
                replay_total_flush_entry_num += map_size;
                replay_flush_num += 1;
            }
            replay_lsn->SetAndNotify(last_entry_lsn);
            log_entry = static_cast<LogEntry *>(logs_queue->Pop());
            finish = log_entry->IsFakeEntry();
        }
        LOG_INFO("[Replay]Wait for replay producer thread to finish...")
        pthread_join(*producer, nullptr);
    } else {
        vector<LogEntry *> log_entries;
        while (!(empty_reply && sync_end)) {
        // sync_end = are_all_worker_done();
        // sync_end = cluster_manager->end_sync_done();
        sync_end = cluster_manager->is_groupcommit_done();
        // usleep(1000);
        uint64_t read_start = GetSystemClock();
        replay_logstore->BatchRead(&log_entries);
        empty_reply = (log_entries.size() == 0);
        uint64_t read_end = GetSystemClock();
        accum_replay_read_log_total_time += read_end - read_start;

        if (!empty_reply) {
            last_entry_lsn = log_entries.back()->GetLSN();
            accum_replay_read_valid_log_total_time += read_end - read_start;
            // TODO(Hippo): optimize => insert a batch of log entries to data
            // store in one call
                accum_replay_read_entry_num += log_entries.size();
                for (auto it = log_entries.begin(); it < log_entries.end();
                     it++) {
                    auto log_entry = *(it);
                    tuple<bool, LogEntry *> res =
                        txnTracker->AppendOrFlush(log_entry);
                    if (!get<0>(res)) {
                        auto legal_entry = get<1>(res);
                        M_ASSERT(legal_entry->IsPrepare(),
                                 "should only get prepared log entry to parse");
                        LogEntry::DeserializeLogData(legal_entry->GetData(),
                                                     &kvs);
                        delete legal_entry;
                    }
                }
                log_entries.clear();
                uint32_t map_size = 0;
                for (auto &it : kvs) {
                    map_size += it.second.size();
                }

                // g_replay_parallel_flush_enable

                if (map_size >= g_replay_flush_threshold) {
                    uint64_t flush_datastore_start = GetSystemClock();
                    size_t async_cnt = 0;
                    // flush kvs to data store and delete elements in kvs
                    for (auto &it : kvs) {
                        uint64_t granule_uid = it.first;
                        auto tbl_id = granule_uid >> 32;
                        auto tbl = db_->GetTable(tbl_id);
                        if (!tbl->IsSysTable()) {
                            auto tbl_name = tbl->GetTableName();
                            uint64_t granule_id = (granule_uid << 32) >> 32;
                            unordered_map<uint64_t, string> *rows =
                                &(it.second);
                            if (rows->size() <= azure_batch_op_limit) {
                                if (g_replay_parallel_flush_enable) {
                                    async_cnt ++;
                                    replay_datastore->AsyncBatchWrite(tbl_name, granule_id, rows);
                               } else {
                                    replay_datastore->BatchWrite(tbl_name, granule_id, rows); 
                               }

                            } else {
                               for (const auto &pair : *rows) {
                                    // Check if the current small map is full
                                    if (currentSmallMap.size() >=
                                        azure_batch_op_limit) {
                                        if (g_replay_parallel_flush_enable) {
                                            async_cnt++;
                                            replay_datastore->AsyncBatchWrite(
                                                tbl_name, granule_id,
                                                &currentSmallMap);
                                        } else {
                                            replay_datastore->BatchWrite(
                                                tbl_name, granule_id,
                                                &currentSmallMap);
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
                                        replay_datastore->BatchWrite(
                                            tbl_name, granule_id, &currentSmallMap);
                                    }
                                    currentSmallMap.clear();
                               }
                            }

                            // add updates to migr_preheat_updates_tracker
                            if ( (g_migr_preheat_enable || g_migr_hybridheat_enable) &&
                                migr_preheat_updates_tracker->contains(
                                    granule_id)) {
                                unordered_set<uint64_t> *updates;
                                migr_preheat_updates_tracker->get(granule_id,
                                                                  updates);
                                for (auto kv_it = rows->begin();
                                     kv_it != rows->end(); ++kv_it) {
                                    updates->insert(kv_it->first);
                                }
                            }
                        }
                    }
                    if (g_replay_parallel_flush_enable) {
                        replay_datastore->WaitForAsync(async_cnt);
                        LOG_INFO("[Replay]: async batch insert finishe with %d async calls", async_cnt);
                        // log_semaphore_->wait();
                    }
                    // replay_lsn->SetAndNotify(last_entry_lsn);
                    // LOG_INFO("[Replay] Update Current LSN to %s After An
                    // Effective Batch Replay", last_entry_lsn.c_str());
                    // replay_lsn->SetAndNotify(last_entry_lsn);
                    // LOG_INFO("XXX[Replay]: Logrepaly update replay lsn to (%s)", ConcurrentLSN::PrintStrFor(last_entry_lsn).c_str());
                    kvs.clear();
                    accum_flush_to_datastore_total_time +=
                        GetSystemClock() - flush_datastore_start;
                    replay_total_flush_entry_num += map_size;
                    replay_flush_num += 1;
                }
                replay_lsn->SetAndNotify(last_entry_lsn);
                // LOG_INFO("XXX[Replay]: Logrepaly update replay lsn to (%s)", ConcurrentLSN::PrintStrFor(last_entry_lsn).c_str());

        } else {
            // LOG_INFO("---------------Replay Empty Batch-------------------"); 
            sleep(1);
        }
        replay_loop_num++;
    }
    // do final flush after all workers stop
    uint32_t map_size = kvs.size();
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
                            replay_datastore->AsyncBatchWrite(tbl_name,
                                                              granule_id, rows);
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
                                replay_datastore->BatchWrite(tbl_name,
                                                             granule_id, &currentSmallMap);
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
        if (g_replay_parallel_flush_enable) {
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
        std::cout<<  "txnTracker is, size of leftover elements is " << txnTracker->GetSize() << ", content is " << log_content << std::endl;
        M_ASSERT(txnTracker->IsEmpty(), "txnTracker is not empty at the end of the replay service, size of leftover elements is %d, content is %s\n", txnTracker->GetSize(), log_content.c_str());
    }

    }



    auto replay_avg_entry_num_per_flush = replay_total_flush_entry_num * 1.0/replay_flush_num;
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
