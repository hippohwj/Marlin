#include "RedisClient.h"
#include "Common.h"
#include "GlobalData.h"
#include "OptionalGlobalData.h"
#include "db/log/LogSeqNum.h"
#include "common/Message.h" 
#include <functional>


// #if REMOTE_STORAGE_TYPE == REMOTE_STORAGE_REDIS

using namespace std;
namespace arboretum {
  

RedisClient::RedisClient(char * host, size_t port) {
  LOG_INFO("Connecting to redis server at %s:%zu", host, port);
  client_ = NEW(cpp_redis::client);
  client_->connect(host, port,
                      [](const std::string &host,
                         std::size_t port,
                         cpp_redis::connect_state status) {
                        if (status == cpp_redis::connect_state::dropped) {
                          LOG_DEBUG("Disconnect from redis server at %s:%zu", host.c_str(), port);
                        }
                      });
  client_->auth(g_storage_pwd, [](cpp_redis::reply &response) {
    LOG_INFO("Redis auth response: %s", response.as_string().c_str());
  });
  if (!g_storage_data_loaded) {
    // LOG_INFO("Flushed all previous data in redis. ");
    // client_->flushall();
  }
  LOG_INFO("Successfully connected to redis server at %s:%zu", host, port);
}

RC RedisClient::StoreSync(const std::string &hset_name, const std::string &key, char *value, size_t sz) {
  std::string s(value, sz);
  client_->hset(hset_name, key, s);
  client_->sync_commit();
  return RC::OK;
}

void RedisClient::BatchStoreSync(uint64_t granule_id, std::multimap<std::string, std::string> map) {
  for (auto & it : map) {
    client_->hset("G-" + std::to_string(granule_id), it.first, it.second);
  }
  client_->sync_commit();
}

void RedisClient::LoadSync(const std::string &hset_name, const std::string & key,
                           std::string &data) {
  auto future_reply = client_->hget(hset_name, key);
  client_->sync_commit();
  auto reply = future_reply.get();
  if (reply.is_error())
    M_ASSERT(false, "Redis receives invalid reply: %s  for key %s to hset(name: %s).", reply.error().c_str(),key.c_str() , hset_name.c_str());
  if (reply.is_null()) {
    M_ASSERT(false, "Redis receives null reply for key %s to hset(name: %s).", key.c_str(), hset_name.c_str());
  }
  data = reply.as_string();
}

void RedisClient::StoreAndLoadSync(uint64_t granule_id,
                                   const std::string &key,
                                   char *value,
                                   size_t sz,
                                   const std::string &load_key,
                                   std::string &load_data) {
  auto script = R"(
      redis.call('hset', KEYS[1], KEYS[2], ARGV[1])
      return redis.call('hget', KEYS[3], KEYS[4]);
    )";
  std::string s(value, sz);
  // TODO: currently assume two keys are from same granule
  std::vector<std::string> keys = {"G-" + std::to_string(granule_id), key,
                         "G-" + std::to_string(granule_id), load_key};
  std::vector<std::string> args = {s};
  cpp_redis::reply rp;
  client_->eval(script, keys, args, [&rp](cpp_redis::reply &reply) {
    rp = reply;
  });
  client_->sync_commit();
  if (!rp.is_string()) {
    LOG_ERROR("key %s not found in redis", load_key.c_str());
  }
  load_data = rp.as_string();
}

int64_t RedisClient::CheckSize(uint64_t granule_id) {
  auto future_reply = client_->hlen("G-" + std::to_string(granule_id));
  client_->sync_commit();
  auto reply = future_reply.get();
  return reply.as_integer();
}

void RedisClient::LogSync(std::string &stream, OID txn_id, std::multimap<std::string, std::string> &log) {
  client_->xadd(stream, std::to_string(txn_id), log);
  client_->sync_commit();
}



void RedisClient::read_logs_next_batch_sync(uint64_t node_id, string read_start, uint16_t batch_size,
                                       const cpp_redis::reply_callback_t &reply_callback) {
    string stream_name = "log-" + std::to_string(node_id);
//    XREAD  count 2 STREAMS log-0  1667952541172-0
//    EVAL "return redis.call('xread', ARGV[1], ARGV[2], KEYS[1], ARGV[3], ARGV[4])" 1 log-10000 STREAMS count 2 1667952541172-0
//    EVAL "return redis.call('xread', ARGV[1], ARGV[2], ARGV[3],KEYS[1], ARGV[4])" 1 log-10000 count 10 STREAMS 0

    auto script = R"(
      return redis.call('xread', ARGV[1], ARGV[2], KEYS[1],ARGV[3], ARGV[4]);
    )";
    // auto script = R"(
    //   return redis.call('xread', ARGV[1], ARGV[2], ARGV[3], ARGV[4], KEYS[1], ARGV[5], ARGV[6]);
    // )";
    std::vector<std::string> keys = {"streams"};
//    std::vector<std::string> keys = {stream_name};
    // string cur_replay_lsn;
    // replay_lsn->get_lsn(cur_replay_lsn);
    std::vector<std::string> args = {"count", std::to_string(batch_size), stream_name, read_start};
    // std::vector<std::string> args = {"count", std::to_string(batch_size), "block", "1000", stream_name, read_start};
//    std::vector<std::string> args = {"count", std::to_string(batch_size), "streams", cur_replay_lsn};
//    clients[0]->eval(script, keys.size(), keys, args, reply_callback);
    client_->eval(script, keys, args, reply_callback);
    client_->sync_commit();
    // LOG_INFO("[Replay] Read Batch From LSN %s", cur_replay_lsn.c_str());

}

//  hscan M_TBL_300G-0 0 COUNT 1
void RedisClient::storeScan(
    const std::string &hset_name, uint32_t batch_size,
    function<string(cpp_redis::reply &reply)> scan_callback) {
    bool reach_end = false;
    string cursor = "0";
    string batch_size_str = std::to_string(batch_size);

    while (!reach_end) {
    //  hscan M_TBL_300G-0 0 COUNT 1
    LOG_INFO("XXX scan batch with cursor %s for obj %s", cursor.c_str(), hset_name.c_str());
    // std::cout<< "XXX scan batch with cursor " << cursor << " for obj " << hset_name << std::endl; 
    auto script = R"(
      return redis.call('hscan', KEYS[1], ARGV[1], ARGV[2], ARGV[3]);
    )";
    std::vector<std::string> keys = {hset_name};
    std::vector<std::string> args = {cursor, "COUNT", batch_size_str};
    // client_->eval(script, keys, args,
    //               [&cursor, &scan_callback](cpp_redis::reply &reply) {
    //                   cursor = scan_callback(reply);
    //               });
    auto future_reply = client_->eval(script, keys, args);
    client_->sync_commit();
          //  assert(future_reply.is_array());
    auto reply = future_reply.get();
    cursor = scan_callback(reply);
    reach_end = (cursor == "0");
    }
}

//TODO(Hippo): used for log replay
void RedisClient::store_sync_data(const std::string &hset_name, unordered_map<uint64_t, string> *kvs) {
//  HSET G-0 k1 v1 k2 v2
//  EVAL "return redis.call('hset', unpack(ARGV))" 0 myset k1 v1 k2 v2
    auto script = R"(
      return redis.call('hset', unpack(ARGV));
    )";
    vector<string> keys = {};
    vector<string> args;
    args.push_back(hset_name);
    // construct args using kvs
    for (auto it = kvs->begin(); it != kvs->end(); ++it) {
        args.push_back(to_string(it->first));
        args.push_back(it->second);
    }
    client_->eval(script, keys, args);
    client_->sync_commit();
}


// used for log replay
void RedisClient::AsyncBatchKVStore(const std::string &hset_name, unordered_map<uint64_t, string> *kvs, const cpp_redis::reply_callback_t &reply_callback) {
//  HSET G-0 k1 v1 k2 v2
//  EVAL "return redis.call('hset', unpack(ARGV))" 0 myset k1 v1 k2 v2
    auto script = R"(
      return redis.call('hset', unpack(ARGV));
    )";
    vector<string> keys = {};
    vector<string> args;
    args.push_back(hset_name);
    // construct args using kvs
    for (auto it = kvs->begin(); it != kvs->end(); ++it) {
        args.push_back(to_string(it->first));
        args.push_back(it->second);
    }
    client_->eval(script, keys, args, reply_callback);
    client_->commit();
}



// // synchronous
// // single partition commit
// void
// RedisClient::log_sync_data(uint64_t node_id, uint64_t txn_id, int status,
//                            string &data) {
//     uint64_t starttime = GetSystemClock();
//     log_sync_data(node_id, txn_id, status, data, [&starttime](cpp_redis::reply &response) {
//         assert(response.is_array());
//         string lsn_str = response.as_array()[0].as_string();
//         TxnManager *txn = txn_table->get_txn(response.as_array()[1].as_integer(),
//                                              false, false);
//         txn->set_txn_lsn_from_str(lsn_str);
//         INC_FLOAT_STATS(log_sync_data, GetSystemClock() - starttime);
//         INC_INT_STATS(num_log_sync_data, 1);
//     });
// }

// // synchronous
// // single partition commit
// void RedisClient::log_sync_data(uint64_t node_id, uint64_t txn_id, int status,
//                            string &data) {
//     uint64_t starttime = GetSystemClock();
//     log_sync_data(node_id, txn_id, status, data, [&starttime](cpp_redis::reply &response) {
//         assert(response.is_array());
//         string lsn_str = response.as_array()[0].as_string();
//         TxnManager *txn = txn_table->get_txn(response.as_array()[1].as_integer(),
//                                              false, false);
//         txn->set_txn_lsn_from_str(lsn_str);
//         uint64_t starttime = response.as_array()[2].as_integer();

//         //TODO(hippo)
//         // INC_FLOAT_STATS(log_sync_data, GetSystemClock() - starttime);
//         // INC_INT_STATS(num_log_sync_data, 1);
//     });
// }

// // synchronous
// // single partition commit
// void RedisClient::log_sync_data(uint64_t node_id, uint64_t txn_id, int status,
//                            string &data) {
//     uint64_t starttime = GetSystemClock();
//     log_sync_data(node_id, txn_id, status, data, [&starttime](cpp_redis::reply &response) {
//         assert(response.is_array());
//         string lsn_str = response.as_array()[0].as_string();
//         TxnManager *txn = txn_table->get_txn(response.as_array()[1].as_integer(),
//                                              false, false);
//         txn->set_txn_lsn_from_str(lsn_str);
//         uint64_t starttime = response.as_array()[2].as_integer();

//         //TODO(hippo)
//         // INC_FLOAT_STATS(log_sync_data, GetSystemClock() - starttime);
//         // INC_INT_STATS(num_log_sync_data, 1);
//     });
// }

// synchronous
// single partition commit
void RedisClient::log_sync_data(uint64_t node_id, uint64_t txn_id, int status,
                           std::string &data, const cpp_redis::reply_callback_t &reply_callback) {
    string stream_name = "log-" + std::to_string(node_id);
    uint64_t starttime = GetSystemClock();
    auto script = R"(
        local lsn = redis.call('xadd', KEYS[1], ARGV[1], ARGV[2], ARGV[3], ARGV[4], ARGV[5])
        return {lsn, tonumber(ARGV[6])}
    )";
    string tid = std::to_string(txn_id);
    std::vector<std::string> keys = {stream_name};
    std::vector<std::string> args = {"*", "data-" + tid, data, "status-" + tid, std::to_string(status), tid, std::to_string(starttime)};

    client_->eval(script, keys, args, reply_callback);
    client_->sync_commit();
}


void RedisClient::logDataAsync(uint64_t node_id, uint64_t txn_id, int status,
                           std::string &data, const cpp_redis::reply_callback_t &reply_callback) {
    
     string stream_name = "log-" + std::to_string(node_id);
    // uint64_t starttime = GetSystemClock();
    auto script = R"(
        local lsn = redis.call('xadd', KEYS[1], ARGV[1], ARGV[2], ARGV[3], ARGV[4], ARGV[5])
        return {lsn}
    )";
    string tid = std::to_string(txn_id);
    std::vector<std::string> keys = {stream_name};
    // std::vector<std::string> args = {"*", "d-" + tid, data, "s-" + tid, std::to_string(status), tid};
    std::vector<std::string> args = {"*", "d-" + tid, data, "s-" + tid, std::to_string(status)};
    client_->eval(script, keys, args, reply_callback);
    client_->commit();
}


// synchronous
// single partition commit
void RedisClient::log_sync_data(uint64_t node_id, uint64_t txn_id, int status,
                           std::string &data, string& lsn) {
    string stream_name = "log-" + std::to_string(node_id);
    // uint64_t starttime = GetSystemClock();
    auto script = R"(
        local lsn = redis.call('xadd', KEYS[1], ARGV[1], ARGV[2], ARGV[3], ARGV[4], ARGV[5])
        return {lsn}
    )";
    string tid = std::to_string(txn_id);
    std::vector<std::string> keys = {stream_name};
    // std::vector<std::string> args = {"*", "d-" + tid, data, "s-" + tid, std::to_string(status), tid};
    std::vector<std::string> args = {"*", "d-" + tid, data, "s-" + tid, std::to_string(status)};

    auto future_reply = client_->eval(script, keys, args);
    client_->sync_commit();
          //  assert(future_reply.is_array());
    auto reply = future_reply.get();
    lsn = reply.as_array()[0].as_string();
        //TODO(hippo)
        // INC_FLOAT_STATS(log_sync_data, GetSystemClock() - starttime);
        // INC_INT_STATS(num_log_sync_data, 1);

  //     auto future_reply = client_->hget("G-" + std::to_string(granule_id), key);
  // client_->sync_commit();
  // auto reply = future_reply.get();
  // if (reply.is_error())
  //   LOG_ERROR("Redis receives invalid reply: %s", reply.error().c_str());
  // if (reply.is_null()) {
  //   LOG_ERROR("Redis receives null reply for key %s.", key.c_str());
  // }
  // data = reply.as_string();
}

void RedisClient::GroupLogSync(uint64_t node_id, vector<LogEntry *> * logs, string& lsn) {
  string stream_name = "log-" + std::to_string(node_id);
  auto script = R"(
      local lsn = redis.call('xadd', unpack(ARGV))
      return {lsn}
  )";

  vector<string> keys = {};
  vector<string> args;
  args.push_back(stream_name);
  args.push_back("*");
  // construct args using kvs
  for(vector<LogEntry *>::iterator it = logs->begin(); it < logs->end(); it++ ){
      LogEntry * entry = *it;
      string tid = to_string(entry->GetTxnId());
      // put field name
      args.push_back("d-" + tid);
      // put field value
      args.push_back(entry->GetData());
      // args.push_back("");
      // put field name
      args.push_back("s-" + tid);
      // put field value
      args.push_back(to_string(entry->GetStatus()));
  }

  auto future_reply = client_->eval(script, keys, args);
  client_->sync_commit();
        //  assert(future_reply.is_array());
  auto reply = future_reply.get();
  lsn = reply.as_array()[0].as_string();
}


}

// #endif // REMOTE_STORAGE_TYPE == REMOTE_STORAGE_REDIS