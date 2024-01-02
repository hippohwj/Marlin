#ifndef README_MD_SRC_REMOTE_AZURETABLECLIENT_H_
#define README_MD_SRC_REMOTE_AZURETABLECLIENT_H_

#include "common/Common.h"
#include "common/Worker.h"
#include <algorithm>
#include <was/storage_account.h>
#include <was/table.h>
#include <cpprest/http_client.h>
#include <cpprest/filestream.h>
#include <cpprest/json.h>
#include <iostream>
#include <string>
using namespace std;
using namespace utility;                    // Common utilities like string conversions
using namespace web;                        // Common features like URIs
using namespace web::http;                  // Common HTTP functionality
using namespace web::http::client;          // HTTP client features
// using namespace concurrency::streams;       // Asynchronous streams

namespace arboretum {
class AzureTableClient {
 public:
  explicit AzureTableClient(const std::string &str, std::atomic<int> * finish_sync) {
    const utility::string_t& storage_connection_string(U(str));
    finished_async_ = finish_sync;
    storage_acct_ = azure::storage::cloud_storage_account::parse(storage_connection_string);
    table_client_ = storage_acct_.create_cloud_table_client();
    LOG_DEBUG("Created azure table client. ");
  }

  void CreateTableIfNotExist(const std::string &tbl_name_input) {
    string tbl_name = ProcessTableName(tbl_name_input);
    tables_[tbl_name] = table_client_.get_table_reference(U(tbl_name));
    try {
      tables_[tbl_name].create_if_not_exists();
      LOG_DEBUG("Created table %s", tbl_name.c_str());
    } catch (const std::exception &e) {
      std::wcout << U("Error: ") << e.what() << std::endl;
      LOG_ERROR("failed to create table and exit.");
    }
  }

  void DeleteTable(const std::string &tbl_name_input) {
    string tbl_name = ProcessTableName(tbl_name_input);
    auto tbl = table_client_.get_table_reference(U(tbl_name));
    try {
      tbl.delete_table();
      LOG_DEBUG("deleted table %s", tbl_name.c_str());
    } catch (const std::exception &e) {
      std::wcout << U("Error: ") << e.what() << std::endl;
      LOG_ERROR("failed to delete table and exit.");
    }
  }

   void DeleteTableIfExist(const std::string &tbl_name_input) {
    string tbl_name = ProcessTableName(tbl_name_input);
    auto tbl = table_client_.get_table_reference(U(tbl_name));
    try {
      tbl.delete_table_if_exists();
      LOG_DEBUG("deleted table %s", tbl_name.c_str());
    } catch (const std::exception &e) {
      std::wcout << U("Error: ") << e.what() << std::endl;
      LOG_ERROR("failed to delete table and exit.");
    }
  }

  RC ScanSync(const std::string &tbl_name_input, std::string partition,
              function<void(uint64_t key, char * preloaded_data)> scan_callback) {
    string tbl_name = ProcessTableName(tbl_name_input);
    if (tables_.find(tbl_name) == tables_.end()) {
      tables_[tbl_name] = table_client_.get_table_reference(U(tbl_name));
    }
    int retry_left = g_remote_req_retries;
    auto table = tables_[tbl_name];
    azure::storage::table_query query;
    azure::storage::table_request_options options;
    azure::storage::operation_context context;
    azure::storage::continuation_token token;

    utility::string_t filter_string =
        azure::storage::table_query::generate_filter_condition(
            _XPLATSTR("PartitionKey"),
            azure::storage::query_comparison_operator::equal, partition);
    query.set_filter_string(filter_string);
    size_t segment_count = 0;
    size_t row_count = 0;
    azure::storage::table_query_segment query_segment;
    do {
      query_segment =
          table.execute_query_segmented(query, token, options, context);
      std::vector<azure::storage::table_entity> results =
          query_segment.results();

      // LOG_INFO("XXX data store parittion scan loop for partition %s returns %d rows", partition.c_str(), results.size());

      for (std::vector<azure::storage::table_entity>::const_iterator
               entity_iterator = results.cbegin();
           entity_iterator != results.cend(); ++entity_iterator) {
          azure::storage::table_entity entity = *entity_iterator;
          string key = entity.row_key();
          string cur_data;
              const azure::storage::table_entity::properties_type &properties =
                  entity.properties();
              auto property = properties.at(U("Data"));
              if (property.property_type() ==
                  azure::storage::edm_type::binary) {
                  auto ret = property.binary_value();
                  cur_data = std::string(ret.begin(), ret.end());
              } else if (property.property_type() ==
                         azure::storage::edm_type::string) {
                  cur_data = property.string_value();
              }

           scan_callback(stoul(key), const_cast<char *>(cur_data.c_str()));
           row_count++;
      }
      ++segment_count;
      token = query_segment.continuation_token();
    } while (!token.empty());

    LOG_INFO("ScanSync for partition %s finishes in %d segment fetches for %d rows", partition.c_str(), segment_count, row_count);

    return RC::OK;
  }

  RC ScanSync(const std::string &tbl_name_input, std::string partition, vector<string> * data) {
    string tbl_name = ProcessTableName(tbl_name_input);
    if (tables_.find(tbl_name) == tables_.end()) {
      tables_[tbl_name] = table_client_.get_table_reference(U(tbl_name));
    }
    int retry_left = g_remote_req_retries;
    auto table = tables_[tbl_name];
    // auto table = storage_acct_.create_cloud_table_client().get_table_reference(U(tbl_name));
    // auto retrieve_operation = azure::storage::table_operation::retrieve_entity(U(partition));
    azure::storage::table_query query;
    azure::storage::table_request_options options;
    azure::storage::operation_context context;
    // print_client_request_id(context, _XPLATSTR(""));

    utility::string_t filter_string = azure::storage::table_query::generate_filter_condition(_XPLATSTR("PartitionKey"), azure::storage::query_comparison_operator::equal, partition);
    query.set_filter_string(filter_string);
    while (retry_left >= 0) {
      try {
          azure::storage::table_query_iterator end_of_result;
          for (azure::storage::table_query_iterator iter =
                   table.execute_query(query, options, context);
               iter != end_of_result; ++iter) {
              string cur_data;
              azure::storage::table_entity entity = *iter;
              const azure::storage::table_entity::properties_type &properties =
                  entity.properties();
              auto property = properties.at(U("Data"));
              if (property.property_type() ==
                  azure::storage::edm_type::binary) {
                  auto ret = property.binary_value();
                  cur_data = std::string(ret.begin(), ret.end());
              } else if (property.property_type() ==
                         azure::storage::edm_type::string) {
                  cur_data = property.string_value();
              }
              data->push_back(cur_data);
          }
          retry_left = -1;
      } catch (const std::exception &e) {
          retry_left--;
          if (retry_left >= 0) {
              if (g_warmup_finished) {
                  auto backoff =
                      Worker::GetThdId() * (g_remote_req_retries - retry_left);
                  LOG_DEBUG(
                      "[thd-%u] Cannot scan data for partition %s: %s. %d-th "
                      "retry after %u ms",
                      Worker::GetThdId(), partition.c_str(), e.what(),
                      g_remote_req_retries - retry_left, backoff);
                  usleep(backoff * 1000);
              } else {
                  sleep(Worker::GetThdId() % 60 + 1);
              }
          } else {
              LOG_ERROR(
                  "[thd-%u] Cannot Scan data for partition %s: %s after "
                  "retries",
                  Worker::GetThdId(), partition.c_str(), e.what());
          }
      }
    }
    return RC::OK;
  }

  

  RC LoadSync(const std::string &tbl_name_input, std::string partition, const std::string &key, std::string &data) {
    string tbl_name = ProcessTableName(tbl_name_input);
    if (tables_.find(tbl_name) == tables_.end()) {
      tables_[tbl_name] = table_client_.get_table_reference(U(tbl_name));
    }
    int retry_left = g_remote_req_retries;
    auto table = tables_[tbl_name];
    // auto table = storage_acct_.create_cloud_table_client().get_table_reference(U(tbl_name));
    auto retrieve_operation = azure::storage::table_operation::retrieve_entity(
        U(partition), U(key));
    while (retry_left >= 0) {
      try {
        auto retrieve_result = table.execute(retrieve_operation);
        azure::storage::table_entity entity = retrieve_result.entity();
        const azure::storage::table_entity::properties_type& properties = entity.properties();
        auto property = properties.at(U("Data"));
        if (property.property_type() == azure::storage::edm_type::binary) {
          auto ret = property.binary_value();
          data = std::string(ret.begin(), ret.end());
        } else if (property.property_type() == azure::storage::edm_type::string){
          data = property.string_value();
        }
        retry_left = -1;
      } catch (const std::exception &e) {
        retry_left--;
        if (retry_left >= 0) {
          if (g_warmup_finished) {
            auto backoff = Worker::GetThdId() * (g_remote_req_retries - retry_left);
            LOG_DEBUG("[thd-%u] Cannot load data %s: %s. %d-th retry after %u ms",
                      Worker::GetThdId(), key.c_str(), e.what(),
                      g_remote_req_retries - retry_left, backoff);
            usleep(backoff * 1000);
          } else {
            sleep(Worker::GetThdId() % 60 + 1);
          }
        } else {
          LOG_ERROR("[thd-%u] Cannot load data %s: %s after retries",
                    Worker::GetThdId(), key.c_str(), e.what());
        }
      }
    }
    return RC::OK;
  }

  uint64_t LoadNumericSync(const std::string &tbl_name_input, std::string partition, const std::string &key) {
    string tbl_name = ProcessTableName(tbl_name_input);
    if (tables_.find(tbl_name) == tables_.end()) {
      tables_[tbl_name] = table_client_.get_table_reference(U(tbl_name));
    }
    auto table = tables_[tbl_name];
    auto retrieve_operation = azure::storage::table_operation::retrieve_entity(
        U(partition), U(key));
    auto retrieve_result = table.execute(retrieve_operation);
    azure::storage::table_entity entity = retrieve_result.entity();
    const azure::storage::table_entity::properties_type& properties = entity.properties();
    try {
      return properties.at(U("Data")).int64_value();
    } catch (const std::exception &e) {
      LOG_ERROR("Cannot load data %s: %s", key.c_str(), e.what());
    }
  }

  void StoreSync(const std::string &tbl_name_input, azure::storage::table_entity &tuple) {
        string tbl_name = ProcessTableName(tbl_name_input);
    auto table = tables_[tbl_name];
    auto insert_op = azure::storage::table_operation::insert_or_replace_entity(tuple);
    try {
      auto result = table.execute(insert_op);
    } catch (const std::exception &e) {
      LOG_ERROR("Failed to StoreSync: %s", e.what());
    }
  }

  void StoreSync(const std::string &tbl_name_input, std::string partition, const std::string &key, char * data, size_t sz) {
        string tbl_name = ProcessTableName(tbl_name_input);

    auto table = tables_[tbl_name];
    azure::storage::table_entity tuple(U(partition), U(key));
    auto& col = tuple.properties();
    col.reserve(1);
    col[U("Data")] = azure::storage::entity_property(
        std::vector<uint8_t>(data, data + sz));
    auto insert_op = azure::storage::table_operation::insert_or_replace_entity(tuple);
    try {
      auto result = table.execute(insert_op);
    } catch (const std::exception &e) {
      LOG_ERROR("Failed to StoreSync: %s", e.what());
    }
  }

  void StoreAsync(const std::string &tbl_name_input, std::string partition,
      const std::string &key, char * data, size_t sz, volatile RC *rc) {
            string tbl_name = ProcessTableName(tbl_name_input);

    auto table = tables_[tbl_name];
    azure::storage::table_entity tuple(U(partition), U(key));
    auto& col = tuple.properties();
    col.reserve(1);
    col[U("Data")] = azure::storage::entity_property(
        std::vector<uint8_t>(data, data + sz));
    auto insert_op = azure::storage::table_operation::insert_or_replace_entity(tuple);
    try {
      auto resp = table.execute_async(insert_op);
      resp.then([=] (azure::storage::table_result result) {
        *rc = RC::OK;
      });
    } catch (const std::exception &e) {
      LOG_ERROR("Failed to StoreSync: %s", e.what());
    }
  }

  void StoreNumericSync(const std::string &tbl_name_input, std::string partition, const std::string &key, int64_t number) {
        string tbl_name = ProcessTableName(tbl_name_input);

    auto table = tables_[tbl_name];
    azure::storage::table_entity tuple(U(partition), U(key));
    auto& col = tuple.properties();
    col.reserve(1);
    col[U("Data")] = azure::storage::entity_property(number);
    auto insert_op = azure::storage::table_operation::insert_or_replace_entity(tuple);
    try {
      auto result = table.execute(insert_op);
    } catch (const std::exception &e) {
      LOG_ERROR("Failed to StoreSync: %s", e.what());
    }
  }

  void StoreAndLoadSync(const std::string &tbl_name_input,
                        std::string store_part, const std::string &key,
                        char *data, size_t sz, std::string load_part,
                        const std::string &load_key, std::string &load_data) {
        string tbl_name = ProcessTableName(tbl_name_input);
    auto table = tables_[tbl_name];
    azure::storage::table_entity tuple(U(store_part), U(key));
    auto& col = tuple.properties();
    col.reserve(1);
    col[U("Data")] = azure::storage::entity_property(std::vector<uint8_t>(data, data + sz));
    auto insert_op = azure::storage::table_operation::insert_or_replace_entity(tuple);
    auto retrieve_operation = azure::storage::table_operation::retrieve_entity(
        U(load_part), U(load_key));
    try {
      auto result = table.execute_async(insert_op);
      auto retrieve_result = table.execute(retrieve_operation);
      azure::storage::table_entity entity = retrieve_result.entity();
      const azure::storage::table_entity::properties_type& properties = entity.properties();
      auto ret = properties.at(U("Data")).binary_value();
      load_data = std::string(ret.begin(), ret.end());
      // wait for both requests to complete.
      result.wait();
    } catch (const std::exception &e) {
      LOG_ERROR("Failed to StoreAndLoad: %s", e.what());
    }
  }

  void BatchStoreSync(const std::string &tbl_name_input, std::string part,
                       unordered_map<uint64_t, string> * map) {
    // LOG_INFO("Success call into BatchStoreSync for parition %s with size %d", part.c_str(), map->size());
    string tbl_name = ProcessTableName(tbl_name_input);
    auto table = tables_[tbl_name];
    azure::storage::table_batch_operation batch_op;
    for (auto & item : *map) {
      azure::storage::table_entity tuple(U(part), U(to_string(item.first)));
      auto& col = tuple.properties();
      col.reserve(1);
      col[U("Data")] = azure::storage::entity_property(
          std::vector<uint8_t>(item.second.begin(), item.second.end()));
      batch_op.insert_or_replace_entity(tuple);
    }
    try {
      std::vector<azure::storage::table_result> results = table.execute_batch(batch_op);
      // for (auto res: results) {
      //     M_ASSERT(res.http_status_code() == 204, "batch sync failed with error code %d", res.http_status_code());
      // }
      LOG_INFO("Success BatchStoreSync for parition %s with size %d", part.c_str(), map->size());
    // } catch (const std::exception &e) {
    } catch (const azure::storage::storage_exception &e) {

      // std::cout << "Batch size is " << map->size() << ", BatchStoreSync encounter exception:" << e.result().http_status_code() << e.what()<< std::endl;

      // LOG_ERROR("Failed to BatchStoreSync: %s", e.what());
      // std::cout << "Batch size is " << map->size() << ", BatchStoreSync encounter exception:" << e.result().http_status_code() << e.result().extended_error().message()<< std::endl;
      // e.result().
    }
    finished_async_->fetch_add(1);
  }


  // void BatchStoreSync(const std::string &tbl_name_input, std::string part,
  //                      unordered_map<uint64_t, string> * map) {
  //   // LOG_INFO("Success call into BatchStoreSync for parition %s with size %d", part.c_str(), map->size());
  //   string tbl_name = ProcessTableName(tbl_name_input);
  //   auto table = tables_[tbl_name];
  //   azure::storage::table_batch_operation batch_op;
  //   for (auto & item : *map) {
  //     azure::storage::table_entity tuple(U(part), U(to_string(item.first)));
  //     auto& col = tuple.properties();
  //     col.reserve(1);
  //     col[U("Data")] = azure::storage::entity_property(
  //         std::vector<uint8_t>(item.second.begin(), item.second.end()));
  //     batch_op.insert_or_replace_entity(tuple);
  //   }
  //   try {
  //     std::vector<azure::storage::table_result> results = table.execute_batch(batch_op);
  //     for (auto res: results) {
  //         M_ASSERT(res.http_status_code() == 204, "batch sync failed with error code %d", res.http_status_code());
  //     }
  //     LOG_INFO("Success BatchStoreSync for parition %s with size %d", part.c_str(), map->size());
  //   // } catch (const std::exception &e) {
  //   } catch (const azure::storage::storage_exception &e) {

  //     std::cout << "Batch size is " << map->size() << ", BatchStoreSync encounter exception:" << e.result().http_status_code() << e.what()<< std::endl;

  //     // LOG_ERROR("Failed to BatchStoreSync: %s", e.what());
  //     // std::cout << "Batch size is " << map->size() << ", BatchStoreSync encounter exception:" << e.result().http_status_code() << e.result().extended_error().message()<< std::endl;
  //     // e.result().
  //   }
  //   finished_async_->fetch_add(1);
  // }


static void BatchSync(std::atomic<int> * finished_async, const std::string &tbl_name_input, std::string part,
                       unordered_map<uint64_t, string> * map) {

    web::http::client::http_client client(U("https://autoscale2023cosmos.table.cosmos.azure.com:443/test"));
          // "DefaultEndpointsProtocol=https;AccountName=autoscale2023cosmos;AccountKey=MZgzUZ5IqrVz4AapZ6SzKKKcWhzxXUWOPTRMm7uSgEWcV18SwGUREIoTR2geivUdJIuoWJnMf1rIACDb8LHDdw==;TableEndpoint=https://autoscale2023cosmos.table.cosmos.azure.com:443/;", finished_async);

    web::http::http_request request(methods::POST);

    // Set headers
    request.headers().add(U("Content-Type"), U("multipart/mixed; boundary=batch_boundary"));
    request.headers().add(U("Authorization"), U("MZgzUZ5IqrVz4AapZ6SzKKKcWhzxXUWOPTRMm7uSgEWcV18SwGUREIoTR2geivUdJIuoWJnMf1rIACDb8LHDdw=="));
    // ... other necessary headers ...

    std::string body;

    for (auto & item : *map) {
        // Create two insert operations
        web::json::value entity = web::json::value::object();
        entity[U("PartitionKey")] = json::value::string(U(part));
        entity[U("RowKey")] = json::value::string(U(to_string(item.first)));
        entity[U("exampleField")] = json::value::string(U("exampleValue1"+part+to_string(item.first)));
        body += create_insert_operation(entity);
    }

    body += "--batch_boundary--\n";

    request.set_body(body);
    cout << "XXX request: " << request.to_string() << endl;

    client.request(request).then([part](http_response response) {
        if (response.status_code() == status_codes::Accepted) {
            auto body = response.extract_string().get();
            std::cout << "Success Response: " << body << "for part " << part << std::endl;
        } else {
            std::cout << "Error: " << response.status_code() << ", response: " <<  response.to_string() << std::endl;
        }
    }).wait();
    finished_async->fetch_add(1); 

}

static std::string create_insert_operation(const json::value& entity) {
    std::ostringstream operation;
    operation << "--batch_boundary\n";
    operation << "Content-Type: application/http\n";
    operation << "Content-Transfer-Encoding: binary\n\n";

    operation << "POST /test() HTTP/1.1\n";
    operation << "Content-Type: application/json\n\n";

    entity.serialize(operation);
    operation << "\n";

    return operation.str();
}






  // void BatchStoreSync(const std::string &tbl_name_input, std::string part,
  //                      unordered_map<uint64_t, string> * map) {
  //   // LOG_INFO("Success call into BatchStoreSync for parition %s with size %d", part.c_str(), map->size());
  //   string tbl_name = ProcessTableName(tbl_name_input);
  //   auto table = tables_[tbl_name];
  //   // azure::storage::table_batch_operation batch_op;
  //   auto finished_async = new std::atomic<int>(0);
  //   int query_num = map->size();
  //   try {
  //      for (auto & item : *map) {
  //        azure::storage::table_entity tuple(U(part), U(to_string(item.first)));
  //        auto& col = tuple.properties();
  //        col.reserve(1);
  //        col[U("Data")] = azure::storage::entity_property(
  //            std::vector<uint8_t>(item.second.begin(), item.second.end()));
  //        // batch_op.insert_or_replace_entity(tuple);
  //        auto insert_op = azure::storage::table_operation::insert_or_replace_entity(tuple);
  //           // std::vector<azure::storage::table_result> results = table.execute_batch(batch_op);
  //          // std::vector<azure::storage::table_result> results = table.execute_async(tuple);
  //          // auto result = table.execute_async(insert_op);
  //          // result.then([=] (azure::storage::table_result res) {
  //          //   M_ASSERT(res.http_status_code() == 204, "batch sync failed with error code %d", res.http_status_code());
  //          //   finished_async->fetch_add(1);
  //          // });
  //          auto res = table.execute(insert_op);
  //          M_ASSERT(res.http_status_code() == 204, "batch sync failed with error code %d", res.http_status_code());
  //      }
  //   } catch (const azure::storage::storage_exception &e) {
  //       std::cout << "Batch " << part.c_str() << ", BatchStoreSync encounter exception:" << e.result().http_status_code() << e.what()<< std::endl;
  //   }

  //   // while (finished_async->load() < query_num) {};
  //   LOG_INFO("Success BatchStoreSync for parition %s with size %d", part.c_str(), query_num);
  //   finished_async_->fetch_add(1);
  // }




  void BatchStoreAsync(const std::string &tbl_name_input, std::string part, unordered_map<uint64_t, string> * map) {
        string tbl_name = ProcessTableName(tbl_name_input);

    auto table = tables_[tbl_name];
    azure::storage::table_batch_operation batch_op;
    for (auto & item : *map) {
      azure::storage::table_entity tuple(U(part), U(to_string(item.first)));
      auto& col = tuple.properties();
      col.reserve(1);
      col[U("Data")] = azure::storage::entity_property(
          std::vector<uint8_t>(item.second.begin(), item.second.end()));
      batch_op.insert_or_replace_entity(tuple);
    }
    try {
      auto resp = table.execute_batch_async(batch_op);
      resp.then([=] (const std::vector<azure::storage::table_result>& results) {
        // finished_async_++;
        // for (auto res: results) {
        //   M_ASSERT(res.http_status_code() == 204, "batch sync failed with error code %d", res.http_status_code());
        // }
        // finished_async_track_++;
        finished_async_->fetch_add(1);
        LOG_INFO("Success BatchStoreAsync");
        // LOG_INFO("Success BatchStoreAsync for parition %s with size %d", part.c_str(), map->size());
      });
    } catch (const azure::storage::storage_exception& e) {
      std::cout << "Batch size is " << map->size() << ", BatchStoreAsync encounter exception:" << e.result().http_status_code() << e.result().extended_error().message()<< std::endl;
      LOG_DEBUG("Warning in BatchStoreAsync: %s", e.what());
      M_ASSERT(false, "BatchStoreAsync encounter exception: %d %s", e.result().http_status_code(), e.result().extended_error().message());
    }
  }

  void WaitForAsync(int cnt) {
   while (finished_async_->load() < cnt) {};
    finished_async_->store(0);
  }

 private:
  azure::storage::cloud_storage_account storage_acct_;
  azure::storage::cloud_table_client table_client_;
  std::unordered_map<std::string, azure::storage::cloud_table> tables_;
  // volatile int finished_async_{0};
  std::atomic<int> * finished_async_;
  // volatile int finished_async_track_{0};

  string ProcessTableName(string tblname) {
    tblname.erase(std::remove(tblname.begin(), tblname.end(), '_'), tblname.end());
    std::transform(tblname.begin(), tblname.end(), tblname.begin(), ::tolower);
    return tblname;
  }

};
}

#endif //README_MD_SRC_REMOTE_AZURETABLECLIENT_H_