#include "gtest/gtest.h"
#include "remote/AzureTableClient.h"
#include "remote/AzureLogClient.h"
#include <future>
#include <thread>
#include <cpprest/http_client.h>
#include <cpprest/filestream.h>
#include <cpprest/json.h>
#include <iostream>
#include <string>

using namespace utility;                    // Common utilities like string conversions
using namespace web;                        // Common features like URIs
using namespace web::http;                  // Common HTTP functionality
using namespace web::http::client;          // HTTP client features
using namespace concurrency::streams;       // Asynchronous streams

using namespace arboretum;

TEST(AzureTableTest, BasicOperations) {
  cout<< "new compiles"<< endl;
   auto finished_async = new std::atomic<int>(0);
  auto client = new AzureTableClient(
      "DefaultEndpointsProtocol=https;AccountName=autoscale2023cosmos;AccountKey=MZgzUZ5IqrVz4AapZ6SzKKKcWhzxXUWOPTRMm7uSgEWcV18SwGUREIoTR2geivUdJIuoWJnMf1rIACDb8LHDdw==;TableEndpoint=https://autoscale2023cosmos.table.cosmos.azure.com:443/;", finished_async);
    //   "DefaultEndpointsProtocol=https;AccountName=autoscale2023;AccountKey=XiBdBjYXhTuRn4LZoy5frE7c0LSTngErnTSp5WFSJPKI+BAaCBsGUfS2jXorkXoprJ5DiB8KJy4H+AStqDiCQw==;EndpointSuffix=core.windows.net");
    //   "DefaultEndpointsProtocol=https;AccountName=bonsai-unlimited;AccountKey=XiBdBjYXhTuRn4LZoy5frE7c0LSTngErnTSp5WFSJPKI+BAaCBsGUfS2jXorkXoprJ5DiB8KJy4H+AStqDiCQw==;TableEndpoint=https://bonsai-unlimited.table.cosmos.azure.com:443/;");
  client->DeleteTableIfExist("test");
  client->CreateTableIfNotExist("test");
  // std::string value = "world";
  // client->StoreSync("test", "0", "hello", (char *) value.c_str(), value.size());
  // LOG_DEBUG("store key=hello, value=world");
  // std::string data;
  // client->LoadSync("test", "0", "hello", data);
  // LOG_DEBUG("load key=hello: value=%s", data.c_str());
  // EXPECT_EQ(data, value);
  // std::unordered_map<uint64_t, std::string> map;
  unordered_map<int, unordered_map<uint64_t, std::string> * > map;
  int batch_size = 60; 
  vector<std::future<void>> futures;
  vector<AzureTableClient *> clients;
  clients.resize(batch_size);
  for (int j = 0; j < batch_size; j++) {
      clients[j] = new AzureTableClient(
      "DefaultEndpointsProtocol=https;AccountName=autoscale2023cosmos;AccountKey=MZgzUZ5IqrVz4AapZ6SzKKKcWhzxXUWOPTRMm7uSgEWcV18SwGUREIoTR2geivUdJIuoWJnMf1rIACDb8LHDdw==;TableEndpoint=https://autoscale2023cosmos.table.cosmos.azure.com:443/;", finished_async);
      clients[j]->CreateTableIfNotExist("test");

    unordered_map<uint64_t, std::string> * inner_map = new unordered_map<uint64_t, std::string>(); 

    for (int i = 0; i < 20; i++) {
      inner_map->insert(std::make_pair(i, std::to_string(i * j)));
    }
    map[j]= inner_map;
  }
  auto starttime = std::chrono::high_resolution_clock::now(); 

  for (int j = 0; j < batch_size; j++) {
    // futures.push_back(std::async(std::launch::async, 
    //                                       &AzureTableClient::BatchStoreSync, 
    //                                       clients[j], 
    //                                       "test", std::to_string(j), map[j]));


    
    futures.push_back(std::async(std::launch::async, 
                                          AzureTableClient::BatchSync, 
                                          finished_async,
                                          "test", std::to_string(j), map[j]));

    // client->BatchStoreSync("test", std::to_string(j), map[j]);

  }
  auto req_endtime = std::chrono::high_resolution_clock::now(); 

  // client->WaitForAsync(batch_size);
  // for (int j = 0; j < batch_size; j++) {
  //   futures[j].get();
  // }

  while (finished_async->load() < batch_size) {};
  finished_async->store(0);

  // auto endtime = GetSystemClock();
  auto endtime = std::chrono::high_resolution_clock::now(); 
  LOG_INFO("XXX batch for 900 entities finish with %d ms(sen reqs %d ms; wait for res %d ms)", std::chrono::duration_cast<std::chrono::milliseconds>(
                            endtime - starttime)
                            .count(), std::chrono::duration_cast<std::chrono::milliseconds>(
                            req_endtime - starttime)
                            .count(), std::chrono::duration_cast<std::chrono::milliseconds>(
                            endtime - req_endtime)
                            .count());
  LOG_DEBUG("completed batch store request");
  for (int j = 0; j < batch_size; j++) {
      map[j]->clear();
  }
  // for (int i = 0; i < 10; i++) {
  //   std::string resp;
  //   client->LoadSync("test", "1", std::to_string(i), resp);
  //   EXPECT_EQ(resp, std::to_string(i));
  // }
  // LOG_DEBUG("verified batch stored tuples");
  // for (int i = 0; i < 5; i++) {
  //   std::string resp;
  //   char val[10];
  //   sprintf(val, "store-%d", i);
  //   client->StoreAndLoadSync("test", "1", std::to_string(i), val, 10,
  //                            "1", std::to_string(i * 2 + 1), resp);
  //   EXPECT_EQ(resp, std::to_string(i * 2 + 1));
  // }
  // client->DeleteTable("test");
}





// void BatchSync(std::atomic<int> * finished_async, const std::string &tbl_name_input, std::string part,
//                        unordered_map<uint64_t, string> * map) {

//     http_client client(U("https://autoscale2023cosmos.table.cosmos.azure.com:443/test"));
//           // "DefaultEndpointsProtocol=https;AccountName=autoscale2023cosmos;AccountKey=MZgzUZ5IqrVz4AapZ6SzKKKcWhzxXUWOPTRMm7uSgEWcV18SwGUREIoTR2geivUdJIuoWJnMf1rIACDb8LHDdw==;TableEndpoint=https://autoscale2023cosmos.table.cosmos.azure.com:443/;", finished_async);

//     http_request request(methods::POST);

//     // Set headers
//     request.headers().add(U("Content-Type"), U("multipart/mixed; boundary=batch_boundary"));
//     request.headers().add(U("Authorization"), U("MZgzUZ5IqrVz4AapZ6SzKKKcWhzxXUWOPTRMm7uSgEWcV18SwGUREIoTR2geivUdJIuoWJnMf1rIACDb8LHDdw=="));
//     // ... other necessary headers ...

//     std::string body;

//     for (auto & item : *map) {
//         // Create two insert operations
//         json::value entity = json::value::object();
//         entity[U("PartitionKey")] = json::value::string(U(part));
//         entity[U("RowKey")] = json::value::string(U(to_string(item.first)));
//         entity[U("exampleField")] = json::value::string(U("exampleValue1"+part+to_string(item.first)));
//         body += create_insert_operation(entity);
//     }

//     body += "--batch_boundary--\n";

//     request.set_body(body);

//     client.request(request).then([part](http_response response) {
//         if (response.status_code() == status_codes::Accepted) {
//             auto body = response.extract_string().get();
//             std::cout << "Success Response: " << body << "for part " << part << std::endl;
//         } else {
//             std::cout << "Error: " << response.status_code() << ", response: " <<  response.to_string() << std::endl;
//         }
//     }).wait();
//     finished_async->fetch_add(1); 

// }

// TEST(AzureTableTest, RandomTable) {
//      auto finished_async = new std::atomic<int>(0);

//   const string str =   "DefaultEndpointsProtocol=https;AccountName=autoscale2023cosmos;AccountKey=MZgzUZ5IqrVz4AapZ6SzKKKcWhzxXUWOPTRMm7uSgEWcV18SwGUREIoTR2geivUdJIuoWJnMf1rIACDb8LHDdw==;TableEndpoint=https://autoscale2023cosmos.table.cosmos.azure.com:443/;";

//   const utility::string_t& storage_connection_string(U(str));

//   // Retrieve the storage account from the connection string.
// azure::storage::cloud_storage_account storage_account = azure::storage::cloud_storage_account::parse(storage_connection_string);

// // Create the table client.
// azure::storage::cloud_table_client table_client = storage_account.create_cloud_table_client();

// // Create a cloud table object for the table.
// azure::storage::cloud_table table = table_client.get_table_reference(U("people"));
//    try {
//      table.delete_table_if_exists();
//      table.create_if_not_exists();
//       LOG_DEBUG("Created table people");
//     } catch (const std::exception &e) {
//       std::wcout << U("Error: ") << e.what() << std::endl;
//       LOG_ERROR("failed to create table and exit.");
//     }

// // Define a batch operation.
// azure::storage::table_batch_operation batch_operation;

// // Create a customer entity and add it to the table.
// azure::storage::table_entity customer1(U("Smith"), U("Jeff"));

// azure::storage::table_entity::properties_type& properties1 = customer1.properties();
// properties1.reserve(2);
// properties1[U("Email")] = azure::storage::entity_property(U("Jeff@contoso.com"));
// properties1[U("Phone")] = azure::storage::entity_property(U("425-555-0104"));

// // Create another customer entity and add it to the table.
// azure::storage::table_entity customer2(U("Smith"), U("Ben"));

// azure::storage::table_entity::properties_type& properties2 = customer2.properties();
// properties2.reserve(2);
// properties2[U("Email")] = azure::storage::entity_property(U("Ben@contoso.com"));
// properties2[U("Phone")] = azure::storage::entity_property(U("425-555-0102"));

// // Create a third customer entity to add to the table.
// azure::storage::table_entity customer3(U("Smith"), U("Denise"));

// azure::storage::table_entity::properties_type& properties3 = customer3.properties();
// properties3.reserve(2);
// properties3[U("Email")] = azure::storage::entity_property(U("Denise@contoso.com"));
// properties3[U("Phone")] = azure::storage::entity_property(U("425-555-0103"));

// // Add customer entities to the batch insert operation.
// batch_operation.insert_or_replace_entity(customer1);
// batch_operation.insert_or_replace_entity(customer2);
// batch_operation.insert_or_replace_entity(customer3);


// auto insert_op1 = azure::storage::table_operation::insert_or_replace_entity(customer1);
// auto insert_op2 = azure::storage::table_operation::insert_or_replace_entity(customer2);
// auto insert_op3 = azure::storage::table_operation::insert_or_replace_entity(customer3);
// try {
//   auto result1 = table.execute(insert_op1);
//   auto result2 = table.execute(insert_op2);
//   auto result3 = table.execute(insert_op3);
// } catch (const std::exception &e) {
//   LOG_ERROR("Failed to StoreSync: %s", e.what());
// }


// // Execute the batch operation.
// std::vector<azure::storage::table_result> results = table.execute_batch(batch_operation);
// LOG_DEBUG("Batch insert success");
// }


// TEST(AzureTableTest, PartitionScan) {
//   auto client = new AzureTableClient(
//       "DefaultEndpointsProtocol=https;AccountName=autoscale2023;AccountKey=XiBdBjYXhTuRn4LZoy5frE7c0LSTngErnTSp5WFSJPKI+BAaCBsGUfS2jXorkXoprJ5DiB8KJy4H+AStqDiCQw==;EndpointSuffix=core.windows.net");
//     //   "DefaultEndpointsProtocol=https;AccountName=bonsai-unlimited;AccountKey=XiBdBjYXhTuRn4LZoy5frE7c0LSTngErnTSp5WFSJPKI+BAaCBsGUfS2jXorkXoprJ5DiB8KJy4H+AStqDiCQw==;TableEndpoint=https://bonsai-unlimited.table.cosmos.azure.com:443/;");
//   client->DeleteTableIfExist("testscan");
//   client->CreateTableIfNotExist("testscan");
//   std::unordered_map<uint64_t, std::string> map;
//   int row_num = 3*1024;
//   int batch_size = 100; 
//   int batch_num = 0;
//   for (int j = 0; j < row_num; j++) {
//     map.insert(std::make_pair(j, std::to_string(j)));
//     if (((j + 1) % batch_size) == 0) {
//       client->BatchStoreAsync("testscan", "1", &map);
//       map.clear();
//       batch_num++;
//       if ((batch_num % 8) == 0) {
//           client->WaitForAsync(8);
//       }
//     }
//   }

//   if (!map.empty()) {
//       client->BatchStoreAsync("testscan", "1", &map);
//       map.clear();
//       batch_num++;
//       if ((batch_num % 8) == 0) {
//           client->WaitForAsync(8);
//       } 
//   } 
//   client->WaitForAsync(batch_num % 8);
//   // for (int j = 0; j < row_num; j++) {
//   //   map.insert(std::make_pair(j, std::to_string(j)));
//   //   if (((j + 1) % batch_size) == 0) {
//   //     client->BatchStoreSync("testscan", "1", &map);
//   //     map.clear();
//   //     batch_num++;
//   //     if (batch_num % 10 == 0) {
//   //       LOG_INFO("XXX insert %d batches", batch_num);
//   //     }
//   //   }
//   // }

//   // if (!map.empty()) {
//   //     client->BatchStoreSync("testscan", "1", &map);
//   //     map.clear();
//   //     batch_num++;
//   // } 
//   LOG_DEBUG("completed batch store request");
//   vector<string> data;
//   size_t count = 0;
//   client->ScanSync("testscan", "1", [&count](string& key, string& loaded_data) {
//         // LOG_INFO("get data for partition 1: %s", (*iter).c_str());
//     EXPECT_EQ(loaded_data, key);
//     // EXPECT_EQ(loaded_data, std::to_string(count));
//     count++;
//   });
//   EXPECT_EQ(count, row_num);
//   LOG_DEBUG("verified batch stored tuples");
//   client->DeleteTable("testscan");
// }

// TEST(AzureTableTest, NumericTest) {
//   auto client = new AzureTableClient(
//           "DefaultEndpointsProtocol=https;AccountName=autoscale2023;AccountKey=XiBdBjYXhTuRn4LZoy5frE7c0LSTngErnTSp5WFSJPKI+BAaCBsGUfS2jXorkXoprJ5DiB8KJy4H+AStqDiCQw==;EndpointSuffix=core.windows.net");
//     //   "DefaultEndpointsProtocol=https;AccountName=bonsai-unlimited;AccountKey=spGVL6WLk4jSsbLYmW3SO1t3MUnp2jFipKDNKDjfB9NESviQRhaw3JHuCGGZ9ZaqdrHSBxeaBCrJACDbjqbjvQ==;TableEndpoint=https://bonsai-unlimited.table.cosmos.azure.com:443/;");
//   client->CreateTableIfNotExist("obj10g0");
//   client->StoreNumericSync("obj10g0", "metadata", "row_cnt_", 10240000);
//   auto ret = client->LoadNumericSync("obj10g0", "metadata", "row_cnt_");
//   EXPECT_EQ(ret, 10240000);
//   client->DeleteTable("obj10g0");
// }

// TEST(AzureTableTest, LogTest) {
//   auto client = new AzureLogClient(
//           "DefaultEndpointsProtocol=https;AccountName=autoscale2023;AccountKey=XiBdBjYXhTuRn4LZoy5frE7c0LSTngErnTSp5WFSJPKI+BAaCBsGUfS2jXorkXoprJ5DiB8KJy4H+AStqDiCQw==;EndpointSuffix=core.windows.net", 0);
//     //   "DefaultEndpointsProtocol=https;AccountName=bonsai-unlimited;AccountKey=spGVL6WLk4jSsbLYmW3SO1t3MUnp2jFipKDNKDjfB9NESviQRhaw3JHuCGGZ9ZaqdrHSBxeaBCrJACDbjqbjvQ==;TableEndpoint=https://bonsai-unlimited.table.cosmos.azure.com:443/;");
//   std::string str="hello llalal new";
//   LogEntry * entry = NEW(LogEntry)(str, 0, 0);
//   vector<uint8_t> buffer;
//   entry->Serialize(&buffer);
//   string lsn = "";
//   client->GroupLogSync(1,&buffer, lsn);
//   // client->DeleteTable("obj10g0");
// }


int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
    ::testing::GTEST_FLAG(filter) = "AzureTableTest.*";
//   ::testing::GTEST_FLAG(filter) = "AzureTableTest.PartitionScan";
  return RUN_ALL_TESTS();
}