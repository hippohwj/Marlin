#include <thread>
#include <algorithm>
#include "gtest/gtest.h"
#include "transport/rpc_client.h"
#include "transport/rpc_server.h"

using namespace std;
using namespace arboretum;
auto server_num = 3;
auto servers = new SundialRPCServerImpl * [server_num];

void start_rpc_server(uint32_t server_id) {
   servers[server_id]->run();
}


TEST(RPCTest, SingleNodeTest) {

  auto rpc_client = new SundialRPCClient();
  // Three Fake Servers in a single node
  char * nodes[server_num] = {"localhost:50051", "localhost:50052", "localhost:50053"};
  for (uint32_t i = 0; i < server_num; i++) {
    servers[i] = new SundialRPCServerImpl(nodes[i]);
  }
  auto _thread_pool = new std::thread * [server_num];
  for (uint32_t i = 0; i < server_num; i++) {
     _thread_pool[i] = new std::thread(start_rpc_server, i);
  }
  //wait for servers to start
  sleep(3);

  uint64_t txn_id = 0;
  // send reqs to all nodes
  for (uint32_t i = 0; i < server_num; i++) {
    SundialRequest request;
    SundialResponse response;
    auto key = i;
    request.set_txn_id(txn_id);
    request.set_node_id(i);
    request.set_request_type( SundialRequest::READ_REQ );
    SundialRequest::ReadRequest * read_request = request.add_read_requests();
    read_request->set_key(i);
    read_request->set_index_id(0);
    read_request->set_access_type(0);
    rpc_client->sendRequest(i, request, response);
    // response
    // parse response
    EXPECT_EQ(1, response.tuple_data_size());
    auto data_size = response.tuple_data(0).size();
    auto data = new char[data_size];
    memcpy(data, response.tuple_data(0).data().c_str(), data_size);
    char *expected_data = (char*)malloc(100 * sizeof(char));
    sprintf(expected_data, "%u-%u", txn_id, key);
    cout<< "received " << data << endl;
    EXPECT_TRUE(strcmp(data, expected_data) == 0);
  } 

  for (uint32_t i = 0; i < server_num; i++) {
     _thread_pool[i]->join(); 
  } 
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  ::testing::GTEST_FLAG(filter) = "RPCTest.*";
  return RUN_ALL_TESTS();
}

