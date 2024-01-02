// #include "system/global.h"
#include "rpc_client.h"
#include "common/Common.h"
#include "common/GlobalData.h"
#include "db/txn/TxnTable.h"
#include "db/ClusterManager.h"

using namespace std;

namespace arboretum {

SundialRPCClient::SundialRPCClient() {
    // // Fake nodes number
    // g_num_nodes = 3;
    _servers = new SundialRPCClientStub * [g_num_nodes];
    _threads = new std::thread * [g_num_nodes];
    int port = 50051;

    // get server names
    uint32_t node_id = 0;
    while (node_id < g_num_nodes) {
        // if (node_id == g_node_id) {
        //     node_id++;
        //     continue;
        // }
        string url = g_cluster_members[node_id] + ":" + to_string(port);
        LOG_INFO("[RPC] init rpc client to - %u at %s", node_id, url.c_str());
        _servers[node_id] = new SundialRPCClientStub(
            grpc::CreateChannel(url, grpc::InsecureChannelCredentials()));
        // spawn a reader thread for each server to indefinitely read completion
        // queue
        _threads[node_id] = new std::thread(AsyncCompleteRpc, this, node_id);
        node_id++;
    }
    LOG_INFO("[RPC] rpc client is initialized!");
}

// SundialRPCClient::SundialRPCClient() {
//     _servers = new SundialRPCClientStub * [g_num_nodes];
//     _threads = new std::thread * [g_num_nodes];
//     // get server names
//     std::ifstream in(ifconfig_file);
//     string line;
//     uint32_t node_id = 0;
//     while ( node_id  < g_num_nodes && getline(in, line) )
//     {
//         if (line[0] == '#')
//             continue;
//         else if ((line[0] == '=' && line[1] == 'l') || node_id == g_num_nodes)
//             break;
//         else {
//             string url = line;
//             if (node_id == g_node_id) {
//               node_id ++;
//               continue;
//             }
//             _servers[node_id] = new SundialRPCClientStub(grpc::CreateChannel(url,
//                 grpc::InsecureChannelCredentials()));
//             // spawn a reader thread for each server to indefinitely read completion
//             // queue
//             _threads[node_id] = new std::thread(AsyncCompleteRpc,
//                                                         this, node_id);
//             LOG_INFO("[RPC] init rpc client to - %u at %s", node_id, url.c_str());
//             node_id ++;
//         }
//     }
//     LOG_INFO("[RPC] rpc client is initialized!");
// }

void
SundialRPCClient::AsyncCompleteRpc(SundialRPCClient * s, uint64_t
node_id) {
    void* got_tag;
    bool ok = false;
    // Block until the next result is available in the completion queue "cq".
    while (true) {
        s->_servers[node_id]->cq_.Next(&got_tag, &ok);
        // The tag in this example is the memory location of the call object
        auto call = static_cast<AsyncClientCall*>(got_tag);
        if (!call->status.ok()) {
            printf("[REQ] client rec response fail from node (%d): (%d) %s\n", node_id, 
                   call->status.error_code(), call->status.error_message().c_str());
            // assert(false);
            continue;
        }
        // handle return value for non-system response
        assert(call->reply->response_type() != SundialResponse::SYS_RESP);
        s->sendRequestDone(call->request, call->reply);
        // Once we're complete, deallocate the call object.
        delete call;
    }

}

RC
SundialRPCClient::sendRequest(uint64_t node_id, SundialRequest &request,
    SundialResponse &response, bool is_storage) {
    ClientContext context;
    request.set_request_time(GetSystemClock());
    Status status;
    // LOG_INFO("rpc send req to node %u", node_id);
    status = _servers[node_id]->contactRemote(&context, request, &response);
    if (!status.ok()) {
        printf("[REQ] client sendRequest fail: (%d) %s\n",
               status.error_code(), status.error_message().c_str());
        fflush(stdout);
        assert(false);
    }
    // LOG_INFO("RPC client send request to node %u successfully", node_id);
	return OK;
}


// RC
// SundialRPCClient::sendRequest(uint64_t node_id, SundialRequest &request,
//     SundialResponse &response, bool is_storage) {
//     if (!glob_manager->active && (request.request_type() !=
//     SundialRequest::SYS_REQ))
//         return FAIL;
//     ClientContext context;
//     request.set_request_time(GetSystemClock());
//     Status status;
//     status = _servers[node_id]->contactRemote(&context, request, &response);
//     if (!status.ok()) {
//         printf("[REQ] client sendRequest fail: (%d) %s\n",
//                status.error_code(), status.error_message().c_str());
//         assert(false);
//     }
//     uint64_t latency = GetSystemClock() - request.request_time();
//     glob_stats->_stats[GET_THD_ID]->_req_msg_avg_latency[response
//     .response_type()] += latency;
//     if (latency > glob_stats->_stats[GET_THD_ID]->_req_msg_max_latency
//     [response.response_type()]) {
//         glob_stats->_stats[GET_THD_ID]->_req_msg_max_latency
//         [response.response_type()] = latency;
//     }
//     if (latency < glob_stats->_stats[GET_THD_ID]->_req_msg_min_latency
//     [response.response_type()]) {
//         glob_stats->_stats[GET_THD_ID]->_req_msg_min_latency
//         [response.response_type()] = latency;
//     }
//     glob_stats->_stats[GET_THD_ID]->_resp_msg_count[ response.response_type() ] ++;
//     glob_stats->_stats[GET_THD_ID]->_resp_msg_size[ response.response_type() ] += response.SpaceUsedLong();
// 	return OK;
// }


RC
SundialRPCClient::sendRequestAsync(ITxn * txn, uint64_t node_id,
                                   SundialRequest &request, SundialResponse
                                   &response)
{
    //TODO(Hippo): enable this FAIL check
    //     if (!glob_manager->active && (request.request_type() !=
    // SundialRequest::TERMINATE_REQ))
    //     return FAIL;
    // LOG_INFO("[node-%u, txn-%lu] send async request-%d to node %u", g_node_id, request
    //         .txn_id(), request.request_type(), node_id);
    // assert( node_id != g_node_id);
    // call object to store rpc data
    AsyncClientCall* call = new AsyncClientCall;;
    request.set_request_time(GetSystemClock());
    //TODO(Hippo): remove this hack and set currect thread id
    // request.set_thread_id(0);
    //TODO(Hippo): add stats for req msgs
    call->response_reader = _servers[node_id]->stub_->PrepareAsynccontactRemote(
            &call->context, request, &_servers[node_id]->cq_);
    // // StartCall initiates the RPC call
    call->request = &request;
    call->response_reader->StartCall();
    call->reply = &response;
    call->response_reader->Finish(call->reply, &(call->status), (void*)call);
	return OK;
}

void SundialRPCClient::sendRequestDone(SundialRequest *request,
                                       SundialResponse *response) {
        // RACE CONDITION (solved): should assign thd id to server thread
        uint64_t thread_id = request->thread_id();
        // uint64_t latency = get_sys_clock() - request->request_time();
        // glob_stats->_stats[thread_id]->_req_msg_avg_latency[response->response_type()]
        // += latency; if (latency >
        // glob_stats->_stats[thread_id]->_req_msg_max_latency
        // [response->response_type()]) {
        //     glob_stats->_stats[thread_id]->_req_msg_max_latency
        //     [response->response_type()] = latency;
        // }
        // if (latency < glob_stats->_stats[thread_id]->_req_msg_min_latency
        // [response->response_type()]) {
        //     glob_stats->_stats[thread_id]->_req_msg_min_latency
        //     [response->response_type()] = latency;
        // }
        // glob_stats->_stats[thread_id]->_resp_msg_count[
        // response->response_type() ]++;
        // glob_stats->_stats[thread_id]->_resp_msg_size[
        // response->response_type() ] += response->SpaceUsedLong();

        uint64_t txn_id = request->txn_id();
        // printf(
        //     "[node-%u, txn-%lu] receive remote reply-%d (response_type=%d)\n",
        //     g_node_id, txn_id, response->request_type(),
        //     response->response_type());
        ITxn *txn;
        switch (response->request_type()) {
        case SundialResponse::READ_REQ:
            // if (cluster_manager->start_sync_done()) {
                txn = txn_table->get_txn(txn_id);
                txn->rpc_semaphore_->decr();
                // LOG_INFO("decrease rpc semaphore for txn id %u", txn_id);
            // }
            break;
        case SundialResponse::PREPARE_REQ:
            txn = txn_table->get_txn(txn_id);
            txn->handle_prepare_resp(response->response_type(),
                                     response->node_id());
            txn->rpc_semaphore_->decr();
            break;
        case SundialResponse::COMMIT_REQ:
            txn = txn_table->get_txn(txn_id);
            txn->rpc_semaphore_->decr();
            break;
        case SundialResponse::ABORT_REQ:
            txn = txn_table->get_txn(txn_id);
            txn->rpc_semaphore_->decr();
            break;
        case SundialResponse::SYS_REQ:
            ((SemaphoreSync *)request->semaphore())->decr();
            break;
        case SundialResponse::CACHE_PREHEAT_REQ:
            txn = txn_table->get_txn(txn_id);
            txn->rpc_semaphore_->decr();
            break;
        case SundialResponse::SUSPEND:
            txn = txn_table->get_txn(txn_id);
            txn->rpc_semaphore_->decr();
            break;
        case SundialResponse::DYNAMAST_MIGR_ST_REQ:
            cout<<"XXX Dynamast receive migr st response";
            txn = txn_table->get_txn(txn_id);
            txn->rpc_semaphore_->decr();
            break;
        case SundialResponse::DYNAMAST_MIGR_END_REQ:
            txn = txn_table->get_txn(txn_id);
            txn->rpc_semaphore_->decr();
            break;
        case SundialResponse::MEM_SYNC_REQ:
            txn = txn_table->get_txn(txn_id);
            txn->rpc_semaphore_->decr();
            break;
        
        default:
            cout<<"XXX Dynamast Unexpected response " << response->request_type() << endl;
            break;
        }
}

}
