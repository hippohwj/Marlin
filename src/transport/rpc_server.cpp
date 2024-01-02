#include "rpc_server.h"
#include "common/GlobalData.h"
#include "common/OptionalGlobalData.h"
#include "db/ITxn.h"
#include "db/txn/TxnTable.h"
#include "db/ClusterManager.h"
#include <thread>
#include <random>



using namespace std;

namespace arboretum {
unordered_map<uint32_t, BlockingQueue<void *> *> SundialRPCServerImpl::CallData::MSG_QUEUES;
vector<std::thread *>  SundialRPCServerImpl::CallData::WORKER_POOL;
thread_local std::unordered_set<uint64_t> suspended_granules;

// SundialRPCServerImpl::~SundialRPCServerImpl(){
//     server_->Shutdown();
//     // Always shutdown the completion queue after the server.
//     cq_->Shutdown();
// }

SundialRPCServerImpl::SundialRPCServerImpl(string& host_url, ARDB * db) {
   server_url = host_url;
   db_ = db;
}

void
SundialRPCServerImpl::run() {
    // // find self address from ifconfig file
    // std::ifstream in(ifconfig_file);
    // string line;
    // uint32_t num_nodes = 0;
    // while (getline (in, line)) {
    //     if (line[0] == '#')
    //         continue;
    //     else {
    //         if (num_nodes == g_node_id)
    //             break;
    //         num_nodes ++;
    //     }
    // }
    // ServerBuilder builder;
    // builder.AddListeningPort(line, grpc::InsecureServerCredentials());


    // node-0:50051
    //TODO(hippo): remove this hack
    LOG_INFO("[Sundial] server url is %s", server_url.c_str());
    // g_num_rpc_server_threads = (g_grpc_server_pool_enable)? 1:8;
    uint32_t num_thds = (g_grpc_server_pool_enable)? 1:g_num_rpc_server_threads;
    ServerBuilder builder;
    builder.AddListeningPort(server_url, grpc::InsecureServerCredentials());
    // builder.AddListeningPort("localhost:50051", grpc::InsecureServerCredentials());
    builder.RegisterService(&service_);
    // cq_ = builder.AddCompletionQueue();
    for (uint32_t i = 0; i < num_thds; i++) {     
        cqs_.emplace_back(builder.AddCompletionQueue());
    }
    server_ = builder.BuildAndStart();
    // set up multiple server threads to start accepting requests
    // XXX(zhihan): only use one cq as it is thread safe. can switch to more cqs if does not scale 
    //_thread_pool = new pthread_t * [NUM_RPC_SERVER_THREADS];

    _thread_pool = new std::thread * [num_thds];
    for (uint32_t i = 0; i < num_thds; i++) {
        // _thread_pool[i] = new std::thread(HandleRpcs, this, i + 1);
        _thread_pool[i] = new std::thread(HandleRpcs, this, cqs_[i].get());
    }

    LOG_INFO("[Sundial] rpc server initialized, listening on %s", server_url.c_str());
}

void SundialRPCServerImpl::HandleWorker(BlockingQueue<void *> * req_queue, uint32_t worker_id) {
    // Spawn a new CallData instance to serve new clients.
    while (true) {
    //   if ( g_migr_albatross && worker_id < g_num_rpc_server_threads) {
    //       granule_activeworker_map.iterativeExecute([worker_id](atomic<uint32_t> * counter){
    //           counter->fetch_or(1 << worker_id);
    //       });
    //   }

      CallData * calldata = static_cast<CallData*>(req_queue->Pop());
      calldata->Execute(worker_id);
    }
}

void SundialRPCServerImpl::HandleRpcs(
    SundialRPCServerImpl * s, ServerCompletionQueue * cq) {
    arboretum::ARDB::InitRand(time(NULL));
    // Spawn a new CallData instance to serve new clients.
    new CallData(&(s->service_), cq);
    void* tag;  // uniquely identifies a request.
    bool ok;
    while (true) {
        // Block waiting to read the next event from the completion queue. The
        // event is uniquely identified by its tag, which in this case is the
        // memory address of a CallData instance.
        // The return value of Next should always be checked. This return value
        // tells us whether there is any kind of event or cq_ is shutting down.
        GPR_ASSERT(cq->Next(&tag, &ok));
        //   GPR_ASSERT(ok);
        if (ok) {
            static_cast<CallData*>(tag)->Proceed();
        }
    }
}

Status
SundialRPCServerImpl::contactRemote(ServerContext* context, const SundialRequest* request,
                     SundialResponse* response) {
    // Calls done_callback->Run() when it goes out of scope.
    // AutoClosureRunner done_runner(done_callback);
    M_ASSERT(false, "shoudn't call this function from contactRemote");
    processContactRemote(context, request, response);
    return Status::OK;
}

void
SundialRPCServerImpl::processContactRemote(ServerContext* context, const SundialRequest* request, 
        SundialResponse* response, uint32_t worker_id) {
    
    uint64_t txn_id = request->txn_id();
    // if ((int) request->request_type() <= (int) SundialResponse::DYNAMAST_MIGR_END_REQ) {
    auto tpe = (SundialResponse::RequestType) ((int)request->request_type());
    response->set_request_type(tpe);
    // }
    response->set_txn_id(txn_id);
    response->set_node_id(g_node_id);
    RC rc = OK;
    ITxn * txn;
    string data;
    // if ( g_migr_albatross && worker_id < g_num_rpc_server_threads) {
    //    granule_activeworker_map.iterativeExecute([worker_id](atomic<uint32_t> * counter){
    //        counter->fetch_or(1 << worker_id);
    //    });
    // }

    switch (request->request_type()) {
        case SundialRequest::READ_REQ: {
        //  if (cluster_manager->start_sync_done()) {

             txn = txn_table->get_txn(txn_id);
             if (txn == nullptr) {
                 txn = new (MemoryAllocator::Alloc(sizeof(ITxn)))
                     ITxn(txn_id, ardb);
                 txn_table->add_txn(txn);
             }
             // for failure case
             // only read and terminate need latch since
             // (1) read can only be concurrent with read and terminate
             // (2) read does not remove txn from txn table when getting txn
             rc = txn->process_read_request(request, response);
            //   if (rc == ABORT) {
            //       int send_node = request->coord_id();
            //       string req_str = "read req abort from node " + to_string(send_node);
            //       txn_table->remove_txn(txn, req_str);
            //       delete txn;
            //   }
             response->set_txn_id(txn_id);
        //  }
         break;
        }
        case SundialRequest::PREPARE_REQ:
        //   printf("[node-%u, txn-%lu] receive remote prepare request\n",
        //          g_node_id, txn_id);
            txn = txn_table->get_txn(txn_id);
            if (txn == nullptr) {
                // txn already cleaned up
                LOG_INFO("Cannot find txn in 2pc prepare phase for txn %u", txn_id);
                response->set_response_type(SundialResponse::PREPARED_ABORT);
                return;
            }
            txn->process_prepare_request(request, response);
            //TODO(Hippo): support for _txn_state and enable remote read-only optimization
            // if (txn->get_txn_state() != TxnManager::PREPARED) {
            //     migr txn src node waits log replaytxn_table->remove_txn(txn);
            //     delete txn;
            // }
            response->set_txn_id(txn_id);
            break;
        case SundialRequest::COMMIT_REQ:
        // //   printf("[node-%u, txn-%lu] receive remote commit request\n",
        // //          g_node_id, txn_id);
            txn = txn_table->get_txn(txn_id, true);
            if (txn == nullptr) {
                LOG_INFO("Cannot find txn in 2pc commit phase for txn %u", txn_id);
                response->set_response_type(SundialResponse::ACK);
                return;
            }
            rc = txn->process_decision_request(request, response, COMMIT);
            {
            int send_node = request->coord_id();
            string req_str_1 = "commit req from node " + to_string(send_node);
            txn_table->remove_txn(txn, req_str_1);
            }
            delete txn;
            response->set_txn_id(txn_id);
            break;
        case SundialRequest::ABORT_REQ:
        // //   printf("[node-%u txn-%lu] receive remote abort request\n",
        // //          g_node_id, txn_id);
            txn = txn_table->get_txn(txn_id, true);
            if (txn == nullptr) {
                response->set_response_type(SundialResponse::ACK);
                return;
            }
            rc = txn->process_decision_request(request, response, ABORT);
            {
            int send_node = request->coord_id();
            string req_str = "abort req from node " + to_string(send_node);
            txn_table->remove_txn(txn, req_str);
            }
            delete txn;
            response->set_txn_id(txn_id);
            break;
        case SundialRequest::MEM_SYNC_REQ: {
            for (int i = 0; i < request->mem_sync_reqs_size(); i++) {
                auto sync_req = request->mem_sync_reqs(i);
                granule_tbl_cache->insert(sync_req.key(), sync_req.value());
                LOG_INFO("XXX Albatross recieve membership update rpc to client server for granule %d to node %d", sync_req.key(), sync_req.value());
            }
            response->set_response_type(SundialResponse::ACK);
            response->set_txn_id(txn_id);
            break;
        }
        case SundialRequest::SYS_REQ: {
          cluster_manager->receive_sync_request();
          break;
        }
        case SundialRequest::MIGR_END_REQ: {
          cluster_manager->receive_migr_thread_finished();
          break;
        }
        case SundialRequest::CACHE_PREHEAT_REQ: {
         for (int i = 0; i < request->cache_preheat_reqs_size(); i++) {
           auto preheat_req = request->cache_preheat_reqs(i);
           auto granule_id = preheat_req.granule_id();
           // add granule id to a global map so that replay thread can be notified
           unordered_set<uint64_t>* emptyset = new std::unordered_set<uint64_t>();
           migr_preheat_updates_tracker->insert(granule_id, emptyset);
         }
         response->set_response_type(SundialResponse::ACK);
         response->set_txn_id(txn_id);
         break;
        }
        case SundialRequest::DYNAMAST_MIGR_ST_REQ: {
          {
            std::lock_guard<std::mutex> lock(mm_mutex);        
            mm_count++;
          }
          cout << "XXX Dynamast start to wait all workers to stop" << endl;
          std::unique_lock<std::mutex> lock(an_mutex);
          active_thread.wait(lock, []{return (an_count == 0);});
          lock.unlock();
          cout << "XXX Dynamast all workers stopped!!!" << endl;
          response->set_response_type(SundialResponse::ACK);
          response->set_txn_id(txn_id);
          break;
        }
        case SundialRequest::DYNAMAST_MIGR_END_REQ: {
          {
            std::lock_guard<std::mutex> lock(mm_mutex);        
            mm_count--;
          }
          migr_mark.notify_all();
          response->set_response_type(SundialResponse::ACK);
          response->set_txn_id(txn_id);
          break;
        }

        default:
         assert(false);
    }
    // the transaction handles the RPC call
    if ( g_migr_albatross && worker_id < g_num_rpc_server_threads) {
       granule_activeworker_map.iterativeExecute([worker_id](atomic<uint32_t> * counter){
           counter->fetch_or(1 << worker_id);
       });
    }
}

SundialRPCServerImpl::CallData::CallData(SundialRPC::AsyncService* service,  ServerCompletionQueue* cq) : service_(service), cq_(cq), responder_(&ctx_), status_(CREATE) {
    Proceed();
}

void
SundialRPCServerImpl::CallData::Execute(uint32_t worker_id) {
    // LOG_INFO("XXX: start to process CallData request: worker id is (nodeid: %d, thdid: %d), request type is %d",  request_.node_id(), request_.thd_id(), (int)request_.request_type());
    processContactRemote(&ctx_, &request_ , &reply_, worker_id);
    status_ = FINISH;
    responder_.Finish(reply_, Status::OK, this); 
}

void
SundialRPCServerImpl::CallData::Proceed() {
    if (status_ == CREATE) {
        //ctx_.AsyncNotifyWhenDone(this);
        service_->RequestcontactRemote(&ctx_, &request_, &responder_, cq_,
                                       cq_, this);
        status_ = PROCESS;
    } else if (status_ == PROCESS) {
        // LOG_INFO("XXX: start to process CallData request: thd_id is %d, request type is %d",  request_.thd_id(), (int)request_.request_type());
        // Spawn a new CallData instance to serve new clients while processing
        new CallData(service_, cq_);
        if (g_grpc_server_pool_enable) {
          uint32_t thd_id = request_.thd_id();
          bool is_migr_client = thd_id > g_num_worker_threads; 
          uint32_t worker_id;
          if (!is_migr_client) {
            uint32_t client_id = request_.node_id() * g_num_worker_threads + (request_.thd_id()-1); 
            worker_id = client_id % g_num_rpc_server_threads; 
          } else {
            uint32_t client_id = request_.node_id() * g_migr_threads_num + (request_.thd_id() - g_num_worker_threads -1); 
            worker_id = client_id + g_num_rpc_server_threads;
          }
          
        //   uint32_t client_id = request_.node_id() * g_num_worker_threads + (request_.thd_id()-1);
        // //   uint32_t worker_id = (request_.node_id() == g_scale_node_id)? (g_num_rpc_server_threads + request_.thd_id()): ARDB::RandUint64(0, g_num_rpc_server_threads - 1);
        //   uint32_t worker_thd_num = (g_node_id == g_scale_node_id)? (g_num_worker_threads + g_num_rpc_server_threads) : g_num_rpc_server_threads; 
        //   uint32_t worker_id = (request_.node_id() == g_scale_node_id)? (g_num_rpc_server_threads + request_.thd_id()): (client_id % worker_thd_num);
          if (MSG_QUEUES.find(worker_id) !=
              MSG_QUEUES.end()) {
              MSG_QUEUES[worker_id]->Push((void *)this);
          } else {
              BlockingQueue<void *> * queue =
                  new BlockingQueue<void *>();
              queue->Push((void *)this);
              MSG_QUEUES[worker_id] = queue;
              auto worker = new std::thread(HandleWorker, queue, worker_id);
              WORKER_POOL.push_back(worker);
          }
        } else {
            M_ASSERT(false, "Unexpected branch");
        //   reply_.set_thd_id(thd_id);
            // Execute();
        }
    } else  {
        GPR_ASSERT(status_ == FINISH);
        delete this;
    }
}


uint32_t SundialRPCServerImpl::CallData::GetThdId() {
    return request_.thd_id();
}

uint32_t SundialRPCServerImpl::CallData::GetNodeId() {
    return request_.node_id();
}

}