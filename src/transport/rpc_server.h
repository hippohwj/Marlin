#ifndef ARBORETUM_SRC_TRANSPORT_RPC_SERVER_H_
#define ARBORETUM_SRC_TRANSPORT_RPC_SERVER_H_

#include "sundial.grpc.pb.h"
#include "sundial.pb.h"
#include <iostream>
#include <memory>
#include <string>
#include <queue>
#include <thread>
#include <grpcpp/grpcpp.h>
#include "common/BlockingQueue.h"
#include "db/ARDB.h"

using grpc::Server;
using grpc::ServerAsyncResponseWriter;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerCompletionQueue;
using grpc::Status;
using sundial_rpc::SundialRequest;
using sundial_rpc::SundialResponse;
using sundial_rpc::SundialRPC;

using namespace std;

namespace arboretum {

class SundialRPCServerImpl final : public SundialRPC::Service {
public:
    SundialRPCServerImpl(string& host_url, ARDB * db);

    class CallData {
    public:
        static unordered_map<uint32_t, BlockingQueue<void *> *> MSG_QUEUES;
        static vector<std::thread *>  WORKER_POOL;
        // CallData(SundialRPC::AsyncService* service, ServerCompletionQueue* cq,
        //          uint32_t thd_id);
        CallData(SundialRPC::AsyncService* service, ServerCompletionQueue* cq);
        // void Proceed(uint32_t thd_id);
        void Proceed();
        void Execute(uint32_t worker_id);
        uint32_t GetThdId();
        uint32_t GetNodeId();
    private:
        enum CallStatus { CREATE, PROCESS, FINISH };
        SundialRPC::AsyncService* service_;
        ServerCompletionQueue* cq_;
        ServerContext ctx_;
        SundialRequest request_;
        SundialResponse reply_;
        ServerAsyncResponseWriter<SundialResponse> responder_;
        CallStatus status_;
    };
    // ~SundialRPCServerImpl();
    void run();
    // static void HandleRpcs(SundialRPCServerImpl * s, uint32_t thd_id);
    static void HandleRpcs(SundialRPCServerImpl * s, ServerCompletionQueue * cq);
    static void HandleWorker(BlockingQueue<void *>  * req_queue, uint32_t worker_id);
    Status contactRemote(ServerContext * context, const SundialRequest* request,
                     SundialResponse* response) override;
    static void processContactRemote(ServerContext* context, const SundialRequest* request,
                     SundialResponse* response, uint32_t worker_id = 0);



private:
    //pthread_t **    _thread_pool;
    std::thread **  _thread_pool;
    // RPCServerThread ** _thread_pool;
    SundialRPC::AsyncService service_;
    std::unique_ptr<Server> server_;
    // std::unique_ptr<ServerCompletionQueue> cq_;
    std::vector<std::unique_ptr<ServerCompletionQueue>> cqs_;
    string server_url;
    ARDB * db_;
};

}

#endif //ARBORETUM_SRC_TRANSPORT_RPC_SERVER_H_