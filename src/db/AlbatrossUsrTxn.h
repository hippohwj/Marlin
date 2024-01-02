#ifndef ARBORETUM_SRC_DB_ALBATROSSUSRTXN_H_
#define ARBORETUM_SRC_DB_ALBATROSSUSRTXN_H_

#include "ITxn.h"

using namespace std;

namespace arboretum {

class AlbatrossUsrTxn: public ITxn {
    public:
        AlbatrossUsrTxn(OID txn_id, ARDB *db): ITxn(txn_id, db){};
        RC Execute(YCSBQuery *query) override;
        RC CommitP1() override;
        RC CommitP2(RC rc) override;
        // void handle_prepare_resp(SundialResponse::ResponseType response,
        //                      uint32_t node_id) override;

    protected:
        YCSBQuery *query_;
        unordered_set<uint32_t> remote_nodes_;

    
    private:
       void GenNodeReqMap(vector<YCSBRequest *>& requests, unordered_map<uint64_t, vector<YCSBRequest *>>& map);
       RC SendReadRequest(unordered_map<uint64_t, vector<YCSBRequest *>> * node_requests_map, OID tbl_id, std::map<uint32_t, RemoteNodeInfo *>& remote_nodes_involved);
       RC ReceiveReadResponse(vector<YCSBRequest *> &remaining_reqs, std::map<uint32_t, RemoteNodeInfo *>& remote_nodes_involved);


       RC SendPrepareRequest(unordered_map<uint64_t, vector<YCSBRequest *>> * node_requests_map, std::map<uint32_t, RemoteNodeInfo *>& remote_nodes_involved);
       RC ReceivePrepareResponse(unordered_map<uint64_t, vector<YCSBRequest *>> & node_requests_map, vector<YCSBRequest *>&remaining_reqs, std::map<uint32_t, RemoteNodeInfo *>& remote_nodes_involved);
       
       
       RC SendCommitRequest(RC rc,unordered_map<uint64_t, vector<YCSBRequest *>> * node_requests_map, std::map<uint32_t, RemoteNodeInfo *>& remote_nodes_involved);
       RC ReceiveCommitResponse(unordered_map<uint64_t, vector<YCSBRequest *>> & node_requests_map, vector<YCSBRequest *>&remaining_reqs, std::map<uint32_t, RemoteNodeInfo *>& remote_nodes_involved);



       
};

}

#endif  // ARBORETUM_SRC_DB_ALBATROSSUSRTXN_H_