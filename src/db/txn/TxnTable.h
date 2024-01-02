#ifndef ARBORETUM_DISTRIBUTED_SRC_DB_TXN_TXNTABLE_H_
#define ARBORETUM_DISTRIBUTED_SRC_DB_TXN_TXNTABLE_H_

#include "db/ITxn.h"

// For Distributed DBMS

namespace arboretum
{

// States of all active transactions are maintained in the per-node TxnTable.
class TxnTable
{
public:
    struct Node {
        ITxn * txn;
        volatile bool valid;
        volatile uint64_t ref;
        Node * next;
        Node() : txn(nullptr), valid(true), ref(0), next(nullptr) {};
    };

    TxnTable();
    // should support 3 methods: add_txn, get_txn, remove_txn
    void add_txn(ITxn * txn);
    void remove_txn(ITxn * txn, string& source);
    void print_txn();

    ITxn * get_txn(uint64_t txn_id, bool remove=false, bool
    validate=false);
   
    void get_txn_by_granule(GID granule_id, vector<ITxn *>& txns);
    void get_all_txns(vector<ITxn *>& txns);
    uint32_t serialize_all_txns(GID granule_id,vector<uint8_t> * buffer,  std::unordered_map<OID, uint32_t>& locks);

    uint32_t get_size();

private:
    struct Bucket {
        Node * first;
        volatile bool latch;
    };

    Bucket ** _buckets;
    uint32_t _txn_table_size;
};
}

#endif //ARBORETUM_DISTRIBUTED_SRC_DB_TXN_TXNTABLE_H_