#include "TxnTable.h"
#include "common/Common.h"
#include "common/Worker.h"
#include "common/GlobalData.h"

namespace arboretum {

#define DEBUG_TXN_TABLE false

TxnTable::TxnTable()
{
    _txn_table_size = g_num_nodes * g_num_worker_threads;
    _buckets = new Bucket * [_txn_table_size];
    for (uint32_t i = 0; i < _txn_table_size; i++) {
        _buckets[i] = (Bucket *) _mm_malloc(sizeof(Bucket), 64);
        _buckets[i]->first = NULL;
        _buckets[i]->latch = false;
    }
}

void
TxnTable::add_txn(ITxn * txn)
{
    // cout << "XXX add txn with id " << txn->GetTxnId() << " in thread " << arboretum::Worker::GetThdId() << endl; 

    // assert(get_txn(txn->GetTxnId()) == NULL);
    M_ASSERT(get_txn(txn->GetTxnId()) == NULL, "cannot add txn with id %d in thread %d", txn->GetTxnId(), arboretum::Worker::GetThdId());

    uint32_t bucket_id = txn->GetTxnId() % _txn_table_size;
    Node * node = new Node; // *) _mm_malloc(sizeof(Node), 64);
    node->txn = txn;
      while ( !ATOM_CAS(_buckets[bucket_id]->latch, false, true) )
        PAUSE
    COMPILER_BARRIER
    // insert to front
    node->next = _buckets[bucket_id]->first;
    _buckets[bucket_id]->first = node;
    COMPILER_BARRIER
    _buckets[bucket_id]->latch = false;
}

ITxn *
TxnTable::get_txn(uint64_t txn_id, bool remove, bool validate)
{
    // print_txn();

    uint32_t bucket_id = txn_id % _txn_table_size;
    Node * node;
    while ( !ATOM_CAS(_buckets[bucket_id]->latch, false, true) )
        PAUSE
    COMPILER_BARRIER
    node = _buckets[bucket_id]->first;
    while (node && node->txn->GetTxnId() != txn_id) {
        node = node->next;
    }
    ITxn * txn = nullptr;
    if (node) {
        if (node->valid || !validate) {
            if (validate && remove)
                node->valid = false;
            // increment reference count for safety
            // no need to be atomic since protected by latch.
            txn = node->txn;
        }
    }
    COMPILER_BARRIER
    _buckets[bucket_id]->latch = false;
    return txn;
}


// void TxnTable::get_txn_by_granule(GID granule_id, vector<ITxn *>& txns) {
//     for(uint32_t i=0; i<_txn_table_size; ++i)
//     {
//         Node * node = _buckets[i]->first;
//         while ( node )
//         {
//             ITxn * txn = node->txn;
//             try{
//                 if(txn->ContainsGranule(granule_id)) {
//                     txns.push_back(txn);
//                 }
//             }
//             catch(...){
//                 // Usually memory access error. We just do nothing.
//                 LOG_INFO("XXX Unexpected Error");
//             }
//             node = node->next;
//         }
//     }
// }

void TxnTable::get_txn_by_granule(GID granule_id, vector<ITxn *>& txns) {
    for (uint32_t i = 0; i < _txn_table_size; i++)
    {
        while ( !ATOM_CAS(_buckets[i]->latch, false, true) )
            PAUSE
        COMPILER_BARRIER

        Node * node = _buckets[i]->first;
        while (node) {
            ITxn * txn = node->txn;
            if((!txn->IsMigrTxn()) && txn->ContainsGranule(granule_id)) {
                txns.push_back(txn);
            }
            node = node->next;
        }

        COMPILER_BARRIER
        _buckets[i]->latch = false;
    }
}


void TxnTable::get_all_txns(vector<ITxn *>& txns) {
   for (uint32_t i = 0; i < _txn_table_size; i++)
    {
        while ( !ATOM_CAS(_buckets[i]->latch, false, true) )
            PAUSE
        COMPILER_BARRIER

        Node * node = _buckets[i]->first;
        while (node) {
            ITxn * txn = node->txn;
            if((!txn->IsMigrTxn())) {
                txns.push_back(txn);
            }
            node = node->next;
        }

        COMPILER_BARRIER
        _buckets[i]->latch = false;
    }

}

uint32_t TxnTable::serialize_all_txns(GID granule_id, vector<uint8_t> * buffer,  std::unordered_map<OID, uint32_t>& lock_tbl) {
    vector<uint8_t> tmp_buffer; 
    uint32_t txn_num = 0;
    for (uint32_t i = 0; i < _txn_table_size; i++)
    {
        while ( !ATOM_CAS(_buckets[i]->latch, false, true) )
            PAUSE
        COMPILER_BARRIER

        Node * node = _buckets[i]->first;
        while (node) {
            ITxn * txn = node->txn;
            // if((!txn->IsMigrTxn()) && txn->ContainsGranule(granule_id)) {
            if((!txn->IsMigrTxn())) {
                txn->Serialize(&tmp_buffer, granule_id);
               vector<Access> * accesses = txn->GetAccess();
               for (auto access: *accesses) {
                 if (access.tbl_->IsUserTable()) {
                    auto tuple = reinterpret_cast<ITuple *>(access.rows_[0].Get());
                    OID lock_key = tuple->GetTID();
                    auto lock_st = access.tbl_->GetLockStatePtr(lock_key); 
                    if (lock_st != nullptr) {
                        lock_tbl[lock_key] = lock_st->load();
                    } 
                 }
               }
               txn_num++;
            }
            node = node->next;
        }

        COMPILER_BARRIER
        _buckets[i]->latch = false;
    }
    LOG_INFO("XXX Serialize Txn table with txn_num %d", txn_num);
    tmp_buffer.insert(tmp_buffer.begin(), CHAR_PTR_CAST(&txn_num), CHAR_PTR_CAST(&txn_num) + sizeof(txn_num));
    buffer->insert(buffer->end(), tmp_buffer.begin(), tmp_buffer.end());
   return txn_num;
}



void
TxnTable::print_txn()
{
    char buffer[4000];
    // sprintf(buffer, "[%u. %lu] Debug Info:\n", g_node_id, glob_manager->get_thd_id());
    sprintf(buffer, "Print txn table:\n");

    // we don't acquire locks
    for(uint32_t i=0; i<_txn_table_size; ++i)
    {
        Node * node = _buckets[i]->first;
        while ( node )
        {
            ITxn * txn = node->txn;
            try{
                sprintf(buffer, "[TXN TABLE] txn id is %u\n", txn->GetTxnId());

                // StoreProcedure * sp = txn->get_store_procedure();
                // QueryBase * qb = sp->get_query();
                // QueryYCSB * ycsbq = (QueryYCSB *)qb;
                // sprintf(buffer, "  [Txn%lu]", txn->get_txn_id());
                // RequestYCSB * reqs = ycsbq->get_requests();
                // for(uint32_t j=0; j<g_req_per_query; ++j)
                // {
                //     RequestYCSB * req = &reqs[j];
                //     switch(req->rtype)
                //     {
                //         case RD:
                //             sprintf(buffer, " Read(%lu)=%u", req->key, req->value);
                //             break;
                //         case WR:
                //             sprintf(buffer, " Write(%lu)=%u", req->key, req->value);
                //             break;
                //         default:
                //             sprintf(buffer, " Unknown(%lu)=%u", req->key, req->value);
                //     }
                // }
            }
            catch(...){
                // Usually memory access error. We just do nothing.
            }
            sprintf(buffer, "Print txn table finish\n");
            node = node->next;
        }
    }

    LOG_INFO("%s", buffer);
}

void
TxnTable::remove_txn(ITxn * txn, string& src)
{
    uint32_t bucket_id = txn->GetTxnId() % _txn_table_size;
    Node * node = nullptr;
    Node * rm_node = nullptr;
    while ( !ATOM_CAS(_buckets[bucket_id]->latch, false, true) )
        PAUSE
    COMPILER_BARRIER
    node = _buckets[bucket_id]->first;
    if (node == nullptr) {
        cout << "XXX remove source: " << src << ", txn id is " << txn->GetTxnId() << ", bucket id is " << bucket_id << endl;

    }
    assert(node);
    // the first node matches
    if (node && node->txn == txn) {
        rm_node = node;
        _buckets[bucket_id]->first = node->next;
    } else {
        while (node->next && node->next->txn != txn)
            node = node->next;
        if (node->next == nullptr) {
            cout << "XXX remove source: " << src << ", txn id is " << txn->GetTxnId() << ", bucket id is " << bucket_id << endl;
        }
        assert(node->next);
        rm_node = node->next;
        node->next = node->next->next;
    }
    COMPILER_BARRIER
    _buckets[bucket_id]->latch = false;
    assert(rm_node);
    delete rm_node;
}

uint32_t
TxnTable::get_size()
{
    uint32_t size = 0;
    for (uint32_t i = 0; i < _txn_table_size; i++)
    {
        while ( !ATOM_CAS(_buckets[i]->latch, false, true) )
            PAUSE
        COMPILER_BARRIER

        Node * node = _buckets[i]->first;
        while (node) {
            size ++;
            // cout << i << ":"
            //      << node->txn->GetTxnId() << "\t"
            //      << node->txn->get_txn_state()
            //      << endl;
            node = node->next;
        }

        COMPILER_BARRIER
        _buckets[i]->latch = false;
    }
    return size;
}

}