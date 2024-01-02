#include "PostheatTxn.h"
#include "remote/ILogStore.h"
#include "transport/rpc_client.h"
#include "common/GlobalData.h"
#include "IDataStore.h"
#include "common/Worker.h"

namespace arboretum {

RC PostheatTxn::Execute(YCSBQuery *query) {
    auto thd_id = arboretum::Worker::GetThdId();
    query_ = query;
    RC rc = OK;
    // TODO: get granule id from query
    size_t granule_id = query->requests[0].key;
    AccessType ac_type = query->requests[0].ac_type;
    LockType lock_type = query->requests[0].lock_type;

    // fake page_size
    size_t page_size = 0;
    auto table = db_->GetTable(query->tbl_id);
    string tbl_name = table->GetTableName();


    // check whether the key's granule exist on this node and  acquire read_lock
    // on G_TBL for current granule
    bool has_wrong_member = false;
    std::tuple<uint64_t, uint64_t> memCorrection;
    char *g_data;
    size_t g_size;
    auto g_table = db_->GetGranuleTable();
    rc = GetTuple(g_table, 0, SearchKey(granule_id), AccessType::READ, g_data,
                  g_size, lock_type);
    if (rc == OK) {
        auto g_schema = g_table->GetSchema();
        auto val = &(g_data[g_schema->GetFieldOffset(1)]);
        uint64_t home_node = *((uint64_t *)val);
        if (home_node != g_node_id) {
            // TODO(Hippo): remove this error assert and add correct
            // response_type for non-global sys txn impl
            memCorrection = make_tuple(granule_id, home_node);
            has_wrong_member = true;
            rc = ABORT;
            cout<< "XXX: wrong node abort" << endl;
        }
    } else {
       cout<< "XXX: get granule tuple abort" << endl;
    }

    // TODO(Hippo): move it out from critical path
    if (has_wrong_member) {
        granule_tbl_cache->insert(get<0>(memCorrection), get<1>(memCorrection));
        LOG_INFO(
            "Update local membership cache with (granule id %d, node "
            "id %d)",
            get<0>(memCorrection), get<1>(memCorrection));
    }

    if (rc == ABORT || rc == FAIL) return rc;
    // executed in single node
    size_t load_count = 0;
    g_data_store->granuleScan(
        tbl_name, granule_id, page_size,
        [this, table, ac_type, lock_type, &load_count,
         &rc](uint64_t key, char *preload_data) {
            // TODO: fill in key from parameter
            char *data;
            size_t size;
            if (preload_data) {
                auto cur =
                    GetTupleOrInsert(table, 0, SearchKey(key), ac_type, data,
                                     size, lock_type, preload_data);
                if (cur != OK) {
                    cout << "XXX: get data tuple abort" << endl;
                    rc = cur;
                } else {
                    // cout << "XXX: Postheat get tuple for key "<< key << endl;
                    load_count++;
                }

            } else {
                M_ASSERT(false, "null pointer for granule scan data");
            }
        });
    // cout << "XXX postheat loads " << load_count << " items" << endl;
    return rc;
}

RC PostheatTxn::Commit(RC rc) {
   //TODO: try to rollback b-tree/cache insertions
  // 2 PC
  M_ASSERT(rc==COMMIT, "hasn't support abort yet");
  string fakelsn ="0";
  cleanup(rc, fakelsn);
  return rc;
}

}