#ifndef ARBORETUM_SRC_DB_POSTHEATTXN_H_
#define ARBORETUM_SRC_DB_POSTHEATTXN_H_

#include "ITxn.h"

using namespace std;

namespace arboretum {

class PostheatTxn: public ITxn {
    public:
        PostheatTxn(OID txn_id, ARDB *db): ITxn(txn_id, db){};
        // void get_data_from_buffer(uint64_t key, uint32_t table_id, vector<WriteCopy *> &all_data);
        RC Execute(YCSBQuery *query) override;
        RC Commit(RC rc) override;

    
    private:
        //TODO(Hippo): remove this hack
        YCSBQuery *query_;
        bool isRetry_ = false;
};

}

#endif  // ARBORETUM_SRC_DB_POSTHEATTXN_H_