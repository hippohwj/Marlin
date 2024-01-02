#ifndef ARBORETUM_SRC_DB_DYNAMASTMIGRTXN_H_
#define ARBORETUM_SRC_DB_DYNAMASTMIGRTXN_H_

#include "MigrationTxn.h"

using namespace std;

namespace arboretum {

class DynamastMigrTxn: public MigrationTxn {
    public:
        DynamastMigrTxn(OID txn_id, ARDB *db): MigrationTxn(txn_id, db){};
        // void get_data_from_buffer(uint64_t key, uint32_t table_id, vector<WriteCopy *> &all_data);
        // RC Preheat(YCSBQuery *query);
        // RC Postheat();
        RC Execute(YCSBQuery *query) override;
        RC Commit(RC rc) override;
        bool IsMigrTxn() override;

    private:
        RC SendMigrMark(bool isStart);
        //TODO(Hippo): remove this hack
        YCSBQuery *query_;
        bool isRetry_ = false;
};

}
#endif  // ARBORETUM_SRC_DB_DYNAMASTMIGRTXN_H_