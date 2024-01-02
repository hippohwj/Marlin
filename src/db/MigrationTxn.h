#ifndef ARBORETUM_SRC_DB_MIGRATIONTXN_H_
#define ARBORETUM_SRC_DB_MIGRATIONTXN_H_

#include "ITxn.h"

using namespace std;

namespace arboretum {

class MigrationTxn: public ITxn {
    public:
        MigrationTxn(OID txn_id, ARDB *db): ITxn(txn_id, db){};
        void get_data_from_buffer(uint64_t key, uint32_t table_id, vector<WriteCopy *> &all_data);
        RC Preheat(YCSBQuery *query);
        RC Postheat();
        RC Execute(YCSBQuery *query) override;
        RC Commit(RC rc) override;

        RC CommitP1() override;
        RC CommitP2(RC rc) override;
        bool IsMigrTxn() override;

        void CleanupCache(RC rc);
        void LockDelta(RC rc);
        void ReleaseDeltaLock(RC rc);

        void setRetry(bool retry);

    protected:
        //TODO(Hippo): remove this hack
        RC LogForLocalAndSys(string &data, uint8_t status_new, string &lsn);
        std::chrono::_V2::system_clock::time_point critical_start;
        bool isRetry_ = false;
        YCSBQuery *query_;

};

}

#endif  // ARBORETUM_SRC_DB_MIGRATIONTXN_H_