#ifndef ARBORETUM_SRC_DB_ALBATROSSMIGRTXN_H_
#define ARBORETUM_SRC_DB_ALBATROSSMIGRTXN_H_

#include "MigrationTxn.h"

using namespace std;

namespace arboretum {

class AlbatrossMigrTxn: public MigrationTxn {
    public:
        AlbatrossMigrTxn(OID txn_id, ARDB *db): MigrationTxn(txn_id, db){};
        // void get_data_from_buffer(uint64_t key, uint32_t table_id, vector<WriteCopy *> &all_data);
        // RC Preheat(YCSBQuery *query);
        // RC Postheat();
        RC Execute(YCSBQuery *query) override;
        // RC Commit(RC rc) override;
        bool IsMigrTxn() override;
        static void SerializeLockTbl(vector<uint8_t> * buffer, unordered_map<OID, uint32_t>& lock_tbl);
        static unordered_map<OID, uint32_t> * DeserializeLockTbl(const uint8_t * buffer);
        static void SerializeTxnTbl(vector<uint8_t> * buffer, vector<ITxn *> & txns);
        static vector<ITxn *> * DeserializeTxnTbl(const uint8_t * buffer);

};

}
#endif  // ARBORETUM_SRC_DB_ALBATROSSMIGRTXN_H_