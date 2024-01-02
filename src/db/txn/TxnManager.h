#ifndef ARBORETUM_SRC_DB_TXN_TXNMANAGER_H_
#define ARBORETUM_SRC_DB_TXN_TXNMANAGER_H_

class TxnManager
{
public:
    enum State {
        RUNNING,
        PREPARED,
        COMMITTED,
        ABORTED,
        FAILED
    };
    TxnManager();
};

#endif //ARBORETUM_SRC_DB_TXN_TXNMANAGER_H_