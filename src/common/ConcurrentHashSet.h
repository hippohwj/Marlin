#ifndef ARBORETUM_SRC_COMMON_CONCURRENTHASHSET_H_
#define ARBORETUM_SRC_COMMON_CONCURRENTHASHSET_H_

#include <unordered_set>
#include "Types.h"
#include <mutex>

using namespace std;

namespace arboretum {

template<typename Key>
class ConcurrentHashSet {
public:
    ConcurrentHashSet() {}

    void insert(Key key);

    void insertBatch(vector<Key> * keys);

    bool contains(Key key);
    
    void remove(Key key);

    uint32_t getLockValue() {
        return lock_.load();
    }

private:
    std::unordered_set<Key> hashset;
    std::atomic<uint32_t> lock_{0};
    std::mutex mutex;
};

}

#endif //ARBORETUM_SRC_COMMON_CONCURRENTHASHSET_H_