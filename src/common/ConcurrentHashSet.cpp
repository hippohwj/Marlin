#include "ConcurrentHashSet.h"
#include "db/CCManager.h"
#include <unordered_set>

namespace arboretum {
    // template<typename Key>
    // void ConcurrentHashSet<Key>::insert(Key key) {
    //     std::lock_guard<std::mutex> lock(mutex);
    //     hashset.insert(key);
    // }

    // template<typename Key>
    // void ConcurrentHashSet<Key>::insertBatch(vector<Key> * keys) {
    //     std::lock_guard<std::mutex> lock(mutex);
    //     for (auto& key:*keys) {
    //         hashset.insert(key);
    //     }
    // }

    // template<typename Key>
    // bool ConcurrentHashSet<Key>::contains(Key key) {
    //     std::lock_guard<std::mutex> lock(mutex);
    //     bool isContain = false;
    //     isContain = (hashset.find(key) != hashset.end());
    //     return isContain;
    // }

    // template<typename Key>
    // void ConcurrentHashSet<Key>::remove(Key key) {
    //     std::lock_guard<std::mutex> lock(mutex);
    //     hashset.erase(key);
    // }

    template<typename Key>
    void ConcurrentHashSet<Key>::insert(Key key) {
        CCManager::WaitUntilAcquireLock(lock_, AccessType::UPDATE);
        hashset.insert(key);
        CCManager::ReleaseLock(lock_, AccessType::UPDATE);
    }

    template<typename Key>
    void ConcurrentHashSet<Key>::insertBatch(vector<Key> * keys) {
        CCManager::WaitUntilAcquireLock(lock_, AccessType::UPDATE);
        for (auto& key:*keys) {
            hashset.insert(key);
        }
        CCManager::ReleaseLock(lock_, AccessType::UPDATE);
    }

    template<typename Key>
    bool ConcurrentHashSet<Key>::contains(Key key) {
        bool isContain = false;
        CCManager::WaitUntilAcquireLock(lock_, AccessType::READ);
        isContain = (hashset.find(key) != hashset.end());
        CCManager::ReleaseLock(lock_, AccessType::READ);
        return isContain;
    }

    template<typename Key>
    void ConcurrentHashSet<Key>::remove(Key key) {
        CCManager::WaitUntilAcquireLock(lock_, AccessType::UPDATE);
        hashset.erase(key);
        CCManager::ReleaseLock(lock_, AccessType::UPDATE);
    }

    template class ConcurrentHashSet<uint64_t>;
}