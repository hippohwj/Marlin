#include "ConcurrentHashMap.h"
#include "db/CCManager.h"
#include <unordered_set>

namespace arboretum {

    template<typename Key, typename Value>
    void ConcurrentHashMap<Key, Value>::insert(Key key, Value value) {
        // std::lock_guard<std::mutex> lock(mutex);
        CCManager::WaitUntilAcquireLock(lock_, AccessType::UPDATE);
        hashmap[key] = value;
        CCManager::ReleaseLock(lock_, AccessType::UPDATE);
    }

    template<typename Key, typename Value>
    void ConcurrentHashMap<Key, Value>::InsertBatch(unordered_map<Key, Value>* map) {
        // std::lock_guard<std::mutex> lock(mutex);
        CCManager::WaitUntilAcquireLock(lock_, AccessType::UPDATE);
        // hashmap.insert(map->begin(), map->end());
        for (auto it = map->begin(); it!= map->end(); it++) {
            hashmap[it->first] = it->second;
        }
        CCManager::ReleaseLock(lock_, AccessType::UPDATE);

    }

    template<typename Key, typename Value>
    string ConcurrentHashMap<Key, Value>::PrintStr() {
        // std::lock_guard<std::mutex> lock(mutex);
        std::stringstream ss;
        ss << "[";
        CCManager::WaitUntilAcquireLock(lock_, AccessType::READ);
        for (auto it = hashmap.begin(); it!= hashmap.end(); it++) {
            ss<< "(" << it->first << ", "<< it->second << ")";
        }
        ss << "]";
        CCManager::ReleaseLock(lock_, AccessType::READ);
        return ss.str();
    }

    template<typename Key, typename Value>
    void ConcurrentHashMap<Key, Value>::get(Key key, Value& value) {
        // std::lock_guard<std::mutex> lock(mutex);
        CCManager::WaitUntilAcquireLock(lock_, AccessType::READ);
        value = hashmap[key];
        CCManager::ReleaseLock(lock_, AccessType::READ);
    }

    template<typename Key, typename Value>
    void ConcurrentHashMap<Key, Value>::getOrDefault(Key key, Value defaultValue, Value& value) {
        // std::lock_guard<std::mutex> lock(mutex);
        CCManager::WaitUntilAcquireLock(lock_, AccessType::READ);
        auto isContain = (hashmap.find(key) != hashmap.end());
        if (isContain) {
            value =  hashmap[key]; 
        } else {
            value = defaultValue;
        }
        CCManager::ReleaseLock(lock_, AccessType::READ);
    }

    template<typename Key, typename Value>
    bool ConcurrentHashMap<Key, Value>::contains(Key key) {
        // std::lock_guard<std::mutex> lock(mutex);
        bool isContain = false;
        CCManager::WaitUntilAcquireLock(lock_, AccessType::READ);
        isContain = (hashmap.find(key) != hashmap.end());
        CCManager::ReleaseLock(lock_, AccessType::READ);
        return isContain;
    }

    template<typename Key, typename Value>
    size_t ConcurrentHashMap<Key, Value>::size() {
        // std::lock_guard<std::mutex> lock(mutex);
        size_t size;
        CCManager::WaitUntilAcquireLock(lock_, AccessType::READ);
        size = hashmap.size();
        CCManager::ReleaseLock(lock_, AccessType::READ);
        return size;
    }

    template<typename Key, typename Value>
    void ConcurrentHashMap<Key, Value>::remove(Key key) {
        // std::lock_guard<std::mutex> lock(mutex);
        CCManager::WaitUntilAcquireLock(lock_, AccessType::UPDATE);
        hashmap.erase(key);
        CCManager::ReleaseLock(lock_, AccessType::UPDATE);
    }

    template<typename Key, typename Value>
    bool ConcurrentHashMap<Key, Value>::iterativeExecute(function<void(Value)> update_function){
        CCManager::WaitUntilAcquireLock(lock_, AccessType::UPDATE);
        for (auto it = hashmap.begin(); it != hashmap.end(); ++it) {
          update_function(it->second);
        }
        CCManager::ReleaseLock(lock_, AccessType::UPDATE);
    }



    template class ConcurrentHashMap<uint64_t, uint64_t>;
    template class ConcurrentHashMap<uint64_t, std::unordered_set<uint64_t> *>;
    template class ConcurrentHashMap<uint64_t, std::atomic<uint32_t> *>;
    template class ConcurrentHashMap<uint64_t, string>;
}