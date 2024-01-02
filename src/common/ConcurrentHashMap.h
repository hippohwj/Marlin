

#ifndef ARBORETUM_SRC_COMMON_CONCURRENTHASHMAP_H_
#define ARBORETUM_SRC_COMMON_CONCURRENTHASHMAP_H_

#include "Types.h"
#include <functional>


using namespace std;

namespace arboretum {

template<typename Key, typename Value>
class ConcurrentHashMap {
public:
    ConcurrentHashMap() {}

    void insert(Key key,Value value);

    void InsertBatch(unordered_map<Key, Value>* map);

    // Value get(uint64_t key);
    void get(Key key, Value& value);
    void getOrDefault(Key key, Value defaultValue, Value& value);

    bool contains(Key key);

    bool iterativeExecute(function<void(Value)> update_function);

    string PrintStr();
    
    void remove(Key key);
    
    size_t size();

private:
    std::unordered_map<Key, Value> hashmap;
    std::atomic<uint32_t> lock_{0};
};

}

#endif //ARBORETUM_SRC_COMMON_CONCURRENTHASHMAP_H_