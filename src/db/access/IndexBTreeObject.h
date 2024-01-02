//
// Created by Zhihan Guo on 4/3/23.
//

#ifndef ARBORETUM_SRC_DB_ACCESS_INDEXBTREEOBJECT_H_
#define ARBORETUM_SRC_DB_ACCESS_INDEXBTREEOBJECT_H_

#include "IIndex.h"

namespace arboretum {

class ITuple;

struct BTreeObjNode {
  OID id_{0}; // for debug
  size_t level_{0};
  SearchKey *keys_{};
  SharedPtr *children_{}; // leaf node: reserve children_[0] for neighbor ptr
  size_t key_cnt_{0};
  RWLock latch_{};
  inline bool IsLeaf() const { return level_ == 0; };
  SharedPtr* SafeInsert(SearchKey &key, void *val);
  bool SafeDelete(SearchKey &key, ITuple *tuple = nullptr);
};

class IndexBTreeObject : public IIndex {

  struct StackData {
    BTreeObjNode * node_{nullptr};
    struct StackData * parent_{nullptr};
  };
  typedef StackData *Stack;

 public:
  IndexBTreeObject();
  SharedPtr* Insert(SearchKey key, ITuple * tuple, bool return_ref=false);
  SharedPtr* Search(SearchKey key, int limit=1);
  void Delete(SearchKey key, ITuple *p_tuple);
  void PrintTree();

 private:
  void Search(SearchKey key, AccessType ac, BTreeObjNode * &parent,
              BTreeObjNode * &child);
  Stack PessimisticSearch(SearchKey key, BTreeObjNode * &parent,
                          BTreeObjNode * &child, AccessType ac);
  SharedPtr* RecursiveInsert(Stack stack, SearchKey key, void * val,
                             BTreeObjNode *child, bool return_ref);
  void RecursiveDelete(Stack stack, SearchKey key, BTreeObjNode *child,
                       ITuple * tuple);
  static OID ScanBranchNode(BTreeObjNode * node, SearchKey key);
  static SharedPtr* ScanLeafNode(BTreeObjNode * node, SearchKey key);

  // helper functions
  BTreeObjNode * AllocNewNode();
  inline bool SplitSafe(BTreeObjNode *node) const {
    return !node || node->key_cnt_ + 1 <= split_bar_;
  }
  inline bool DeleteSafe(BTreeObjNode *node) const {
    return !node || node->key_cnt_ > 1;
  }
  inline bool IsRoot(BTreeObjNode *node) { return node == root_.Get(); };
  static inline BTreeObjNode * AccessNode(SharedPtr& ptr) {
    return reinterpret_cast<BTreeObjNode *>(ptr.Get());
  }
  static inline BTreeObjNode * AccessNode(SharedPtr& ptr, bool exclusive) {
    auto node = AccessNode(ptr);
    if (node) node->latch_.Lock(exclusive);
    return node;
  }

  SharedPtr root_{};
  size_t fanout_{6};
  size_t height_{0};
  size_t split_bar_{0};
  size_t num_nodes_{0};
};
}

#endif //ARBORETUM_SRC_DB_ACCESS_INDEXBTREEOBJECT_H_
