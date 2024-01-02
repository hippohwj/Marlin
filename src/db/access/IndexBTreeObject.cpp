//
// Created by Zhihan Guo on 4/3/23.
//

#include "IndexBTreeObject.h"
#include "ITuple.h"

namespace arboretum {

IndexBTreeObject::IndexBTreeObject() {
  fanout_ = g_idx_btree_fanout;
  split_bar_ = std::min((size_t)(fanout_ * g_idx_btree_split_ratio), fanout_ - 1);
  auto root_node = AllocNewNode();
  root_.Init(root_node);
  LOG_INFO("Created btree index with fanout = %zu and split_threshold = %zu",
           fanout_, split_bar_);
}

SharedPtr* IndexBTreeObject::Search(SearchKey key, int limit) {
  auto result = NEW_SZ(SharedPtr, limit);
  BTreeObjNode * parent;
  BTreeObjNode * child;
  Search(key, AccessType::READ, parent, child);
  // check if key in child
  int cnt = 0;
  for (auto i = 0; i < child->key_cnt_; i++) {
    if (child->keys_[i] == key) {
      // TODO: check if this increment the ref cnt.
      result[cnt].InitFrom(child->children_[i + 1]);
      cnt++;
      if (cnt == limit)
        break;
    }
  }
  child->latch_.Unlock();
  if (cnt == 0) {
    DEALLOC(result);
    return nullptr;
  }
  return result;
}

SharedPtr* IndexBTreeObject::Insert(SearchKey key, ITuple * tuple, bool return_ref) {
  BTreeObjNode * parent;
  BTreeObjNode * child;
  Search(key, AccessType::INSERT, parent, child);
  // the last two layers are EX locked, see if child and parent will split
  SharedPtr * ptr;
  if (!SplitSafe(child) && !SplitSafe(parent)) {
    // restart with all exclusive lock from non-split safe point.
    child->latch_.Unlock();
    parent->latch_.Unlock();
    // LOG_DEBUG("may split more than 2 levels, start pessimistic search!");
    auto stack = PessimisticSearch(key, parent, child, INSERT);
    // insert bottom-up recursively with a stack.
    return RecursiveInsert(stack, key, tuple, child, return_ref);
  } else {
    auto stack = NEW(StackData);
    stack->parent_ = nullptr;
    stack->node_ = parent;
    return RecursiveInsert(stack, key, tuple, child, return_ref);
  }
  return nullptr;
}

void
IndexBTreeObject::Search(SearchKey key, AccessType ac,
                         BTreeObjNode *&parent, BTreeObjNode *&child) {
  // top-down traverse, acquire ex lock on second last level if ac == WRITE
  auto is_update = false;
  parent = nullptr;
  child = AccessNode(root_, (ac == INSERT || ac == DELETE)
  && (height_ == 0));
  while (true) {
    if (child->IsLeaf()) {
      if (ac != INSERT && ac != DELETE)
        if (parent) parent->latch_.Unlock();
      break;
    }
    // search in the keys, find the offset of child
    auto offset = ScanBranchNode(child, key);
    // no need to use parent which will become grandparent
    if (parent) parent->latch_.Unlock();
    if (ac == UPDATE) {
      // TODO: currently assuming update does not change index key.
      if (child->level_ == 0) is_update = true;
    } else if (ac == INSERT || ac == DELETE) {
      if (child->level_ <= 2) is_update = true;
    }
    parent = child;
    child = AccessNode(child->children_[offset], is_update);
  }
}

IndexBTreeObject::Stack
IndexBTreeObject::PessimisticSearch(SearchKey key, BTreeObjNode *&parent,
                                    BTreeObjNode *&child, AccessType ac) {
  // top-down traverse, acquire ex lock on second last level if ac == WRITE
  Stack stack = nullptr;
  auto is_update = true;
  parent = nullptr;
  child = AccessNode(root_, is_update);
  assert(height_ != 0);
  while (true) {
    if (child->IsLeaf()) {
      break;
    }
    // search in the keys, find the offset of child
    auto offset = ScanBranchNode(child, key);
    // save stack if it is pessmistic and may split
    auto new_stack = NEW(StackData);
    new_stack->node_ = child;
    new_stack->parent_ = stack;
    stack = new_stack;
    // no need to use parent which will become grandparent
    // TODO: check delete safe for delete operations
    bool safe = (ac == INSERT && SplitSafe(child)) ||
        (ac == DELETE && DeleteSafe(child) );
    if (safe && parent) {
      // free entire stack recursively
      auto parent_stack = stack->parent_;
      assert(parent_stack->node_ == parent);
      while (parent_stack) {
        if (parent_stack->node_) parent_stack->node_->latch_.Unlock();
        auto to_delete = parent_stack;
        parent_stack = parent_stack->parent_;
        DEALLOC(to_delete);
      }
      stack->parent_ = nullptr;
    }
    parent = child;
    child = AccessNode(child->children_[offset], is_update);
  }
  return stack;
}

SharedPtr*
IndexBTreeObject::RecursiveInsert(Stack stack, SearchKey key, void * val,
                                  BTreeObjNode *child, bool return_ref) {
  auto parent = stack ? stack->node_ : nullptr;
  SharedPtr * ref = nullptr;
  if (SplitSafe(child)) {
    // insert into child and return
    auto ptr = child->SafeInsert(key, val);
    if (return_ref && child->IsLeaf()) {
      ref = NEW(SharedPtr);
      ref->InitFrom(*ptr);
    }
    child->latch_.Unlock();
    // delete remaining stack
    while (stack) {
      if (stack->node_) stack->node_->latch_.Unlock();
      auto used_stack = stack;
      stack = stack->parent_;
      DEALLOC(used_stack);
    }
  } else {
    child->SafeInsert(key, val);
    // split child and insert new key separator into parent
    auto split_pos = child->key_cnt_ / 2;
    auto split_key = child->keys_[split_pos];
    assert(split_pos - 1 >= 0);
    assert(split_pos + 1 < child->key_cnt_);
    auto new_node = AllocNewNode();
    new_node->level_ = child->level_;
    // move from split pos to new node
    // non leaf: [1, 3, 5], [(,1), [1, 3), [3,5), [5, )], split pos = 1
    // key: [5] moved to new node (offset=2),
    // value: [(, 1], [1, 3)] moved to new node (offset = 2)
    // leaf: [1,2,3], [-, 1, 2, 3], split pos = 1, 2 is separator key
    // key: [2,3] moved to new node (offset = 1)
    auto move_start = child->IsLeaf() ? split_pos : split_pos + 1;
    auto num_ele = child->key_cnt_ - move_start;
    memmove(&new_node->keys_[0], &child->keys_[move_start],
            num_ele * sizeof(SearchKey));
    if(child->IsLeaf()) {
      memmove(&new_node->children_[1], &child->children_[split_pos + 1],
              num_ele * sizeof(SharedPtr));
    } else {
      memmove(&new_node->children_[0], &child->children_[split_pos + 1],
              (num_ele + 1) * sizeof(SharedPtr));
    }
    child->key_cnt_ = split_pos;
    new_node->key_cnt_ = num_ele;
    if (return_ref && child->IsLeaf()) {
      auto ptr = ScanLeafNode(key < new_node->keys_[0] ? child : new_node, key);
      ref = NEW(SharedPtr);
      ref->InitFrom(*ptr);
    }
    if (IsRoot(child)) {
      // split root by adding a new root
      auto new_root = AllocNewNode();
      new_root->level_ = child->level_ + 1;
      new_root->children_[0].Init(child);
      new_root->SafeInsert(split_key, new_node);
      height_++;
      // this line must happen in the end to avoid others touching new root
      root_.Init(new_root);
      child->latch_.Unlock();
      return ref;
    }
    child->latch_.Unlock();
    // insert into parent, parent may still be null even child cannot be root
    // in case it is split safe and not added!
    if (stack) {
      auto used_stack = stack;
      stack = stack->parent_;
      DEALLOC(used_stack);
      RecursiveInsert(stack, split_key, new_node, parent, return_ref);
    }
  }
  return ref;
}

OID IndexBTreeObject::ScanBranchNode(BTreeObjNode *node, SearchKey key) {
  // TODO: optimize to use binary search or interpolation search
  // if internal node, return the offset of the pointer to child
  // e.g. [1,3,5], [p0,p1,p2,p3]
  for (size_t i = 0; i < node->key_cnt_; i++) {
    if (key < node->keys_[i]) {
      return i;
    } else if (key == node->keys_[i]) {
      return i + 1;
    } else if (key > node->keys_[i]) {
      continue;
    }
  }
  // out of bound
  return node->key_cnt_;
}

BTreeObjNode * IndexBTreeObject::AllocNewNode() {
  auto sz = sizeof(BTreeObjNode) + sizeof(SearchKey) * fanout_ +
          sizeof(SharedPtr) * (fanout_ + 1);
  auto new_node = new (MemoryAllocator::Alloc(sz)) BTreeObjNode();
  new_node->keys_ = (SearchKey *) ((char *) new_node + sizeof(BTreeObjNode));
  new_node->children_ = (SharedPtr *) ((char *) new_node->keys_ +
      sizeof(SearchKey) * fanout_);
  for (int i = 0; i < fanout_ + 1; i++) {
    new (&new_node->children_[i]) SharedPtr();
  }
  new_node->id_ = num_nodes_;
  num_nodes_++;
  return new_node;
}

SharedPtr* BTreeObjNode::SafeInsert(SearchKey &key, void * val) {
  // find the right insert position
  OID idx;
  for (idx = 0; idx < key_cnt_; idx++) {
    if (keys_[idx] > key) {
      break;
    }
  }
  if (idx != key_cnt_) {
    // need to shift
    auto shift_num = key_cnt_ - idx;
    memmove(&keys_[idx + 1], &keys_[idx],
            sizeof(SearchKey) * shift_num);
    memmove(&children_[idx + 2], &children_[idx + 1],
            sizeof(SharedPtr) * shift_num);
  }
  keys_[idx] = key;
  // init ref count = 1.
  if (IsLeaf()) {
    children_[idx + 1].Init(reinterpret_cast<ITuple *>(val));
  } else {
    children_[idx + 1].Init(reinterpret_cast<BTreeObjNode *>(val));
  }
  key_cnt_++;
  return &children_[idx + 1];
}

bool BTreeObjNode::SafeDelete(SearchKey &key, ITuple *tuple) {
  bool empty = false;
  OID idx;
  if (IsLeaf()) {
    for (idx = 0; idx < key_cnt_; idx++) {
      //TODO(hippo): XXX!!! enable tuple check instead of checking key only
      if (key == keys_[idx] && children_[idx + 1].Get() == tuple) break;
      // if (key == keys_[idx]) break;
    }
    if (idx >= key_cnt_) {
      // auto args = .format("delete (%lu) does not exsit in leaf",
      // key.ToUInt64());
      // LOG_ERROR("Key to delete (%lu) does not exist in leaf", key.ToUInt64());
      cout <<  "XXX Key to delete does not exist in leaf " << key.ToUInt64() << "; granule id is " << GetGranuleID(key.ToUInt64())<< endl;
      cout << "keys: ";
     for (idx = 0; idx < key_cnt_; idx++) {
      //TODO(hippo): XXX!!! enable tuple check instead of checking key only
      // if (key == keys_[idx] && children_[idx + 1].Get() == tuple) break;
      std::cout <<  keys_[idx].ToUInt64() << ", ";
      }
      cout << endl;
      throw std::invalid_argument( "delete does not exsit in leaf");
      //  LOG_ERROR("Key to delete (%lu) does not exist in leaf",
      //  key.ToUInt64());
    } else {
      // must dealloc here. otherwise memmove will overwrite
      children_[idx + 1].Free();
      //    if (!children_[idx + 1].Free()) {
      //      reinterpret_cast<ITuple *>(children_[idx +
      //      1].Get())->buf_latch_.unlock();
      //    }
      auto shift_num = key_cnt_ - idx - 1;
      if (shift_num > 0) {
        memmove(&keys_[idx], &keys_[idx + 1], sizeof(SearchKey) * shift_num);
        memmove(&children_[idx + 1], &children_[idx + 2],
                sizeof(SharedPtr) * shift_num);
      }
      key_cnt_--;
      empty = (key_cnt_ == 0);
    }

  } else {
    for (idx = 0; idx < key_cnt_; idx++) {
      if (key < keys_[idx]) break;
    }
    // delete right key
    children_[idx].Free();
    // move keys
    auto shift_num = key_cnt_ > idx ? key_cnt_ - idx - 1 : 0;
    if (shift_num > 0) {
      memmove(&keys_[idx], &keys_[idx + 1],
              sizeof(SearchKey) * shift_num);
    }
    // move ptrs
    shift_num = key_cnt_ - idx;
    if (shift_num > 0) {
      memmove(&children_[idx], &children_[idx + 1],
              sizeof(SharedPtr) * shift_num);
    }
    if (key_cnt_ == 0) {
      empty = true;
    } else {
      key_cnt_--;
    }
  }
  return empty;
}

void IndexBTreeObject::Delete(SearchKey key, ITuple * tuple) {
  BTreeObjNode * parent;
  BTreeObjNode * child;
  Search(key, AccessType::DELETE, parent, child);
  // the last two layers are EX locked, see if child and parent will split
  if (!DeleteSafe(child) && !DeleteSafe(parent)) {
    // restart with all exclusive lock from non-split safe point.
    child->latch_.Unlock();
    parent->latch_.Unlock();
    // LOG_DEBUG("may split more than 2 levels, start pessimistic search!");
    // PrintTree();
    auto stack = PessimisticSearch(key, parent, child, DELETE);
    // insert bottom-up recursively with a stack.
    RecursiveDelete(stack, key, child, tuple);
  } else {
    auto stack = NEW(StackData);
    stack->parent_ = nullptr;
    stack->node_ = parent;
    RecursiveDelete(stack, key, child, tuple);
  }
}

void IndexBTreeObject::RecursiveDelete(IndexBTreeObject::Stack stack,
                                       SearchKey key, BTreeObjNode *child,
                                       ITuple * tuple) {
  auto parent = stack ? stack->node_ : nullptr;
  if (DeleteSafe(child)) {
    child->SafeDelete(key, tuple);
    child->latch_.Unlock();
    // delete remaining stack
    while (stack) {
      if (stack->node_) stack->node_->latch_.Unlock();
      auto used_stack = stack;
      stack = stack->parent_;
      DEALLOC(used_stack);
    }
  } else {
    // LOG_DEBUG("node (level = %zu) is not safe, split", child->level_);
    bool empty = child->SafeDelete(key, tuple);
    child->latch_.Unlock();
    // insert into parent, parent may still be null even child cannot be root
    // in case it is split safe and not added!
    if (empty) {
      if (stack) {
        auto used_stack = stack;
        stack = stack->parent_;
        DEALLOC(used_stack);
        RecursiveDelete(stack, key, parent, nullptr);
      }
    } else {
      // free remaining stack
      while (stack) {
        if (stack->node_) stack->node_->latch_.Unlock();
        auto used_stack = stack;
        stack = stack->parent_;
        DEALLOC(used_stack);
      }
    }
  }
}

void IndexBTreeObject::PrintTree() {
  LOG_DEBUG("==== Print Tree ====");
  std::queue<BTreeObjNode *> queue;
  auto node = AccessNode(root_);
  queue.push(node);
  while (!queue.empty()) {
    node = queue.front();
    queue.pop();
    if (!node)
      continue;
    // assert(!node->latch_.IsLocked());
    std::cout << "node-" << node->id_ << " (level=" << node->level_
              << ", #keys=" << node->key_cnt_ << ")" << std::endl;
    if (!node->IsLeaf() && node->key_cnt_ == 0) {
      auto child = AccessNode(node->children_[0]);
      std::cout << "\t[ptr=node-" << child->id_ << "]" << std::endl;
      queue.push(child);
      continue;
    }
    for (auto i = 0; i < node->key_cnt_; i++) {
      if (node->IsLeaf()) {
        std::cout << "\t(key=" << node->keys_[i].ToString() << ", row=" <<
                  node->children_[i+1].Get() << "), ";
        continue;
      } else {
        if (i == 0) {
          auto child = AccessNode(node->children_[0]);
          std::cout << "\t[ptr=node-" << child->id_;
          queue.push(child);
        }
        auto child = AccessNode(node->children_[i + 1]);
        std::cout << ", key=" << node->keys_[i].ToString() << ", ptr=node-"
                  << child->id_;
        if (i == node->key_cnt_ - 1)
          std::cout << "]";
        queue.push(child);
      }
    }
    std::cout << std::endl;
  }
  LOG_DEBUG("==== End Print ====");
}

SharedPtr *IndexBTreeObject::ScanLeafNode(BTreeObjNode *node, SearchKey key) {
  for (size_t i = 0; i < node->key_cnt_; i++) {
    if (key < node->keys_[i]) {
      break;
    } else if (key == node->keys_[i]) {
      return &node->children_[i + 1];
    }
  }
  return nullptr;
}

} // arboretum