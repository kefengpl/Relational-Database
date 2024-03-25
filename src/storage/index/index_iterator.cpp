/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(LeafPage *cur_page, int cursor, BufferPoolManagerInstance *bpm)
    : cur_page_{cur_page}, cur_cursor_{cursor}, buffer_pool_manager_{bpm} {}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() = default;  // NOLINT

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool { return cur_page_ == nullptr && cur_cursor_ == 0; }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & { return cur_page_->GetArray()[cur_cursor_]; }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  //! \note 此时是最后一个元素
  if ((cur_page_->GetNextPageId() == INVALID_PAGE_ID) && (cur_cursor_ == cur_page_->GetKeyNum() - 1)) {
    cur_page_ = nullptr;
    cur_cursor_ = 0;
    buffer_pool_manager_ = nullptr;
    return *this;
  }
  if (cur_cursor_ != cur_page_->GetKeyNum() - 1) {
    ++cur_cursor_;
    return *this;
  }
  // 需要把下一页读入的情况，需要把 cursor 的值刷新为0；此外，cur_page_ 变为下一个 page
  ReadPageGuard page_guard{buffer_pool_manager_->FetchPageRead(cur_page_->GetNextPageId())};
  cur_page_ = page_guard.As<LeafPage>();
  cur_cursor_ = 0;
  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
