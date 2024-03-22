#include "storage/page/b_plus_tree_page.h"

namespace bustub {

/*
 * Helper methods to get/set page type
 * Page type enum class is defined in b_plus_tree_page.h
 */
auto BPlusTreePage::IsLeafPage() const -> bool { return page_type_ == IndexPageType::LEAF_PAGE; }
auto BPlusTreePage::IsRootPage() const -> bool { return parent_page_id_ == INVALID_PAGE_ID; }
void BPlusTreePage::SetPageType(IndexPageType page_type) { page_type_ = page_type; }

/*
 * Helper methods to get/set size (number of key/value pairs stored in that
 * page)
 */
auto BPlusTreePage::GetSize() const -> int { return size_; }
void BPlusTreePage::SetSize(int size) { size_ = size; }
void BPlusTreePage::IncreaseSize(int amount) { size_ += amount; }

/*
 * Helper methods to get/set max size (capacity) of the page
 */
auto BPlusTreePage::GetMaxSize() const -> int { return max_size_; }
void BPlusTreePage::SetMaxSize(int size) { max_size_ = size; }

/**
 * Helper method to get min page size
 * Generally, min page size == max page size / 2
 * @note 非叶子结点：统计的是指针个数 ceil(n / 2)
 * @note 叶子结点：统计的是 key 的个数 ceil ((n - 1) / 2)
 * @note 根结点有豁免权，如果只有根这一个结点，那么它可以不包含任何指针；此外，它至少包含一个指针。
 */
auto BPlusTreePage::GetMinSize() const -> int {
  if (IsLeafPage()) {
    return std::ceil((static_cast<double>(max_size_) - 1) / 2);  // 非根叶子结点：恰好对半或者超过半数(比如 3/5)
  }  // 非根的内部结点：key 恰好对半或者不足半数(比如 2/5)
  return std::ceil(static_cast<double>(max_size_) / 2);
}

/*
 * Helper methods to get/set parent page id
 */
auto BPlusTreePage::GetParentPageId() const -> page_id_t { return parent_page_id_; }
void BPlusTreePage::SetParentPageId(page_id_t parent_page_id) { parent_page_id_ = parent_page_id; }

/*
 * Helper methods to get/set self page id
 */
auto BPlusTreePage::GetPageId() const -> page_id_t { return page_id_; }
void BPlusTreePage::SetPageId(page_id_t page_id) { page_id_ = page_id; }

/*
 * Helper methods to set lsn
 */
void BPlusTreePage::SetLSN(lsn_t lsn) { lsn_ = lsn; }

auto BPlusTreePage::GetKeyNum() -> int {
  if (IsLeafPage()) {
    return size_;  // 叶子结点：键值对个数
  }
  return size_ - 1;  // 非叶子结点：键值对(包括第一对key是空的)个数 - 1
}
auto BPlusTreePage::GetMinKeyNum() -> int {
  if (IsLeafPage()) {
    return std::ceil((static_cast<double>(max_size_) - 1) / 2);
  }
  return std::ceil(static_cast<double>(max_size_) / 2) - 1;
}

auto BPlusTreePage::IsFull() -> bool { return GetKeyNum() == max_size_ - 1; }

auto BPlusTreePage::GtHalfFull() -> bool { return GetKeyNum() > GetMinKeyNum(); }

}  // namespace bustub
