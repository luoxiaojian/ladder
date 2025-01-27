#ifndef LADDER_PROPERTY_TABLE_H
#define LADDER_PROPERTY_TABLE_H

#include <string>
#include <unordered_map>
#include <vector>

#include "property/column.h"

namespace ladder {

class Table {
 public:
  Table() : row_num_(0) {}
  ~Table() {
    for (auto column : columns_) {
      if (column != nullptr) {
        delete column;
      }
    }
  }

  void open(const std::string& prefix,
            const std::vector<std::pair<std::string, DataType>>& header) {
    size_t col_num = header.size();
    columns_.resize(col_num, nullptr);
    size_t min_row_num = std::numeric_limits<size_t>::max();
    for (size_t i = 0; i < col_num; ++i) {
      columns_[i] = create_column(header[i].second);
      columns_[i]->open(prefix + "_col_" + std::to_string(i));
      header_[header[i].first] = i;

      min_row_num = std::min(min_row_num, columns_[i]->size());
    }

    if (min_row_num == std::numeric_limits<size_t>::max()) {
      row_num_ = 0;
    } else {
      row_num_ = min_row_num;
    }
  }

  size_t row_num() const { return row_num_; }
  size_t col_num() const { return columns_.size(); }

  const IColumn* get_column_by_index(size_t idx) const { return columns_[idx]; }
  const IColumn* get_column_by_name(const std::string& name) const {
    auto it = header_.find(name);
    if (it == header_.end()) {
      return nullptr;
    }
    return columns_[it->second];
  }

 private:
  std::vector<IColumn*> columns_;
  std::unordered_map<std::string, size_t> header_;
  size_t row_num_;
};

}  // namespace ladder

#endif  // LADDER_PROPERTY_TABLE_H
