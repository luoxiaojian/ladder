#ifndef LADDER_PROPERTY_COLUMN_H
#define LADDER_PROPERTY_COLUMN_H

#include <string>
#include <unordered_map>
#include <vector>

#include "property/types.h"
#include "utils.h"

namespace ladder {

class IColumn {
 public:
  virtual ~IColumn() = default;

  virtual void open(const std::string& prefix) = 0;
  virtual size_t size() = 0;
};

template <typename T>
class NumericColumn : public IColumn {
 public:
  NumericColumn() = default;
  ~NumericColumn() = default;

  void open(const std::string& prefix) override {
    load_from_file(prefix, data_);
  }

  inline size_t size() override { return data_.size(); }

  inline T get(size_t idx) const { return data_[idx]; }

 private:
  std::vector<T> data_;
};

class StringColumn : public IColumn {
 public:
  StringColumn() = default;
  ~StringColumn() = default;

  void open(const std::string& prefix) override {
    std::string offsets_fname = prefix + "_offset";
    load_from_file(offsets_fname, offsets_);

    std::string lengths_fname = prefix + "_length";
    load_from_file(lengths_fname, lengths_);

    std::string content_fname = prefix + "_content";
    load_from_file(content_fname, content_);
  }

  inline size_t size() override { return offsets_.size(); }

  inline std::string_view get(size_t idx) const {
    return std::string_view(&content_[offsets_[idx]], lengths_[idx]);
  }

 private:
  std::vector<size_t> offsets_;
  std::vector<uint16_t> lengths_;
  std::vector<char> content_;
};

class LCStringColumn : public IColumn {
 public:
  LCStringColumn() = default;
  ~LCStringColumn() = default;

  void open(const std::string& prefix) override {
    std::string index_fname = prefix + "_index";
    load_from_file(index_fname, index_);

    data_.open(prefix + "_data");

    size_t data_size = data_.size();
    table_.clear();
    for (size_t i = 0; i < data_size; ++i) {
      std::string key = std::string(data_.get(i));
      table_[key] = static_cast<uint16_t>(i);
    }
  }

  inline size_t size() override { return index_.size(); }

  inline std::string_view get(size_t idx) const {
    size_t offset = index_[idx];
    return data_.get(offset);
  }

 private:
  std::vector<uint16_t> index_;
  StringColumn data_;
  std::unordered_map<std::string, uint16_t> table_;
};

IColumn* create_column(DataType dt) {
  switch (dt) {
  case DataType::kInt32:
    return new NumericColumn<int32_t>();
  case DataType::kUInt32:
    return new NumericColumn<uint32_t>();
  case DataType::kInt64:
    return new NumericColumn<int64_t>();
  case DataType::kUInt64:
    return new NumericColumn<uint64_t>();
  case DataType::kFloat:
    return new NumericColumn<float>();
  case DataType::kDouble:
    return new NumericColumn<double>();
  case DataType::kDate:
    return new NumericColumn<Date>();
  case DataType::kDateTime:
    return new NumericColumn<DateTime>();
  case DataType::kString:
    return new StringColumn();
  case DataType::kLCString:
    return new LCStringColumn();
  case DataType::kID:
    return new NumericColumn<gid_t>();
  default:
    return nullptr;
  }
}

}  // namespace ladder

#endif  // LADDER_PROPERTY_COLUMN_H
