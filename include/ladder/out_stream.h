#ifndef LADDER_LADDER_OUT_STREAM_H_
#define LADDER_LADDER_OUT_STREAM_H_

#include <string.h>

#include <string>
#include <string_view>
#include <vector>

namespace ladder {

class OutStream {
 public:
  OutStream(const std::vector<std::vector<char>>& buffers)
      : buffers_(buffers), idx_(0), offset_(0) {
    while (idx_ < buffers_.size() && buffers_[idx_].empty()) {
      idx_ += 1;
    }
  }
  ~OutStream() = default;

  bool empty() const { return (idx_ == buffers_.size()); }

  size_t Read(char* data, size_t size) {
    CHECK_LT(idx_, buffers_.size());
    size_t remaining = buffers_[idx_].size() - offset_;
    size_t read_size = std::min(remaining, size);
    CHECK_LE(offset_ + read_size, buffers_[idx_].size());
    memcpy(data, buffers_[idx_].data() + offset_, read_size);
    offset_ += read_size;
    while (idx_ < buffers_.size() && offset_ == buffers_[idx_].size()) {
      idx_++;
      offset_ = 0;
    }
    return read_size;
  }

  std::string_view TakeSlice(size_t size) {
    size_t remaining = buffers_[idx_].size() - offset_;
    size_t read_size = std::min(remaining, size);
    std::string_view ret(buffers_[idx_].data() + offset_, read_size);
    offset_ += read_size;
    if (offset_ == buffers_[idx_].size()) {
      idx_++;
      offset_ = 0;
    }
    return ret;
  }

 private:
  const std::vector<std::vector<char>>& buffers_;
  size_t idx_;
  size_t offset_;
};

template <typename T>
OutStream& operator>>(OutStream& out, T& data) {
  std::cerr << "Unsupported type" << std::endl;
  return out;
}

template <>
OutStream& operator>>(OutStream& out, int8_t& data) {
  out.Read(reinterpret_cast<char*>(&data), sizeof(data));
  return out;
}

template <>
OutStream& operator>>(OutStream& out, int32_t& data) {
  out.Read(reinterpret_cast<char*>(&data), sizeof(data));
  return out;
}

template <>
OutStream& operator>>(OutStream& out, int64_t& data) {
  out.Read(reinterpret_cast<char*>(&data), sizeof(data));
  return out;
}

template <>
OutStream& operator>>(OutStream& out, uint8_t& data) {
  out.Read(reinterpret_cast<char*>(&data), sizeof(data));
  return out;
}

template <>
OutStream& operator>>(OutStream& out, uint32_t& data) {
  out.Read(reinterpret_cast<char*>(&data), sizeof(data));
  return out;
}

template <>
OutStream& operator>>(OutStream& out, uint64_t& data) {
  out.Read(reinterpret_cast<char*>(&data), sizeof(data));
  return out;
}

template <>
OutStream& operator>>(OutStream& out, Date& data) {
  out.Read(reinterpret_cast<char*>(&data), sizeof(data));
  return out;
}

template <>
OutStream& operator>>(OutStream& out, DateTime& data) {
  out.Read(reinterpret_cast<char*>(&data), sizeof(data));
  return out;
}

template <>
OutStream& operator>>(OutStream& out, std::string_view& data) {
  size_t length;
  out >> length;
  data = out.TakeSlice(length);
  return out;
}

template <>
OutStream& operator>>(OutStream& out, std::string& data) {
  size_t length;
  out >> length;
  std::string_view view = out.TakeSlice(length);
  data = std::string(view);
  return out;
}

}  // namespace ladder

#endif  // LADDER_LADDER_OUT_STREAM_H_