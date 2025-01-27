#ifndef LADDER_LADDER_OUT_STREAM_H_
#define LADDER_LADDER_OUT_STREAM_H_

#include <string.h>

#include <vector>

namespace ladder {

class OutStream {
 public:
  OutStream(std::vector<std::vector<char>>&& buffers)
      : buffers_(std::move(buffers)), idx_(0), offset_(0) {}
  ~OutStream() = default;

  bool empty() const { return buffers_.empty(); }

  size_t Read(char* data, size_t size) {
    size_t remaining = buffers_[idx_].size() - offset_;
    size_t read_size = std::min(remaining, size);
    memcpy(data, buffers_[idx_].data() + offset_, read_size);
    offset_ += read_size;
    if (offset_ == buffers_[idx_].size()) {
      idx_++;
      offset_ = 0;
    }
    return read_size;
  }

 private:
  std::vector<std::vector<char>> buffers_;
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

}  // namespace ladder

#endif  // LADDER_LADDER_OUT_STREAM_H_