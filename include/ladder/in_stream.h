#ifndef LADDER_LADDER_IN_STREAM_H_
#define LADDER_LADDER_IN_STREAM_H_

#include <vector>

namespace ladder {

class InStream {
 public:
  InStream() = default;
  ~InStream() = default;

  size_t size() const { return buffer_.size(); }

  void write(const char* data, size_t size) {
    buffer_.insert(buffer_.end(), data, data + size);
  }

  std::vector<char>& buffer() { return buffer_; }
  const std::vector<char>& buffer() const { return buffer_; }

 private:
  std::vector<char> buffer_;
};

template <typename T>
InStream& operator<<(InStream& in, const T& data) {
  std::cerr << "Unsupported type" << std::endl;
  return in;
}

template <>
InStream& operator<<(InStream& in, const int8_t& data) {
  in.write(reinterpret_cast<const char*>(&data), sizeof(data));
  return in;
}

template <>
InStream& operator<<(InStream& in, const int32_t& data) {
  in.write(reinterpret_cast<const char*>(&data), sizeof(data));
  return in;
}

template <>
InStream& operator<<(InStream& in, const int64_t& data) {
  in.write(reinterpret_cast<const char*>(&data), sizeof(data));
  return in;
}

template <>
InStream& operator<<(InStream& in, const uint8_t& data) {
  in.write(reinterpret_cast<const char*>(&data), sizeof(data));
  return in;
}

template <>
InStream& operator<<(InStream& in, const uint32_t& data) {
  in.write(reinterpret_cast<const char*>(&data), sizeof(data));
  return in;
}

template <>
InStream& operator<<(InStream& in, const uint64_t& data) {
  in.write(reinterpret_cast<const char*>(&data), sizeof(data));
  return in;
}

template <>
InStream& operator<<(InStream& in, const Date& data) {
  in.write(reinterpret_cast<const char*>(&data), sizeof(data));
  return in;
}

template <>
InStream& operator<<(InStream& in, const DateTime& data) {
  in.write(reinterpret_cast<const char*>(&data), sizeof(data));
  return in;
}

template <>
InStream& operator<<(InStream& in, const std::string_view& data) {
  size_t length = data.size();
  in << length;
  in.write(reinterpret_cast<const char*>(data.data()), length);
  return in;
}

template <>
InStream& operator<<(InStream& in, const std::string& data) {
  size_t length = data.size();
  in << length;
  in.write(reinterpret_cast<const char*>(data.data()), length);
  return in;
}

}  // namespace ladder

#endif  // LADDER_LADDER_IN_STREAM_H_