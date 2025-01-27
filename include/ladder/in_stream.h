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

InStream& operator<<(InStream& in, int data) {
  in.write(reinterpret_cast<const char*>(&data), sizeof(data));
  return in;
}

}  // namespace ladder

#endif  // LADDER_LADDER_IN_STREAM_H_