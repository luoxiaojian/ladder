#ifndef LADDER_GRAPH_TYPES_H_
#define LADDER_GRAPH_TYPES_H_

#include <cstdint>

namespace ladder {

using label_t = uint8_t;
using vertex_t = uint64_t;
using gid_t = uint64_t;

class AdjList {
  class iterator {
   public:
    iterator(const gid_t* ptr) : ptr_(ptr) {}

    inline gid_t get_neighbor() const { return *ptr_; }

    inline iterator& operator++() {
      ++ptr_;
      return *this;
    }

    inline iterator operator++(int) {
      iterator tmp = *this;
      ++ptr_;
      return tmp;
    }

    inline bool operator==(const iterator& rhs) const {
      return ptr_ == rhs.ptr_;
    }
    inline bool operator!=(const iterator& rhs) const {
      return ptr_ != rhs.ptr_;
    }

    inline const gid_t& operator*() const { return *ptr_; }

   private:
    const gid_t* ptr_;
  };

 public:
  AdjList(const gid_t* start, int deg) : start_(start), end_(start + deg) {}

  iterator begin() const { return iterator(start_); }
  iterator end() const { return iterator(end_); }

  static AdjList empty() { return AdjList(nullptr, 0); }

 private:
  const gid_t* start_;
  const gid_t* end_;
};

class AdjOffsetList {
  class iterator {
   public:
    iterator(const gid_t* ptr, size_t offset) : ptr_(ptr), offset_(offset) {}

    inline gid_t get_neighbor() const { return *ptr_; }
    inline size_t get_offset() const { return offset_; }

    inline iterator& operator++() {
      ++ptr_;
      ++offset_;
      return *this;
    }

    inline iterator operator++(int) {
      iterator tmp = *this;
      ++ptr_;
      ++offset_;
      return tmp;
    }

    bool operator==(const iterator& rhs) const { return ptr_ == rhs.ptr_; }
    bool operator!=(const iterator& rhs) const { return ptr_ != rhs.ptr_; }

   private:
    const gid_t* ptr_;
    size_t offset_;
  };

 public:
  AdjOffsetList(const gid_t* start, int deg, size_t offset)
      : start_(start),
        end_(start + deg),
        offset_start_(offset),
        offset_end_(offset + deg) {}

  iterator begin() const { return iterator(start_, offset_start_); }
  iterator end() const { return iterator(end_, offset_end_); }

  static AdjOffsetList empty() { return AdjOffsetList(nullptr, 0, 0); }

 private:
  const gid_t* start_;
  const gid_t* end_;
  size_t offset_start_;
  size_t offset_end_;
};

}  // namespace ladder

#endif  // LADDER_GRAPH_TYPES_H_