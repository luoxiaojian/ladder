#ifndef LADDER_GRAPH_INDEXER_H
#define LADDER_GRAPH_INDEXER_H

#include <string>
#include <vector>

#include "graph/types.h"
#include "utils.h"

namespace ladder {

size_t hash_vertex(gid_t x) {
  x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9;
  x = (x ^ (x >> 27)) * 0x94d049bb133111eb;
  x = x ^ (x >> 31);
  return x;
}

class Indexer {
  static constexpr size_t INITIAL_SIZE = 16;
  static constexpr double MAX_LOAD_FACTOR = 0.875;
  static size_t calc_table_size(size_t keys_size) {
    size_t size = INITIAL_SIZE;
    while (true) {
      double load_factor = static_cast<double>(keys_size) / size;
      if (load_factor < MAX_LOAD_FACTOR) {
        break;
      }
      size *= 2;
    }
    return size;
  }

 public:
  Indexer() = default;
  ~Indexer() = default;

  void open(const std::string& prefix) {
    std::string key_fname = prefix + "_keys";
    load_from_file(key_fname, keys_);

    size_t table_size = calc_table_size(keys_.size());
    indices_.clear();
    indices_.resize(table_size, std::numeric_limits<vertex_t>::max());

    for (auto key : keys_) {
      size_t hash = hash_vertex(key) % table_size;
      while (indices_[hash] != std::numeric_limits<vertex_t>::max()) {
        hash = (hash + 1) % table_size;
      }
      indices_[hash] = key;
    }
  }

  bool get_key(size_t index, gid_t& key) const {
    if (index >= keys_.size()) {
      return false;
    }
    key = keys_[index];
    return true;
  }

  bool get_index(gid_t key, vertex_t& index) const {
    size_t table_size = indices_.size();
    size_t hash = hash_vertex(key) % table_size;
    while (indices_[hash] != key) {
      if (indices_[hash] == std::numeric_limits<vertex_t>::max()) {
        return false;
      }
      hash = (hash + 1) % table_size;
    }
    index = hash;
    return true;
  }

  size_t size() const { return keys_.size(); }

 private:
  std::vector<gid_t> keys_;
  std::vector<vertex_t> indices_;
};

}  // namespace ladder

#endif  // LADDER_GRAPH_INDEXER_H
