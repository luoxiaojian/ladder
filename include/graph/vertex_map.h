#ifndef LADDER_GRAPH_VERTEX_MAP_H
#define LADDER_GRAPH_VERTEX_MAP_H

#include <vector>

#include "graph/indexer.h"

namespace ladder {

int get_partition(gid_t global_id, int worker_num, int server_num) {
  size_t magic_num = global_id / server_num;
  return (global_id - magic_num * server_num) * worker_num +
         magic_num % worker_num;
}

class VertexMap {
  static constexpr size_t LABEL_SHIFT_BITS =
      8 * (sizeof(gid_t) - sizeof(label_t));
  static constexpr gid_t OID_MASK = (1ULL << LABEL_SHIFT_BITS) - 1;

 public:
  VertexMap() = default;
  ~VertexMap() = default;

  void open(const std::string& prefix, label_t num_labels) {
    indexers_.resize(num_labels);
    vertices_num_.resize(num_labels);
    for (label_t i = 0; i < num_labels; ++i) {
      indexers_[i].open(prefix + "_" + std::to_string(i));
      vertices_num_[i] = indexers_[i].size();
    }
  }

  inline label_t get_label_id(gid_t global_id) const {
    return static_cast<label_t>(global_id >> LABEL_SHIFT_BITS);
  }

  inline bool get_internal_id(gid_t global_id, vertex_t& internal_id) const {
    label_t label = get_label_id(global_id);
    return indexers_[label].get_index(global_id, internal_id);
  }

  bool get_global_id(label_t label, vertex_t internal_id,
                     gid_t& global_id) const {
    return indexers_[label].get_key(internal_id, global_id);
  }

  bool get_original_id(label_t label, vertex_t internal_id,
                       gid_t& original_id) const {
    gid_t global_id;
    if (get_global_id(label, internal_id, global_id)) {
      original_id = get_original_id(global_id);
      return true;
    } else {
      return false;
    }
  }

  gid_t get_original_id(gid_t global_id) const { return global_id & OID_MASK; }

  size_t get_vertices_num(label_t label) const { return vertices_num_[label]; }

  bool is_valid_vertex(label_t label, vertex_t v) const {
    return v < indexers_[label].size();
  }

 private:
  label_t label_num_;
  std::vector<size_t> vertices_num_;
  std::vector<Indexer> indexers_;

  // std::vector<std::vector<bool>> tombs_;
};

}  // namespace ladder

#endif  // LADDER_GRAPH_VERTEX_MAP_H
