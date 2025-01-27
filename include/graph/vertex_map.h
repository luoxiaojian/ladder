#ifndef LADDER_GRAPH_VERTEX_MAP_H
#define LADDER_GRAPH_VERTEX_MAP_H

#include <vector>

#include "graph/indexer.h"

namespace ladder {

class VertexMap {
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

 private:
  label_t label_num_;
  std::vector<size_t> vertices_num_;
  std::vector<Indexer> indexers_;

  // std::vector<std::vector<bool>> tombs_;
};

}  // namespace ladder

#endif  // LADDER_GRAPH_VERTEX_MAP_H
