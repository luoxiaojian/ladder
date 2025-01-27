#ifndef LADDER_GRAPH_SCSR_H
#define LADDER_GRAPH_SCSR_H

#include <string>
#include <vector>

#include "graph/i_csr.h"
#include "graph/types.h"
#include "utils.h"

namespace ladder {

class SCsr : public ICsr {
 public:
  SCsr() = default;
  ~SCsr() = default;

  void open(const std::string& prefix) {
    std::string nbr_list_fname = prefix + "_nbrs";
    load_from_file(nbr_list_fname, nbr_list_);

    std::vector<size_t> meta;
    std::string meta_fname = prefix + "_meta";
    load_from_file(meta_fname, meta);

    vertex_num_ = meta[0];
    edge_num_ = meta[1];
  }

  inline size_t vertex_num() override { return vertex_num_; }

  inline size_t edge_num() override { return edge_num_; }

  inline int degree(vertex_t u) override {
    return (nbr_list_[u] == std::numeric_limits<gid_t>::max()) ? 0 : 1;
  }

  inline AdjList get_edges(vertex_t u) override {
    return AdjList(&nbr_list_[u], degree(u));
  }

  inline AdjOffsetList get_edges_with_offset(vertex_t u) override {
    return AdjOffsetList(&nbr_list_[u], degree(u), u);
  }

 private:
  std::vector<gid_t> nbr_list_;
  size_t vertex_num_;
  size_t edge_num_;
};

}  // namespace ladder

#endif  // LADDER_GRAPH_SCSR_H
