#ifndef LADDER_GRAPH_CSR_H
#define LADDER_GRAPH_CSR_H

#include <string>
#include <vector>

#include "graph/i_csr.h"
#include "graph/types.h"
#include "utils.h"

namespace ladder {

class Csr : public ICsr {
 public:
  Csr() = default;
  ~Csr() = default;

  void open(const std::string& prefix) {
    std::string nbr_list_fname = prefix + "_nbrs";
    load_from_file(nbr_list_fname, neighbors_);

    std::string offset_fname = prefix + "_offsets";
    load_from_file(offset_fname, offsets_);

    std::string degree_fname = prefix + "_degree";
    load_from_file(degree_fname, degree_);

    std::string meta_fname = prefix + "_meta";
    std::vector<size_t> meta;
    load_from_file(meta_fname, meta);
    edge_num_ = meta[0];
  }

  inline size_t vertex_num() override { return offsets_.size(); }

  inline size_t edge_num() override { return edge_num_; }

  inline int degree(vertex_t u) override { return degree_[u]; }

  inline AdjList get_edges(vertex_t u) override {
    return AdjList(&neighbors_[offsets_[u]], degree(u));
  }

  inline AdjOffsetList get_edges_with_offset(vertex_t u) override {
    size_t start_offset = offsets_[u];
    return AdjOffsetList(&neighbors_[start_offset], degree(u), start_offset);
  }

 private:
  std::vector<gid_t> neighbors_;
  std::vector<size_t> offsets_;
  std::vector<int> degree_;

  size_t edge_num_;
};

}  // namespace ladder

#endif  // LADDER_GRAPH_CSR_H
