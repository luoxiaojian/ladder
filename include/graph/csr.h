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

  void open(const std::string& prefix) override {
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

  inline size_t vertex_num() const override { return offsets_.size(); }

  inline size_t edge_num() const override { return edge_num_; }

  inline int degree(vertex_t u) const override {
    if (u >= degree_.size()) {
      return 0;
    } else {
      return degree_[u];
    }
  }

  inline AdjList get_edges(vertex_t u) const override {
    int deg = degree(u);
    return deg == 0 ? AdjList::empty() : AdjList(&neighbors_[offsets_[u]], deg);
  }

  inline AdjList get_partial_edges(vertex_t u, int part_i,
                                   int part_num) const override {
    int deg = degree(u);
    int part_size = (deg + part_num - 1) / part_num;
    int start = std::min(part_i * part_size, deg);
    int end = std::min(start + part_size, deg);
    return start == end
               ? AdjList::empty()
               : AdjList(&neighbors_[offsets_[u] + start], end - start);
  }

  inline AdjOffsetList get_edges_with_offset(vertex_t u) const override {
    int deg = degree(u);
    return deg == 0 ? AdjOffsetList::empty()
                    : AdjOffsetList(&neighbors_[offsets_[u]], deg, offsets_[u]);
  }

 private:
  std::vector<gid_t> neighbors_;
  std::vector<size_t> offsets_;
  std::vector<int> degree_;

  size_t edge_num_;
};

}  // namespace ladder

#endif  // LADDER_GRAPH_CSR_H
