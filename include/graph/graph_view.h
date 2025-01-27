#ifndef LADDER_GRAPH_GRAPH_VIEW_H
#define LADDER_GRAPH_GRAPH_VIEW_H

#include "graph/csr.h"
#include "graph/scsr.h"

namespace ladder {

class GraphView {
 public:
  GraphView(const ICsr* csr) : csr_(*dynamic_cast<const Csr*>(csr)) {}
  ~GraphView() = default;

  AdjList get_edges(vertex_t v) const { return csr_.get_edges(v); }
  AdjList get_partial_edges(vertex_t v, int part_i, int part_num) const {
    return csr_.get_partial_edges(v, part_i, part_num);
  }
  AdjOffsetList get_edges_with_offset(vertex_t v) const {
    return csr_.get_edges_with_offset(v);
  }

 private:
  const Csr& csr_;
};

class SingleGraphView {
 public:
  SingleGraphView(const ICsr* csr) : scsr_(*dynamic_cast<const SCsr*>(csr)) {}
  ~SingleGraphView() = default;

  AdjList get_edges(vertex_t v) const { return scsr_.get_edges(v); }
  AdjList get_partial_edges(vertex_t v, int part_i, int part_num) const {
    return scsr_.get_partial_edges(v, part_i, part_num);
  }
  AdjOffsetList get_edges_with_offset(vertex_t v) const {
    return scsr_.get_edges_with_offset(v);
  }

 private:
  const SCsr& scsr_;
};

}  // namespace ladder

#endif  // LADDER_GRAPH_GRAPH_VIEW_H
