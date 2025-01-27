#ifndef LADDER_GRAPH_I_CSR_H
#define LADDER_GRAPH_I_CSR_H

#include <stddef.h>

#include "graph/types.h"

namespace ladder {

class ICsr {
 public:
  virtual size_t vertex_num() = 0;
  virtual size_t edge_num() = 0;
  virtual int degree(vertex_t u) = 0;

  virtual AdjList get_edges(vertex_t u) = 0;
  virtual AdjOffsetList get_edges_with_offset(vertex_t u) = 0;
};

}  // namespace ladder

#endif  // LADDER_GRAPH_I_CSR_H
