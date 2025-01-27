#ifndef LADDER_GRAPH_GRAPH_DB_H_
#define LADDER_GRAPH_GRAPH_DB_H_

#include <unordered_map>
#include <vector>

#include "graph/i_csr.h"
#include "graph/vertex_map.h"
#include "property/table.h"

namespace ladder {

class GraphDB {
 private:
  int partition_id_;
  int partition_num_;
  label_t vertex_label_num_;
  label_t edge_label_num_;

  std::vector<ICsr*> ie_;
  std::vector<ICsr*> oe_;

  std::vector<Table> vertex_props_;
  std::unordered_map<size_t, Table> ie_props_;
  std::unordered_map<size_t, Table> oe_props_;

  VertexMap vertex_map_;
};

}  // namespace ladder

#endif  // LADDER_GRAPH_GRAPH_DB_H_