#ifndef LADDER_GRAPH_SCHEMA_H
#define LADDER_GRAPH_SCHEMA_H

#include <map>
#include <set>
#include <string>
#include <tuple>
#include <unordered_map>
#include <vector>

#include "graph/types.h"
#include "property/types.h"

namespace ladder {

enum class PartitionType {
  kDynamic,
  kStatic,
};

struct LabelTriplet {
 public:
  LabelTriplet(label_t src, label_t edge, label_t dst)
      : src_label(src), edge_label(edge), dst_label(dst) {}
  ~LabelTriplet() = default;

  inline bool operator==(const LabelTriplet& rhs) const {
    return src_label == rhs.src_label && edge_label == rhs.edge_label &&
           dst_label == rhs.dst_label;
  }

  inline bool operator<(const LabelTriplet& rhs) const {
    if (src_label != rhs.src_label) {
      return src_label < rhs.src_label;
    }
    if (edge_label != rhs.edge_label) {
      return edge_label < rhs.edge_label;
    }
    return dst_label < rhs.dst_label;
  }

  label_t src_label;
  label_t edge_label;
  label_t dst_label;
};

class Schema {
 public:
  Schema() = default;
  ~Schema() = default;

  void open(const std::string& json_file);

 private:
  std::unordered_map<std::string, label_t> vertex_type_to_id_;
  std::unordered_map<std::string, label_t> edge_type_to_id_;

  std::vector<std::unordered_map<std::string, std::pair<DataType, size_t>>>
      vertex_prop_meta_;
  std::vector<std::vector<std::string, DataType>> vertex_prop_vec_;
  std::vector<PartitionType> vertex_partition_type_;

  std::map<LabelTriplet,
           std::unordered_map<std::string, std::pair<DataType, size_t>>>
      edge_prop_meta_;
  std::map<LabelTriplet, std::vector<std::pair<std::string, DataType>>>
      edge_prop_vec_;
  std::set<LabelTriplet> ie_single_;
  std::set<LabelTriplet> oe_single_;
};

}  // namespace ladder

#endif  // LADDER_GRAPH_SCHEMA_H
