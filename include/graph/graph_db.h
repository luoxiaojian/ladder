#ifndef LADDER_GRAPH_GRAPH_DB_H_
#define LADDER_GRAPH_GRAPH_DB_H_

#include <unordered_map>
#include <vector>

#include "glog/logging.h"
#include "graph/csr.h"
#include "graph/graph_view.h"
#include "graph/i_csr.h"
#include "graph/schema.h"
#include "graph/scsr.h"
#include "graph/vertex_map.h"
#include "property/table.h"

namespace ladder {

enum class Direction {
  kOutgoing,
  kIncoming,
};

class GraphDB {
 public:
  GraphDB() = default;
  ~GraphDB() {
    for (auto ie : ie_) {
      if (ie != nullptr) {
        delete ie;
      }
    }
    for (auto oe : oe_) {
      if (oe != nullptr) {
        delete oe;
      }
    }
  }

  void open(const std::string& prefix, int partition_id, int partition_num) {
    partition_id_ = partition_id;
    partition_num_ = partition_num;
    LOG(INFO) << "before open schema...";

    schema_.open(prefix + "/graph_schema/schema.json");
    LOG(INFO) << "after open schema...";
    vertex_label_num_ = schema_.vertex_label_num();
    edge_label_num_ = schema_.edge_label_num();

    std::string partition_binary_prefix =
        prefix + "/graph_data_bin/partition_" + std::to_string(partition_id_);

    vertex_map_.open(partition_binary_prefix + "/vm", vertex_label_num_);
    LOG(INFO) << "after open vertex map...";
    vertex_props_.resize(vertex_label_num_);
    for (label_t i = 0; i < vertex_label_num_; ++i) {
      LOG(INFO) << "vertex prop - " << (int) i;
      vertex_props_[i].open(
          partition_binary_prefix + "/vp_" + std::to_string(i),
          schema_.get_vertex_header(i));
    }
    LOG(INFO) << "after open vertex props...";

    size_t csr_list_size = static_cast<size_t>(vertex_label_num_) *
                           static_cast<size_t>(edge_label_num_) *
                           static_cast<size_t>(vertex_label_num_);
    ie_.resize(csr_list_size, nullptr);
    oe_.resize(csr_list_size, nullptr);

    for (label_t src_label = 0; src_label < vertex_label_num_; ++src_label) {
      for (label_t edge_label = 0; edge_label < edge_label_num_; ++edge_label) {
        for (label_t dst_label = 0; dst_label < vertex_label_num_;
             ++dst_label) {
          if (schema_.exist_edge_triplet(src_label, edge_label, dst_label)) {
            LOG(INFO) << "edge: src -" << (int) src_label << ", edge - "
                      << (int) edge_label << ", dst - " << (int) dst_label;
            size_t idx = edge_label_to_index(src_label, edge_label, dst_label);
            if (schema_.oe_is_single(src_label, edge_label, dst_label)) {
              oe_[idx] = new SCsr();
            } else {
              oe_[idx] = new Csr();
            }
            if (schema_.ie_is_single(src_label, edge_label, dst_label)) {
              ie_[idx] = new SCsr();
            } else {
              ie_[idx] = new Csr();
            }
            ie_[idx]->open(partition_binary_prefix + "/ie_" +
                           std::to_string(src_label) + "_" +
                           std::to_string(edge_label) + "_" +
                           std::to_string(dst_label));
            oe_[idx]->open(partition_binary_prefix + "/oe_" +
                           std::to_string(src_label) + "_" +
                           std::to_string(edge_label) + "_" +
                           std::to_string(dst_label));
            const auto& header =
                schema_.get_edge_header(src_label, edge_label, dst_label);
            if (header.size() > 0) {
              ie_props_[idx].open(partition_binary_prefix + "/iep_" +
                                      std::to_string(src_label) + "_" +
                                      std::to_string(edge_label) + "_" +
                                      std::to_string(dst_label),
                                  header);
              oe_props_[idx].open(partition_binary_prefix + "/oep_" +
                                      std::to_string(src_label) + "_" +
                                      std::to_string(edge_label) + "_" +
                                      std::to_string(dst_label),
                                  header);
            }
          }
        }
      }
    }
    LOG(INFO) << "after open csrs...";
  }

  GraphView get_graph_view(label_t src_label, label_t edge_label,
                           label_t dst_label, Direction dir) const {
    size_t idx = edge_label_to_index(src_label, edge_label, dst_label);
    if (dir == Direction::kOutgoing) {
      return GraphView(oe_[idx]);
    } else {
      return GraphView(ie_[idx]);
    }
  }

  SingleGraphView get_single_graph_view(label_t src_label, label_t edge_label,
                                        label_t dst_label,
                                        Direction dir) const {
    size_t idx = edge_label_to_index(src_label, edge_label, dst_label);
    if (dir == Direction::kOutgoing) {
      return SingleGraphView(oe_[idx]);
    } else {
      return SingleGraphView(ie_[idx]);
    }
  }

  const ICsr* get_csr(label_t src_label, label_t edge_label, label_t dst_label,
                      Direction dir) const {
    size_t idx = edge_label_to_index(src_label, edge_label, dst_label);
    if (dir == Direction::kOutgoing) {
      return oe_[idx];
    } else {
      return ie_[idx];
    }
  }

  const IColumn* get_vertex_property(label_t label,
                                     const std::string& name) const {
    return vertex_props_[label].get_column_by_name(name);
  }

  const VertexMap& vertex_map() const { return vertex_map_; }

  const Schema& schema() const { return schema_; }

 private:
  size_t edge_label_to_index(label_t src_label, label_t edge_label,
                             label_t dst_label) const {
    return static_cast<size_t>(src_label) *
               static_cast<size_t>(edge_label_num_) *
               static_cast<size_t>(vertex_label_num_) +
           static_cast<size_t>(dst_label) *
               static_cast<size_t>(edge_label_num_) +
           static_cast<size_t>(edge_label);
  }

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
  Schema schema_;
};

}  // namespace ladder

#endif  // LADDER_GRAPH_GRAPH_DB_H_