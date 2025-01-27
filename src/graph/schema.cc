#include "graph/schema.h"

#include <fstream>
#include <iostream>

#include "nlohmann_json/json.hpp"

using json = nlohmann::json;

namespace ladder {

DataType json_type_str_to_enum(const std::string& val) {
  if (val == "String") {
    return DataType::kString;
  } else if (val == "DateTime") {
    return DataType::kDateTime;
  } else if (val == "Date") {
    return DataType::kDate;
  } else if (val == "LCString") {
    return DataType::kLCString;
  } else if (val == "Int32") {
    return DataType::kInt32;
  } else {
    std::cerr << "Error: unsupported data type " << val << std::endl;
    return DataType::kNull;
  }
}

PartitionType json_partition_str_to_enum(const std::string& val) {
  if (val == "Dynamic") {
    return PartitionType::kDynamic;
  } else if (val == "Static") {
    return PartitionType::kStatic;
  } else {
    std::cerr << "Error: unsupported partition type " << val << std::endl;
    return PartitionType::kDynamic;
  }
}

void Schema::open(const std::string& json_file) {
  std::ifstream file(json_file);
  if (!file.is_open()) {
    std::cerr << "Error: cannot open file " << json_file << std::endl;
    return;
  }

  std::string json_str((std::istreambuf_iterator<char>(file)),
                       std::istreambuf_iterator<char>());
  file.close();

  vertex_type_to_id_.clear();
  vertex_prop_meta_.clear();
  vertex_prop_vec_.clear();
  vertex_partition_type_.clear();

  try {
    json j = json::parse(json_str);
    label_t vertex_label = 0;
    for (auto& vertex_node : j["vertex"]) {
      std::string label_name = vertex_node["label"].get<std::string>();
      // std::cout << "vertex label - " << label_name << std::endl;
      vertex_type_to_id_[label_name] = vertex_label++;
      std::string partition_type =
          vertex_node["partition_type"].get<std::string>();
      vertex_partition_type_.push_back(
          json_partition_str_to_enum(partition_type));
      // std::cout << "partition type - " << partition_type << std::endl;

      std::vector<std::pair<std::string, DataType>> prop_vec;
      std::unordered_map<std::string, std::pair<DataType, size_t>> prop_meta;
      for (auto& prop_node : vertex_node["properties"]) {
        std::string prop_name = prop_node["name"].get<std::string>();
        // std::cout << "property name - " << prop_name << std::endl;
        std::string prop_type = prop_node["data_type"].get<std::string>();
        // std::cout << "property type - " << prop_type << std::endl;
        DataType prop_type_enum = json_type_str_to_enum(prop_type);

        prop_vec.push_back(std::make_pair(prop_name, prop_type_enum));
        prop_meta[prop_name] = {prop_type_enum, prop_vec.size() - 1};
      }

      vertex_prop_vec_.push_back(std::move(prop_vec));
      vertex_prop_meta_.push_back(std::move(prop_meta));
    }

    edge_type_to_id_.clear();
    edge_prop_meta_.clear();
    edge_prop_vec_.clear();
    ie_single_.clear();
    oe_single_.clear();
    label_t cur_edge_label = 0;
    for (auto& edge_node : j["edge"]) {
      std::string src_label_name = edge_node["src_label"].get<std::string>();
      std::string edge_label_name = edge_node["label"].get<std::string>();
      std::string dst_label_name = edge_node["dst_label"].get<std::string>();
      // std::cout << "edge triplet: " << src_label_name << " - "
      //           << edge_label_name << " - " << dst_label_name << std::endl;

      if (vertex_type_to_id_.find(src_label_name) == vertex_type_to_id_.end() ||
          vertex_type_to_id_.find(dst_label_name) == vertex_type_to_id_.end()) {
        std::cerr << "Error: vertex label not found" << std::endl;
        continue;
      }
      label_t src_label = vertex_type_to_id_[src_label_name];
      label_t dst_label = vertex_type_to_id_[dst_label_name];
      if (edge_type_to_id_.find(edge_label_name) == edge_type_to_id_.end()) {
        edge_type_to_id_[edge_label_name] = cur_edge_label++;
      }
      label_t edge_label = edge_type_to_id_[edge_label_name];

      LabelTriplet triplet(src_label, edge_label, dst_label);
      if (edge_node.contains("oe_strategy")) {
        std::string oe_strategy = edge_node["oe_strategy"].get<std::string>();
        if (oe_strategy == "Single") {
          oe_single_.insert(triplet);
        }
      }
      if (edge_node.contains("ie_strategy")) {
        std::string ie_strategy = edge_node["ie_strategy"].get<std::string>();
        if (ie_strategy == "Single") {
          ie_single_.insert(triplet);
        }
      }

      std::unordered_map<std::string, std::pair<DataType, size_t>> prop_meta;
      std::vector<std::pair<std::string, DataType>> prop_vec;
      if (edge_node.contains("properties")) {
        for (auto& prop_node : edge_node["properties"]) {
          std::string prop_name = prop_node["name"].get<std::string>();
          std::string prop_type = prop_node["data_type"].get<std::string>();
          DataType prop_type_enum = json_type_str_to_enum(prop_type);

          prop_vec.push_back(std::make_pair(prop_name, prop_type_enum));
          prop_meta[prop_name] = {prop_type_enum, prop_vec.size() - 1};
        }
      }
      edge_prop_meta_.insert({triplet, prop_meta});
      edge_prop_vec_.insert({triplet, prop_vec});
    }
  } catch (const json::exception& e) {
    std::cerr << "Error: " << e.what() << std::endl;
  }
}

}  // namespace ladder
