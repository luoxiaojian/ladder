#include <assert.h>

#include <queue>
#include <string_view>

#include "graph/graph_db.h"
#include "graph/graph_view.h"
#include "graph/types.h"
#include "ladder/context.h"
#include "ladder/dataflow.h"
#include "ladder/in_stream.h"
#include "ladder/operator.h"
#include "ladder/out_stream.h"

namespace ladder {

class Resource {};

class GraphStore {
 public:
  GraphStore(const GraphDB& graph_db)
      : subgraph_2_1_7_in(graph_db.get_csr(2, 1, 7, Direction::kIncoming)),
        subgraph_2_3_2_in(graph_db.get_csr(2, 3, 2, Direction::kIncoming)),
        subgraph_3_1_7_in(graph_db.get_csr(3, 1, 7, Direction::kIncoming)),
        subgraph_2_1_7_out(graph_db.get_csr(2, 1, 7, Direction::kOutgoing)),
        subgraph_2_3_3_in(graph_db.get_csr(2, 3, 3, Direction::kIncoming)),
        property_name_7(*dynamic_cast<const StringColumn*>(
            graph_db.get_vertex_property(7, "name"))),
        graph_db_(graph_db) {}
  ~GraphStore() {}

  size_t get_vertices_num(label_t label) const {
    return graph_db_.vertex_map().get_vertices_num(label);
  }

  bool is_valid_vertex(label_t label, vertex_t v) const {
    return graph_db_.vertex_map().is_valid_vertex(label, v);
  }

  bool get_global_id(label_t label, vertex_t v, gid_t& global_id) const {
    return graph_db_.vertex_map().get_global_id(label, v, global_id);
  }

  bool get_internal_id(gid_t global_id, vertex_t& internal_id) const {
    return graph_db_.vertex_map().get_internal_id(global_id, internal_id);
  }

  label_t get_label_id(gid_t global_id) const {
    return graph_db_.vertex_map().get_label_id(global_id);
  }

  GraphView subgraph_2_1_7_in;
  GraphView subgraph_2_3_2_in;
  GraphView subgraph_3_1_7_in;
  GraphView subgraph_2_1_7_out;
  GraphView subgraph_2_3_3_in;

  const StringColumn& property_name_7;

  const GraphDB& graph_db_;
};

class GraphJobContext : public IContext {
 public:
  GraphJobContext(const GraphDB& graph_db) : graph(graph_db) {}

  Resource resource;
  GraphStore graph;
};

class Stream1 : public INullaryOperator {
 public:
  void Execute(IContext& context, std::vector<InStream>& output) override {
    auto& casted_context = dynamic_cast<GraphJobContext&>(context);
    auto& graph = casted_context.graph;
    size_t vnum = graph.get_vertices_num(7);
    std::string tag = casted_context.get_param("tag");
    auto& self_output = output[casted_context.global_worker_id()];
    for (vertex_t i = 0; i < vnum; ++i) {
      if (graph.is_valid_vertex(7, i)) {
        std::string_view tag_name = graph.property_name_7.get(i);
        if (tag_name == tag) {
          gid_t vertex_global_id;
          if (graph.get_global_id(7, i, vertex_global_id)) {
            self_output << vertex_global_id;
          }
        }
      }
    }
  }
};

class Stream2 : public IUnaryOperator {
 public:
  void Execute(IContext& context, OutStream& input,
               std::vector<InStream>& output) override {
    auto& casted_context = dynamic_cast<GraphJobContext&>(context);
    auto& graph = casted_context.graph;

    gid_t cur_global_id;
    while (!input.empty()) {
      input >> cur_global_id;
      vertex_t vertex_id;
      if (graph.get_internal_id(cur_global_id, vertex_id)) {
        for (auto& e : graph.subgraph_2_1_7_in.get_partial_edges(
                 vertex_id, casted_context.local_worker_id(),
                 casted_context.local_worker_num())) {
          int target_worker =
              get_partition(e, casted_context.local_worker_num(),
                            casted_context.server_num());
          output[target_worker] << cur_global_id << e;
        }
        for (auto& e : graph.subgraph_3_1_7_in.get_partial_edges(
                 vertex_id, casted_context.local_worker_id(),
                 casted_context.local_worker_num())) {
          int target_worker =
              get_partition(e, casted_context.local_worker_num(),
                            casted_context.server_num());
          output[target_worker] << cur_global_id << e;
        }
      }
    }
  }
};

class Stream3 : public IUnaryOperator {
 public:
  void Execute(IContext& context, OutStream& input,
               std::vector<InStream>& output) override {
    auto& casted_context = dynamic_cast<GraphJobContext&>(context);
    auto& graph = casted_context.graph;

    gid_t tag, message;
    while (!input.empty()) {
      input >> tag >> message;
      vertex_t vertex_id;
      if (graph.get_internal_id(message, vertex_id)) {
        label_t vertex_label = graph.get_label_id(message);
        if (vertex_label == 2) {
          for (auto& e : graph.subgraph_2_3_2_in.get_edges(vertex_id)) {
            int target_worker =
                get_partition(e, casted_context.local_worker_num(),
                              casted_context.server_num());
            output[target_worker] << tag << message << e;
          }
        } else {
          assert(vertex_label == 3);
          for (auto& e : graph.subgraph_2_3_3_in.get_edges(vertex_id)) {
            int target_worker =
                get_partition(e, casted_context.local_worker_num(),
                              casted_context.server_num());
            output[target_worker] << tag << message << e;
          }
        }
      }
    }
  }
};

class Stream4 : public IUnaryOperator {
 public:
  void Execute(IContext& context, OutStream& input,
               std::vector<InStream>& output) override {
    auto& casted_context = dynamic_cast<GraphJobContext&>(context);
    auto& graph = casted_context.graph;

    std::unordered_map<gid_t, int> tag_count;

    gid_t tag, message, reply;
    while (!input.empty()) {
      input >> tag >> message >> reply;
      vertex_t vertex_id;
      if (graph.get_internal_id(reply, vertex_id)) {
        label_t vertex_label = graph.get_label_id(reply);
        assert(vertex_label == 2);
        bool not_has_tag = true;
        for (auto& e : graph.subgraph_2_1_7_out.get_edges(vertex_id)) {
          if (e == tag) {
            not_has_tag = false;
            break;
          }
        }
        if (not_has_tag) {
          for (auto& e : graph.subgraph_2_1_7_out.get_edges(vertex_id)) {
            tag_count[e] += 1;
          }
        }
      }
    }
    for (auto& pair : tag_count) {
      int target_worker =
          get_partition(pair.first, casted_context.local_worker_num(),
                        casted_context.server_num());
      output[target_worker] << pair.first << pair.second;
    }
  }
};

class Stream5 : public IUnaryOperator {
  struct Compare {
    bool operator()(const std::pair<int, std::string_view>& a,
                    const std::pair<int, std::string_view>& b) {
      if (a.first == b.first) {
        return a.second < b.second;
      } else {
        return a.first > b.first;
      }
    }
  };

 public:
  void Execute(IContext& context, OutStream& input,
               std::vector<InStream>& output) override {
    auto& casted_context = dynamic_cast<GraphJobContext&>(context);
    auto& graph = casted_context.graph;

    std::unordered_map<gid_t, int> tag_count;
    gid_t tag;
    int count;
    while (!input.empty()) {
      input >> tag >> count;
      tag_count[tag] += count;
    }

    std::priority_queue<std::pair<int, std::string_view>,
                        std::vector<std::pair<int, std::string_view>>, Compare>
        pq;
    vertex_t tag_id;
    for (auto& pair : tag_count) {
      if (pq.size() < 100) {
        if (graph.get_internal_id(pair.first, tag_id)) {
          pq.emplace(pair.second, graph.property_name_7.get(tag_id));
        }
      } else {
        if (pq.top().first < pair.second) {
          if (graph.get_internal_id(pair.first, tag_id)) {
            pq.pop();
            pq.emplace(pair.second, graph.property_name_7.get(tag_id));
          }
        } else if (pq.top().first == pair.second) {
          if (graph.get_internal_id(pair.first, tag_id)) {
            std::string_view tag_name = graph.property_name_7.get(tag_id);
            if (tag_name < pq.top().second) {
              pq.pop();
              pq.emplace(pair.second, tag_name);
            }
          }
        }
      }
    }

    auto& root_output = output[0];
    while (!pq.empty()) {
      root_output << pq.top().first << pq.top().second;
      pq.pop();
    }
  }
};

class Stream6 : public IUnaryOperator {
  struct Compare {
    bool operator()(const std::pair<int, std::string_view>& a,
                    const std::pair<int, std::string_view>& b) {
      if (a.first == b.first) {
        return a.second < b.second;
      } else {
        return a.first > b.first;
      }
    }
  };

 public:
  void Execute(IContext& context, OutStream& input,
               std::vector<InStream>& output) override {
    std::priority_queue<std::pair<int, std::string_view>,
                        std::vector<std::pair<int, std::string_view>>, Compare>
        pq;

    std::string_view tag;
    int count;
    while (!input.empty()) {
      input >> count >> tag;

      if (pq.size() < 100) {
        pq.emplace(count, tag);
      } else {
        if (pq.top().first < count) {
          pq.pop();
          pq.emplace(count, tag);
        } else if (pq.top().first == count) {
          if (pq.top().second > tag) {
            pq.pop();
            pq.emplace(count, tag);
          }
        }
      }
    }

    std::vector<std::pair<int, std::string_view>> reversed_pq;
    while (!pq.empty()) {
      reversed_pq.push_back(pq.top());
      pq.pop();
    }

    auto& self_output = output[context.global_worker_id()];
    for (auto it = reversed_pq.rbegin(); it != reversed_pq.rend(); ++it) {
      std::string ret =
          std::to_string(it->first) + "|" + std::string(it->second);
      self_output << ret;
      // self_output << it->first << it->second;
    }
  }
};

}  // namespace ladder

extern "C" void* create_dataflow() {
  auto dataflow = new ladder::DataFlow();
  int op_1 =
      dataflow->add_nullary_operator(std::make_unique<ladder::Stream1>());
  int op_2 =
      dataflow->add_unary_operator(std::make_unique<ladder::Stream2>(), op_1);
  int op_3 =
      dataflow->add_unary_operator(std::make_unique<ladder::Stream3>(), op_2);
  int op_4 =
      dataflow->add_unary_operator(std::make_unique<ladder::Stream4>(), op_3);
  int op_5 =
      dataflow->add_unary_operator(std::make_unique<ladder::Stream5>(), op_4);
  int op_6 =
      dataflow->add_unary_operator(std::make_unique<ladder::Stream6>(), op_5);
  dataflow->sink(op_6);
  return dataflow;
}

extern "C" void delete_dataflow(void* dataflow) {
  delete static_cast<ladder::DataFlow*>(dataflow);
}

extern "C" void* create_context(const ladder::GraphDB* graph) {
  return new ladder::GraphJobContext(*graph);
}

extern "C" void delete_context(void* context) {
  delete static_cast<ladder::GraphJobContext*>(context);
}