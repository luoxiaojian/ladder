#include <queue>

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

class Stream1 : public IOperator {
 public:
  void Execute(IContext& context, OutStream& input,
               std::vector<InStream>& output) override {
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

class Stream2 : public IOperator {
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

class Stream3 : public IOperator {
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

class Stream4 : public IOperator {
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
        label_t vertex_label = graph.get_label_id(message);
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

class Stream5 : public IOperator {
  struct Compare {
    bool operator()(const std::pair<gid_t, int>& a,
                    const std::pair<gid_t, int>& b) {
      return a.second < b.second;
    }
  };

 public:
  void Execute(IContext& context, OutStream& input,
               std::vector<InStream>& output) override {
    std::unordered_map<gid_t, int> tag_count;
    gid_t tag, count;
    while (!input.empty()) {
      input >> tag >> count;
      tag_count[tag] += count;
    }

    std::priority_queue<std::pair<gid_t, int>,
                        std::vector<std::pair<gid_t, int>>, Compare>
        pq;
    for (auto& pair : tag_count) {
      if (pq.size() < 100) {
        pq.push(pair);
      } else {
        if (pq.top().second < pair.second) {
          pq.pop();
          pq.push(pair);
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

class Stream6 : public IOperator {
  struct Compare {
    bool operator()(const std::pair<gid_t, int>& a,
                    const std::pair<gid_t, int>& b) {
      return a.second < b.second;
    }
  };

 public:
  void Execute(IContext& context, OutStream& input,
               std::vector<InStream>& output) override {
    std::priority_queue<std::pair<gid_t, int>,
                        std::vector<std::pair<gid_t, int>>, Compare>
        pq;

    gid_t tag, count;
    while (!input.empty()) {
      input >> tag >> count;

      if (pq.size() < 100) {
        pq.push(std::make_pair(tag, count));
      } else {
        if (pq.top().second < count) {
          pq.pop();
          pq.push(std::make_pair(tag, count));
        }
      }
    }

    std::vector<std::pair<gid_t, int>> reversed_pq;
    while (!pq.empty()) {
      reversed_pq.push_back(pq.top());
      pq.pop();
    }

    auto& self_output = output[context.global_worker_id()];
    for (auto it = reversed_pq.rbegin(); it != reversed_pq.rend(); ++it) {
      self_output << it->first << it->second;
    }
  }
};

}  // namespace ladder

extern "C" void* create_dataflow() {
  auto dataflow = new ladder::DataFlow();
  dataflow->add_operator(std::make_unique<ladder::Stream1>());
  dataflow->add_operator(std::make_unique<ladder::Stream2>());
  dataflow->add_operator(std::make_unique<ladder::Stream3>());
  dataflow->add_operator(std::make_unique<ladder::Stream4>());
  dataflow->add_operator(std::make_unique<ladder::Stream5>());
  dataflow->add_operator(std::make_unique<ladder::Stream6>());
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