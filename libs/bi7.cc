#include "context.h"
#include "dataflow.h"
#include "graph/graph_db.h"
#include "graph/types.h"
#include "in_stream.h"
#include "operator.h"
#include "out_stream.h"

namespace ladder {

class Resource {};

class GraphStore {
 public:
  void reset(const GraphDB& graph_db) {}

  uint64_t get_vertices_num(label_t label) const { return 0; }

  bool is_valid_vertex(label_t label, uint64_t v) const { return false; }

  uint64_t get_global_id(label_t label, uint64_t v) const { return 0; }
};

class GraphJobContext : public IGraphJobContext {
 public:
  void reset(const GraphDB& graph_db) override { graph.reset(graph_db); }

  Resource resource;
  GraphStore graph;
};

class Stream1 : public IOperator {
 public:
  void Execute(IContext& context, OutStream& input,
               std::vector<InStream>& output) override {
    auto& casted_context = dynamic_cast<GraphJobContext&>(context);
    uint64_t vnum = casted_context.graph.get_vertices_num(0);
    for (uint64_t i = 0; i < vnum; ++i) {
      if (casted_context.graph.is_valid_vertex(0, i)) {
        output[casted_context.worker_id()]
            << casted_context.graph.get_global_id(0, i);
      }
    }
  }
};

}  // namespace ladder

extern "C" void* create_dataflow(int worker_id, int worker_num) {
  auto dataflow = new ladder::DataFlow();
  auto context = std::make_unique<ladder::GraphJobContext>();
  context->set_worker_id(worker_id);
  context->set_worker_num(worker_num);
  dataflow->set_context(std::move(context));
  dataflow->add_operator(std::make_unique<ladder::Stream1>());
  return dataflow;
}