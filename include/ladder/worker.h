#ifndef LADDER_LADDER_WORKER_H
#define LADDER_LADDER_WORKER_H

#include <map>
#include <string>

#include "graph/graph_db.h"
#include "ladder/app.h"
#include "ladder/communicator.h"
#include "ladder/dataflow.h"

namespace ladder {

class Worker {
 public:
  Worker(int worker_num, int server_id, int server_num)
      : worker_num_(worker_num),
        server_id_(server_id),
        server_num_(server_num) {}

  ~Worker() = default;

  bool Step(std::vector<DataFlow>& dataflows, const Scope& scope) {}

  void Eval(const GraphDB& graph, const App& app,
            const std::map<std::string, std::string>& params) {
    DataFlow* dataflow = app.create_dataflow();
    std::vector<IContext*> contexts;
    for (int i = 0; i < worker_num_; ++i) {
      IContext* ctx = app.create_context(&graph);
      ctx->set_worker_id(i);
      ctx->set_worker_num(worker_num_);
      ctx->set_server_id(server_id_);
      ctx->set_server_num(server_num_);
      ctx->clear_params();
      for (auto& pair : params) {
        ctx->set_param(pair.first, pair.second);
      }
      contexts.push_back(ctx);
    }

    Communicator comm;
    DataFlowRunner runner(*dataflow);

    while (!runner.Terminated()) {
      auto messages_out = runner.StepStart();
      auto messages_in = comm.shuffle(std::move(message_out));
      runner.StepFinish(std::move(messages_in));
    }
  }

 private:
  int worker_num_;
  int server_id_;
  int server_num_;
};

}  // namespace ladder

#endif  // LADDER_LADDER_WORKER_H
