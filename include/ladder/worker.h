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
      : server_id_(server_id) {
    comm_spec_.init(worker_num, server_num);
  }

  ~Worker() = default;

  void Eval(const GraphDB& graph, const App& app,
            const std::map<std::string, std::string>& params) {
    DataFlow* dataflow = app.create_dataflow();
    std::vector<IContext*> contexts;
    for (int i = 0; i < comm_spec_.local_worker_num(); ++i) {
      IContext* ctx = app.create_context(&graph);
      ctx->set_comm_spec(i, server_id_, comm_spec_);
      ctx->clear_params();
      for (auto& pair : params) {
        ctx->set_param(pair.first, pair.second);
      }
      contexts.push_back(ctx);
    }

    Communicator comm(server_id_, comm_spec_);
    DataFlowRunner runner(*dataflow, contexts, comm_spec_);

    int round = 0;
    while (!runner.Terminated()) {
      ++round;
      auto messages_out = runner.StepStart();
      auto messages_in = comm.shuffle(std::move(messages_out));
      runner.StepFinish(std::move(messages_in));
    }

    auto& output = runner.get_sink();
    for (int i = 0; i < comm_spec_.local_worker_num(); ++i) {
      if (!output.get(i).empty()) {
        std::cout << "worker - " << i << ": " << std::endl;
        OutStream os(output.get(i));
        while (!os.empty()) {
          std::string val;
          os >> val;
          std::cout << val << std::endl;
        }
      }
    }
  }

  void EvalBatch(
      const GraphDB& graph, const App& app,
      const std::vector<std::map<std::string, std::string>>& params) {
    DataFlow* dataflow = app.create_dataflow();
    Communicator comm(server_id_, comm_spec_);

    for (auto& param : params) {
      std::vector<IContext*> contexts;
      for (int i = 0; i < comm_spec_.local_worker_num(); ++i) {
        IContext* ctx = app.create_context(&graph);
        ctx->set_comm_spec(i, server_id_, comm_spec_);
        ctx->clear_params();
        for (auto& pair : param) {
          ctx->set_param(pair.first, pair.second);
        }
        contexts.push_back(ctx);
      }

      DataFlowRunner runner(*dataflow, contexts, comm_spec_);

      int round = 0;
      while (!runner.Terminated()) {
        ++round;
        auto messages_out = runner.StepStart();
        auto messages_in = comm.shuffle(std::move(messages_out));
        runner.StepFinish(std::move(messages_in));
      }

      auto& output = runner.get_sink();
      for (int i = 0; i < comm_spec_.local_worker_num(); ++i) {
        if (!output.get(i).empty()) {
          std::cout << "worker - " << i << ": " << std::endl;
          OutStream os(output.get(i));
          while (!os.empty()) {
            std::string val;
            os >> val;
            std::cout << val << std::endl;
          }
        }
      }
    }
  }

 private:
  int server_id_;
  CommSpec comm_spec_;
};

}  // namespace ladder

#endif  // LADDER_LADDER_WORKER_H
