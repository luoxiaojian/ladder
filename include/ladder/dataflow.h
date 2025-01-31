#ifndef LADDER_LADDER_DATAFLOW_H_
#define LADDER_LADDER_DATAFLOW_H_

#include <assert.h>

#include <memory>
#include <queue>
#include <thread>
#include <vector>

#include "ladder/communicator.h"
#include "ladder/context.h"
#include "ladder/operator.h"

namespace ladder {

class DataFlowRunner;

class DataFlow {
 public:
  DataFlow() : lvl_(0), sink_op_(-1) {}
  ~DataFlow() = default;

  int add_nullary_operator(std::unique_ptr<INullaryOperator>&& op) {
    operators_.emplace_back(std::move(op));
    std::vector<int> cur;
    upstreams_.emplace_back(std::move(cur));

    return operators_.size() - 1;
  }

  int add_unary_operator(std::unique_ptr<IUnaryOperator>&& op, int upstream) {
    operators_.emplace_back(std::move(op));
    std::vector<int> cur;
    cur.push_back(upstream);
    upstreams_.emplace_back(std::move(cur));

    return operators_.size() - 1;
  }

  int add_binary_operator(std::unique_ptr<IBinaryOperator>&& op, int upstream0,
                          int upstream1) {
    operators_.emplace_back(std::move(op));
    std::vector<int> cur;
    cur.push_back(upstream0);
    cur.push_back(upstream1);
    upstreams_.emplace_back(std::move(cur));

    return operators_.size() - 1;
  }

  void sink(int op_id) {
    sink_op_ = op_id;
    generate_order();
  }

 private:
  void generate_order() {
    std::vector<std::set<int>> deps;
    int op_num = upstreams_.size();
    deps.resize(op_num);
    std::set<int> not_scheduled;

    std::deque<int> que;
    que.push_back(sink_op_);
    not_scheduled.insert(sink_op_);
    while (!que.empty()) {
      auto cur = que.front();
      que.pop_front();

      for (auto v : upstreams_[cur]) {
        if (not_scheduled.find(v) == not_scheduled.end()) {
          not_scheduled.insert(v);
          que.push_back(v);
        }
      }
    }

    for (auto v : not_scheduled) {
      for (auto u : upstreams_[v]) {
        deps[v].insert(u);
      }
    }

    std::vector<std::vector<int>> batches;

    while (true) {
      std::set<int> cur_batch;
      for (auto v : not_scheduled) {
        if (deps[v].empty()) {
          cur_batch.insert(v);
        }
      }
      if (cur_batch.empty()) {
        break;
      }
      std::vector<int> batch_vec;
      for (auto v : cur_batch) {
        not_scheduled.erase(v);
        batch_vec.push_back(v);
      }
      batches.emplace_back(std::move(batch_vec));
      if (not_scheduled.empty()) {
        break;
      }
      for (auto v : not_scheduled) {
        auto& dep = deps[v];
        for (auto c : cur_batch) {
          dep.erase(c);
        }
      }
    }

    output_refcount_.clear();
    output_refcount_.resize(op_num, 0);
    order_.clear();
    for (auto& batch : batches) {
      for (auto v : batch) {
        order_.push_back(v);
        for (auto u : upstreams_[v]) {
          output_refcount_[u] += 1;
        }
      }
    }
  }

  friend class DataFlowRunner;

  std::vector<std::unique_ptr<IOperator>> operators_;
  std::vector<std::vector<int>> upstreams_;
  std::vector<int> order_;
  std::vector<int> output_refcount_;
  int sink_op_;
  int lvl_;
};

class MessageSlot {
 public:
  MessageSlot() : ref_count_(0) {}
  void init(int ref_count) { ref_count_ = ref_count; }

  const std::vector<std::vector<char>>& get(int worker_id) const {
    return messages_.get(worker_id);
  }

  MessageBatch& get_batch() { return messages_; }
  const MessageBatch& get_batch() const { return messages_; }

  void deref() {
    --ref_count_;
    if (ref_count_ == 0) {
      messages_.clear();
    }
  }

  void ingest(MessageBatch&& messages) { messages_ = std::move(messages); }

 private:
  MessageBatch messages_;
  int ref_count_;
};

class DataFlowRunner {
 public:
  DataFlowRunner(const DataFlow& dataflow, std::vector<IContext*>& contexts,
                 const CommSpec& comm_spec)
      : dataflow_(dataflow),
        contexts_(contexts),
        comm_spec_(comm_spec),
        cur_step_(0) {
    slots_.resize(dataflow.operators_.size());
  }

  MessageBatch StepStart() {
    int global_worker_num = comm_spec_.global_worker_num();
    MessageBatch ret(global_worker_num);
    if (cur_step_ == dataflow_.order_.size()) {
      return ret;
    }
    int cur_op = dataflow_.order_[cur_step_];
    std::vector<std::queue<std::pair<int, std::vector<char>>>> message_queues(
        comm_spec_.local_worker_num());

    OperatorType op_type = dataflow_.operators_[cur_op]->type();
    if (op_type == OperatorType::kNullary) {
      std::vector<std::thread> threads;
      for (int i = 0; i < comm_spec_.local_worker_num(); ++i) {
        threads.emplace_back(
            [&, this](int tid) {
              std::vector<InStream> output(global_worker_num);
              dynamic_cast<INullaryOperator*>(
                  dataflow_.operators_[cur_op].get())
                  ->Execute(*contexts_[tid], output);
              for (int i = 0; i < global_worker_num; ++i) {
                if (output[i].size() != 0) {
                  message_queues[tid].emplace(i, std::move(output[i].buffer()));
                }
              }
            },
            i);
      }
      for (auto& thrd : threads) {
        thrd.join();
      }
    } else if (op_type == OperatorType::kUnary) {
      int upstream = dataflow_.upstreams_[cur_op].at(0);
      std::vector<OutStream> inputs;
      for (int i = 0; i < comm_spec_.local_worker_num(); ++i) {
        inputs.emplace_back(slots_[upstream].get(i));
      }

      std::vector<std::thread> threads;
      for (int i = 0; i < comm_spec_.local_worker_num(); ++i) {
        threads.emplace_back(
            [&, this](int tid) {
              std::vector<InStream> output(global_worker_num);
              dynamic_cast<IUnaryOperator*>(dataflow_.operators_[cur_op].get())
                  ->Execute(*contexts_[tid], inputs[tid], output);
              for (int i = 0; i < global_worker_num; ++i) {
                if (output[i].size() != 0) {
                  message_queues[tid].emplace(i, std::move(output[i].buffer()));
                }
              }
            },
            i);
      }
      for (auto& thrd : threads) {
        thrd.join();
      }

      slots_[upstream].deref();
    } else {
      assert(op_type == OperatorType::kBinary);
      int upstream0 = dataflow_.upstreams_[cur_op].at(0);
      int upstream1 = dataflow_.upstreams_[cur_op].at(1);
      std::vector<OutStream> inputs0, inputs1;
      for (int i = 0; i < comm_spec_.local_worker_num(); ++i) {
        inputs0.emplace_back(slots_[upstream0].get(i));
        inputs1.emplace_back(slots_[upstream1].get(i));
      }

      std::vector<std::thread> threads;
      for (int i = 0; i < comm_spec_.local_worker_num(); ++i) {
        threads.emplace_back(
            [&](int tid) {
              std::vector<InStream> output(global_worker_num);
              dynamic_cast<IBinaryOperator*>(dataflow_.operators_[cur_op].get())
                  ->Execute(*contexts_[tid], inputs0[tid], inputs1[tid],
                            output);
              for (int i = 0; i < global_worker_num; ++i) {
                if (output[i].size() != 0) {
                  message_queues[tid].emplace(i, std::move(output[i].buffer()));
                }
              }
            },
            i);
      }

      for (auto& thrd : threads) {
        thrd.join();
      }

      slots_[upstream0].deref();
      slots_[upstream1].deref();
    }

    for (auto& que : message_queues) {
      while (!que.empty()) {
        auto& top = que.front();
        ret.put(top.first, std::move(top.second));
        que.pop();
      }
    }

    return ret;
  }

  void StepFinish(MessageBatch&& messages) {
    int cur_op = dataflow_.order_[cur_step_++];
    slots_[cur_op].init(dataflow_.output_refcount_[cur_op]);
    slots_[cur_op].ingest(std::move(messages));
  }

  bool Terminated() const { return cur_step_ == dataflow_.order_.size(); }

  MessageBatch& get_sink() { return slots_[dataflow_.sink_op_].get_batch(); }

 private:
  const DataFlow& dataflow_;
  std::vector<IContext*>& contexts_;
  std::vector<MessageSlot> slots_;
  CommSpec comm_spec_;
  size_t cur_step_;
};

}  // namespace ladder

#endif  // LADDER_LADDER_DATAFLOW_H_