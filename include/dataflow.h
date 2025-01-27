#ifndef LADDER_DATAFLOW_H_
#define LADDER_DATAFLOW_H_

#include <memory>
#include <vector>

#include "context.h"
#include "operator.h"

namespace ladder {

class DataFlow {
 public:
  DataFlow() : ctx_(nullptr) {}
  ~DataFlow() = default;

  IContext& get_context() { return *ctx_; }
  const IContext& get_context() const { return *ctx_; }
  void set_context(std::unique_ptr<IContext>&& ctx) { ctx_ = std::move(ctx); }

  size_t operator_size() const { return operators_.size(); }
  IOperator& get_operator(size_t idx) { return *operators_[idx]; }
  const IOperator& get_operator(size_t idx) const { return *operators_[idx]; }

  void add_operator(std::unique_ptr<IOperator>&& op) {
    operators_.push_back(std::move(op));
  }

 private:
  std::unique_ptr<IContext> ctx_;
  std::vector<std::unique_ptr<IOperator>> operators_;
};

}  // namespace ladder

#endif  // LADDER_DATAFLOW_H_