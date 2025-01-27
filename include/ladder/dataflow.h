#ifndef LADDER_LADDER_DATAFLOW_H_
#define LADDER_LADDER_DATAFLOW_H_

#include <memory>
#include <vector>

#include "context.h"
#include "operator.h"

namespace ladder {

class Scope {
 public:
  Scope() = default;
  ~Scope() = default;

  void start() {
    stat_.clear();
    stat_.emplace_back(0, 0);
  }

  void next() { stat_.rbegin()->second++; }

  void enter() { stat_.emplace_back(0, 0); }

  void leave() { stat_.resize(stat_.size() - 1); }

  void feedback() {
    auto& last = &stat_.rbegin();
    last->first += 1;
    last->second = 0;
  }

  int get_depth() const { return stat_.size(); }

  int get_iteration(int lvl) const { return stat_[lvl].first; }

  int get_index(int lvl) const { return stat_[lvl].second; }

 private:
  std::vector<std::pair<int, int>> stat_;
};

class DataFlow {
 public:
  DataFlow() : lvl_(0) {}
  ~DataFlow() = default;

  void add_operator(std::unique_ptr<IOperator>&& op) {
    operators_.push_back(std::move(op));
  }

  bool step(Scope& cur, bool msg) {
    assert(cur.get_depth() == 1);
    cur.next();
    return true;
  }

  IOperator& get_operator(const Scope& scope) {
    assert(cur.get_depth() == 1);
    int idx = scope.get_index(lvl_);
    return *operators_[idx];
  }
  const IOperator& get_operator(const Scope& scope) const {
    assert(cur.get_depth() == 1);
    int idx = scope.get_index(lvl_);
    return *operators_[idx];
  }

 private:
  std::vector<std::unique_ptr<IOperator>> operators_;
  int lvl_;
};

}  // namespace ladder

#endif  // LADDER_LADDER_DATAFLOW_H_