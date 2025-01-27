#ifndef LADDER_CONTEXT_H_
#define LADDER_CONTEXT_H_

#include <map>
#include <string>

#include "graph/graph_db.h"

namespace ladder {

class IContext {
 public:
  IContext() = default;
  virtual ~IContext() = default;

  void set_worker_id(int id) { worker_id_ = id; }
  void set_worker_num(int num) { worker_num_ = num; }

  int worker_id() const { return worker_id_; }
  int worker_num() const { return worker_num_; }

  void clear_params() { params_.clear(); }
  void set_param(const std::string& key, const std::string& value) {
    params_[key] = value;
  }
  const std::string& get_param(const std::string& key) const {
    return params_.at(key);
  }

 private:
  int worker_id_;
  int worker_num_;

  std::map<std::string, std::string> params_;
};

class IGraphJobContext : public IContext {
 public:
  IGraphJobContext() = default;
  virtual ~IGraphJobContext() = default;

  virtual void reset(const GraphDB& graph_db) = 0;
};

}  // namespace ladder

#endif  // LADDER_CONTEXT_H_
