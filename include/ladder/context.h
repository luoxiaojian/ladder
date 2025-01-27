#ifndef LADDER_LADDER_CONTEXT_H_
#define LADDER_LADDER_CONTEXT_H_

#include <map>
#include <string>

#include "../graph/graph_db.h"

namespace ladder {

class IContext {
 public:
  IContext() = default;
  virtual ~IContext() = default;

  void set_worker_id(int id) { worker_id_ = id; }
  void set_worker_num(int num) { worker_num_ = num; }
  void set_server_id(int id) { server_id_ = id; }
  void set_server_num(int num) { server_num_ = num; }

  int local_worker_id() const { return worker_id_; }
  int local_worker_num() const { return worker_num_; }

  int server_num() const { return server_num_; }

  int global_worker_id() const { return worker_id_ + server_id_ * worker_num_; }
  int global_worker_num() const { return worker_num_ * server_num_; }

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
  int server_id_;
  int server_num_;

  std::map<std::string, std::string> params_;
};

}  // namespace ladder

#endif  // LADDER_LADDER_CONTEXT_H_
