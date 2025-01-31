#ifndef LADDER_LADDER_CONTEXT_H_
#define LADDER_LADDER_CONTEXT_H_

#include <map>
#include <string>

#include "graph/graph_db.h"
#include "ladder/communicator.h"

namespace ladder {

class IContext {
 public:
  IContext() = default;
  virtual ~IContext() = default;

  void set_comm_spec(int worker_id, int server_id, const CommSpec& comm_spec) {
    worker_id_ = worker_id;
    server_id_ = server_id;
    comm_spec_ = comm_spec;
  }

  int local_worker_id() const { return worker_id_; }
  int local_worker_num() const { return comm_spec_.local_worker_num(); }

  int server_num() const { return comm_spec_.server_num(); }

  int global_worker_id() const {
    return comm_spec_.get_global_worker_id(server_id_, worker_id_);
  }
  int global_worker_num() const { return comm_spec_.global_worker_num(); }

  void clear_params() { params_.clear(); }
  void set_param(const std::string& key, const std::string& value) {
    params_[key] = value;
  }
  const std::string& get_param(const std::string& key) const {
    return params_.at(key);
  }

 private:
  int worker_id_;
  int server_id_;
  CommSpec comm_spec_;

  std::map<std::string, std::string> params_;
};

}  // namespace ladder

#endif  // LADDER_LADDER_CONTEXT_H_
