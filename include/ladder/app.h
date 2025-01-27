#ifndef LADDER_LADDER_APP_H
#define LADDER_LADDER_APP_H

#include <dlfcn.h>

#include <iostream>

#include "graph/graph_db.h"
#include "ladder/context.h"
#include "ladder/dataflow.h"

namespace ladder {

typedef void* (*create_dataflow_func)();
typedef void* (*create_context_func)(const GraphDB*);
typedef void (*delete_dataflow_func)(void*);
typedef void (*delete_context_func)(void*);

class App {
 public:
  App(const std::string& path) {
    handle_ = dlopen(path.c_str(), RTLD_LAZY);
    if (!handle_) {
      std::cerr << "Cannot open library: " << dlerror() << std::endl;
      return;
    }

    *(void**) (&create_dataflow_) = dlsym(handle_, "create_dataflow");
    *(void**) (&create_context_) = dlsym(handle_, "create_context");

    if (!create_dataflow_ || !create_context_) {
      std::cerr << "Cannot load symbols: " << dlerror() << std::endl;
      dlclose(handle_);
      handle_ = nullptr;
      return;
    }
  }
  ~App() {
    if (handle_) {
      dlclose(handle_);
    }
  }

  DataFlow* create_dataflow() const {
    return reinterpret_cast<DataFlow*>(create_dataflow_());
  }

  IContext* create_context(const GraphDB* graph) const {
    return reinterpret_cast<IContext*>(create_context_(graph));
  }

  void delete_dataflow(DataFlow* dataflow) { delete_dataflow_(dataflow); }

  void delete_context(IContext* context) { delete_context_(context); }

 private:
  void* handle_;
  create_dataflow_func create_dataflow_;
  create_context_func create_context_;
  delete_dataflow_func delete_dataflow_;
  delete_context_func delete_context_;
};

}  // namespace ladder

#endif  // LADDER_LADDER_APP_H
