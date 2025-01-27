#include <string>

#include "graph/graph_db.h"

int main(int argc, char** argv) {
  std::string prefix = argv[1];
  int partition_id = atoi(argv[2]);
  int partition_num = atoi(argv[3]);
  ladder::GraphDB graph;
  graph.open(prefix, partition_id, partition_num);
}