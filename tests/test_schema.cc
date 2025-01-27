#include <string>

#include "graph/schema.h"

int main(int argc, char** argv) {
  std::string path = argv[1];
  ladder::Schema schema;
  schema.open(path);

  return 0;
}