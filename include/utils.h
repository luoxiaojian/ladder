#ifndef LADDER_UTILS_H_
#define LADDER_UTILS_H_

#include <fstream>
#include <iostream>
#include <string>
#include <vector>

namespace ladder {

size_t get_file_size(const std::string& fname) {
  std::ifstream file(fname, std::ios::ate | std::ios::binary);
  if (!file.is_open()) {
    std::cerr << "Error: cannot open file " << fname << std::endl;
    return 0;
  }
  return file.tellg();
}

template <typename T>
void load_from_file(const std::string& fname, std::vector<T>& data) {
  size_t file_size = get_file_size(fname);
  size_t vec_size = file_size / sizeof(T);
  std::ifstream file(fname, std::ios::binary);
  if (!file.is_open()) {
    std::cerr << "Error: cannot open file " << fname << std::endl;
    return;
  }
  data.resize(vec_size);
  file.read(reinterpret_cast<char*>(data.data()), vec_size * sizeof(T));
}

}  // namespace ladder

#endif  // LADDER_UTILS_H_