#include <mpi.h>
#include <stdio.h>

#include <chrono>
#include <fstream>

#include "graph/graph_db.h"
#include "ladder/app.h"
#include "ladder/worker.h"
#include "nlohmann_json/json.hpp"

using json = nlohmann::json;

std::map<int, std::vector<std::map<std::string, std::string>>>
parse_query_config(const std::string& path) {
  std::ifstream file(path);
  std::map<int, std::vector<std::map<std::string, std::string>>> ret;
  if (!file.is_open()) {
    std::cerr << "Failed to open config file: " << path << std::endl;
    return ret;
  }

  std::string line;
  while (getline(file, line)) {
    size_t pos = line.find('|');
    if (pos == std::string::npos) {
      std::cerr << "Wrong format: " << line << std::endl;
      continue;
    }

    int id = std::stoi(line.substr(0, pos));
    std::string jsonString = line.substr(pos + 1);

    try {
      auto j = json::parse(jsonString);
      std::map<std::string, std::string> dataMap;
      for (auto& [key, value] : j.items()) {
        dataMap[key] = value.get<std::string>();
      }

      ret[id].push_back(dataMap);
    } catch (const std::exception& e) {
      std::cerr << "Failed to parse json: " << e.what() << std::endl;
    }
  }

  file.close();
  return ret;
}

int main(int argc, char** argv) {
  std::string prefix = argv[1];
  std::string lib_prefix = argv[2];
  std::string query_config = argv[3];

  int rank, size;
  int provided;
  MPI_Init_thread(NULL, NULL, MPI_THREAD_MULTIPLE, &provided);

  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &size);

  ladder::GraphDB graph;
  graph.open(prefix, rank, size);

  {
    int worker_num = std::thread::hardware_concurrency();
    int reduced_worker_num = 0;
    MPI_Allreduce(&worker_num, &reduced_worker_num, 1, MPI_INT, MPI_MIN,
                  MPI_COMM_WORLD);
    ladder::Worker worker(reduced_worker_num, rank, size);
    auto queries = parse_query_config(query_config);
    for (auto& pair : queries) {
      std::string lib_path =
          lib_prefix + "/libbi" + std::to_string(pair.first) + ".so";
      ladder::App app(lib_path);

      MPI_Barrier(MPI_COMM_WORLD);
      auto start = std::chrono::high_resolution_clock::now();
      worker.EvalBatch(graph, app, pair.second);
      MPI_Barrier(MPI_COMM_WORLD);
      auto end = std::chrono::high_resolution_clock::now();
      auto duration =
          std::chrono::duration_cast<std::chrono::microseconds>(end - start);
      double total_seconds = static_cast<double>(duration.count()) / 1000000.0;
      double avg_us = static_cast<double>(duration.count()) /
                      static_cast<double>(pair.second.size());
      std::cout << "Execute " << pair.second.size() << " bi" << pair.first
                << " queries takes: " << total_seconds << " s, avg = " << avg_us
                << " us" << std::endl;
    }
  }

  MPI_Finalize();

  return 0;
}