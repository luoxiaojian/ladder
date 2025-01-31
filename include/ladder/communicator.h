#ifndef LADDER_LADDER_COMMUNICATOR_H
#define LADDER_LADDER_COMMUNICATOR_H

#include <mpi.h>

#include <thread>
#include <vector>

namespace ladder {

class CommSpec {
 public:
  CommSpec() : worker_num_(1), server_num_(1) {}
  ~CommSpec() = default;

  void init(int worker_num, int server_num) {
    worker_num_ = worker_num;
    server_num_ = server_num;
  }

  int get_global_worker_id(int server_id, int local_worker_id) const {
    return server_id * worker_num_ + local_worker_id;
  }

  int get_local_worker_id(int global_worker_id) const {
    return global_worker_id % worker_num_;
  }

  int get_server_id(int global_worker_id) const {
    return global_worker_id / worker_num_;
  }

  int global_worker_num() const { return worker_num_ * server_num_; }

  int local_worker_num() const { return worker_num_; }

  int server_num() const { return server_num_; }

 private:
  int worker_num_;
  int server_num_;
};

class MessageBatch {
 public:
  MessageBatch() {}
  MessageBatch(int global_worker_num) { messages_.resize(global_worker_num); }

  void put(int dst, std::vector<char>&& batch) {
    messages_[dst].emplace_back(std::move(batch));
  }

  std::vector<std::vector<char>>& get(int src) { return messages_[src]; }
  const std::vector<std::vector<char>>& get(int src) const {
    return messages_[src];
  }

  void clear() { messages_.clear(); }

  size_t size() const { return messages_.size(); }

 private:
  std::vector<std::vector<std::vector<char>>> messages_;
};

#define BUFFER_BATCH (1024 * 1024 * 16)

void send_buffer(const void* data, size_t size, int dst_server_id,
                 MPI_Comm comm) {
  int iter = size / BUFFER_BATCH;
  const char* ptr = static_cast<const char*>(data);
  for (int i = 0; i < iter; ++i) {
    MPI_Send(ptr, BUFFER_BATCH, MPI_CHAR, dst_server_id, 0, comm);
    ptr += BUFFER_BATCH;
  }
  size_t remaining = size % BUFFER_BATCH;
  MPI_Send(ptr, remaining, MPI_CHAR, dst_server_id, 0, comm);
}

void recv_buffer(void* data, size_t size, int src_server_id, MPI_Comm comm) {
  int iter = size / BUFFER_BATCH;
  char* ptr = static_cast<char*>(data);
  for (int i = 0; i < iter; ++i) {
    MPI_Recv(ptr, BUFFER_BATCH, MPI_CHAR, src_server_id, 0, comm,
             MPI_STATUS_IGNORE);
    ptr += BUFFER_BATCH;
  }
  size_t remaining = size % BUFFER_BATCH;
  MPI_Recv(ptr, remaining, MPI_CHAR, src_server_id, 0, comm, MPI_STATUS_IGNORE);
}

void send_vecs(const std::vector<std::vector<char>>& vecs, int dst_server_id,
               MPI_Comm comm) {
  std::vector<size_t> vec_sizes;
  for (auto& vec : vecs) {
    vec_sizes.push_back(vec.size());
  }
  size_t num = vec_sizes.size();
  MPI_Send(&num, sizeof(size_t), MPI_CHAR, dst_server_id, 0, comm);
  send_buffer(vec_sizes.data(), vec_sizes.size() * sizeof(size_t),
              dst_server_id, comm);
  for (size_t i = 0; i < num; ++i) {
    send_buffer(vecs[i].data(), vec_sizes[i], dst_server_id, comm);
  }
}

void recv_vecs(std::vector<char>& vec, int src_server_id, MPI_Comm comm) {
  size_t num;
  MPI_Recv(&num, sizeof(size_t), MPI_CHAR, src_server_id, 0, comm,
           MPI_STATUS_IGNORE);
  std::vector<size_t> vec_sizes;
  vec_sizes.resize(num);
  recv_buffer(vec_sizes.data(), vec_sizes.size() * sizeof(size_t),
              src_server_id, comm);
  size_t total_length = 0;
  for (auto len : vec_sizes) {
    total_length += len;
  }
  vec.resize(total_length);
  char* ptr = vec.data();
  for (auto len : vec_sizes) {
    recv_buffer(ptr, len, src_server_id, comm);
    ptr += len;
  }
}

#undef BUFFER_BATCH

class Communicator {
 public:
  Communicator(int server_id, const CommSpec& comm_spec)
      : comm_spec_(comm_spec) {
    MPI_Comm_dup(MPI_COMM_WORLD, &comm_);
    int rank, size;
    MPI_Comm_rank(comm_, &rank);
    MPI_Comm_size(comm_, &size);
    CHECK_EQ(rank, server_id);
    CHECK_EQ(size, comm_spec.server_num());

    server_id_ = server_id;
  }
  ~Communicator() { MPI_Comm_free(&comm_); }

  MessageBatch shuffle(MessageBatch&& input) {
    CHECK_EQ(input.size(), comm_spec_.global_worker_num());
    MessageBatch output(comm_spec_.local_worker_num());

    std::thread send_thread([&, this]() {
      for (int i = 1; i < comm_spec_.server_num(); ++i) {
        int target_server_id = (server_id_ + i) % comm_spec_.server_num();
        for (int j = 0; j < comm_spec_.local_worker_num(); ++j) {
          int global_worker_id =
              comm_spec_.get_global_worker_id(target_server_id, j);
          send_vecs(input.get(global_worker_id), target_server_id, comm_);
        }
      }
    });

    std::thread recv_thread([&, this]() {
      for (int i = 1; i < comm_spec_.server_num(); ++i) {
        int source_server_id = (server_id_ + comm_spec_.server_num() - i) %
                               comm_spec_.server_num();
        for (int j = 0; j < comm_spec_.local_worker_num(); ++j) {
          std::vector<char> buf;
          recv_vecs(buf, source_server_id, comm_);
          output.put(j, std::move(buf));
        }
      }
      for (int j = 0; j < comm_spec_.local_worker_num(); ++j) {
        int global_worker_id = comm_spec_.get_global_worker_id(server_id_, j);
        for (auto& vec : input.get(global_worker_id)) {
          output.put(j, std::move(vec));
        }
      }
    });

    recv_thread.join();
    send_thread.join();

    return output;
  }

 private:
  MPI_Comm comm_;
  int server_id_;
  CommSpec comm_spec_;
};

}  // namespace ladder

#endif  // LADDER_LADDER_COMMUNICATOR_H
