#ifndef LADDER_LADDER_COMMUNICATOR_H
#define LADDER_LADDER_COMMUNICATOR_H

namespace ladder {

class Communicator {
 public:
  Communicator() = default;
  ~Communicator() = default;

  void Send(const std::string& msg) { std::cout << msg << std::endl; }
};

}  // namespace ladder

#endif  // LADDER_LADDER_COMMUNICATOR_H
