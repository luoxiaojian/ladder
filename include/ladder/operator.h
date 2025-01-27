#ifndef LADDER_LADDER_OPERATOR_H_
#define LADDER_LADDER_OPERATOR_H_

#include "in_stream.h"
#include "out_stream.h"

namespace ladder {

class IOperator {
 public:
  virtual ~IOperator() = default;

  virtual void Execute(IContext& context, OutStream& input,
                       std::vector<InStream>& output) = 0;
};

}  // namespace ladder

#endif  // LADDER_LADDER_OPERATOR_H_