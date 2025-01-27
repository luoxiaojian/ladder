#ifndef LADDER_LADDER_OPERATOR_H_
#define LADDER_LADDER_OPERATOR_H_

#include "in_stream.h"
#include "out_stream.h"

namespace ladder {

enum class OperatorType {
  kNullary,
  kUnary,
  kBinary,
};

class IOperator {
 public:
  virtual ~IOperator() = default;
  virtual OperatorType type() const = 0;
};

class INullaryOperator : public IOperator {
 public:
  virtual void Execute(IContext& context, std::vector<InStream>& output) = 0;
  OperatorType type() const override { return OperatorType::kNullary; }
};

class IUnaryOperator : public IOperator {
 public:
  virtual void Execute(IContext& context, OutStream& input,
                       std::vector<InStream>& output) = 0;
  OperatorType type() const override { return OperatorType::kUnary; }
};

class IBinaryOperator : public IOperator {
 public:
  virtual void Execute(IContext& context, OutStream& input0, OutStream& input1,
                       std::vector<InStream>& output) = 0;
  OperatorType type() const override { return OperatorType::kBinary; }
};

}  // namespace ladder

#endif  // LADDER_LADDER_OPERATOR_H_