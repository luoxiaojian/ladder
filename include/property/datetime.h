#ifndef LADDER_PROPERTY_DATETIME_H
#define LADDER_PROPERTY_DATETIME_H

#include <cstdint>

namespace ladder {

struct DateTime {
 public:
  DateTime() : value_(0) {}
  DateTime(int64_t value) : value_(value) {}
  int to_i64() const { return value_; }

 private:
  int64_t value_;
};

}  // namespace ladder

#endif  // LADDER_PROPERTY_DATETIME_H
