#ifndef LADDER_PROPERTY_DATE_H
#define LADDER_PROPERTY_DATE_H

namespace ladder {

struct Date {
 public:
  Date() : value_(0) {}
  Date(int value) : value_(value) {}
  int to_i32() const { return value_; }

 private:
  int value_;
};

}  // namespace ladder

#endif  // LADDER_PROPERTY_DATE_H
