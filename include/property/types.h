#ifndef LADDER_PROPERTY_TYPES_H
#define LADDER_PROPERTY_TYPES_H

namespace ladder {

enum class DataType {
  kInt32,
  kUInt32,
  kInt64,
  kUInt64,
  kFloat,
  kDouble,
  kDate,
  kDateTime,
  kString,
  kLCString,
  kID,
  kNull,
};

}  // namespace ladder

#endif  // LADDER_PROPERTY_TYPES_H
