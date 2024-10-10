#ifndef STARKWARE_UTILS_ERROR_HANDLING_H_
#define STARKWARE_UTILS_ERROR_HANDLING_H_

#include <exception>
#include <string>
#include <utility>

namespace starkware {

class StarkwareException : public std::exception {
 public:
  explicit StarkwareException(std::string message) : message_(std::move(message)) {}
  const char* what() const noexcept { return message_.c_str(); }  // NOLINT

 private:
  std::string message_;
};

/*
  We use "do {} while(false);" pattern to force the user to use ; after the macro.
*/
#define ASSERT(cond, msg)            \
  do {                               \
    if (!(cond)) {                   \
      throw StarkwareException(msg); \
    }                                \
  } while (false)

}  // namespace starkware

#endif  // STARKWARE_UTILS_ERROR_HANDLING_H_
