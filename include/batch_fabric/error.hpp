#pragma once
#include <cstdint>
#include <string>
#include <string_view>

namespace batch_fabric {

// Stable, structured error taxonomy. Ordinary control flow uses Result/Error,
// not exceptions. Code values are part of the public, serialized protocol and
// must not be renumbered without a protocol/version change.
enum class ErrorCode : std::uint32_t {
  ok = 0,
  invalid_argument,
  invalid_state,
  not_found,
  already_exists,
  capacity_exhausted,
  deferred,
  deadline_expired,
  cancelled,
  stale_epoch,
  stale_worker,
  stale_attempt,
  incompatible,
  batch_sealed,
  batch_too_large,
  split_required,
  merge_rejected,
  transport_failure,
  persistence_failure,
  corruption,
  unsupported,
  internal
};

std::string_view to_string(ErrorCode code) noexcept;

class Error {
 public:
  Error() = default;
  explicit Error(ErrorCode code, std::string message = {}, std::string category = {});
  ErrorCode code() const noexcept { return code_; }
  bool ok() const noexcept { return code_ == ErrorCode::ok; }
  const std::string& message() const noexcept { return message_; }
  const std::string& category() const noexcept { return category_; }
  std::string to_string() const;
  friend bool operator==(const Error& a, const Error& b) noexcept {
    return a.code_ == b.code_ && a.message_ == b.message_;
  }
  friend bool operator!=(const Error& a, const Error& b) noexcept { return !(a == b); }

 private:
  ErrorCode code_ = ErrorCode::ok;
  std::string message_;
  std::string category_;
};

// Convenience factory helpers used across the codebase.
inline Error make_error(ErrorCode code, std::string msg = {}, std::string category = {}) {
  return Error(code, std::move(msg), std::move(category));
}

}  // namespace batch_fabric
