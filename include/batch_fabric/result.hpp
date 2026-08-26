#pragma once
#include "batch_fabric/error.hpp"
#include <variant>
#include <utility>
#include <string>

namespace batch_fabric {

// Result<T> holds either a value of type T or an Error. Normal control flow
// returns a Result; exceptions are reserved for truly exceptional paths.
template <typename T>
class Result {
 public:
  Result() : data_(std::in_place_index<1>, Error(ErrorCode::internal, "uninitialized result")) {}
  Result(T value) : data_(std::in_place_index<0>, std::move(value)) {}
  Result(Error error) : data_(std::in_place_index<1>, std::move(error)) {}

  static Result<T> ok(T value) { return Result<T>(std::move(value)); }
  static Result<T> err(ErrorCode code, std::string message = {}, std::string category = {}) {
    return Result<T>(Error(code, std::move(message), std::move(category)));
  }
  static Result<T> err(Error error) { return Result<T>(std::move(error)); }

  bool ok() const noexcept { return data_.index() == 0; }
  bool has_value() const noexcept { return ok(); }
  bool is_error() const noexcept { return !ok(); }
  explicit operator bool() const noexcept { return ok(); }

  const T& value() const { return std::get<0>(data_); }
  T& value() { return std::get<0>(data_); }
  T&& move_value() && { return std::move(std::get<0>(data_)); }
  T&& move_value() & { return std::move(std::get<0>(data_)); }

  const Error& error() const noexcept {
    if (data_.index() == 1) return std::get<1>(data_);
    static const Error kOk;
    return kOk;
  }

  template <typename U>
  T value_or(U&& fallback) const {
    return ok() ? std::get<0>(data_) : static_cast<T>(std::forward<U>(fallback));
  }

 private:
  std::variant<T, Error> data_;
};

// Error-typed result without a value payload.
template <>
class Result<void> {
 public:
  Result() = default;
  Result(const Error& error) : error_(error) {}
  Result(Error&& error) : error_(std::move(error)) {}

  static Result<void> success() { return Result<void>(); }
  static Result<void> err(ErrorCode code, std::string message = {}, std::string category = {}) {
    return Result<void>(Error(code, std::move(message), std::move(category)));
  }
  static Result<void> err(Error error) { return Result<void>(std::move(error)); }

  bool ok() const noexcept { return error_.ok(); }
  bool is_error() const noexcept { return !ok(); }
  explicit operator bool() const noexcept { return ok(); }
  const Error& error() const noexcept { return error_; }

 private:
  Error error_;
};

}  // namespace batch_fabric