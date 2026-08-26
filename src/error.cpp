#include "batch_fabric/error.hpp"

namespace batch_fabric {

std::string_view to_string(ErrorCode code) noexcept {
  switch (code) {
    case ErrorCode::ok: return "ok";
    case ErrorCode::invalid_argument: return "invalid_argument";
    case ErrorCode::invalid_state: return "invalid_state";
    case ErrorCode::not_found: return "not_found";
    case ErrorCode::already_exists: return "already_exists";
    case ErrorCode::capacity_exhausted: return "capacity_exhausted";
    case ErrorCode::deferred: return "deferred";
    case ErrorCode::deadline_expired: return "deadline_expired";
    case ErrorCode::cancelled: return "cancelled";
    case ErrorCode::stale_epoch: return "stale_epoch";
    case ErrorCode::stale_worker: return "stale_worker";
    case ErrorCode::stale_attempt: return "stale_attempt";
    case ErrorCode::incompatible: return "incompatible";
    case ErrorCode::batch_sealed: return "batch_sealed";
    case ErrorCode::batch_too_large: return "batch_too_large";
    case ErrorCode::split_required: return "split_required";
    case ErrorCode::merge_rejected: return "merge_rejected";
    case ErrorCode::transport_failure: return "transport_failure";
    case ErrorCode::persistence_failure: return "persistence_failure";
    case ErrorCode::corruption: return "corruption";
    case ErrorCode::unsupported: return "unsupported";
    case ErrorCode::internal: return "internal";
  }
  return "unknown";
}

Error::Error(ErrorCode code, std::string message, std::string category)
    : code_(code), message_(std::move(message)), category_(std::move(category)) {}

std::string Error::to_string() const {
  std::string out;
  out += std::string(::batch_fabric::to_string(code_));
  if (!category_.empty()) {
    out += "[";
    out += category_;
    out += "]";
  }
  if (!message_.empty()) {
    out += ": ";
    out += message_;
  }
  return out;
}

}  // namespace batch_fabric
