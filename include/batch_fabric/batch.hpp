#pragma once
#include "batch_fabric/compatibility.hpp"
#include "batch_fabric/error.hpp"
#include "batch_fabric/identity.hpp"
#include "batch_fabric/id.hpp"
#include "batch_fabric/policy.hpp"
#include <string>
#include <vector>

namespace batch_fabric {

// Per-member authoritative outcome as reported by an executor. Exactly one
// terminal outcome exists per request attempt.
struct MemberCompletion {
  RequestId request;
  AttemptId attempt;
  CompletionStatus status;
  std::string result;      // opaque result payload on success
  ErrorCode error_code = ErrorCode::ok;
  std::string error_message;
};

// The full completion report for a batch dispatched to a worker. Authority is
// bound to the exact active identities (epoch, boot id, generation).
struct BatchCompletion {
  WorkerId worker;
  WorkerBootId boot;
  BatchEpoch epoch;
  Generation generation;
  BatchId batch;
  std::vector<MemberCompletion> members;
};

// A request attempt's placement within a batch. Used by the explain system.
struct Placement {
  RequestId request;
  AttemptId attempt;
  TenantId tenant;
  TimePoint entered_ns = 0;
};

// Running resource totals for a batch. Used by formation to enforce budgets
// without recomputing from members on every decision.
struct BatchAccum {
  std::uint32_t count = 0;
  std::uint64_t input_tokens = 0;
  std::uint64_t output_tokens = 0;
  std::uint64_t work = 0;
  std::uint64_t memory = 0;
};

}  // namespace batch_fabric