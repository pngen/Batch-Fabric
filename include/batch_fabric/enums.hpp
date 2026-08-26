#pragma once
#include <cstdint>
#include <string_view>
#include <optional>

namespace batch_fabric {

// Phase of inference work. PREFILL and DECODE are always distinct batching
// domains; they are never grouped unless an explicit policy and executor
// contract prove a cross-phase batch is valid.
enum class Phase : std::uint8_t { prefill = 0, decode = 1 };

constexpr std::string_view to_string(Phase p) noexcept {
  switch (p) {
    case Phase::prefill: return "prefill";
    case Phase::decode: return "decode";
  }
  return "unknown";
}

enum class LatencyClass : std::uint8_t {
  batch = 0,
  low_latency = 1,
  interactive = 2,
  deadline_bounded = 3,
  best_effort = 4
};

constexpr std::string_view to_string(LatencyClass c) noexcept {
  switch (c) {
    case LatencyClass::batch: return "batch";
    case LatencyClass::low_latency: return "low_latency";
    case LatencyClass::interactive: return "interactive";
    case LatencyClass::deadline_bounded: return "deadline_bounded";
    case LatencyClass::best_effort: return "best_effort";
  }
  return "unknown";
}

enum class PriorityClass : std::uint8_t {
  high = 0,
  normal = 1,
  low = 2,
  background = 3
};

constexpr std::string_view to_string(PriorityClass p) noexcept {
  switch (p) {
    case PriorityClass::high: return "high";
    case PriorityClass::normal: return "normal";
    case PriorityClass::low: return "low";
    case PriorityClass::background: return "background";
  }
  return "unknown";
}

// Request lifecycle state machine. Transitions are validated and illegal
// transitions fail deterministically.
enum class RequestState : std::uint8_t {
  created = 0,
  eligible = 1,
  waiting = 2,
  reserved = 3,
  batched = 4,
  sealed = 5,
  dispatched = 6,
  running = 7,
  completed = 8,
  // terminal / error
  cancelled = 9,
  expired = 10,
  rejected = 11,
  failed = 12
};

constexpr std::string_view to_string(RequestState s) noexcept {
  switch (s) {
    case RequestState::created: return "created";
    case RequestState::eligible: return "eligible";
    case RequestState::waiting: return "waiting";
    case RequestState::reserved: return "reserved";
    case RequestState::batched: return "batched";
    case RequestState::sealed: return "sealed";
    case RequestState::dispatched: return "dispatched";
    case RequestState::running: return "running";
    case RequestState::completed: return "completed";
    case RequestState::cancelled: return "cancelled";
    case RequestState::expired: return "expired";
    case RequestState::rejected: return "rejected";
    case RequestState::failed: return "failed";
  }
  return "unknown";
}

constexpr bool is_request_terminal(RequestState s) noexcept {
  return s == RequestState::cancelled || s == RequestState::expired ||
         s == RequestState::rejected || s == RequestState::failed ||
         s == RequestState::completed;
}

constexpr bool is_request_active(RequestState s) noexcept {
  return s == RequestState::eligible || s == RequestState::waiting ||
         s == RequestState::reserved || s == RequestState::batched ||
         s == RequestState::sealed || s == RequestState::dispatched ||
         s == RequestState::running;
}

// Batch lifecycle state machine. A batch may accept members only while
// Forming; once Sealed membership is immutable.
enum class BatchState : std::uint8_t {
  forming = 0,
  sealed = 1,
  dispatched = 2,
  running = 3,
  partially_completed = 4,
  completed = 5,
  // terminal / error
  cancelled = 6,
  failed = 7,
  superseded = 8
};

constexpr std::string_view to_string(BatchState s) noexcept {
  switch (s) {
    case BatchState::forming: return "forming";
    case BatchState::sealed: return "sealed";
    case BatchState::dispatched: return "dispatched";
    case BatchState::running: return "running";
    case BatchState::partially_completed: return "partially_completed";
    case BatchState::completed: return "completed";
    case BatchState::cancelled: return "cancelled";
    case BatchState::failed: return "failed";
    case BatchState::superseded: return "superseded";
  }
  return "unknown";
}

constexpr bool is_batch_terminal(BatchState s) noexcept {
  return s == BatchState::cancelled || s == BatchState::failed ||
         s == BatchState::completed || s == BatchState::superseded;
}

// Why a batch sealed.
enum class SealReason : std::uint8_t {
  max_count_reached = 0,
  max_tokens_reached = 1,
  max_work_reached = 2,
  max_wait_elapsed = 3,
  deadline_derived = 4,
  policy_derived = 5,
  immediate = 6,
  queue_drained = 7,
  executor_required = 8,
  minimum_batch_met = 9,
  admission_fill = 10
};

constexpr std::string_view to_string(SealReason r) noexcept {
  switch (r) {
    case SealReason::max_count_reached: return "max_count_reached";
    case SealReason::max_tokens_reached: return "max_tokens_reached";
    case SealReason::max_work_reached: return "max_work_reached";
    case SealReason::max_wait_elapsed: return "max_wait_elapsed";
    case SealReason::deadline_derived: return "deadline_derived";
    case SealReason::policy_derived: return "policy_derived";
    case SealReason::immediate: return "immediate";
    case SealReason::queue_drained: return "queue_drained";
    case SealReason::executor_required: return "executor_required";
    case SealReason::minimum_batch_met: return "minimum_batch_met";
    case SealReason::admission_fill: return "admission_fill";
  }
  return "unknown";
}

enum class SplitReason : std::uint8_t {
  capacity_shrink = 0,
  token_violation = 1,
  work_violation = 2,
  deadline_pressure = 3,
  worker_batch_size_constraint = 4,
  cancellation = 5,
  partial_readiness = 6,
  policy = 7,
  executor_rejection = 8,
  oversized = 9
};

constexpr std::string_view to_string(SplitReason r) noexcept {
  switch (r) {
    case SplitReason::capacity_shrink: return "capacity_shrink";
    case SplitReason::token_violation: return "token_violation";
    case SplitReason::work_violation: return "work_violation";
    case SplitReason::deadline_pressure: return "deadline_pressure";
    case SplitReason::worker_batch_size_constraint: return "worker_batch_size_constraint";
    case SplitReason::cancellation: return "cancellation";
    case SplitReason::partial_readiness: return "partial_readiness";
    case SplitReason::policy: return "policy";
    case SplitReason::executor_rejection: return "executor_rejection";
    case SplitReason::oversized: return "oversized";
  }
  return "unknown";
}

enum class MergeReason : std::uint8_t {
  policy = 0,
  aggregate_budget_valid = 1,
  compatibility_match = 2,
  executor_allowed = 3,
  latency_valid = 4
};

constexpr std::string_view to_string(MergeReason r) noexcept {
  switch (r) {
    case MergeReason::policy: return "policy";
    case MergeReason::aggregate_budget_valid: return "aggregate_budget_valid";
    case MergeReason::compatibility_match: return "compatibility_match";
    case MergeReason::executor_allowed: return "executor_allowed";
    case MergeReason::latency_valid: return "latency_valid";
  }
  return "unknown";
}

enum class CancellationReason : std::uint8_t {
  client_request = 0,
  deadline_expired = 1,
  preempted = 2,
  superseded = 3,
  shutdown = 4,
  executor_error = 5
};

constexpr std::string_view to_string(CancellationReason r) noexcept {
  switch (r) {
    case CancellationReason::client_request: return "client_request";
    case CancellationReason::deadline_expired: return "deadline_expired";
    case CancellationReason::preempted: return "preempted";
    case CancellationReason::superseded: return "superseded";
    case CancellationReason::shutdown: return "shutdown";
    case CancellationReason::executor_error: return "executor_error";
  }
  return "unknown";
}

// Per-member outcome. Exactly one authoritative terminal outcome per attempt.
enum class CompletionStatus : std::uint8_t {
  success = 0,
  retryable_failure = 1,
  non_retryable_failure = 2,
  cancelled = 3,
  expired = 4,
  stale = 5,
  deferred = 6
};

constexpr std::string_view to_string(CompletionStatus s) noexcept {
  switch (s) {
    case CompletionStatus::success: return "success";
    case CompletionStatus::retryable_failure: return "retryable_failure";
    case CompletionStatus::non_retryable_failure: return "non_retryable_failure";
    case CompletionStatus::cancelled: return "cancelled";
    case CompletionStatus::expired: return "expired";
    case CompletionStatus::stale: return "stale";
    case CompletionStatus::deferred: return "deferred";
  }
  return "unknown";
}

enum class CompatibilityDecision : std::uint8_t {
  compatible = 0,
  incompatible_revision = 1,
  incompatible_adapter = 2,
  incompatible_phase = 3,
  incompatible_shape = 4,
  incompatible_executor = 5,
  incompatible_dtype = 6,
  incompatible_backend = 7,
  incompatible_device = 8,
  incompatible_execution_mode = 9,
  incompatible_sampling = 10,
  incompatible_extension = 11,
  incompatible_deadline = 12,
  incompatible_model = 13,
  compatible_deferred = 14
};

constexpr std::string_view to_string(CompatibilityDecision d) noexcept {
  switch (d) {
    case CompatibilityDecision::compatible: return "compatible";
    case CompatibilityDecision::incompatible_revision: return "incompatible_revision";
    case CompatibilityDecision::incompatible_adapter: return "incompatible_adapter";
    case CompatibilityDecision::incompatible_phase: return "incompatible_phase";
    case CompatibilityDecision::incompatible_shape: return "incompatible_shape";
    case CompatibilityDecision::incompatible_executor: return "incompatible_executor";
    case CompatibilityDecision::incompatible_dtype: return "incompatible_dtype";
    case CompatibilityDecision::incompatible_backend: return "incompatible_backend";
    case CompatibilityDecision::incompatible_device: return "incompatible_device";
    case CompatibilityDecision::incompatible_execution_mode: return "incompatible_execution_mode";
    case CompatibilityDecision::incompatible_sampling: return "incompatible_sampling";
    case CompatibilityDecision::incompatible_extension: return "incompatible_extension";
    case CompatibilityDecision::incompatible_deadline: return "incompatible_deadline";
    case CompatibilityDecision::incompatible_model: return "incompatible_model";
    case CompatibilityDecision::compatible_deferred: return "compatible_deferred";
  }
  return "unknown";
}

enum class FormationDecision : std::uint8_t {
  noop = 0,
  wait_for_more = 1,
  seal = 2,
  split = 3,
  form_another = 4,
  reject = 5,
  defer = 6
};

constexpr std::string_view to_string(FormationDecision d) noexcept {
  switch (d) {
    case FormationDecision::noop: return "noop";
    case FormationDecision::wait_for_more: return "wait_for_more";
    case FormationDecision::seal: return "seal";
    case FormationDecision::split: return "split";
    case FormationDecision::form_another: return "form_another";
    case FormationDecision::reject: return "reject";
    case FormationDecision::defer: return "defer";
  }
  return "unknown";
}

enum class AdmissionDecision : std::uint8_t {
  admitted = 0,
  deferred = 1,
  capacity_exhausted = 2,
  expired = 3,
  oversized = 4,
  rejected = 5
};

constexpr std::string_view to_string(AdmissionDecision d) noexcept {
  switch (d) {
    case AdmissionDecision::admitted: return "admitted";
    case AdmissionDecision::deferred: return "deferred";
    case AdmissionDecision::capacity_exhausted: return "capacity_exhausted";
    case AdmissionDecision::expired: return "expired";
    case AdmissionDecision::oversized: return "oversized";
    case AdmissionDecision::rejected: return "rejected";
  }
  return "unknown";
}

// Enum parsing from stable string names (used by CLI and config). Returns
// std::nullopt on an unknown name.
std::optional<Phase> parse_phase(std::string_view s) noexcept;
std::optional<LatencyClass> parse_latency_class(std::string_view s) noexcept;
std::optional<PriorityClass> parse_priority_class(std::string_view s) noexcept;

}  // namespace batch_fabric
