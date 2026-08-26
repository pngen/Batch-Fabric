#pragma once
#include "batch_fabric/clock.hpp"
#include "batch_fabric/enums.hpp"
#include "batch_fabric/id.hpp"
#include <array>
#include <cstdint>
#include <map>

namespace batch_fabric {

// Aggregate token/work/memory estimates for a single request.
struct TokenEstimate {
  std::uint64_t input = 0;
  std::uint64_t output = 0;
};

struct WorkEstimate {
  std::uint64_t value = 0;
};

constexpr std::int64_t kNoDeadline = INT64_MAX;

// Absolute and relative deadline expressed on the monotonic clock. A value of
// 0 (or kNoDeadline) means "no deadline" and is never treated as expired.
struct Deadline {
  TimePoint absolute_ns = 0;  // 0 => no deadline
  bool has_deadline() const noexcept { return absolute_ns > 0; }
  bool expired(TimePoint now) const noexcept {
    return has_deadline() && now >= absolute_ns;
  }
  TimePoint remaining(TimePoint now) const noexcept {
    if (!has_deadline()) return kNoDeadline;
    TimePoint r = absolute_ns - now;
    return r < 0 ? 0 : r;
  }
};

// Hard per-batch budgets. A zero field means "unbounded" for that dimension.
struct BatchBudget {
  std::uint32_t max_requests = 0;       // 0 => unlimited count
  std::uint64_t max_input_tokens = 0;   // 0 => unlimited
  std::uint64_t max_output_tokens = 0;  // 0 => unlimited
  std::uint64_t max_work = 0;           // 0 => unlimited
  std::uint64_t max_memory_bytes = 0;   // 0 => unlimited

  bool requests_unlimited() const noexcept { return max_requests == 0; }

  // Would adding (count, in, out, work, mem) to a batch currently holding
  // (cur_count, cur_in, cur_out, cur_work, cur_mem) violate any limit?
  bool allows_add(std::uint32_t cur_count, std::uint64_t cur_in,
                  std::uint64_t cur_out, std::uint64_t cur_work,
                  std::uint64_t cur_mem, std::uint32_t add_count,
                  std::uint64_t add_in, std::uint64_t add_out,
                  std::uint64_t add_work, std::uint64_t add_mem) const noexcept {
    auto count = static_cast<std::uint64_t>(cur_count) + add_count;
    if (max_requests != 0 && count > max_requests) return false;
    if (max_input_tokens != 0 && cur_in + add_in > max_input_tokens) return false;
    if (max_output_tokens != 0 && cur_out + add_out > max_output_tokens) return false;
    if (max_work != 0 && cur_work + add_work > max_work) return false;
    if (max_memory_bytes != 0 && cur_mem + add_mem > max_memory_bytes) return false;
    return true;
  }
};

// Splice of wait/latency constraints.
struct WaitPolicy {
  TimePoint global_max_wait_ns = 0;          // 0 => no global max wait
  std::map<LatencyClass, TimePoint> latency_max_wait_ns;  // per-class
  bool immediate_seal_when_solo = false;     // low-volume latency request never waits
};

struct FairnessPolicy {
  bool enabled = true;
  std::map<TenantId, std::uint32_t> weights;   // default weight 1
  std::uint32_t max_outstanding_per_tenant = 0;  // 0 => unlimited
  std::uint32_t max_contribution_per_batch = 0;  // 0 => unlimited
  std::uint32_t default_weight = 1;
  std::uint32_t weight(TenantId t) const {
    auto it = weights.find(t);
    return it == weights.end() ? default_weight : it->second;
  }
};

struct SplitPolicy {
  bool enabled = true;
  std::uint32_t max_depth = 4;
  bool preserve_fairness = true;
};

struct MergePolicy {
  bool enabled = true;
  std::uint32_t max_merges_per_cycle = 8;
  bool require_same_epoch = true;
};

struct RetryPolicy {
  bool enabled = true;
  std::uint32_t max_attempts = 3;   // including first
  TimePoint backoff_base_ns = 1000000;  // 1 ms
  TimePoint backoff_max_ns = 1000000000;  // 1 s
  TimePoint retry_deadline_gap_ns = 0;   // min headroom to retry
};

// Full, bounded subset of batching constraints governing deterministic
// formation, sealing, splitting, and merging.
struct BatchConstraints {
  BatchBudget budget;
  TimePoint global_max_wait_ns = 0;
  std::uint32_t minimum_preferred_batch = 1;
  bool allow_cross_phase = false;
  std::map<LatencyClass, TimePoint> latency_waits_ns;
};

// The complete policy bundle a BatchFabric instance is configured with.
struct BatchPolicy {
  BatchConstraints constraints;
  FairnessPolicy fairness;
  WaitPolicy wait;
  SplitPolicy split;
  MergePolicy merge;
  RetryPolicy retry;
};

}  // namespace batch_fabric
