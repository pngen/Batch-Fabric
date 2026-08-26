#pragma once
#include "batch_fabric/clock.hpp"
#include "batch_fabric/id.hpp"
#include "batch_fabric/identity.hpp"
#include "batch_fabric/policy.hpp"
#include <string>
#include <vector>

namespace batch_fabric {

// The batch former is the unit policy engine behind the governing question:
// which requests execute together, when a batch seals, and when waiting for a
// larger batch is no longer worth the latency cost. It is a pure policy
// calculator; the scheduler owns the live state and applies its decisions.
class BatchFormer {
 public:
  struct Candidate {
    RequestId request;
    TenantId tenant;
    TokenEstimate tokens;
    WorkEstimate work;
    std::uint64_t memory = 0;
    Deadline deadline;
    LatencyClass latency = LatencyClass::batch;
    TimePoint arrived_ns = 0;
  };

  struct FormingBatch {
    BatchId batch;
    CompatibilityKey key;
    Phase phase = Phase::prefill;
    TimePoint formed_ns = 0;
    std::uint32_t count = 0;
    std::uint64_t input_tokens = 0;
    std::uint64_t output_tokens = 0;
    std::uint64_t work = 0;
    std::uint64_t memory = 0;
    TimePoint allowed_wait_ns = kNoDeadline;  // computed by the scheduler
  };

  enum class Action { noop, wait, seal, split, reject };

  struct Decision {
    Action action = Action::wait;
    SealReason seal_reason = SealReason::max_wait_elapsed;
    std::string detail;
  };

  explicit BatchFormer(const BatchPolicy& policy, const std::shared_ptr<Clock>& clock);

  // Can a candidate be added to a forming batch without violating the budget
  // (count, tokens, work, memory)?
  static bool fits(const FormingBatch& batch, const Candidate& c, const BatchBudget& budget);

  // Decide whether the given forming batch (already greedily filled) should
  // wait for more, seal now, or reject/defer. Deterministic given inputs.
  Decision decide(const FormingBatch& batch, const std::vector<Candidate>& candidates,
                  TimePoint now) const;

  const BatchPolicy& policy() const { return policy_; }

 private:
  BatchPolicy policy_;
  std::shared_ptr<Clock> clock_;
};

struct MergeOutcome {
  std::vector<BatchId> sources;
  BatchId merged;
  MergeReason reason;
  std::string detail;
};

struct FormationReport {
  std::vector<BatchId> sealed_batches;
  std::vector<BatchId> split_batches;
  std::vector<MergeOutcome> merges;
  std::vector<RequestId> rejected;
  std::vector<RequestId> deferred;
  std::size_t forming = 0;
};

}  // namespace batch_fabric
