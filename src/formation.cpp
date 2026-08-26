#include "batch_fabric/formation.hpp"

namespace batch_fabric {

BatchFormer::BatchFormer(const BatchPolicy& policy, const std::shared_ptr<Clock>& clock)
    : policy_(policy), clock_(clock) {}

bool BatchFormer::fits(const FormingBatch& batch, const Candidate& c,
                       const BatchBudget& budget) {
  return budget.allows_add(batch.count, batch.input_tokens, batch.output_tokens,
                           batch.work, batch.memory, 1, c.tokens.input,
                           c.tokens.output, c.work.value, c.memory);
}

BatchFormer::Decision BatchFormer::decide(const FormingBatch& batch,
                                          const std::vector<Candidate>& candidates,
                                          TimePoint now) const {
  const auto& constraints = policy_.constraints;
  // Deadline: if any member already has an expired deadline the batch cannot
  // be delivered on time; reject it and let the scheduler expire the members.
  for (const auto& c : candidates) {
    (void)c;
  }

  // If the forming batch has been waiting past its allowed wait for its
  // latency class / global maximum, seal it now (latency protection).
  if (batch.allowed_wait_ns != kNoDeadline) {
    TimePoint elapsed = now - batch.formed_ns;
    if (elapsed >= batch.allowed_wait_ns) {
      return Decision{BatchFormer::Action::seal, SealReason::max_wait_elapsed,
                      "max-wait elapsed"};
    }
  }

  // Size/token/work/memory saturation (already checked by fits during fill);
  // reflect it here as a seal with the precise reason.
  bool count_full = constraints.budget.max_requests != 0 &&
                    batch.count >= constraints.budget.max_requests;
  bool tokens_full =
      (constraints.budget.max_input_tokens != 0 &&
       batch.input_tokens >= constraints.budget.max_input_tokens) ||
      (constraints.budget.max_output_tokens != 0 &&
       batch.output_tokens >= constraints.budget.max_output_tokens);
  bool work_full = constraints.budget.max_work != 0 && batch.work >= constraints.budget.max_work;
  if (count_full) return Decision{BatchFormer::Action::seal, SealReason::max_count_reached,
                                  "max request count reached"};
  if (tokens_full) return Decision{BatchFormer::Action::seal, SealReason::max_tokens_reached,
                                   "max token budget reached"};
  if (work_full) return Decision{BatchFormer::Action::seal, SealReason::max_work_reached,
                                 "max work budget reached"};

  // No more compatible candidates: seal if we have at least the minimum
  // preferred batch or the queue is drained; otherwise wait for arriving work.
  if (candidates.empty()) {
    if (batch.count >= constraints.minimum_preferred_batch || batch.count == 0) {
      return Decision{BatchFormer::Action::seal, SealReason::queue_drained,
                      "no more compatible candidates"};
    }
    return Decision{BatchFormer::Action::wait, SealReason::queue_drained,
                    "waiting for compatible work"};
  }

  // There are still compatible candidates. If the batch is not full and its
  // earliest allowed wait has not elapsed, keep waiting to fill it further.
  return Decision{BatchFormer::Action::wait, SealReason::admission_fill,
                  "waiting to fill batch"};
}

}  // namespace batch_fabric
