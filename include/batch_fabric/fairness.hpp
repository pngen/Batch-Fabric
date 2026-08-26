#pragma once
#include "batch_fabric/id.hpp"
#include "batch_fabric/policy.hpp"
#include <cstdint>
#include <map>
#include <vector>

namespace batch_fabric {

// Per-tenant fairness controller. Tracks served work relative to configured
// weights and selects the candidate tenant with the greatest debit (least
// served relative to its weight). Deterministic tie-breaking by tenant value
// ensures reproducible batch membership.
class FairnessController {
 public:
  explicit FairnessController(const FairnessPolicy& policy);
  void note_submission(TenantId t, std::uint64_t work);
  // Attribute work to a tenant at placement time (reserved into a batch).
  void note_placed(TenantId t, std::uint64_t work);
  void note_completed(TenantId t, std::uint64_t work);
  // Choose the next tenant among candidates; returns the chosen tenant, or
  // TenantId(0) if candidates is empty.
  TenantId select_next(const std::vector<TenantId>& candidates) const;
  bool at_outstanding_cap(TenantId t) const;
  std::uint64_t served(TenantId t) const;
  std::int64_t debt(TenantId t) const;  // >0 means under-served relative to weight
  std::map<TenantId, std::int64_t> debts() const;
  std::map<TenantId, std::uint64_t> served_all() const;
  void restore_served(TenantId t, std::uint64_t served);
  const FairnessPolicy& policy() const { return policy_; }
  void reset();

 private:
  FairnessPolicy policy_;
  std::map<TenantId, std::uint64_t> served_;
  std::map<TenantId, std::uint64_t> outstanding_;
  std::map<TenantId, std::uint64_t> pending_;
};

}  // namespace batch_fabric