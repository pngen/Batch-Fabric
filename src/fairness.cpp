#include "batch_fabric/fairness.hpp"

namespace batch_fabric {

FairnessController::FairnessController(const FairnessPolicy& policy) : policy_(policy) {}

void FairnessController::note_submission(TenantId t, std::uint64_t work) {
  outstanding_[t] += work;
}

void FairnessController::note_placed(TenantId t, std::uint64_t work) {
  served_[t] += work;
  outstanding_[t] += work;
}

void FairnessController::note_completed(TenantId t, std::uint64_t work) {
  if (outstanding_[t] > work) {
    outstanding_[t] -= work;
  } else {
    outstanding_[t] = 0;
  }
}

std::uint64_t FairnessController::served(TenantId t) const {
  auto it = served_.find(t);
  return it == served_.end() ? 0 : it->second;
}

std::int64_t FairnessController::debt(TenantId t) const {
  std::uint64_t w = policy_.weight(t);
  if (w == 0) w = policy_.default_weight;
  if (w == 0) w = 1;
  return static_cast<std::int64_t>(served(t) / w);
}

namespace {
bool is_more_under_served(std::uint64_t sa, std::uint64_t wa, std::uint64_t sb,
                          std::uint64_t wb) {
  if (wa == 0) wa = 1;
  if (wb == 0) wb = 1;
  long double ra = static_cast<long double>(sa) / static_cast<long double>(wa);
  long double rb = static_cast<long double>(sb) / static_cast<long double>(wb);
  return ra < rb;
}
}  // namespace

TenantId FairnessController::select_next(const std::vector<TenantId>& candidates) const {
  if (candidates.empty()) return TenantId(0);
  TenantId best = candidates[0];
  for (std::size_t i = 1; i < candidates.size(); ++i) {
    std::uint64_t wa = policy_.weight(best);
    std::uint64_t wb = policy_.weight(candidates[i]);
    bool best_under = is_more_under_served(served(best), wa, served(candidates[i]), wb);
    bool cand_under = is_more_under_served(served(candidates[i]), wb, served(best), wa);
    if (cand_under && !best_under) {
      best = candidates[i];
    } else if (!cand_under && !best_under) {
      if (candidates[i].value < best.value) best = candidates[i];
    } else if (cand_under && best_under) {
      long double ra = static_cast<long double>(served(best)) / static_cast<long double>(wa);
      long double rb = static_cast<long double>(served(candidates[i])) / static_cast<long double>(wb);
      if (rb < ra) {
        best = candidates[i];
      } else if (rb == ra && candidates[i].value < best.value) {
        best = candidates[i];
      }
    }
  }
  return best;
}

bool FairnessController::at_outstanding_cap(TenantId t) const {
  if (policy_.max_outstanding_per_tenant == 0) return false;
  auto it = outstanding_.find(t);
  std::uint64_t out = it == outstanding_.end() ? 0 : it->second;
  return out >= policy_.max_outstanding_per_tenant;
}

std::map<TenantId, std::int64_t> FairnessController::debts() const {
  std::map<TenantId, std::int64_t> out;
  for (const auto& [t, s] : served_) {
    std::uint64_t w = policy_.weight(t);
    if (w == 0) w = policy_.default_weight;
    if (w == 0) w = 1;
    out[t] = static_cast<std::int64_t>(s / w);
  }
  return out;
}

std::map<TenantId, std::uint64_t> FairnessController::served_all() const {
  return served_;
}

void FairnessController::restore_served(TenantId t, std::uint64_t served) {
  served_[t] = served;
  if (outstanding_.find(t) == outstanding_.end()) outstanding_[t] = 0;
}

void FairnessController::reset() {
  served_.clear();
  outstanding_.clear();
  pending_.clear();
}

}  // namespace batch_fabric