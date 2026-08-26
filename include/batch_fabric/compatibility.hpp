#pragma once
#include "batch_fabric/enums.hpp"
#include "batch_fabric/identity.hpp"
#include "batch_fabric/id.hpp"
#include <unordered_map>
#include <vector>

namespace batch_fabric {

// Evaluate whether two descriptors may legally execute together in one batch.
// Returns a specific incompatible_* reason when they may not. A false-positive
// "compatible" result is a correctness bug.
CompatibilityDecision evaluate_compatibility(const ModelDescriptor& a,
                                             const ModelDescriptor& b);

// Index of waiting requests keyed by canonical CompatibilityKey. This makes
// formation membership lookup O(1) per candidate instead of scanning the whole
// queue. The index is deliberately used as the primary membership structure.
class CompatibilityIndex {
 public:
  void add(const CompatibilityKey& key, RequestId id);
  void remove(const CompatibilityKey& key, RequestId id);
  const std::vector<RequestId>* bucket(const CompatibilityKey& key) const;
  std::size_t bucket_count() const;
  const std::unordered_map<CompatibilityKey, std::vector<RequestId>>& buckets() const;
  void clear();

 private:
  std::unordered_map<CompatibilityKey, std::vector<RequestId>> buckets_;
};

}  // namespace batch_fabric
