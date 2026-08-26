#include "batch_fabric/compatibility.hpp"
#include <algorithm>

namespace batch_fabric {

CompatibilityDecision evaluate_compatibility(const ModelDescriptor& a,
                                             const ModelDescriptor& b) {
  // Field-by-field, ordered by correctness importance. Returns the first
  // incompatibility found so the caller gets a precise, explainable reason.
  if (a.model != b.model) return CompatibilityDecision::incompatible_model;
  if (a.revision != b.revision) return CompatibilityDecision::incompatible_revision;
  if (a.adapter != b.adapter) return CompatibilityDecision::incompatible_adapter;
  if (a.phase != b.phase) return CompatibilityDecision::incompatible_phase;
  if (a.dtype != b.dtype) return CompatibilityDecision::incompatible_dtype;
  if (a.backend != b.backend) return CompatibilityDecision::incompatible_backend;
  if (a.execution_mode != b.execution_mode)
    return CompatibilityDecision::incompatible_execution_mode;
  if (a.device != b.device) return CompatibilityDecision::incompatible_device;
  if (a.shape != b.shape) return CompatibilityDecision::incompatible_shape;
  if (a.sampling_semantics != b.sampling_semantics)
    return CompatibilityDecision::incompatible_sampling;
  if (a.extensions != b.extensions)
    return CompatibilityDecision::incompatible_extension;
  return CompatibilityDecision::compatible;
}

void CompatibilityIndex::add(const CompatibilityKey& key, RequestId id) {
  buckets_[key].push_back(id);
}

void CompatibilityIndex::remove(const CompatibilityKey& key, RequestId id) {
  auto it = buckets_.find(key);
  if (it == buckets_.end()) return;
  auto& vec = it->second;
  auto pos = std::find(vec.begin(), vec.end(), id);
  if (pos != vec.end()) vec.erase(pos);
  if (vec.empty()) buckets_.erase(it);
}

const std::vector<RequestId>* CompatibilityIndex::bucket(const CompatibilityKey& key) const {
  auto it = buckets_.find(key);
  return it == buckets_.end() ? nullptr : &it->second;
}

std::size_t CompatibilityIndex::bucket_count() const { return buckets_.size(); }

const std::unordered_map<CompatibilityKey, std::vector<RequestId>>&
CompatibilityIndex::buckets() const {
  return buckets_;
}

void CompatibilityIndex::clear() { buckets_.clear(); }

}  // namespace batch_fabric
