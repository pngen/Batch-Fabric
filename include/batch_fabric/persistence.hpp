#pragma once
#include "batch_fabric/batch.hpp"
#include "batch_fabric/identity.hpp"
#include "batch_fabric/io.hpp"
#include "batch_fabric/policy.hpp"
#include "batch_fabric/request.hpp"
#include "batch_fabric/result.hpp"
#include <cstdint>
#include <map>
#include <string>
#include <vector>

namespace batch_fabric {

// Persistent, scheduler-owned batching metadata. Only the metadata needed for
// coherent recovery is persisted; external execution is never pretended to
// have survived.
struct PersistedRequest {
  RequestId id;
  TenantId tenant;
  SessionId session;
  SequenceId sequence;
  ModelDescriptor descriptor;
  TokenEstimate tokens;
  WorkEstimate work;
  std::uint64_t memory_bytes = 0;
  Deadline deadline;
  LatencyClass latency = LatencyClass::batch;
  PriorityClass priority = PriorityClass::normal;
  std::string payload;
  RequestState state = RequestState::created;
  TimePoint submitted_ns = 0;
  BatchId batch;
  AttemptId current_attempt;
  std::vector<AttemptId> attempt_ids;
};

struct PersistedAttempt {
  AttemptId id;
  std::uint32_t number = 0;
  RequestState state = RequestState::created;
  BatchId batch;
  CompletionStatus outcome = CompletionStatus::success;
  std::string result;
  TimePoint started = 0;
  TimePoint completed = 0;
};

struct PersistedBatch {
  BatchId id;
  Generation generation;
  BatchEpoch epoch;
  BatchState state = BatchState::forming;
  CompatibilityKey key;
  std::vector<RequestId> members;
  BatchAccum used;
  TimePoint formed_ns = 0;
  TimePoint sealed_ns = 0;
  TimePoint dispatched_ns = 0;
  TimePoint completed_ns = 0;
  SealReason seal_reason = SealReason::max_wait_elapsed;
  WorkerId worker;
  WorkerBootId boot;
  std::vector<BatchId> parents;
  std::vector<BatchId> children;
};

struct PersistedRuntime {
  BatchEpoch epoch;
  std::uint64_t next_request = 1;
  std::uint64_t next_batch = 1;
  std::uint64_t next_attempt = 1;
  std::uint64_t next_generation = 1;
  std::vector<PersistedRequest> requests;
  std::vector<PersistedAttempt> attempts;
  std::vector<PersistedBatch> batches;
  std::map<std::string, std::uint64_t> fairness_served;
};

// Durability store with atomic writes (temp file, flush, atomic replace) and a
// SHA-256 integrity checksum. Loads reject corruption instead of guessing.
class PersistenceStore {
 public:
  static constexpr std::uint32_t kMagic = 0x46435254;   // "FCRT"
  static constexpr std::uint32_t kFormatVersion = 1;
  static constexpr std::size_t kMaxRecords = 10u * 1024u * 1024u;

  Result<void> save(const PersistedRuntime& rt, const std::string& path) const;
  Result<PersistedRuntime> load(const std::string& path) const;

 private:
  Result<PersistedRuntime> parse(const std::vector<std::uint8_t>& bytes) const;
};

}  // namespace batch_fabric