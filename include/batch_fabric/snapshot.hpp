#pragma once
#include "batch_fabric/enums.hpp"
#include "batch_fabric/id.hpp"
#include "batch_fabric/identity.hpp"
#include <string>
#include <vector>

namespace batch_fabric {

struct BatchSnapshotEntry {
  BatchId batch;
  Generation generation;
  BatchEpoch epoch;
  BatchState state;
  CompatibilityKey key;
  std::size_t member_count = 0;
  WorkerId worker;
  WorkerBootId boot;
  SealReason seal_reason;
};

struct RequestSnapshotEntry {
  RequestId request;
  TenantId tenant;
  RequestState state;
  BatchId batch;
  AttemptId attempt;
  std::uint32_t attempt_number = 0;
  bool expired = false;
};

struct WorkerSnapshotEntry {
  WorkerId worker;
  WorkerBootId boot;
  bool live = false;
  std::uint32_t active_batch_count = 0;
  std::uint64_t completed_batches = 0;
};

// Point-in-time snapshot of the whole runtime state.
struct BatchSnapshot {
  std::vector<BatchSnapshotEntry> batches;
  std::vector<RequestSnapshotEntry> requests;
  std::vector<WorkerSnapshotEntry> workers;
  BatchEpoch epoch;
  TimePoint now = 0;
};

}  // namespace batch_fabric
