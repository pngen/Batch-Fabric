#pragma once
#include "batch_fabric/identity.hpp"
#include "batch_fabric/id.hpp"
#include "batch_fabric/policy.hpp"
#include <string>
#include <vector>

namespace batch_fabric {

// Public, caller-visible description of an inference request submitted to the
// runtime. Batch Fabric never executes the payload itself; it carries the
// metadata needed to form legally batchable groups and to hand work to an
// external executor with an explicit contract.
struct RequestMetadata {
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
  std::string payload;  // opaque, forwarded verbatim to the executor
};

}  // namespace batch_fabric
