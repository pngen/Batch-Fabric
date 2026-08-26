#pragma once
#include "batch_fabric/id.hpp"
#include <cstdint>
#include <map>
#include <string>

namespace batch_fabric {

// Aggregate counters for the whole runtime.
struct BatchStats {
  std::uint64_t submitted = 0;
  std::uint64_t admitted = 0;
  std::uint64_t deferred = 0;
  std::uint64_t rejected = 0;
  std::uint64_t expired = 0;
  std::uint64_t cancelled = 0;
  std::uint64_t batches_formed = 0;
  std::uint64_t batches_sealed = 0;
  std::uint64_t batches_dispatched = 0;
  std::uint64_t batches_completed = 0;
  std::uint64_t batches_split = 0;
  std::uint64_t batches_merged = 0;
  std::uint64_t requests_completed = 0;
  std::uint64_t requests_retried = 0;
  std::uint64_t retryable_failures = 0;
  std::uint64_t stale_rejections = 0;
  std::uint64_t waiting_now = 0;
  std::uint64_t forming_now = 0;
  std::uint64_t running_now = 0;
  std::uint64_t reserved_now = 0;
  std::uint64_t total_work_completed = 0;  // units of completed runtime work
  double total_wait_time_ns = 0.0;
  std::map<std::string, std::uint64_t> fairness_served;  // tenant -> work served
};

}  // namespace batch_fabric
