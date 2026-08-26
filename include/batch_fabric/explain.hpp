#pragma once
#include "batch_fabric/batch.hpp"
#include "batch_fabric/enums.hpp"
#include "batch_fabric/id.hpp"
#include <string>
#include <vector>

namespace batch_fabric {

// A single explanation step describing why a request did or did not join a
// batch, why a batch sealed/split/merged, or why a worker was eligible.
struct ExplainEntry {
  std::string category;  // e.g. "compatibility", "formation", "seal", "split"
  std::string detail;
};

// Structured explanation report. Rendering is done by the CLI; the values are
// machine-readable so tests can assert on reasons.
struct ExplainResult {
  RequestId request;
  std::string summary;
  std::vector<ExplainEntry> entries;
};

}  // namespace batch_fabric
