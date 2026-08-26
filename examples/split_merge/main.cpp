#include "BFExample.hpp"
#include <cstdio>
using namespace batch_fabric;
int main() {
  BatchFabric f(bfex::config(bfex::policy()));
  for (int i = 0; i < 6; ++i) { auto r = f.submit(bfex::meta("r", TenantId(1))); if (!r.ok()) return 1; }
  auto rep = f.tick();
  if (rep.sealed_batches.empty()) return 1;
  BatchId b = rep.sealed_batches.front();
  auto sp = f.split(b, SplitReason::worker_batch_size_constraint);
  std::printf("split_merge: split batch -> child %s\n", sp.ok() ? sp.value().string().c_str() : "failed");
  // Merge two small compatible batches if the aggregates are valid.
  auto m = f.merge(b, sp.value(), MergeReason::policy);
  std::printf("  merge attempt -> %s\n", m.ok() ? ("merged " + m.value().string()).c_str() : "rejected (correctly)");
  return 0;
}
