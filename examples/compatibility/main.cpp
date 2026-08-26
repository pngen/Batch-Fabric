#include "BFExample.hpp"
#include <cstdio>
using namespace batch_fabric;
int main() {
  ModelDescriptor a = bfex::meta("r1", TenantId(1)).descriptor;
  ModelDescriptor b = bfex::meta("r2", TenantId(1)).descriptor;
  ModelDescriptor c = bfex::meta("r1", TenantId(1)).descriptor;
  auto ka = CompatibilityKey::build(a);
  auto kb = CompatibilityKey::build(b);
  std::printf("compatibility:\n");
  std::printf("  same revision compatible: %s\n", to_string(evaluate_compatibility(a, c)).data());
  std::printf("  different revision:       %s\n", to_string(evaluate_compatibility(a, b)).data());
  std::printf("  key(r1)=%s\n  key(r2)=%s\n", ka.hex().c_str(), kb.hex().c_str());
  // Demonstrate they never batch.
  BatchFabric f(bfex::config(bfex::policy()));
  auto r1 = f.submit(bfex::meta("r1", TenantId(1)));
  auto r2 = f.submit(bfex::meta("r2", TenantId(1)));
  auto rep = f.tick();
  std::printf("  incompatible requests produced %zu batches (never merged)\n", rep.sealed_batches.size());
  if (!r1.ok() || !r2.ok()) return 1;
  return 0;
}
