#include "BFExample.hpp"
#include <cstdio>
using namespace batch_fabric;
int main() {
  auto pol = bfex::policy();
  pol.constraints.allow_cross_phase = false;  // default: never mix
  BatchFabric f(bfex::config(pol));
  // prefill requests
  for (int i = 0; i < 3; ++i) { auto r = f.submit(bfex::meta("r", TenantId(1), Phase::prefill, 100, 5, 1000)); (void)r; }
  // decode requests
  for (int i = 0; i < 3; ++i) { auto r = f.submit(bfex::meta("r", TenantId(1), Phase::decode, 10, 1, 10)); (void)r; }
  auto rep = f.tick();
  auto snap = f.snapshot();
  std::printf("prefill_decode: %zu sealed batches\n", rep.sealed_batches.size());
  for (auto& b : snap.batches) {
    std::printf("  batch state=%s members=%zu\n", to_string(b.state).data(), b.member_count);
    (void)b;
  }
  return 0;
}