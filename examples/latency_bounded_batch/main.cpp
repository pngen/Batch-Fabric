#include "BFExample.hpp"
#include <cstdio>
using namespace batch_fabric;
int main() {
  auto pol = bfex::policy();
  pol.constraints.minimum_preferred_batch = 2;   // wait for a bigger batch
  pol.wait.latency_max_wait_ns[LatencyClass::low_latency] = 500000;  // 0.5ms
  auto clk = std::make_shared<SimulatedClock>(0);
  BatchFabricConfig cfg = bfex::config(pol);
  cfg.clock = clk;
  BatchFabric f(cfg);
  // a single low-latency request must NOT wait indefinitely for a larger batch
  auto r = f.submit(bfex::meta("r", TenantId(1), Phase::decode, 5, 1, 10, LatencyClass::low_latency));
  if (!r.ok()) return 1;
  auto rep0 = f.tick();
  std::printf("latency_bounded_batch: before wait, sealed=%zu (expected 0)\n", rep0.sealed_batches.size());
  clk->advance(600000);  // 0.6ms > latency max-wait
  auto rep = f.tick();
  std::printf("  after 0.6ms, sealed=%zu (latency protected)\n", rep.sealed_batches.size());
  return rep.sealed_batches.size() >= 1 ? 0 : 1;
}
