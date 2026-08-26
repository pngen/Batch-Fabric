#include "batch_fabric/batch_fabric.hpp"
#include "testfw.hpp"
#include <algorithm>
#include <chrono>
#include <cstdio>
#include <memory>
#include <vector>

using namespace batch_fabric;

namespace {

RequestMetadata meta(const std::string& rev, TenantId t, std::uint64_t work = 100) {
  RequestMetadata m;
  m.tenant = t;
  m.descriptor.model = ModelIdentity("m");
  m.descriptor.revision = ModelRevision(rev);
  m.descriptor.adapter = AdapterIdentity("base");
  m.descriptor.phase = Phase::prefill;
  m.descriptor.shape = ShapeKey("s1");
  m.tokens.input = 10; m.tokens.output = 5; m.work.value = work;
  return m;
}

BatchPolicy pol(std::uint32_t max_batch) {
  BatchPolicy p;
  p.constraints.budget.max_requests = max_batch;
  p.constraints.budget.max_input_tokens = 100000;
  p.constraints.budget.max_work = 100000;
  p.constraints.global_max_wait_ns = 1000000;
  p.constraints.minimum_preferred_batch = 1;
  p.wait.global_max_wait_ns = 1000000;
  return p;
}

std::uint64_t now_ns() {
  return static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
      std::chrono::steady_clock::now().time_since_epoch()).count());
}

// End-to-end: submit N requests, form/seed, dispatch complete. Returns ns.
std::uint64_t run_full(std::uint32_t n, std::uint32_t max_batch, std::uint64_t* completed) {
  auto clk = std::make_shared<SimulatedClock>(0);
  BatchFabricConfig cfg; cfg.policy = pol(max_batch); cfg.clock = clk;
  BatchFabric fabric(cfg);
  std::uint64_t t0 = now_ns();
  for (std::uint32_t i = 0; i < n; ++i) {
    auto r = fabric.submit(meta("r", TenantId(1)));
    if (!r.ok()) { *completed = 0; return now_ns() - t0; }
  }
  ExecutorCapability cap; cap.worker = WorkerId(0); cap.backend = Backend::cpu;
  CpuExecutor exec(cap);
  std::uint64_t done = 0;
  for (int iter = 0; iter < 1000; ++iter) {
    auto rep = fabric.tick();
    for (auto bid : rep.sealed_batches) {
      auto dr = fabric.dispatch_and_run(bid, exec);
      if (dr.ok()) done += /* count */ 0;
    }
    if (fabric.stats().requests_completed >= n) break;
  }
  auto st = fabric.stats();
  *completed = st.requests_completed;
  return now_ns() - t0;
}

}  // namespace

int main() {
  std::printf("Batch Fabric benchmark smoke (completed runtime work)\n");

  // submission / compatibility-key / formation aggregation
  {
    auto clk = std::make_shared<SimulatedClock>(0);
    BatchFabricConfig cfg; cfg.policy = pol(64); cfg.clock = clk;
    BatchFabric fabric(cfg);
    std::uint64_t t0 = now_ns();
    const int N = 1000;
    for (int i = 0; i < N; ++i) { auto r = fabric.submit(meta("r", TenantId(1))); if(!r.ok()){} }
    std::uint64_t t1 = now_ns();
    double submit_us = static_cast<double>(t1 - t0) / 1000.0;
    std::printf("submit aggregation: %d requests in %.1f us -> %.1f Kops/s\n", N, submit_us,
                N / (submit_us / 1e6) / 1000.0);

    // compatibility-key computation
    t0 = now_ns();
    RequestMetadata m = meta("r", TenantId(1));
    volatile std::uint64_t sink = 0;
    for (int i = 0; i < N; ++i) { sink += CompatibilityKey::build(m.descriptor).digest()[0]; }
    t1 = now_ns();
    double key_us = static_cast<double>(t1 - t0) / 1000.0;
    std::printf("compatibility-key: %d in %.1f us -> %.1f Kops/s\n", N, key_us, N/(key_us/1e6)/1000.0);
    (void)sink;
  }

  // batch-size scaling (end-to-end completed work)
  std::uint32_t sizes[] = {1, 2, 4, 8, 16, 32, 64};
  std::printf("\nbatch-size scaling: end-to-end completed throughput (requests/s)\n");
  for (auto sz : sizes) {
    std::uint64_t completed = 0;
    std::uint64_t ns = run_full(sz * 8, sz, &completed);  // 8 batches of size sz
    double ms = static_cast<double>(ns) / 1e6;
    double reqs = static_cast<double>(std::min<std::uint64_t>(completed, sz * 8));
    double throughput = ms > 0 ? reqs / (ms / 1000.0) : 0.0;
    std::printf("  size=%u  completed=%llu  time=%.2f ms  throughput=%.1f req/s\n", sz,
                static_cast<unsigned long long>(completed), ms, throughput);
  }

  // queue-depth scaling: form + seal time, completed batches
  std::printf("\nqueue-depth scaling: formation/seal cost\n");
  for (int depth : {100, 1000, 10000}) {
    auto clk = std::make_shared<SimulatedClock>(0);
    BatchFabricConfig cfg; cfg.policy = pol(64); cfg.clock = clk;
    BatchFabric fabric(cfg);
    for (int i = 0; i < depth; ++i) { auto r = fabric.submit(meta("r", TenantId(1))); if(!r.ok()){} }
    std::uint64_t t0 = now_ns();
    auto rep = fabric.tick();
    std::uint64_t t1 = now_ns();
    double ms = static_cast<double>(t1 - t0) / 1e6;
    std::printf("  depth=%d  sealed_batches=%zu  time=%.2f ms (%.1f us/req)\n", depth,
                rep.sealed_batches.size(), ms, ms * 1000.0 / depth);
  }

  return testfw::exit_code();
}
