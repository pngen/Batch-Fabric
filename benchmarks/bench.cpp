#include "batch_fabric/batch_fabric.hpp"
#include "batch_fabric/executor.hpp"
#include <algorithm>
#include <chrono>
#include <cstdio>
#include <memory>
#include <string>
#include <thread>
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

BatchPolicy pol(std::uint32_t maxb) {
  BatchPolicy p;
  p.constraints.budget.max_requests = maxb;
  p.constraints.budget.max_input_tokens = 100000;
  p.constraints.budget.max_work = 100000;
  p.constraints.global_max_wait_ns = 1000000;
  p.constraints.minimum_preferred_batch = 1;
  p.wait.global_max_wait_ns = 1000000;
  return p;
}

std::uint64_t ns() {
  return static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
      std::chrono::steady_clock::now().time_since_epoch()).count());
}

CpuExecutor cpu() {
  ExecutorCapability c; c.worker = WorkerId(0); c.backend = Backend::cpu;
  return CpuExecutor(c);
}

void drain(BatchFabric& f) {
  CpuExecutor e = cpu();
  for (int i = 0; i < 100000; ++i) {
    auto rep = f.tick();
    for (auto b : rep.sealed_batches) { auto dr = f.dispatch_and_run(b, e); (void)dr; }
    if (f.stats().requests_completed >= f.stats().submitted) break;
  }
}

}  // namespace

int main() {
  std::setvbuf(stdout, nullptr, _IONBF, 0);
  std::printf("Batch Fabric benchmark (completed runtime work)\n");

  {
    BatchFabric f([&]{ BatchFabricConfig c; c.policy=pol(64); c.clock=std::make_shared<SimulatedClock>(0); return c; }());
    const int N = 10000;
    std::uint64_t t0 = ns();
    for (int i = 0; i < N; ++i) { auto r = f.submit(meta("r", TenantId(1))); (void)r; }
    std::uint64_t t1 = ns();
    std::printf("  submission             total=%.2f ms  %.1f Kops/s\n", (t1-t0)/1e6, N/((t1-t0)/1e9)/1000.0);
  }

  {
    RequestMetadata m = meta("r", TenantId(1));
    const int N = 100000;
    std::uint64_t t0 = ns();
    volatile std::uint64_t sink = 0;
    for (int i = 0; i < N; ++i) sink += CompatibilityKey::build(m.descriptor).digest()[0];
    std::uint64_t t1 = ns();
    std::printf("  compatibility-key      total=%.2f ms  %.1f Kops/s\n", (t1-t0)/1e6, N/((t1-t0)/1e9)/1000.0);
    (void)sink;
  }

  std::printf("  batch-size scaling (end-to-end completed work):\n");
  for (std::uint32_t sz : {1u,2u,4u,8u,16u,32u,64u}) {
    BatchFabric f([&]{ BatchFabricConfig c; c.policy=pol(sz); c.clock=std::make_shared<SimulatedClock>(0); return c; }());
    const int N = 5000;
    for (int i = 0; i < N; ++i) { auto r = f.submit(meta("r", TenantId(1))); (void)r; }
    CpuExecutor e = cpu();
    std::uint64_t t0 = ns();
    for (int it = 0; it < 100000; ++it) {
      auto rep = f.tick();
      for (auto b : rep.sealed_batches) { auto dr = f.dispatch_and_run(b, e); (void)dr; }
      if (f.stats().requests_completed >= (std::uint64_t)N) break;
    }
    std::uint64_t t1 = ns();
    double ms = (t1-t0)/1e6;
    std::printf("    size=%u  completed=%llu  %.1f req/s\n", sz,
                (unsigned long long)f.stats().requests_completed,
                f.stats().requests_completed / (ms/1000.0));
  }

  std::printf("  queue-depth scaling (formation/seal):\n");
  for (int depth : {1000, 10000, 100000}) {
    BatchFabric f([&]{ BatchFabricConfig c; c.policy=pol(64); c.clock=std::make_shared<SimulatedClock>(0); return c; }());
    for (int i = 0; i < depth; ++i) { auto r = f.submit(meta("r", TenantId(1))); (void)r; }
    std::uint64_t t0 = ns();
    auto rep = f.tick();
    std::uint64_t t1 = ns();
    std::printf("    depth=%d  sealed=%zu  %.2f ms (%.2f us/req)\n", depth, rep.sealed_batches.size(),
                (t1-t0)/1e6, (t1-t0)/1000.0/depth);
  }

  {
    const int THREADS = 4, PER = 2500;
    BatchFabric f([&]{ BatchFabricConfig c; c.policy=pol(16); c.clock=std::make_shared<SimulatedClock>(0); return c; }());
    std::uint64_t t0 = ns();
    std::vector<std::thread> th;
    for (int t = 0; t < THREADS; ++t)
      th.emplace_back([&, t]{ for (int i = 0; i < PER; ++i) { auto r = f.submit(meta("r", TenantId(1+t))); (void)r; } });
    for (auto& x : th) x.join();
    CpuExecutor e = cpu();
    for (int it = 0; it < 100000; ++it) {
      auto rep = f.tick();
      for (auto b : rep.sealed_batches) { auto dr = f.dispatch_and_run(b, e); (void)dr; }
      if (f.stats().requests_completed >= (std::uint64_t)(THREADS*PER)) break;
    }
    std::uint64_t t1 = ns();
    std::printf("  multithreaded (%d threads) completed %llu in %.1f ms -> %.0f req/s\n", THREADS,
                (unsigned long long)f.stats().requests_completed, (t1-t0)/1e6,
                (double)f.stats().requests_completed/((t1-t0)/1e9));
  }

  {
    BatchFabric f([&]{ BatchFabricConfig c; c.policy=pol(64); c.clock=std::make_shared<SimulatedClock>(0); return c; }());
    for (int i = 0; i < 5000; ++i) { auto r = f.submit(meta("r", TenantId(1))); (void)r; }
    drain(f);
    const std::string path = "_bf_bench_state.bfstate";
    std::uint64_t t0 = ns();
    f.persist_to(path);
    std::uint64_t t1 = ns();
    BatchFabric g([&]{ BatchFabricConfig c; c.policy=pol(64); c.clock=std::make_shared<SimulatedClock>(0); return c; }());
    g.recover_from(path);
    std::uint64_t t2 = ns();
    std::printf("  persistence serialize=%.2f ms, recover=%.2f ms (requests=%zu)\n", (t1-t0)/1e6,
                (t2-t1)/1e6, g.snapshot().requests.size());
    std::remove(path.c_str());
  }

  {
    BatchFabric f([&]{ BatchFabricConfig c; c.policy=pol(64); c.clock=std::make_shared<SimulatedClock>(0); return c; }());
    for (int i = 0; i < 5000; ++i) { auto r = f.submit(meta("r", TenantId(1))); (void)r; }
    drain(f);
    std::uint64_t t0 = ns();
    for (int i = 0; i < 100; ++i) { auto s = f.snapshot(); (void)s; }
    std::uint64_t t1 = ns();
    std::printf("  snapshot                %.2f ms per snapshot (100 runs)\n", (t1-t0)/1e6/100.0);
  }

  std::printf("benchmark complete\n");
  return 0;
}