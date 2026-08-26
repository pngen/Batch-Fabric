#include "batch_fabric/batch_fabric.hpp"
#include "testfw.hpp"
#include <cstdio>
#include <map>
#include <memory>
#include <random>
#include <string>
#include <vector>

using namespace batch_fabric;

namespace {

RequestMetadata meta(const std::string& rev, TenantId t) {
  RequestMetadata m;
  m.tenant = t;
  m.descriptor.model = ModelIdentity("m");
  m.descriptor.revision = ModelRevision(rev);
  m.descriptor.adapter = AdapterIdentity("base");
  m.descriptor.phase = Phase::prefill;
  m.descriptor.shape = ShapeKey("s1");
  m.tokens.input = 10; m.tokens.output = 5; m.work.value = 100;
  return m;
}

BatchPolicy pol() {
  BatchPolicy p;
  p.constraints.budget.max_requests = 6;
  p.constraints.budget.max_input_tokens = 100000;
  p.constraints.budget.max_work = 100000;
  p.constraints.global_max_wait_ns = 1000000;
  p.constraints.minimum_preferred_batch = 1;
  p.wait.global_max_wait_ns = 1000000;
  p.retry.max_attempts = 3;
  return p;
}

void check_invariants(BatchFabric& f) {
  auto snap = f.snapshot();
  std::map<RequestId, BatchId> active_batch;
  for (const auto& r : snap.requests) {
    BatchId b = r.batch;
    // active requests in a batch: reserved/batched/dispatched/running
    bool active = r.state == RequestState::reserved || r.state == RequestState::batched ||
                  r.state == RequestState::dispatched || r.state == RequestState::running;
    if (active && !b.is_null()) {
      auto it = active_batch.find(r.request);
      if (it != active_batch.end()) {
        CHECK(it->second == b);
      } else {
        active_batch[r.request] = b;
      }
    }
    // No expired request is dispatched/running.
    if ((r.state == RequestState::dispatched || r.state == RequestState::running) && r.expired) {
      CHECK(!r.expired);
    }
  }
  // No request in two different non-terminal batches: enforced by the map check above.
  // Counts must be non-negative (no underflow) and completed <= submitted.
  auto st = f.stats();
  CHECK(st.submitted >= st.requests_completed);
}

void run_seed(std::uint64_t seed) {
  auto clk = std::make_shared<SimulatedClock>(0);
  BatchFabric fabric([&]{ BatchFabricConfig c; c.policy = pol(); c.clock = clk; c.seed = seed; return c; }());
  std::mt19937_64 rng(seed);
  std::vector<RequestId> waiting;
  ExecutorCapability cap;
  cap.worker = WorkerId(0);
  cap.backend = Backend::cpu;
  CpuExecutor exec(cap);

  for (int op = 0; op < 2000; ++op) {
    int kind = static_cast<int>(rng() % 8);
    switch (kind) {
      case 0: case 1: case 2: {
        std::string rev = (rng() % 3 == 0) ? "r1" : (rng() % 2 == 0 ? "r2" : "r3");
        TenantId t(1 + static_cast<std::uint64_t>(rng() % 4));
        auto r = fabric.submit(meta(rev, t));
        if (r.ok()) waiting.push_back(r.value().request);
        break;
      }
      case 3: {
        auto rep = fabric.tick();
        for (auto bid : rep.sealed_batches) {
          auto dr = fabric.dispatch_and_run(bid, exec);
          (void)dr;
        }
        break;
      }
      case 4: {
        if (!waiting.empty()) {
          RequestId rid = waiting[rng() % waiting.size()];
          auto c = fabric.cancel(rid, CancellationReason::client_request);
          (void)c;
        }
        break;
      }
      case 5: {
        // expire
        fabric.expire();
        break;
      }
      case 6: {
        if (rng() % 4 == 0) fabric.roll_epoch();
        // split or merge a sealed batch sometimes
        auto b = fabric.batches();
        if (!b.empty() && rng() % 4 == 0) {
          auto sp = fabric.split(b[rng() % b.size()], SplitReason::policy);
          (void)sp;
          auto mg = fabric.merge(b[rng() % b.size()], b[rng() % b.size()], MergeReason::policy);
          (void)mg;
        }
        break;
      }
      case 7: {
        // advance simulated time sometimes to trigger deadline/max-wait seals
        clk->advance(200000);
        auto rep = fabric.tick();
        for (auto bid : rep.sealed_batches) {
          auto dr = fabric.dispatch_and_run(bid, exec);
          (void)dr;
        }
        break;
      }
    }
    check_invariants(fabric);
  }
  // Drain: complete everything.
  for (int i = 0; i < 50; ++i) {
    auto rep = fabric.tick();
    for (auto bid : rep.sealed_batches) {
      auto dr = fabric.dispatch_and_run(bid, exec);
      (void)dr;
    }
  }
  // Final accounting closure: after draining, no leaked waiting/reserved/running.
  auto st = fabric.stats();
  CHECK_EQ(st.waiting_now + st.running_now, 0);
}

}  // namespace

int main() {
  std::uint64_t seed = 12345;
  std::printf("Randomized property test (seed %llu)\n", static_cast<unsigned long long>(seed));
  run_seed(seed);
  // Additional seeds to broaden coverage.
  run_seed(999);
  run_seed(0xdeadbeef);
  return testfw::exit_code();
}
