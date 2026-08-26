#include "batch_fabric/batch_fabric.hpp"
#include "testfw.hpp"
#include <cstdio>
#include <memory>

using namespace batch_fabric;

namespace {

RequestMetadata make_meta(const std::string& model, const std::string& rev, Phase phase,
                          TenantId tenant, std::uint64_t in = 10, std::uint64_t out = 5,
                          std::uint64_t work = 100, LatencyClass lat = LatencyClass::batch,
                          std::int64_t deadline_ns = 0) {
  RequestMetadata m;
  m.tenant = tenant;
  m.descriptor.model = ModelIdentity(model);
  m.descriptor.revision = ModelRevision(rev);
  m.descriptor.adapter = AdapterIdentity("base");
  m.descriptor.phase = phase;
  m.descriptor.dtype = Dtype::float16;
  m.descriptor.backend = Backend::cpu;
  m.descriptor.shape = ShapeKey("s1");
  m.descriptor.sampling_semantics = "default";
  m.tokens.input = in;
  m.tokens.output = out;
  m.work.value = work;
  m.memory_bytes = 0;
  if (deadline_ns != 0) m.deadline.absolute_ns = deadline_ns;
  m.latency = lat;
  return m;
}

BatchFabricConfig make_config(const std::shared_ptr<Clock>& clock, const BatchPolicy& policy) {
  BatchFabricConfig cfg;
  cfg.policy = policy;
  cfg.clock = clock;
  cfg.start_time_ns = 0;
  return cfg;
}

BatchPolicy default_policy() {
  BatchPolicy p;
  p.constraints.budget.max_requests = 8;
  p.constraints.budget.max_input_tokens = 100000;
  p.constraints.budget.max_work = 100000;
  p.constraints.global_max_wait_ns = 1000000;
  p.constraints.minimum_preferred_batch = 1;
  p.wait.global_max_wait_ns = 1000000;
  p.wait.latency_max_wait_ns[LatencyClass::low_latency] = 500000;
  p.wait.immediate_seal_when_solo = true;
  p.retry.max_attempts = 3;
  return p;
}

BatchPolicy wait_policy() {
  BatchPolicy p;
  p.constraints.budget.max_requests = 8;
  p.constraints.budget.max_input_tokens = 100000;
  p.constraints.budget.max_work = 100000;
  p.constraints.global_max_wait_ns = 1000000;
  p.constraints.minimum_preferred_batch = 2;
  p.wait.global_max_wait_ns = 1000000;
  p.wait.latency_max_wait_ns[LatencyClass::low_latency] = 500000;
  p.wait.latency_max_wait_ns[LatencyClass::deadline_bounded] = 500000;
  p.retry.max_attempts = 3;
  return p;
}

CpuExecutor make_cpu(WorkerId w = WorkerId(0)) {
  ExecutorCapability cap;
  cap.worker = w;
  cap.backend = Backend::cpu;
  return CpuExecutor(cap);
}

}  // namespace

int main() {
  {
  auto d = Sha256::digest("abc");
    CHECK_EQ(to_hex(d), "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad");
  }

  {
  Result<int> r = Result<int>::ok(7);
    CHECK(r.ok());
    CHECK_EQ(r.value(), 7);
    Result<int> e = Result<int>::err(ErrorCode::not_found, "x");
    CHECK(!e.ok());
    CHECK_EQ(e.error().code(), ErrorCode::not_found);
    Result<void> v = Result<void>::success();
    CHECK(v.ok());
  }

  {
  ModelDescriptor a = make_meta("m", "r", Phase::prefill, TenantId(1)).descriptor;
    ModelDescriptor a2 = make_meta("m", "r", Phase::prefill, TenantId(5)).descriptor;
    ModelDescriptor b = make_meta("m", "r2", Phase::prefill, TenantId(1)).descriptor;
    CompatibilityKey ka = CompatibilityKey::build(a);
    CompatibilityKey ka2 = CompatibilityKey::build(a2);
    CompatibilityKey kb = CompatibilityKey::build(b);
    CHECK(ka == ka2);
    CHECK(ka != kb);
    CHECK_EQ(evaluate_compatibility(a, a2), CompatibilityDecision::compatible);
    CHECK_EQ(evaluate_compatibility(a, b), CompatibilityDecision::incompatible_revision);
  }

  {
    auto clk = std::make_shared<SimulatedClock>(0);
    BatchFabric fabric(make_config(clk, default_policy()));
  for (int i = 0; i < 4; ++i) {
      auto r = fabric.submit(make_meta("m", "r", Phase::prefill, TenantId(1)));
      REQUIRE(r.ok());
    }
    auto rep = fabric.tick();
    CHECK(rep.sealed_batches.size() >= 1);
    auto b = rep.sealed_batches.front();
    CpuExecutor exec = make_cpu();
    auto dr = fabric.dispatch_and_run(b, exec);
    CHECK(dr.ok());
    auto st = fabric.stats();
    CHECK_EQ(st.requests_completed, 4);
    CHECK_EQ(st.waiting_now, 0);
    CHECK_EQ(st.running_now, 0);
  }

  {
    auto clk = std::make_shared<SimulatedClock>(0);
    BatchFabric fabric(make_config(clk, wait_policy()));
  auto r = fabric.submit(make_meta("m", "r", Phase::decode, TenantId(1), 5, 1, 10,
                                     LatencyClass::low_latency));
    REQUIRE(r.ok());
    CHECK(fabric.waiting().size() >= 1);
    auto rep0 = fabric.tick();
    CHECK(rep0.sealed_batches.empty());
    clk->advance(600000);
    auto rep = fabric.tick();
    CHECK(rep.sealed_batches.size() >= 1);
  }

  {
    auto clk = std::make_shared<SimulatedClock>(0);
    BatchFabric fabric(make_config(clk, wait_policy()));
    RequestMetadata expired = make_meta("m", "r", Phase::prefill, TenantId(1), 5, 1, 10);
  expired.deadline.absolute_ns = 500;
    auto r = fabric.submit(expired, /*now*/ 1000);
    REQUIRE(r.ok());
    CHECK_EQ(r.value().admission, AdmissionDecision::expired);

    RequestMetadata near = make_meta("m", "r", Phase::prefill, TenantId(1), 5, 1, 10,
                                     LatencyClass::deadline_bounded);
    near.deadline.absolute_ns = 200000;
    auto r2 = fabric.submit(near, /*now*/ 0);
    REQUIRE(r2.ok());
    clk->advance(150000);
    auto rep = fabric.tick();
    CHECK(rep.sealed_batches.size() >= 1);
    CpuExecutor exec = make_cpu();
    auto dr = fabric.dispatch_and_run(rep.sealed_batches.front(), exec);
    CHECK(dr.ok());
    CHECK(fabric.stats().requests_completed >= 1);
  }

  {
    auto clk = std::make_shared<SimulatedClock>(0);
    auto pol = default_policy();
    pol.fairness.enabled = true;
  pol.fairness.weights[TenantId(1)] = 8;
    pol.fairness.weights[TenantId(2)] = 1;
    BatchFabric fabric(make_config(clk, pol));
    TenantId A(1), B(2);
    for (int i = 0; i < 40; ++i) {
      auto ra = fabric.submit(make_meta("m", "r", Phase::prefill, A));
      auto rb = fabric.submit(make_meta("m", "r", Phase::prefill, B));
      REQUIRE(ra.ok() && rb.ok());
    }
    for (int iter = 0; iter < 30; ++iter) {
      auto rep = fabric.tick();
      CpuExecutor exec = make_cpu();
      for (auto bid : rep.sealed_batches) {
        auto dr = fabric.dispatch_and_run(bid, exec);
        (void)dr;
      }
    }
    auto st = fabric.stats();
    CHECK(st.requests_completed > 0);
  }

  {
    auto clk = std::make_shared<SimulatedClock>(0);
    BatchFabric fabric(make_config(clk, default_policy()));
  for (int i = 0; i < 6; ++i) {
      auto r = fabric.submit(make_meta("m", "r", Phase::prefill, TenantId(1)));
      REQUIRE(r.ok());
    }
    auto rep = fabric.tick();
    CHECK(rep.sealed_batches.size() >= 1);
    BatchId b = rep.sealed_batches.front();
    auto split = fabric.split(b, SplitReason::policy);
    CHECK(split.ok());
    CHECK(!split.value().is_null());
  }

  {
    auto clk = std::make_shared<SimulatedClock>(0);
    BatchFabric fabric(make_config(clk, default_policy()));
    auto r1 = fabric.submit(make_meta("m", "r", Phase::prefill, TenantId(1)));
    auto r2 = fabric.submit(make_meta("m", "r", Phase::prefill, TenantId(1)));
    REQUIRE(r1.ok() && r2.ok());
  auto c = fabric.cancel(r1.value().request, CancellationReason::client_request);
    CHECK(c.ok());
    CHECK_EQ(fabric.count_in_state(RequestState::cancelled), 1);
    auto rep = fabric.tick();
    CHECK(rep.sealed_batches.size() >= 1);
    auto c2 = fabric.cancel(r2.value().request, CancellationReason::client_request);
    CHECK(c2.ok());
  }

  {
    auto clk = std::make_shared<SimulatedClock>(0);
    BatchFabric fabric(make_config(clk, default_policy()));
    auto ra = fabric.submit(make_meta("m", "r", Phase::prefill, TenantId(1)));
    auto rb = fabric.submit(make_meta("m", "r", Phase::prefill, TenantId(1)));
    REQUIRE(ra.ok() && rb.ok());
    auto rep = fabric.tick();
    CHECK(rep.sealed_batches.size() >= 1);
    BatchId b = rep.sealed_batches.front();
    CpuExecutor exec = make_cpu();
    auto dr = fabric.dispatch_and_run(b, exec);
    CHECK(dr.ok());
  CHECK(fabric.stats().requests_completed >= 2);
    CHECK(fabric.stats().requests_retried == 0);
  }

  {
    auto clk = std::make_shared<SimulatedClock>(0);
    BatchFabric fabric(make_config(clk, default_policy()));
    auto r = fabric.submit(make_meta("m", "r", Phase::prefill, TenantId(1)));
    REQUIRE(r.ok());
  auto ex = fabric.explain(r.value().request);
    CHECK(!ex.summary.empty());
    CHECK(!ex.entries.empty());
  }

  return testfw::exit_code();
}