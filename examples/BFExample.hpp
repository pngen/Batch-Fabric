#pragma once
// Shared helpers for Batch Fabric examples.
#include "batch_fabric/batch_fabric.hpp"
#include "batch_fabric/executor.hpp"
#include <memory>

namespace bfex {

inline batch_fabric::RequestMetadata meta(const std::string& rev, batch_fabric::TenantId tenant,
                                          batch_fabric::Phase phase = batch_fabric::Phase::prefill,
                                          std::uint64_t in = 10, std::uint64_t out = 5,
                                          std::uint64_t work = 100,
                                          batch_fabric::LatencyClass lat = batch_fabric::LatencyClass::batch) {
  batch_fabric::RequestMetadata m;
  m.tenant = tenant;
  m.descriptor.model = batch_fabric::ModelIdentity("m");
  m.descriptor.revision = batch_fabric::ModelRevision(rev);
  m.descriptor.adapter = batch_fabric::AdapterIdentity("base");
  m.descriptor.phase = phase;
  m.descriptor.dtype = batch_fabric::Dtype::float16;
  m.descriptor.backend = batch_fabric::Backend::cpu;
  m.descriptor.shape = batch_fabric::ShapeKey("s1");
  m.descriptor.sampling_semantics = "default";
  m.tokens.input = in;
  m.tokens.output = out;
  m.work.value = work;
  m.latency = lat;
  return m;
}

inline batch_fabric::BatchPolicy policy(std::uint32_t max_batch = 64) {
  batch_fabric::BatchPolicy p;
  p.constraints.budget.max_requests = max_batch;
  p.constraints.budget.max_input_tokens = 100000;
  p.constraints.budget.max_work = 100000;
  p.constraints.global_max_wait_ns = 1000000;
  p.constraints.minimum_preferred_batch = 1;
  p.wait.global_max_wait_ns = 1000000;
  p.retry.max_attempts = 3;
  return p;
}

inline batch_fabric::BatchFabricConfig config(const batch_fabric::BatchPolicy& p) {
  batch_fabric::BatchFabricConfig c;
  c.policy = p;
  c.clock = std::make_shared<batch_fabric::SimulatedClock>(0);
  return c;
}

inline batch_fabric::CpuExecutor cpu() {
  batch_fabric::ExecutorCapability cap;
  cap.worker = batch_fabric::WorkerId(0);
  cap.backend = batch_fabric::Backend::cpu;
  return batch_fabric::CpuExecutor(cap);
}

inline void drain(batch_fabric::BatchFabric& f) {
  batch_fabric::CpuExecutor e = cpu();
  for (int i = 0; i < 1000; ++i) {
    auto rep = f.tick();
    for (auto b : rep.sealed_batches) {
      auto dr = f.dispatch_and_run(b, e);
      (void)dr;
    }
    if (f.stats().requests_completed >= f.stats().submitted) break;
  }
}

}  // namespace bfex
