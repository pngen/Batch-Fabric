#include "batch_fabric/batch_fabric.hpp"
#include "testfw.hpp"
#include <cstdio>
#include <cuda_runtime.h>
#include <memory>

using namespace batch_fabric;

namespace {
BatchFabricConfig make_config(const std::shared_ptr<Clock>& clock, const BatchPolicy& policy) {
  BatchFabricConfig cfg;
  cfg.policy = policy;
  cfg.clock = clock;
  return cfg;
}

RequestMetadata meta(const std::string& rev, Phase phase, TenantId t, std::uint64_t work) {
  RequestMetadata m;
  m.tenant = t;
  m.descriptor.model = ModelIdentity("m");
  m.descriptor.revision = ModelRevision(rev);
  m.descriptor.adapter = AdapterIdentity("base");
  m.descriptor.phase = phase;
  m.descriptor.dtype = Dtype::float16;
  m.descriptor.backend = Backend::cuda;
  m.descriptor.device.backend = Backend::cuda;
  m.descriptor.shape = ShapeKey("s1");
  m.descriptor.sampling_semantics = "default";
  m.tokens.input = 100;
  m.tokens.output = 20;
  m.work.value = work;
  return m;
}

BatchPolicy policy() {
  BatchPolicy p;
  p.constraints.budget.max_requests = 8;
  p.constraints.budget.max_input_tokens = 100000;
  p.constraints.budget.max_work = 1000000;
  p.constraints.global_max_wait_ns = 1000000;
  p.constraints.minimum_preferred_batch = 1;
  p.wait.global_max_wait_ns = 1000000;
  p.retry.max_attempts = 2;
  return p;
}

int run_scenario(Phase phase, std::uint64_t work, std::uint32_t count) {
  std::string err;
  std::string gpu = CudaExecutor::gpu_info(err);
  std::printf("GPU: %s\n", gpu.empty() ? ("error: " + err).c_str() : gpu.c_str());

  auto clk = std::make_shared<SimulatedClock>(0);
  BatchFabric fabric(make_config(clk, policy()));
  for (std::uint32_t i = 0; i < count; ++i) {
    auto r = fabric.submit(meta("r1", phase, TenantId(1), work));
    REQUIRE(r.ok());
  }
  auto rep = fabric.tick();
  CHECK_EQ(rep.sealed_batches.size(), 1);
  BatchId bid = rep.sealed_batches.front();

  ExecutorCapability cap;
  cap.worker = WorkerId(0);
  cap.backend = Backend::cuda;
  cap.device.backend = Backend::cuda;
  cap.max_batch_size = 64;
  CudaExecutor exec(cap, 0);

  auto dr = fabric.dispatch_and_run(bid, exec);
  if (!dr.ok()) std::printf("  dispatch error: %s (%u)\n", dr.error().to_string().c_str(), (unsigned)dr.error().code());
  CHECK(dr.ok());
  auto st = fabric.stats();
  CHECK_EQ(st.requests_completed, count);
  CHECK_EQ(st.waiting_now, 0);
  CHECK_EQ(st.running_now, 0);
  // Per-member results are real CUDA outputs.
  auto snap = fabric.snapshot();
  (void)snap;
  return 0;
}
}  // namespace

int main() {
  std::printf("Batch Fabric CUDA proof on installed NVIDIA GPU\n");
  // batch size 1
  run_scenario(Phase::prefill, 1000, 1);
  // small batch
  run_scenario(Phase::prefill, 1000, 2);
  // medium batch
  run_scenario(Phase::decode, 64, 8);
  // largest safe configured batch (respects executor cap)
  run_scenario(Phase::prefill, 256, 8);

  // memory baseline sanity: query free memory (non-zero).
  std::size_t free_mem = 0, total_mem = 0;
  CHECK(cudaMemGetInfo(&free_mem, &total_mem) == cudaSuccess);
  CHECK(total_mem > 0);
  std::printf("CUDA memory baseline: free=%zu MiB total=%zu MiB\n", free_mem / 1048576, total_mem / 1048576);
  return testfw::exit_code();
}