#pragma once
#include "batch_fabric/batch.hpp"
#include "batch_fabric/identity.hpp"
#include "batch_fabric/id.hpp"
#include "batch_fabric/policy.hpp"
#include "batch_fabric/result.hpp"
#include <set>
#include <string>
#include <vector>

namespace batch_fabric {

// Capability profile of an executor/worker. Batch Fabric never intentionally
// emits a batch that violates a known capability.
struct ExecutorCapability {
  WorkerId worker;
  Backend backend = Backend::cpu;
  DeviceCapability device;
  std::uint32_t max_batch_size = 0;    // 0 => unlimited
  std::uint64_t max_tokens = 0;        // aggregate input+output, 0 => unlimited
  std::uint64_t max_work = 0;          // 0 => unlimited
  std::uint64_t max_memory_bytes = 0;  // 0 => unlimited
  bool supports_cancellation = false;

  // A capability satisfies a request descriptor + batch size where the
  // executor can legally consume it.
  bool supports(const ModelDescriptor& desc, std::uint32_t batch_size,
                std::uint64_t aggregate_tokens, std::uint64_t aggregate_work,
                std::uint64_t aggregate_memory) const noexcept;
};

// Per-member input to an execution.
struct MemberWork {
  RequestId request;
  AttemptId attempt;
  std::uint64_t work = 0;
  std::uint64_t input_tokens = 0;
  std::uint64_t output_tokens = 0;
  std::string payload;
};

// A sealed batch handed to an executor/worker.
struct BatchExecution {
  BatchId batch;
  Generation generation;
  BatchEpoch epoch;
  WorkerId worker;
  ModelDescriptor descriptor;
  std::vector<MemberWork> members;
};

// Per-member output returned by an executor.
struct MemberResult {
  RequestId request;
  AttemptId attempt;
  std::string output;
};

// Abstract executor. Implementations are fully synchronous for the CPU/CUDA
// backends; the TCP transport wraps a remote executor with the same contract.
class IExecutor {
 public:
  virtual ~IExecutor() = default;
  virtual std::string name() const = 0;
  virtual const ExecutorCapability& capability() const = 0;
  virtual Result<std::vector<MemberResult>> execute(const BatchExecution& batch) = 0;
  virtual bool cancel(const BatchId& batch) noexcept { (void)batch; return false; }
};

// Deterministic CPU executor for tests and CPU-only deployments. It performs a
// real (if cheap) computation per member to prove the dispatch/complete path
// rather than fabricating results.
class CpuExecutor final : public IExecutor {
 public:
  explicit CpuExecutor(const ExecutorCapability& cap);
  std::string name() const override { return "cpu"; }
  const ExecutorCapability& capability() const override { return cap_; }
  Result<std::vector<MemberResult>> execute(const BatchExecution& batch) override;

 private:
  ExecutorCapability cap_;
};

// Real CUDA-backed executor. It performs bounded, kernel-launched workloads on
// the installed NVIDIA GPU (allocations, host/device transfer, kernel launch,
// device synchronization, per-member verification, and cleanup) to prove that
// Batch Fabric forms, seals, dispatches, executes, verifies, and reaps a real
// accelerator batch. It is not an inference engine.
class CudaExecutor final : public IExecutor {
 public:
  CudaExecutor(const ExecutorCapability& cap, int device = 0);
  ~CudaExecutor() override;
  std::string name() const override { return "cuda"; }
  const ExecutorCapability& capability() const override { return cap_; }
  Result<std::vector<MemberResult>> execute(const BatchExecution& batch) override;
  bool cancel(const BatchId& batch) noexcept override;

  // Query the installed GPU. Returns a human-readable line and sets err on
  // failure.
  static std::string gpu_info(std::string& err);

 private:
  ExecutorCapability cap_;
  int device_ = 0;
};

}  // namespace batch_fabric