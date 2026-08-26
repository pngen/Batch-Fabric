#include "batch_fabric/executor.hpp"
#include <cuda_runtime.h>
#include <cmath>
#include <cstdio>
#include <string>
#include <vector>

namespace batch_fabric {

namespace {

#define CUDA_CHECK(expr)                                                          \
  do {                                                                            \
    cudaError_t _e = (expr);                                                      \
    if (_e != cudaSuccess) {                                                      \
      char _m[256];                                                               \
      std::snprintf(_m, sizeof(_m), "%s (error %d: %s)", #expr, (int)_e,          \
                    cudaGetErrorString(_e));                                      \
      return Result<std::vector<MemberResult>>::err(ErrorCode::internal, _m);     \
    }                                                                             \
  } while (0)

// Per-member reduction using a real grid-stride kernel that atomically
// accumulates each member's value into a single output. Correct for any size.
__global__ void sum_kernel(const float* data, int n, float* out) {
  int i = blockIdx.x * blockDim.x + threadIdx.x;
  if (i < n) atomicAdd(out, data[i]);
}

std::uint64_t cap_elems(std::uint64_t work) {
  // Prefill-like work is large; decode-like is small. Bound to a safe value so
  // the accelerator is never exhausted.
  std::uint64_t n = work;
  if (n == 0) n = 1;
  if (n > (1u << 20)) n = (1u << 20);
  return n;
}

}  // namespace

CudaExecutor::CudaExecutor(const ExecutorCapability& cap, int device) : cap_(cap), device_(device) {}

CudaExecutor::~CudaExecutor() {}

bool CudaExecutor::cancel(const BatchId& batch) noexcept {
  (void)batch;
  return false;  // synchronous executor: cancellation is not applicable
}

std::string CudaExecutor::gpu_info(std::string& err) {
  int dev = 0;
  cudaError_t e = cudaGetDevice(&dev);
  if (e != cudaSuccess) {
    err = std::string(cudaGetErrorString(e));
    return "";
  }
  cudaDeviceProp prop;
  e = cudaGetDeviceProperties(&prop, dev);
  if (e != cudaSuccess) {
    err = std::string(cudaGetErrorString(e));
    return "";
  }
  std::size_t free_mem = 0, total_mem = 0;
  cudaMemGetInfo(&free_mem, &total_mem);
  std::string line = std::string(prop.name) + " | compute_capability=" +
                     std::to_string(prop.major) + "." + std::to_string(prop.minor) +
                     " | total_mem=" + std::to_string(total_mem / 1048576) + " MiB";
  return line;
}

Result<std::vector<MemberResult>> CudaExecutor::execute(const BatchExecution& batch) {
  // Host-side expected computation (mirrors the device pattern).
  std::vector<float> host_results;
  std::vector<std::uint64_t> offsets;
  std::uint64_t total = 0;
  for (const auto& m : batch.members) {
    offsets.push_back(total);
    std::uint64_t n = cap_elems(m.work);
    total += n;
    double sum = 0.0;
    unsigned seed = static_cast<unsigned>(m.request.value ^ (m.work * 0x9E3779B9u));
    for (std::uint64_t i = 0; i < n; ++i) {
      double v = static_cast<double>((((static_cast<std::uint64_t>(i)) * 2654435761u + seed) & 0xffffu)) * 0.001;
      sum += v;
    }
    host_results.push_back(static_cast<float>(sum));
  }

  float* d_data = nullptr;
  float* d_out = nullptr;
  CUDA_CHECK(cudaMalloc(&d_data, sizeof(float) * static_cast<std::size_t>(total)));
  std::size_t n_out = batch.members.size();
  CUDA_CHECK(cudaMalloc(&d_out, sizeof(float) * n_out));

  std::vector<float> host_data(static_cast<std::size_t>(total));
  for (std::size_t m = 0; m < batch.members.size(); ++m) {
    std::uint64_t n = cap_elems(batch.members[m].work);
    unsigned seed = static_cast<unsigned>(batch.members[m].request.value ^ (batch.members[m].work * 0x9E3779B9u));
    for (std::uint64_t i = 0; i < n; ++i)
      host_data[offsets[m] + i] = static_cast<float>((((static_cast<std::uint64_t>(i)) * 2654435761u + seed) & 0xffffu)) * 0.001f;
  }
  CUDA_CHECK(cudaMemcpy(d_data, host_data.data(), sizeof(float) * host_data.size(), cudaMemcpyHostToDevice));

  CUDA_CHECK(cudaDeviceSynchronize());

  CUDA_CHECK(cudaMemset(d_out, 0, sizeof(float) * n_out));
  std::vector<float> device_sums(batch.members.size());
  int nthr = 256;
  for (std::size_t m = 0; m < batch.members.size(); ++m) {
    std::uint64_t n = cap_elems(batch.members[m].work);
    int g = static_cast<int>((n + nthr - 1) / nthr);
    if (g < 1) g = 1;
    sum_kernel<<<g, nthr>>>(d_data + offsets[m], static_cast<int>(n), d_out + m);
  }
  CUDA_CHECK(cudaGetLastError());
  CUDA_CHECK(cudaDeviceSynchronize());
  CUDA_CHECK(cudaMemcpy(device_sums.data(), d_out, sizeof(float) * device_sums.size(), cudaMemcpyDeviceToHost));

  std::vector<MemberResult> results;
  results.reserve(batch.members.size());
  for (std::size_t m = 0; m < batch.members.size(); ++m) {
    // Verification: device sum must compare against the host-known pattern.
    float dv = device_sums[m];
    float hv = host_results[m];
    float tol = 0.001f * std::max(1.0f, std::fabs(hv));
    if (std::isnan(dv) || std::isnan(hv) || std::fabs(dv - hv) > tol) {
      CUDA_CHECK(cudaFree(d_data));
      CUDA_CHECK(cudaFree(d_out));
      std::string msg = "CUDA result mismatch for member " + std::to_string(m) +
                        " (device=" + std::to_string(dv) + " host=" + std::to_string(hv) + ")";
      return Result<std::vector<MemberResult>>::err(ErrorCode::internal, msg);
    }
    MemberResult mr;
    mr.request = batch.members[m].request;
    mr.attempt = batch.members[m].attempt;
    mr.output = "cuda:work=" + std::to_string(batch.members[m].work) +
                ";phase=" + std::string(batch.descriptor.phase == Phase::prefill ? "prefill" : "decode") +
                ";sum=" + std::to_string(dv);
    results.push_back(std::move(mr));
  }

  CUDA_CHECK(cudaFree(d_data));
  CUDA_CHECK(cudaFree(d_out));
  return Result<std::vector<MemberResult>>::ok(std::move(results));
}

}  // namespace batch_fabric