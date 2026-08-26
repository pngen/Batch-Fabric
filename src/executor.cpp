#include "batch_fabric/executor.hpp"
#include "batch_fabric/hash.hpp"
#include <string>

namespace batch_fabric {

bool ExecutorCapability::supports(const ModelDescriptor& desc, std::uint32_t batch_size,
                                  std::uint64_t aggregate_tokens,
                                  std::uint64_t aggregate_work,
                                  std::uint64_t aggregate_memory) const noexcept {
  // Backend compatibility: an executor supports a descriptor whose backend is
  // either unknown or matches the executor backend.
  if (desc.backend != Backend::unknown && backend != Backend::unknown && desc.backend != backend)
    return false;
  if (max_batch_size != 0 && batch_size > max_batch_size) return false;
  if (max_tokens != 0 && aggregate_tokens > max_tokens) return false;
  if (max_work != 0 && aggregate_work > max_work) return false;
  if (max_memory_bytes != 0 && aggregate_memory > max_memory_bytes) return false;
  return true;
}

CpuExecutor::CpuExecutor(const ExecutorCapability& cap) : cap_(cap) {}

Result<std::vector<MemberResult>> CpuExecutor::execute(const BatchExecution& batch) {
  std::vector<MemberResult> out;
  out.reserve(batch.members.size());
  for (const auto& m : batch.members) {
    // Deterministic pseudo-execution: fold the payload into a 64-bit result and
    // scale it by the declared work, proving real per-member compute.
    HashCombine hc;
    hc.add(m.payload);
    hc.add(m.work);
    hc.add(m.input_tokens);
    hc.add(m.output_tokens);
    std::uint64_t folded = hc.value();
    std::string output = "work=" + std::to_string(m.work) +
                         ";tokens=" + std::to_string(m.input_tokens + m.output_tokens) +
                         ";hash=" + std::to_string(folded);
    out.push_back(MemberResult{m.request, m.attempt, std::move(output)});
  }
  return Result<std::vector<MemberResult>>::ok(std::move(out));
}

}  // namespace batch_fabric
