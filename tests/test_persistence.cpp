#include "batch_fabric/batch_fabric.hpp"
#include "batch_fabric/persistence.hpp"
#include "testfw.hpp"
#include <cstdio>
#include <cstring>
#include <fstream>
#include <memory>
#include <vector>

using namespace batch_fabric;

namespace {
RequestMetadata meta(TenantId t) {
  RequestMetadata m;
  m.tenant = t;
  m.descriptor.model = ModelIdentity("m");
  m.descriptor.revision = ModelRevision("r");
  m.descriptor.adapter = AdapterIdentity("base");
  m.descriptor.phase = Phase::prefill;
  m.descriptor.shape = ShapeKey("s1");
  m.tokens.input = 10; m.tokens.output = 5; m.work.value = 100;
  return m;
}
BatchPolicy pol() {
  BatchPolicy p;
  p.constraints.budget.max_requests = 8;
  p.constraints.budget.max_input_tokens = 100000;
  p.constraints.budget.max_work = 100000;
  p.constraints.global_max_wait_ns = 1000000;
  p.constraints.minimum_preferred_batch = 1;
  p.wait.global_max_wait_ns = 1000000;
  return p;
}
std::vector<std::uint8_t> read_file(const std::string& path) {
  std::ifstream in(path, std::ios::binary | std::ios::ate);
  if (!in) return {};
  auto n = in.tellg();
  std::vector<std::uint8_t> b(static_cast<std::size_t>(n));
  in.seekg(0); in.read(reinterpret_cast<char*>(b.data()), n);
  return b;
}
void write_file(const std::string& path, const std::vector<std::uint8_t>& b) {
  std::ofstream out(path, std::ios::binary | std::ios::trunc);
  out.write(reinterpret_cast<const char*>(b.data()), static_cast<std::streamsize>(b.size()));
}
}  // namespace

int main() {
  const std::string valid_path = "_bf_persist_valid.bfstate";
  const std::string corrupt_path = "_bf_persist_corrupt.bfstate";

  // Build a runtime and persist it.
  auto clk = std::make_shared<SimulatedClock>(0);
  BatchFabricConfig cfg;
  cfg.policy = pol();
  cfg.clock = clk;
  {
    BatchFabric fabric(cfg);
    for (int i = 0; i < 5; ++i) {
      auto r = fabric.submit(meta(TenantId(1)));
      REQUIRE(r.ok());
    }
    auto rep = fabric.tick();
    CHECK(rep.sealed_batches.size() >= 1);
    auto p = fabric.persist_to(valid_path);
    CHECK(p.ok());
  }

  // Recovery reloads request/batch/epoch state.
  {
    BatchFabric recovered(cfg);
    auto r = recovered.recover_from(valid_path);
    if (!r.ok()) std::printf("recover error: %s\n", r.error().to_string().c_str());
    CHECK(r.ok());
    CHECK(recovered.count_in_state(RequestState::batched) +
              recovered.count_in_state(RequestState::reserved) +
              recovered.count_in_state(RequestState::waiting) >= 1);
    CHECK(recovered.epoch().value == 1);
  }

  // Corruption rejection.
  {
    auto bytes = read_file(valid_path);
    CHECK(!bytes.empty());

    // bad magic
    auto bad_magic = bytes;
    if (bad_magic.size() > 4) bad_magic[0] ^= 0xff;
    write_file(corrupt_path, bad_magic);
    {
      BatchFabric f(cfg);
      auto r = f.recover_from(corrupt_path);
      CHECK(!r.ok());
      CHECK(r.error().code() == ErrorCode::corruption);
    }

    // bad checksum (flip a payload byte)
    auto bad_checksum = bytes;
    if (bad_checksum.size() > 8) bad_checksum[16] ^= 0x01;
    write_file(corrupt_path, bad_checksum);
    {
      BatchFabric f(cfg);
      auto r = f.recover_from(corrupt_path);
      CHECK(!r.ok());
      CHECK(r.error().code() == ErrorCode::corruption);
    }

    // truncation
    auto truncated = std::vector<std::uint8_t>(bytes.begin(), bytes.begin() + 20);
    write_file(corrupt_path, truncated);
    {
      BatchFabric f(cfg);
      auto r = f.recover_from(corrupt_path);
      CHECK(!r.ok());
      CHECK(r.error().code() == ErrorCode::corruption);
    }
  }

  std::remove(valid_path.c_str());
  std::remove(corrupt_path.c_str());
  return testfw::exit_code();
}