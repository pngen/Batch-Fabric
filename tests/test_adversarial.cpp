#define NOMINMAX
#include "batch_fabric/batch_fabric.hpp"
#include "batch_fabric/transport.hpp"
#include "testfw.hpp"
#include <chrono>
#include <cstdio>
#include <cstring>
#include <limits>
#include <memory>
#include <thread>

#include <winsock2.h>
#include <ws2tcpip.h>
#pragma comment(lib, "ws2_32.lib")

using namespace batch_fabric;

namespace {

RequestMetadata meta(const std::string& rev, TenantId t, std::uint64_t in = 10,
                     std::uint64_t out = 5, std::uint64_t work = 100) {
  RequestMetadata m;
  m.tenant = t;
  m.descriptor.model = ModelIdentity("m");
  m.descriptor.revision = ModelRevision(rev);
  m.descriptor.adapter = AdapterIdentity("base");
  m.descriptor.phase = Phase::prefill;
  m.descriptor.shape = ShapeKey("s1");
  m.tokens.input = in; m.tokens.output = out; m.work.value = work;
  return m;
}

BatchPolicy pol() {
  BatchPolicy p;
  p.constraints.budget.max_requests = 100;
  p.constraints.budget.max_input_tokens = 100000;
  p.constraints.budget.max_work = 100000;
  p.constraints.global_max_wait_ns = 1000000;
  p.constraints.minimum_preferred_batch = 1;
  p.wait.global_max_wait_ns = 1000000;
  p.retry.max_attempts = 3;
  return p;
}

CpuExecutor exec0() {
  ExecutorCapability cap; cap.worker = WorkerId(0); cap.backend = Backend::cpu;
  return CpuExecutor(cap);
}

}  // namespace

int main() {
  {
    auto clk = std::make_shared<SimulatedClock>(0);
    BatchFabricConfig cfg; cfg.policy = pol(); cfg.clock = clk;
    BatchFabric fabric(cfg);
    RequestMetadata huge = meta("r", TenantId(1));
    huge.tokens.input = std::numeric_limits<std::uint64_t>::max();
    auto r2 = fabric.submit(huge);
    REQUIRE(r2.ok());
    CHECK(r2.value().admission == AdmissionDecision::oversized);
  }

  {
    auto clk = std::make_shared<SimulatedClock>(0);
    BatchFabricConfig cfg; cfg.policy = pol(); cfg.clock = clk;
    BatchFabric fabric(cfg);
    auto r = fabric.submit(meta("r", TenantId(1), 0, 0, 0));
    REQUIRE(r.ok());
    CHECK(r.value().admission == AdmissionDecision::admitted);
    auto rep = fabric.tick();
    CHECK(rep.sealed_batches.size() >= 1);
    CpuExecutor exec = exec0();
    auto dr = fabric.dispatch_and_run(rep.sealed_batches.front(), exec);
    CHECK(dr.ok());
  }

  {
    auto clk = std::make_shared<SimulatedClock>(0);
    BatchFabricConfig cfg; cfg.policy = pol(); cfg.clock = clk;
    BatchFabric fabric(cfg);
    for (int i = 0; i < 2; ++i) { auto r = fabric.submit(meta("r", TenantId(1))); REQUIRE(r.ok()); }
    auto rep = fabric.tick();
    CHECK(rep.sealed_batches.size() >= 1);
    CpuExecutor exec = exec0();
    auto dr = fabric.dispatch_and_run(rep.sealed_batches.front(), exec);
    CHECK(dr.ok());
    std::uint64_t before = fabric.stats().requests_completed;
    auto dup = fabric.complete(BatchCompletion{});
    CHECK(!dup.ok());
    CHECK_EQ(fabric.stats().requests_completed, before);
  }

  {
    auto clk = std::make_shared<SimulatedClock>(0);
    BatchFabricConfig cfg; cfg.policy = pol(); cfg.clock = clk;
    BatchFabric fabric(cfg);
    auto r1 = fabric.submit(meta("r", TenantId(1)));
    auto r2 = fabric.submit(meta("r", TenantId(1)));
    REQUIRE(r1.ok() && r2.ok());
    auto c = fabric.cancel(r1.value().request, CancellationReason::client_request);
    CHECK(c.ok());
    auto rep = fabric.tick();
    CHECK(rep.sealed_batches.size() >= 1);
    CHECK_EQ(fabric.count_in_state(RequestState::cancelled), 1);
  }

  {
    auto clk = std::make_shared<SimulatedClock>(0);
    BatchFabricConfig cfg; cfg.policy = pol(); cfg.clock = clk;
    BatchFabric fabric(cfg);
    for (int i = 0; i < 2; ++i) { auto r = fabric.submit(meta("r", TenantId(1))); REQUIRE(r.ok()); }
    auto rep = fabric.tick();
    CHECK(rep.sealed_batches.size() >= 1);
    BatchId bid = rep.sealed_batches.front();

    // Register a real worker and dispatch a live batch at the current epoch.
    ExecutorCapability cap5; cap5.worker = WorkerId(5); cap5.backend = Backend::cpu;
    fabric.register_worker(WorkerId(5), WorkerBootId(1), cap5);
    CpuExecutor exec5(cap5);
    auto dr = fabric.dispatch_and_run(bid, exec5);
    CHECK(dr.ok());

    // Roll the epoch; the pre-roll batch authority is now stale.
    fabric.roll_epoch();

    BatchCompletion old_epoch;
    old_epoch.epoch = BatchEpoch(1);  // pre-roll epoch
    old_epoch.worker = WorkerId(5);
    old_epoch.boot = WorkerBootId(1);
    old_epoch.batch = bid;
    old_epoch.generation = Generation(1);
    auto se = fabric.complete(old_epoch);
    CHECK(!se.ok());
    CHECK(se.error().code() == ErrorCode::stale_epoch);

    BatchCompletion sw;
    sw.epoch = fabric.epoch();
    sw.worker = WorkerId(7);  // never registered -> stale worker
    sw.boot = WorkerBootId(99);
    sw.batch = bid;
    sw.generation = Generation(1);
    auto sww = fabric.complete(sw);
    CHECK(!sww.ok());
    CHECK(sww.error().code() == ErrorCode::stale_worker);

    BatchCompletion sg;
    sg.epoch = fabric.epoch();
    sg.worker = WorkerId(5);
    sg.boot = WorkerBootId(1);
    sg.batch = bid;
    sg.generation = Generation(999);  // stale generation -> stale attempt
    auto sgg = fabric.complete(sg);
    CHECK(!sgg.ok());
    CHECK(sgg.error().code() == ErrorCode::stale_attempt);
  }

  {
    auto clk = std::make_shared<SimulatedClock>(0);
    BatchFabricConfig cfg; cfg.policy = pol(); cfg.clock = clk;
    { BatchFabric fabric(cfg); auto r = fabric.submit(meta("r", TenantId(1))); REQUIRE(r.ok()); }
    { BatchFabric fabric(cfg); auto r = fabric.submit(meta("r", TenantId(1))); REQUIRE(r.ok());
      auto rep = fabric.tick();
      for (auto b : rep.sealed_batches) { CpuExecutor exec = exec0(); auto dr = fabric.dispatch_and_run(b, exec); (void)dr; } }
  }

  {
    Server server;
    Error err;
    CHECK(server.bind(0, err));
    std::uint16_t port = server.port();
    bool accepted = false, rejected = false;
    std::thread th([&] {
      std::this_thread::sleep_for(std::chrono::milliseconds(50));
      Error ae;
      Channel* ch = server.accept(ae);
      if (!ch) { accepted = false; return; }
      accepted = true;
      FrameMessage m; Error re;
      bool ok = ch->recv(m, re);
      rejected = !ok;
      delete ch;
    });
    {
      WSADATA d; WSAStartup(MAKEWORD(2,2), &d);
      SOCKET s = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
      sockaddr_in a; std::memset(&a,0,sizeof(a));
      a.sin_family = AF_INET; a.sin_port = htons(port); inet_pton(AF_INET,"127.0.0.1",&a.sin_addr);
      if (::connect(s, reinterpret_cast<sockaddr*>(&a), sizeof(a)) == 0) {
        std::uint8_t zero[4] = {0,0,0,0};
        ::send(s, reinterpret_cast<char*>(zero), 4, 0);
        closesocket(s);
      }
      WSACleanup();
    }
    th.join();
    CHECK(accepted);
    CHECK(rejected);
    server.close();
  }

  return testfw::exit_code();
}