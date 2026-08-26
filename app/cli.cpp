#include "batch_fabric/batch_fabric.hpp"
#include "batch_fabric/executor.hpp"
#include "batch_fabric/persistence.hpp"
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <string>
#include <vector>

#ifdef _WIN32
#include <windows.h>
#endif

using namespace batch_fabric;

namespace {

const char* kHelp =
    "Batch Fabric CLI\n"
    "\n"
    "usage: batchfabric <command> [options]\n"
    "\n"
    "serve <port>                        run the distributed coordinator\n"
    "worker <port> <worker_id> <boot>    run a worker against a coordinator\n"
    "submit --state <s> [--tenant T] [--model M] [--rev R] [--phase P] [--in N] [--work W]\n"
    "cancel --state <s> --request <id>\n"
    "seal --state <s> --batch <id>\n"
    "split --state <s> --batch <id>\n"
    "merge --state <s> --batch-a <id> --batch-b <id>\n"
    "status --state <s>\n"
    "waiting --state <s>\n"
    "batches --state <s>\n"
    "workers --state <s>\n"
    "stats --state <s>\n"
    "snapshot --state <s>\n"
    "explain --state <s> --request <id>\n"
    "recover --state <s> <path>\n"
    "bench [--requests N] [--batch M]\n"
    "print --version\n";

std::string arg_value(const std::vector<std::string>& args, const std::string& name) {
  for (std::size_t i = 0; i + 1 < args.size(); ++i)
    if (args[i] == name) return args[i + 1];
  return {};
}

BatchPolicy default_policy() {
  BatchPolicy p;
  p.constraints.budget.max_requests = 64;
  p.constraints.budget.max_input_tokens = 100000;
  p.constraints.budget.max_work = 100000;
  p.constraints.global_max_wait_ns = 1000000;
  p.constraints.minimum_preferred_batch = 1;
  p.wait.global_max_wait_ns = 1000000;
  p.retry.max_attempts = 3;
  return p;
}

BatchFabricConfig make_config() {
  BatchFabricConfig cfg;
  cfg.policy = default_policy();
  cfg.clock = std::make_shared<RealMonotonicClock>();
  return cfg;
}

void load_or_init(BatchFabric& fabric, const std::string& state) {
  if (!state.empty()) {
    auto r = fabric.recover_from(state);
    if (!r.ok()) std::fprintf(stderr, "note: no prior state at %s (%s)\n", state.c_str(),
                              r.error().message().c_str());
  }
}

void persist(BatchFabric& fabric, const std::string& state) {
  if (!state.empty()) {
    auto r = fabric.persist_to(state);
    if (!r.ok()) std::fprintf(stderr, "persist failed: %s\n", r.error().to_string().c_str());
  }
}

#ifdef _WIN32
std::string exe_dir() {
  char buf[MAX_PATH];
  GetModuleFileNameA(nullptr, buf, sizeof(buf));
  std::string p(buf);
  auto pos = p.find_last_of("\\/");
  if (pos != std::string::npos) p = p.substr(0, pos);
  return p;
}

void spawn(const std::string& exe, const std::string& args) {
  std::string cmd = "\"" + exe + "\" " + args;
  STARTUPINFOA si{};
  si.cb = sizeof(si);
  PROCESS_INFORMATION pi{};
  char* cmdline = new char[cmd.size() + 1];
  std::memcpy(cmdline, cmd.c_str(), cmd.size() + 1);
  CreateProcessA(nullptr, cmdline, nullptr, nullptr, FALSE, 0, nullptr, nullptr, &si, &pi);
  delete[] cmdline;
}
#else
std::string exe_dir() { return "."; }
void spawn(const std::string& exe, const std::string& args) {
  std::string cmd = """ + exe + "" " + args;
  std::system(cmd.c_str());
}
#endif

void run_serve(std::vector<std::string>& args) {
  std::string port = args.size() > 1 ? args[1] : "3090";
  std::string exe = exe_dir() + "\\bf_coordinator.exe";
  spawn(exe, port);
  std::printf("serve: launched coordinator on port %s (runs in background).\n", port.c_str());
}

void run_worker(std::vector<std::string>& args) {
  std::string port = args.size() > 1 ? args[1] : "3090";
  std::string id = args.size() > 2 ? args[2] : "1";
  std::string boot = args.size() > 3 ? args[3] : "1";
  std::string exe = exe_dir() + "\\bf_worker.exe";
  spawn(exe, port + " " + id + " " + boot);
  std::printf("worker %s boot %s launched against %s\n", id.c_str(), boot.c_str(), port.c_str());
}

void run_submit(std::vector<std::string>& args, const std::string& state) {
  BatchFabric fabric(make_config());
  load_or_init(fabric, state);
  RequestMetadata m;
  m.tenant = TenantId(std::strtoull(arg_value(args, "--tenant").c_str(), nullptr, 10));
  m.descriptor.model = ModelIdentity(arg_value(args, "--model").empty() ? "m" : arg_value(args, "--model"));
  m.descriptor.revision = ModelRevision(arg_value(args, "--rev").empty() ? "r" : arg_value(args, "--rev"));
  m.descriptor.adapter = AdapterIdentity("base");
  auto ph = parse_phase(arg_value(args, "--phase"));
  m.descriptor.phase = ph.value_or(Phase::prefill);
  m.descriptor.dtype = Dtype::float16;
  m.descriptor.backend = Backend::cpu;
  m.descriptor.shape = ShapeKey("s1");
  m.descriptor.sampling_semantics = "default";
  m.tokens.input = std::strtoull(arg_value(args, "--in").c_str(), nullptr, 10);
  m.tokens.output = 5;
  m.work.value = std::strtoull(arg_value(args, "--work").c_str(), nullptr, 10);
  if (m.work.value == 0) m.work.value = 100;
  m.payload = arg_value(args, "--payload");
  auto r = fabric.submit(m);
  if (r.ok()) {
    auto rep = fabric.tick();
    std::printf("submitted request=%s admission=%s\n", r.value().request.string().c_str(),
                std::string(to_string(r.value().admission)).c_str());
    std::printf("sealed batches this tick: %zu\n", rep.sealed_batches.size());
    CpuExecutor exec([]{ ExecutorCapability c; c.worker=WorkerId(0); c.backend=Backend::cpu; return c; }());
    for (auto b : rep.sealed_batches) {
      auto dr = fabric.dispatch_and_run(b, exec);
      if (dr.ok()) std::printf("dispatched+completed batch=%s\n", dr.value().string().c_str());
    }
  } else {
    std::printf("submit failed: %s\n", r.error().to_string().c_str());
  }
  persist(fabric, state);
}

void run_cancel(std::vector<std::string>& args, const std::string& state) {
  BatchFabric fabric(make_config());
  load_or_init(fabric, state);
  std::string id = arg_value(args, "--request");
  auto r = fabric.cancel(RequestId(std::strtoull(id.c_str(), nullptr, 10)), CancellationReason::client_request);
  std::printf("cancel: %s\n", r.ok() ? "ok" : r.error().to_string().c_str());
  persist(fabric, state);
}

void run_seal_split_merge(std::vector<std::string>& args, const std::string& state, const std::string& cmd) {
  BatchFabric fabric(make_config());
  load_or_init(fabric, state);
  if (cmd == "seal") {
    auto id = BatchId(std::strtoull(arg_value(args, "--batch").c_str(), nullptr, 10));
    auto r = fabric.seal(id, SealReason::policy_derived);
    std::printf("seal: %s\n", r.ok() ? "ok" : r.error().to_string().c_str());
  } else if (cmd == "split") {
    auto id = BatchId(std::strtoull(arg_value(args, "--batch").c_str(), nullptr, 10));
    auto r = fabric.split(id, SplitReason::policy);
    std::printf("split: %s\n", r.ok() ? ("child " + r.value().string()).c_str() : r.error().to_string().c_str());
  } else if (cmd == "merge") {
    auto a = BatchId(std::strtoull(arg_value(args, "--batch-a").c_str(), nullptr, 10));
    auto b = BatchId(std::strtoull(arg_value(args, "--batch-b").c_str(), nullptr, 10));
    auto r = fabric.merge(a, b, MergeReason::policy);
    std::printf("merge: %s\n", r.ok() ? ("merged " + r.value().string()).c_str() : r.error().to_string().c_str());
  }
  persist(fabric, state);
}

void run_status(std::vector<std::string>&, const std::string& state) {
  BatchFabric fabric(make_config());
  load_or_init(fabric, state);
  auto st = fabric.stats();
  std::printf("submitted=%llu admitted=%llu completed=%llu batches_sealed=%llu waiting=%llu forming=%llu running=%llu reserved=%llu\n",
              (unsigned long long)st.submitted, (unsigned long long)st.admitted,
              (unsigned long long)st.requests_completed, (unsigned long long)st.batches_sealed,
              (unsigned long long)st.waiting_now, (unsigned long long)st.forming_now,
              (unsigned long long)st.running_now, (unsigned long long)st.reserved_now);
}

void run_waiting(std::vector<std::string>&, const std::string& state) {
  BatchFabric fabric(make_config());
  load_or_init(fabric, state);
  auto w = fabric.waiting();
  std::printf("waiting_requests=%zu\n", w.size());
  for (auto id : w) std::printf("  %s\n", id.string().c_str());
}

void run_batches(std::vector<std::string>&, const std::string& state) {
  BatchFabric fabric(make_config());
  load_or_init(fabric, state);
  auto b = fabric.batches();
  std::printf("batches=%zu\n", b.size());
  for (auto id : b) std::printf("  %s\n", id.string().c_str());
}

void run_workers(std::vector<std::string>&, const std::string& state) {
  BatchFabric fabric(make_config());
  load_or_init(fabric, state);
  auto w = fabric.worker_list();
  std::printf("workers=%zu\n", w.size());
  for (auto& e : w)
    std::printf("  worker=%llu boot=%llu live=%d active=%u\n", (unsigned long long)e.worker.value,
                (unsigned long long)e.boot.value, (int)e.live, e.active_batch_count);
}

void run_snapshot(std::vector<std::string>&, const std::string& state) {
  BatchFabric fabric(make_config());
  load_or_init(fabric, state);
  auto s = fabric.snapshot();
  std::printf("epoch=%llu batches=%zu requests=%zu workers=%zu\n", (unsigned long long)s.epoch.value,
              s.batches.size(), s.requests.size(), s.workers.size());
  for (auto& b : s.batches)
    std::printf("  batch=%s state=%s members=%zu\n", b.batch.string().c_str(),
                std::string(to_string(b.state)).c_str(), b.member_count);
  for (auto& r : s.requests)
    std::printf("  req=%s tenant=%llu state=%s batch=%s\n", r.request.string().c_str(),
                (unsigned long long)r.tenant.value, std::string(to_string(r.state)).c_str(),
                r.batch.string().c_str());
}

void run_explain(std::vector<std::string>& args, const std::string& state) {
  BatchFabric fabric(make_config());
  load_or_init(fabric, state);
  RequestId id(std::strtoull(arg_value(args, "--request").c_str(), nullptr, 10));
  auto ex = fabric.explain(id);
  std::printf("%s\n", ex.summary.c_str());
  for (auto& e : ex.entries) std::printf("  [%s] %s\n", e.category.c_str(), e.detail.c_str());
}

void run_recover(std::vector<std::string>& args, const std::string& state) {
  BatchFabric fabric(make_config());
  std::string path = state;
  if (path.empty() && args.size() > 1) path = args[1];
  auto r = fabric.recover_from(path);
  std::printf("recover: %s\n", r.ok() ? "ok" : r.error().to_string().c_str());
  if (r.ok()) {
    auto s = fabric.snapshot();
    std::printf("  recovered requests=%zu batches=%zu\n", s.requests.size(), s.batches.size());
  }
}

void run_bench(std::vector<std::string>& args) {
  int n = std::atoi(arg_value(args, "--requests").c_str());
  if (n <= 0) n = 1000;
  BatchFabric fabric(make_config());
  RequestMetadata m;
  m.tenant = TenantId(1);
  m.descriptor.model = ModelIdentity("m");
  m.descriptor.revision = ModelRevision("r");
  m.descriptor.adapter = AdapterIdentity("base");
  m.descriptor.phase = Phase::prefill;
  m.descriptor.shape = ShapeKey("s1");
  m.tokens.input = 10; m.tokens.output = 5; m.work.value = 100;
  auto t0 = std::chrono::steady_clock::now();
  for (int i = 0; i < n; ++i) { auto r = fabric.submit(m); if (!r.ok()) break; }
  auto t1 = std::chrono::steady_clock::now();
  CpuExecutor exec([]{ ExecutorCapability c; c.worker=WorkerId(0); c.backend=Backend::cpu; return c; }());
  for (int iter = 0; iter < 100000; ++iter) {
    auto rep = fabric.tick();
    for (auto b : rep.sealed_batches) { auto dr = fabric.dispatch_and_run(b, exec); (void)dr; }
    if (fabric.stats().requests_completed >= (std::uint64_t)n) break;
  }
  auto t2 = std::chrono::steady_clock::now();
  double submit_ms = std::chrono::duration<double, std::milli>(t1 - t0).count();
  double total_ms = std::chrono::duration<double, std::milli>(t2 - t0).count();
  auto st = fabric.stats();
  std::printf("bench: submitted=%d completed=%llu submit=%.2fms (%.0f req/s) end_to_end=%.2fms (%.0f req/s)\n",
              n, (unsigned long long)st.requests_completed, submit_ms, n / (submit_ms / 1000.0),
              total_ms, (double)st.requests_completed / (total_ms / 1000.0));
}

}  // namespace

int main(int argc, char** argv) {
  if (argc < 2) { std::printf("%s", kHelp); return 0; }
  std::vector<std::string> args(argv + 1, argv + argc);
  std::string cmd = args[0];
  if (cmd == "--help" || cmd == "help" || cmd == "-h") { std::printf("%s", kHelp); return 0; }
  if (cmd == "--version") { std::printf("Batch Fabric 1.0.0\n"); return 0; }
  std::string state = arg_value(args, "--state");
  if (cmd == "serve") { run_serve(args); return 0; }
  if (cmd == "worker") { run_worker(args); return 0; }
  if (cmd == "submit") { run_submit(args, state); return 0; }
  if (cmd == "cancel") { run_cancel(args, state); return 0; }
  if (cmd == "seal" || cmd == "split" || cmd == "merge") { run_seal_split_merge(args, state, cmd); return 0; }
  if (cmd == "status") { run_status(args, state); return 0; }
  if (cmd == "waiting") { run_waiting(args, state); return 0; }
  if (cmd == "batches") { run_batches(args, state); return 0; }
  if (cmd == "workers") { run_workers(args, state); return 0; }
  if (cmd == "stats") { run_status(args, state); return 0; }
  if (cmd == "snapshot") { run_snapshot(args, state); return 0; }
  if (cmd == "explain") { run_explain(args, state); return 0; }
  if (cmd == "recover") { run_recover(args, state); return 0; }
  if (cmd == "bench") { run_bench(args); return 0; }
  std::printf("%s", kHelp);
  return 0;
}