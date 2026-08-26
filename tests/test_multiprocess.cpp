#define NOMINMAX
#include "batch_fabric/batch_fabric.hpp"
#include "batch_fabric/protocol.hpp"
#include "batch_fabric/transport.hpp"
#include "testfw.hpp"

#include <windows.h>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <string>
#include <thread>
#include <vector>

using namespace batch_fabric;

namespace {

std::string exe_dir() {
  char buf[MAX_PATH];
  GetModuleFileNameA(nullptr, buf, sizeof(buf));
  std::string p(buf);
  auto pos = p.find_last_of("\\/");   // strip executable name
  if (pos != std::string::npos) p = p.substr(0, pos);
  // The test lives in <build>/tests; the coordinator/worker binaries are in
  // <build>/, so step up one more level.
  auto pos2 = p.find_last_of("\\/");
  if (pos2 != std::string::npos) p = p.substr(0, pos2);
  return p;
}

HANDLE launch(const std::string& exe, const std::string& args) {
  std::string cmd = "\"" + exe + "\" " + args;
  STARTUPINFOA si{};
  si.cb = sizeof(si);
  PROCESS_INFORMATION pi{};
  char* cmdline = new char[cmd.size() + 1];
  std::memcpy(cmdline, cmd.c_str(), cmd.size() + 1);
  BOOL ok = CreateProcessA(nullptr, cmdline, nullptr, nullptr, FALSE, 0, nullptr, nullptr, &si, &pi);
  delete[] cmdline;
  if (!ok) return nullptr;
  CloseHandle(pi.hThread);
  return pi.hProcess;
}

void kill(HANDLE h) {
  if (h) {
    TerminateProcess(h, 0);
    WaitForSingleObject(h, 3000);
    CloseHandle(h);
  }
}

RequestMetadata meta(const std::string& rev, TenantId t, bool phase_dec = false) {
  RequestMetadata m;
  m.tenant = t;
  m.descriptor.model = ModelIdentity("m");
  m.descriptor.revision = ModelRevision(rev);
  m.descriptor.adapter = AdapterIdentity("base");
  m.descriptor.phase = phase_dec ? Phase::decode : Phase::prefill;
  m.descriptor.dtype = Dtype::float16;
  m.descriptor.backend = Backend::cpu;
  m.descriptor.shape = ShapeKey("s1");
  m.descriptor.sampling_semantics = "default";
  m.tokens.input = 10;
  m.tokens.output = 5;
  m.work.value = 100;
  return m;
}

WireSubmitResp submit(Channel& ch, const RequestMetadata& m) {
  WireSubmit ws;
  ws.meta = m;
  FrameMessage fm;
  fm.type = MessageType::submit;
  fm.body = encode_submit(ws);
  Error e;
  ch.send(fm, e);
  FrameMessage rsp;
  Error re;
  if (!ch.recv(rsp, re)) return WireSubmitResp{};
  WireSubmitResp out;
  decode_submit_resp(rsp.body, out);
  return out;
}

WireStatusResp status(Channel& ch) {
  FrameMessage fm;
  fm.type = MessageType::status;
  fm.body = encode_status(WireStatus{});
  Error e;
  ch.send(fm, e);
  FrameMessage rsp;
  Error re;
  ch.recv(rsp, re);
  WireStatusResp out;
  decode_status_resp(rsp.body, out);
  return out;
}

WireAck send_complete(Channel& ch, const WireComplete& wc) {
  FrameMessage fm;
  fm.type = MessageType::complete;
  fm.body = encode_complete(wc);
  Error e;
  ch.send(fm, e);
  FrameMessage rsp;
  Error re;
  if (!ch.recv(rsp, re)) {
    WireAck a;
    a.ok = false;
    a.code = ErrorCode::transport_failure;
    a.message = "no ack";
    return a;
  }
  WireAck out;
  decode_ack(rsp.body, out);
  return out;
}

void roll_epoch(Channel& ch) {
  FrameMessage fm;
  fm.type = MessageType::roll_epoch;
  fm.body = encode_roll_epoch(WireRollEpoch{});
  Error e;
  ch.send(fm, e);
  FrameMessage rsp;
  Error re;
  ch.recv(rsp, re);
  WireAck a;
  decode_ack(rsp.body, a);
}

void wait_until(Channel& ch, std::uint64_t target, int timeout_ms) {
  auto t0 = std::chrono::steady_clock::now();
  while (std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - t0).count() < timeout_ms) {
    auto st = status(ch);
    if (st.stats.requests_completed >= target) return;
    std::this_thread::sleep_for(std::chrono::milliseconds(25));
  }
  CHECK(false);
}

}  // namespace

int main(int argc, char** argv) {
  std::printf("Batch Fabric multiprocess atomic authority-transition proof\n");
  std::string dir = exe_dir();
  std::string coord = dir + "\\bf_coordinator.exe";
  std::string worker = dir + "\\bf_worker.exe";
  std::uint16_t port = 3311;
  if (argc > 1) port = static_cast<std::uint16_t>(std::atoi(argv[1]));

  HANDLE hc = launch(coord, std::to_string(port));
  CHECK(hc != nullptr);
  std::this_thread::sleep_for(std::chrono::milliseconds(600));
  HANDLE w1 = launch(worker, std::to_string(port) + " 1 1");
  HANDLE w2 = launch(worker, std::to_string(port) + " 2 2");
  CHECK(w1 != nullptr);
  CHECK(w2 != nullptr);
  std::this_thread::sleep_for(std::chrono::milliseconds(600));

  Channel ch;
  Error err;
  bool conn = ch.connect("127.0.0.1", port, err);
  CHECK(conn);

  BatchId first_batch;
  for (int i = 0; i < 4; ++i) {
    auto r = submit(ch, meta("r", TenantId(1)));
    CHECK(r.admission == AdmissionDecision::admitted);
    if (i == 0) first_batch = r.batch;
  }
  wait_until(ch, 4, 10000);

  kill(w1);

  w1 = launch(worker, std::to_string(port) + " 1 3");
  CHECK(w1 != nullptr);
  std::this_thread::sleep_for(std::chrono::milliseconds(600));

  roll_epoch(ch);

  {
    WireComplete sc;
    sc.completion.epoch = BatchEpoch(1);
    sc.completion.worker = WorkerId(1);
    sc.completion.boot = WorkerBootId(1);
    sc.completion.generation = Generation(1);
    sc.completion.batch = first_batch;
    MemberCompletion mc;
    sc.completion.members.push_back(mc);
    auto ack = send_complete(ch, sc);
    CHECK(!ack.ok);
    CHECK(ack.code == ErrorCode::stale_epoch);
  }

  {
    WireComplete sc;
    sc.completion.epoch = BatchEpoch(2);
    sc.completion.worker = WorkerId(1);
    sc.completion.boot = WorkerBootId(1);
    sc.completion.generation = Generation(1);
    sc.completion.batch = first_batch;
    MemberCompletion mc;
    sc.completion.members.push_back(mc);
    auto ack = send_complete(ch, sc);
    CHECK(!ack.ok);
    CHECK(ack.code == ErrorCode::stale_worker);
  }

  BatchId fresh_batch;
  for (int i = 0; i < 4; ++i) {
    auto r = submit(ch, meta("r", TenantId(1)));
    CHECK(r.admission == AdmissionDecision::admitted);
    if (i == 0) fresh_batch = r.batch;
  }
  wait_until(ch, 8, 10000);

  {
    WireComplete sc;
    sc.completion.epoch = BatchEpoch(2);
    sc.completion.worker = WorkerId(1);
    sc.completion.boot = WorkerBootId(3);
    sc.completion.generation = Generation(9999);
    sc.completion.batch = fresh_batch;
    MemberCompletion mc;
    sc.completion.members.push_back(mc);
    auto ack = send_complete(ch, sc);
    CHECK(!ack.ok);
    CHECK(ack.code == ErrorCode::stale_attempt);
  }

  {
    auto st = status(ch);
    CHECK_EQ(st.stats.waiting_now, 0);
    CHECK_EQ(st.stats.reserved_now, 0);
    CHECK_EQ(st.stats.running_now, 0);
    CHECK_EQ(st.stats.requests_completed, 8);
  }

  {
    FrameMessage fm;
    fm.type = MessageType::shutdown;
    fm.body = {};
    Error e;
    ch.send(fm, e);
    FrameMessage rsp;
    Error re;
    ch.recv(rsp, re);
  }
  Sleep(300);
  kill(w1);
  kill(w2);
  kill(hc);

  std::printf("multiprocess atomic authority transition completed\n");
  return testfw::exit_code();
}