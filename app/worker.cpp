#include "batch_fabric/batch_fabric.hpp"
#include "batch_fabric/executor.hpp"
#include "batch_fabric/protocol.hpp"
#include "batch_fabric/transport.hpp"
#include <cstdio>
#include <cstdlib>

using namespace batch_fabric;

int main(int argc, char** argv) {
  if (argc < 4) {
    std::printf("usage: worker <port> <worker_id> <boot_id>\n");
    return 1;
  }
  std::uint16_t port = static_cast<std::uint16_t>(std::atoi(argv[1]));
  WorkerId worker(static_cast<std::uint64_t>(std::atoll(argv[2])));
  WorkerBootId boot(static_cast<std::uint64_t>(std::atoll(argv[3])));

  Channel ch;
  Error err;
  if (!ch.connect("127.0.0.1", port, err)) {
    std::printf("worker %llu connect failed: %s\n", static_cast<unsigned long long>(worker.value),
                err.to_string().c_str());
    return 1;
  }
  WireRegister reg;
  reg.worker = worker;
  reg.boot = boot;
  reg.cap.worker = worker;
  reg.cap.backend = Backend::cpu;
  reg.cap.max_batch_size = 64;
  FrameMessage rf;
  rf.type = MessageType::register_worker;
  rf.body = encode_register(reg);
  if (!ch.send(rf, err)) {
    std::printf("worker %llu register send failed\n", static_cast<unsigned long long>(worker.value));
    return 1;
  }
  ExecutorCapability cap = reg.cap;
  CpuExecutor exec(cap);
  std::printf("worker %llu boot %llu connected\n", static_cast<unsigned long long>(worker.value),
              static_cast<unsigned long long>(boot.value));
  std::fflush(stdout);

  while (true) {
    FrameMessage fm;
    Error re;
    if (!ch.recv(fm, re)) {
      std::printf("worker %llu lost connection\n", static_cast<unsigned long long>(worker.value));
      return 0;
    }
    if (fm.type == MessageType::dispatch) {
      WireDispatch wd;
      if (!decode_dispatch(fm.body, wd)) continue;
      auto res = exec.execute(wd.exec);
      WireComplete wc;
      wc.completion.worker = worker;
      wc.completion.boot = boot;
      wc.completion.epoch = wd.exec.epoch;
      wc.completion.generation = wd.exec.generation;
      wc.completion.batch = wd.exec.batch;
      if (!res.ok()) {
        for (auto& m : wd.exec.members) {
          MemberCompletion mc;
          mc.request = m.request;
          mc.attempt = m.attempt;
          mc.status = CompletionStatus::retryable_failure;
          mc.error_code = res.error().code();
          mc.error_message = res.error().message();
          wc.completion.members.push_back(mc);
        }
      } else {
        auto members = res.move_value();
        for (auto& mr : members) {
          MemberCompletion mc;
          mc.request = mr.request;
          mc.attempt = mr.attempt;
          mc.status = CompletionStatus::success;
          mc.result = mr.output;
          wc.completion.members.push_back(mc);
        }
      }
      FrameMessage cf;
      cf.type = MessageType::complete;
      cf.body = encode_complete(wc);
      Error se;
      ch.send(cf, se);
    } else if (fm.type == MessageType::shutdown) {
      return 0;
    }
  }
}
