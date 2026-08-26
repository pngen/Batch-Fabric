#include "batch_fabric/batch_fabric.hpp"
#include "batch_fabric/protocol.hpp"
#include "batch_fabric/transport.hpp"
#include <atomic>
#include <cstdio>
#include <cstdlib>
#include <map>
#include <mutex>
#include <thread>

using namespace batch_fabric;

namespace {
BatchPolicy coord_policy() {
  BatchPolicy p;
  p.constraints.budget.max_requests = 8;
  p.constraints.budget.max_input_tokens = 100000;
  p.constraints.budget.max_work = 100000;
  p.constraints.global_max_wait_ns = 1000000;
  p.constraints.minimum_preferred_batch = 1;
  p.wait.global_max_wait_ns = 1000000;
  p.retry.max_attempts = 3;
  return p;
}

void send_ack(Channel& ch, bool ok, ErrorCode code, const std::string& msg) {
  WireAck a;
  a.ok = ok;
  a.code = code;
  a.message = msg;
  FrameMessage fm;
  fm.type = MessageType::ack;
  fm.body = encode_ack(a);
  Error e;
  ch.send(fm, e);
}
}  // namespace

int main(int argc, char** argv) {
  std::uint16_t port = 3090;
  if (argc > 1) port = static_cast<std::uint16_t>(std::atoi(argv[1]));
  BatchFabricConfig cfg;
  cfg.policy = coord_policy();
  BatchFabric fabric(cfg);
  Server server;
  Error err;
  if (!server.bind(port, err)) {
    std::printf("coordinator bind failed: %s\n", err.to_string().c_str());
    return 1;
  }
  std::printf("coordinator listening on %u\n", static_cast<unsigned>(server.port()));
  std::fflush(stdout);

  struct WorkerSlot { Channel* ch = nullptr; WorkerBootId boot; };
  std::map<WorkerId, WorkerSlot> workers;
  std::mutex mu;
  std::atomic<bool> running{true};

  while (running) {
    Error ae;
    Channel* ch = server.accept(ae);
    if (!ch) break;
    std::thread([&, ch] {
      FrameMessage first;
      Error re;
      if (!ch->recv(first, re)) { delete ch; return; }
      if (first.type == MessageType::register_worker) {
        WireRegister reg;
        if (!decode_register(first.body, reg)) { delete ch; return; }
        {
          std::lock_guard<std::mutex> g(mu);
          WorkerSlot s;
          s.ch = ch;
          s.boot = reg.boot;
          workers[reg.worker] = s;
        }
        fabric.register_worker(reg.worker, reg.boot, reg.cap);
        std::printf("coord: worker %llu boot %llu registered\n",
                    static_cast<unsigned long long>(reg.worker.value),
                    static_cast<unsigned long long>(reg.boot.value));
        std::fflush(stdout);
        while (running) {
          FrameMessage m;
          Error r2;
          if (!ch->recv(m, r2)) {
            std::lock_guard<std::mutex> g(mu);
            workers.erase(reg.worker);
            std::printf("coord: worker %llu disconnected\n",
                        static_cast<unsigned long long>(reg.worker.value));
            std::fflush(stdout);
            break;
          }
          if (m.type == MessageType::complete) {
            WireComplete wc;
            if (!decode_complete(m.body, wc)) { send_ack(*ch, false, ErrorCode::transport_failure, "bad complete"); continue; }
            auto rr = fabric.complete(wc.completion);
            send_ack(*ch, rr.ok(), rr.error().code(), rr.ok() ? std::string("ok") : rr.error().message());
          } else if (m.type == MessageType::shutdown) {
            running = false;
            delete ch;
            return;
          }
        }
        delete ch;
      } else {
        // Driver connection. The very first message was already read as "first";
        // process it, then read subsequent messages at the end of each iteration.
        FrameMessage m = first;
        while (running) {
          if (m.type == MessageType::submit) {
            WireSubmit ws;
            if (!decode_submit(m.body, ws)) { send_ack(*ch, false, ErrorCode::transport_failure, "bad submit"); }
            else {
              auto sr = fabric.submit(ws.meta);
              if (!sr.ok()) { send_ack(*ch, false, sr.error().code(), sr.error().message()); }
              else {
                auto rep = fabric.tick();
                for (auto bid : rep.sealed_batches) {
                  WorkerId w; w.value = 0;
                  {
                    std::lock_guard<std::mutex> g(mu);
                    if (!workers.empty()) w = workers.begin()->first;
                  }
                  if (w.value == 0) break;
                  WorkerBootId boot;
                  Channel* wch = nullptr;
                  {
                    std::lock_guard<std::mutex> g(mu);
                    auto it = workers.find(w);
                    if (it != workers.end()) { wch = it->second.ch; boot = it->second.boot; }
                  }
                  auto pd = fabric.prepare_dispatch(bid, w, boot);
                  if (pd.ok()) {
                    WireDispatch wd;
                    wd.exec = pd.move_value();
                    FrameMessage df;
                    df.type = MessageType::dispatch;
                    df.body = encode_dispatch(wd);
                    Error se;
                    if (wch) wch->send(df, se);
                  }
                }
                WireSubmitResp resp;
                resp.request = sr.value().request;
                resp.attempt = sr.value().attempt;
                resp.admission = sr.value().admission;
                resp.reason = sr.value().reason;
                if (!rep.sealed_batches.empty()) resp.batch = rep.sealed_batches.front();
                FrameMessage rfm;
                rfm.type = MessageType::submit;
                rfm.body = encode_submit_resp(resp);
                Error se;
                ch->send(rfm, se);
              }
            }
          } else if (m.type == MessageType::status) {
            WireStatusResp sr;
            sr.stats = fabric.stats();
            FrameMessage rfm;
            rfm.type = MessageType::status_response;
            rfm.body = encode_status_resp(sr);
            Error se;
            ch->send(rfm, se);
          } else if (m.type == MessageType::roll_epoch) {
            WireRollEpoch wr;
            if (decode_roll_epoch(m.body, wr)) fabric.roll_epoch();
            send_ack(*ch, true, ErrorCode::ok, "epoch rolled");
          } else if (m.type == MessageType::complete) {
            WireComplete wc;
            if (!decode_complete(m.body, wc)) { send_ack(*ch, false, ErrorCode::transport_failure, "bad complete"); }
            else {
              auto rr = fabric.complete(wc.completion);
              send_ack(*ch, rr.ok(), rr.error().code(), rr.ok() ? std::string("ok") : rr.error().message());
            }
          } else if (m.type == MessageType::cancel) {
            WireCancel wc;
            if (!decode_cancel(m.body, wc)) { send_ack(*ch, false, ErrorCode::transport_failure, "bad cancel"); }
            else {
              auto rr = fabric.cancel(wc.request, wc.reason);
              send_ack(*ch, rr.ok(), rr.error().code(), rr.ok() ? std::string("ok") : rr.error().message());
            }
          } else if (m.type == MessageType::shutdown) {
            send_ack(*ch, true, ErrorCode::ok, "shutdown");
            running = false;
            delete ch;
            return;
          }
          Error r2;
          if (!ch->recv(m, r2)) { delete ch; return; }
        }
        delete ch;
      }
    }).detach();
  }

  for (auto& kv : workers) delete kv.second.ch;
  workers.clear();
  return 0;
}