#pragma once
#include "batch_fabric/batch.hpp"
#include "batch_fabric/executor.hpp"
#include "batch_fabric/id.hpp"
#include "batch_fabric/io.hpp"
#include "batch_fabric/policy.hpp"
#include "batch_fabric/request.hpp"
#include "batch_fabric/stats.hpp"
#include <cstdint>
#include <string>
#include <vector>

namespace batch_fabric {

struct WireRegister {
  WorkerId worker;
  WorkerBootId boot;
  ExecutorCapability cap;
};

struct WireSubmit {
  RequestMetadata meta;
};

struct WireSubmitResp {
  RequestId request;
  AttemptId attempt;
  AdmissionDecision admission;
  std::string reason;
  BatchId batch;  // first sealed batch produced by this submit (may be null)
};

struct WireDispatch {
  BatchExecution exec;
};

struct WireComplete {
  BatchCompletion completion;
};

struct WireAck {
  bool ok = false;
  ErrorCode code = ErrorCode::ok;
  std::string message;
};

struct WireRollEpoch {
  BatchEpoch epoch;
};

struct WireStatus {};

struct WireStatusResp {
  BatchStats stats;
};

struct WireCancel {
  RequestId request;
  CancellationReason reason;
};

struct WireShutdown {};

std::vector<std::uint8_t> encode_register(const WireRegister& m);
bool decode_register(const std::vector<std::uint8_t>& b, WireRegister& m);
std::vector<std::uint8_t> encode_submit(const WireSubmit& m);
bool decode_submit(const std::vector<std::uint8_t>& b, WireSubmit& m);
std::vector<std::uint8_t> encode_submit_resp(const WireSubmitResp& m);
bool decode_submit_resp(const std::vector<std::uint8_t>& b, WireSubmitResp& m);
std::vector<std::uint8_t> encode_dispatch(const WireDispatch& m);
bool decode_dispatch(const std::vector<std::uint8_t>& b, WireDispatch& m);
std::vector<std::uint8_t> encode_complete(const WireComplete& m);
bool decode_complete(const std::vector<std::uint8_t>& b, WireComplete& m);
std::vector<std::uint8_t> encode_ack(const WireAck& m);
bool decode_ack(const std::vector<std::uint8_t>& b, WireAck& m);
std::vector<std::uint8_t> encode_roll_epoch(const WireRollEpoch& m);
bool decode_roll_epoch(const std::vector<std::uint8_t>& b, WireRollEpoch& m);
std::vector<std::uint8_t> encode_status(const WireStatus& m);
bool decode_status(const std::vector<std::uint8_t>& b, WireStatus& m);
std::vector<std::uint8_t> encode_status_resp(const WireStatusResp& m);
bool decode_status_resp(const std::vector<std::uint8_t>& b, WireStatusResp& m);
std::vector<std::uint8_t> encode_cancel(const WireCancel& m);
bool decode_cancel(const std::vector<std::uint8_t>& b, WireCancel& m);

}  // namespace batch_fabric