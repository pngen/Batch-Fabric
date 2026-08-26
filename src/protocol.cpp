#include "batch_fabric/protocol.hpp"
#include "batch_fabric/identity.hpp"

namespace batch_fabric {

namespace {

void write_desc(ByteWriter& w, const ModelDescriptor& d) {
  w.u8(1);
  w.string(d.model.value);
  w.string(d.revision.value);
  w.string(d.adapter.value);
  w.string(d.tokenizer.value);
  w.u8(static_cast<std::uint8_t>(d.phase));
  w.u8(static_cast<std::uint8_t>(d.dtype));
  w.u8(static_cast<std::uint8_t>(d.backend));
  w.u8(static_cast<std::uint8_t>(d.execution_mode));
  w.u8(static_cast<std::uint8_t>(d.device.backend));
  w.u32(d.device.compute_major);
  w.u32(d.device.compute_minor);
  w.u64(d.device.min_memory_bytes);
  w.string(d.shape.value);
  w.string(d.sampling_semantics);
  w.u32(static_cast<std::uint32_t>(d.extensions.size()));
  for (const auto& e : d.extensions) w.string(e);
}

bool read_desc(ByteReader& r, ModelDescriptor& d) {
  std::uint8_t ver;
  if (!r.u8(ver) || ver != 1) return false;
  if (!r.string(d.model.value) || !r.string(d.revision.value) || !r.string(d.adapter.value) ||
      !r.string(d.tokenizer.value))
    return false;
  std::uint8_t phase, dtype, backend, mode, dbackend;
  if (!r.u8(phase) || !r.u8(dtype) || !r.u8(backend) || !r.u8(mode) || !r.u8(dbackend)) return false;
  std::uint32_t cmaj, cmin;
  std::uint64_t mem;
  if (!r.u32(cmaj) || !r.u32(cmin) || !r.u64(mem)) return false;
  std::string shape, sampling;
  if (!r.string(shape) || !r.string(sampling)) return false;
  std::uint32_t n_ext;
  if (!r.u32(n_ext) || n_ext > 1024) return false;
  d.extensions.clear();
  for (std::uint32_t i = 0; i < n_ext; ++i) {
    std::string e;
    if (!r.string(e)) return false;
    d.extensions.push_back(std::move(e));
  }
  d.phase = static_cast<Phase>(phase);
  d.dtype = static_cast<Dtype>(dtype);
  d.backend = static_cast<Backend>(backend);
  d.execution_mode = static_cast<ExecutionMode>(mode);
  d.device.backend = static_cast<Backend>(dbackend);
  d.device.compute_major = cmaj;
  d.device.compute_minor = cmin;
  d.device.min_memory_bytes = mem;
  d.shape = ShapeKey(shape);
  d.sampling_semantics = sampling;
  return true;
}

void write_cap(ByteWriter& w, const ExecutorCapability& c) {
  w.id(c.worker);
  w.u8(static_cast<std::uint8_t>(c.backend));
  w.u8(static_cast<std::uint8_t>(c.device.backend));
  w.u32(c.device.compute_major);
  w.u32(c.device.compute_minor);
  w.u64(c.device.min_memory_bytes);
  w.u32(c.max_batch_size);
  w.u64(c.max_tokens);
  w.u64(c.max_work);
  w.u64(c.max_memory_bytes);
  w.u8(c.supports_cancellation ? 1 : 0);
}

bool read_cap(ByteReader& r, ExecutorCapability& c) {
  std::uint64_t wv, minmem, maxt, maxw, maxmem;
  std::uint8_t backend, db, canc;
  std::uint32_t cmaj, cmin, maxb;
  if (!r.u64(wv) || !r.u8(backend) || !r.u8(db) || !r.u32(cmaj) || !r.u32(cmin) || !r.u64(minmem) ||
      !r.u32(maxb) || !r.u64(maxt) || !r.u64(maxw) || !r.u64(maxmem) || !r.u8(canc))
    return false;
  c.worker = WorkerId(wv);
  c.backend = static_cast<Backend>(backend);
  c.device.backend = static_cast<Backend>(db);
  c.device.compute_major = cmaj;
  c.device.compute_minor = cmin;
  c.device.min_memory_bytes = minmem;
  c.max_batch_size = maxb;
  c.max_tokens = maxt;
  c.max_work = maxw;
  c.max_memory_bytes = maxmem;
  c.supports_cancellation = canc != 0;
  return true;
}

void write_meta(ByteWriter& w, const RequestMetadata& m) {
  w.id(m.tenant);
  w.id(m.session);
  w.id(m.sequence);
  write_desc(w, m.descriptor);
  w.u64(m.tokens.input);
  w.u64(m.tokens.output);
  w.u64(m.work.value);
  w.u64(m.memory_bytes);
  w.i64(m.deadline.absolute_ns);
  w.u8(static_cast<std::uint8_t>(m.latency));
  w.u8(static_cast<std::uint8_t>(m.priority));
  w.string(m.payload);
}

bool read_meta(ByteReader& r, RequestMetadata& m) {
  std::uint64_t tenant, session, seq, tin, tout, tw, mem;
  std::int64_t dl;
  std::uint8_t lat, pri;
  if (!r.u64(tenant) || !r.u64(session) || !r.u64(seq)) return false;
  m.tenant = TenantId(tenant);
  m.session = SessionId(session);
  m.sequence = SequenceId(seq);
  if (!read_desc(r, m.descriptor)) return false;
  if (!r.u64(tin) || !r.u64(tout) || !r.u64(tw) || !r.u64(mem) || !r.i64(dl) || !r.u8(lat) ||
      !r.u8(pri))
    return false;
  m.tokens.input = tin;
  m.tokens.output = tout;
  m.work.value = tw;
  m.memory_bytes = mem;
  m.deadline.absolute_ns = dl;
  m.latency = static_cast<LatencyClass>(lat);
  m.priority = static_cast<PriorityClass>(pri);
  return r.string(m.payload);
}

void write_exec(ByteWriter& w, const BatchExecution& e) {
  w.id(e.batch);
  w.id(e.generation);
  w.id(e.epoch);
  w.id(e.worker);
  write_desc(w, e.descriptor);
  w.u32(static_cast<std::uint32_t>(e.members.size()));
  for (const auto& m : e.members) {
    w.id(m.request);
    w.id(m.attempt);
    w.u64(m.work);
    w.u64(m.input_tokens);
    w.u64(m.output_tokens);
    w.string(m.payload);
  }
}

bool read_exec(ByteReader& r, BatchExecution& e) {
  if (!r.id(e.batch) || !r.id(e.generation) || !r.id(e.epoch) || !r.id(e.worker)) return false;
  if (!read_desc(r, e.descriptor)) return false;
  std::uint32_t n;
  if (!r.u32(n) || n > 1u << 20) return false;
  e.members.clear();
  for (std::uint32_t i = 0; i < n; ++i) {
    MemberWork mw;
    if (!r.id(mw.request) || !r.id(mw.attempt) || !r.u64(mw.work) || !r.u64(mw.input_tokens) ||
        !r.u64(mw.output_tokens) || !r.string(mw.payload))
      return false;
    e.members.push_back(std::move(mw));
  }
  return true;
}

void write_compl(ByteWriter& w, const BatchCompletion& c) {
  w.id(c.worker);
  w.id(c.boot);
  w.id(c.epoch);
  w.id(c.generation);
  w.id(c.batch);
  w.u32(static_cast<std::uint32_t>(c.members.size()));
  for (const auto& m : c.members) {
    w.id(m.request);
    w.id(m.attempt);
    w.u8(static_cast<std::uint8_t>(m.status));
    w.string(m.result);
    w.u32(static_cast<std::uint32_t>(m.error_code));
    w.string(m.error_message);
  }
}

bool read_compl(ByteReader& r, BatchCompletion& c) {
  if (!r.id(c.worker) || !r.id(c.boot) || !r.id(c.epoch) || !r.id(c.generation) || !r.id(c.batch))
    return false;
  std::uint32_t n;
  if (!r.u32(n) || n > 1u << 20) return false;
  c.members.clear();
  for (std::uint32_t i = 0; i < n; ++i) {
    MemberCompletion mc;
    std::uint8_t status;
    std::uint32_t ec;
    if (!r.id(mc.request) || !r.id(mc.attempt) || !r.u8(status) || !r.string(mc.result) ||
        !r.u32(ec) || !r.string(mc.error_message))
      return false;
    mc.status = static_cast<CompletionStatus>(status);
    mc.error_code = static_cast<ErrorCode>(ec);
    c.members.push_back(std::move(mc));
  }
  return true;
}

bool ok_reader(ByteReader& r) { return r.remaining() == 0; }

}  // namespace

std::vector<std::uint8_t> encode_register(const WireRegister& m) {
  ByteWriter w;
  w.id(m.worker);
  w.id(m.boot);
  write_cap(w, m.cap);
  return w.take();
}
bool decode_register(const std::vector<std::uint8_t>& b, WireRegister& m) {
  ByteReader r(b.data(), b.size());
  if (!r.id(m.worker) || !r.id(m.boot) || !read_cap(r, m.cap)) return false;
  return ok_reader(r);
}

std::vector<std::uint8_t> encode_submit(const WireSubmit& m) {
  ByteWriter w;
  write_meta(w, m.meta);
  return w.take();
}
bool decode_submit(const std::vector<std::uint8_t>& b, WireSubmit& m) {
  ByteReader r(b.data(), b.size());
  if (!read_meta(r, m.meta)) return false;
  return ok_reader(r);
}

std::vector<std::uint8_t> encode_submit_resp(const WireSubmitResp& m) {
  ByteWriter w;
  w.id(m.request);
  w.id(m.attempt);
  w.u8(static_cast<std::uint8_t>(m.admission));
  w.string(m.reason);
  w.id(m.batch);
  return w.take();
}
bool decode_submit_resp(const std::vector<std::uint8_t>& b, WireSubmitResp& m) {
  ByteReader r(b.data(), b.size());
  std::uint8_t adm;
  if (!r.id(m.request) || !r.id(m.attempt) || !r.u8(adm) || !r.string(m.reason) || !r.id(m.batch))
    return false;
  m.admission = static_cast<AdmissionDecision>(adm);
  return ok_reader(r);
}

std::vector<std::uint8_t> encode_dispatch(const WireDispatch& m) {
  ByteWriter w;
  write_exec(w, m.exec);
  return w.take();
}
bool decode_dispatch(const std::vector<std::uint8_t>& b, WireDispatch& m) {
  ByteReader r(b.data(), b.size());
  if (!read_exec(r, m.exec)) return false;
  return ok_reader(r);
}

std::vector<std::uint8_t> encode_complete(const WireComplete& m) {
  ByteWriter w;
  write_compl(w, m.completion);
  return w.take();
}
bool decode_complete(const std::vector<std::uint8_t>& b, WireComplete& m) {
  ByteReader r(b.data(), b.size());
  if (!read_compl(r, m.completion)) return false;
  return ok_reader(r);
}

std::vector<std::uint8_t> encode_ack(const WireAck& m) {
  ByteWriter w;
  w.u8(m.ok ? 1 : 0);
  w.u32(static_cast<std::uint32_t>(m.code));
  w.string(m.message);
  return w.take();
}
bool decode_ack(const std::vector<std::uint8_t>& b, WireAck& m) {
  ByteReader r(b.data(), b.size());
  std::uint8_t ok;
  std::uint32_t code;
  if (!r.u8(ok) || !r.u32(code) || !r.string(m.message)) return false;
  m.ok = ok != 0;
  m.code = static_cast<ErrorCode>(code);
  return ok_reader(r);
}

std::vector<std::uint8_t> encode_roll_epoch(const WireRollEpoch& m) {
  ByteWriter w;
  w.id(m.epoch);
  return w.take();
}
bool decode_roll_epoch(const std::vector<std::uint8_t>& b, WireRollEpoch& m) {
  ByteReader r(b.data(), b.size());
  if (!r.id(m.epoch)) return false;
  return ok_reader(r);
}

std::vector<std::uint8_t> encode_status(const WireStatus&) { return {}; }
bool decode_status(const std::vector<std::uint8_t>& b, WireStatus&) { return b.empty(); }

std::vector<std::uint8_t> encode_status_resp(const WireStatusResp& m) {
  ByteWriter w;
  w.u64(m.stats.submitted);
  w.u64(m.stats.admitted);
  w.u64(m.stats.deferred);
  w.u64(m.stats.rejected);
  w.u64(m.stats.expired);
  w.u64(m.stats.cancelled);
  w.u64(m.stats.batches_formed);
  w.u64(m.stats.batches_sealed);
  w.u64(m.stats.batches_dispatched);
  w.u64(m.stats.batches_completed);
  w.u64(m.stats.batches_split);
  w.u64(m.stats.batches_merged);
  w.u64(m.stats.requests_completed);
  w.u64(m.stats.requests_retried);
  w.u64(m.stats.retryable_failures);
  w.u64(m.stats.stale_rejections);
  w.u64(m.stats.waiting_now);
  w.u64(m.stats.forming_now);
  w.u64(m.stats.running_now);
  w.u64(m.stats.reserved_now);
  w.u64(m.stats.total_work_completed);
  return w.take();
}
bool decode_status_resp(const std::vector<std::uint8_t>& b, WireStatusResp& m) {
  ByteReader r(b.data(), b.size());
  std::uint64_t vals[21];
  for (int i = 0; i < 21; ++i)
    if (!r.u64(vals[i])) return false;
  m.stats.submitted = vals[0];
  m.stats.admitted = vals[1];
  m.stats.deferred = vals[2];
  m.stats.rejected = vals[3];
  m.stats.expired = vals[4];
  m.stats.cancelled = vals[5];
  m.stats.batches_formed = vals[6];
  m.stats.batches_sealed = vals[7];
  m.stats.batches_dispatched = vals[8];
  m.stats.batches_completed = vals[9];
  m.stats.batches_split = vals[10];
  m.stats.batches_merged = vals[11];
  m.stats.requests_completed = vals[12];
  m.stats.requests_retried = vals[13];
  m.stats.retryable_failures = vals[14];
  m.stats.stale_rejections = vals[15];
  m.stats.waiting_now = vals[16];
  m.stats.forming_now = vals[17];
  m.stats.running_now = vals[18];
  m.stats.reserved_now = vals[19];
  m.stats.total_work_completed = vals[20];
  return ok_reader(r);
}

std::vector<std::uint8_t> encode_cancel(const WireCancel& m) {
  ByteWriter w;
  w.id(m.request);
  w.u8(static_cast<std::uint8_t>(m.reason));
  return w.take();
}
bool decode_cancel(const std::vector<std::uint8_t>& b, WireCancel& m) {
  ByteReader r(b.data(), b.size());
  std::uint8_t reason;
  if (!r.id(m.request) || !r.u8(reason)) return false;
  m.reason = static_cast<CancellationReason>(reason);
  return ok_reader(r);
}

}  // namespace batch_fabric