#include "batch_fabric/persistence.hpp"
#include "batch_fabric/hash.hpp"
#include <algorithm>
#include <cstring>
#include <fstream>
#include <map>
#include <set>

#ifdef _WIN32
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <windows.h>
#else
#include <cstdio>
#endif

namespace batch_fabric {

namespace {

bool enum_ok(std::uint8_t v, std::uint8_t max) { return v <= max; }
// unknown (255) is a valid "not specified" sentinel for optional enums.
bool enum_ok_unknown(std::uint8_t v, std::uint8_t max) { return v <= max || v == 255; }

void write_descriptor(ByteWriter& w, const ModelDescriptor& d) {
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

bool read_descriptor(ByteReader& r, ModelDescriptor& d) {
  std::uint8_t ver;
  if (!r.u8(ver)) return false;
  if (ver != 1) return false;
  if (!r.string(d.model.value) || !r.string(d.revision.value) || !r.string(d.adapter.value) ||
      !r.string(d.tokenizer.value))
    return false;
  std::uint8_t phase, dtype, backend, mode, dbackend;
  std::uint32_t cmaj, cmin;
  std::uint64_t mem;
  if (!r.u8(phase) || !r.u8(dtype) || !r.u8(backend) || !r.u8(mode) || !r.u8(dbackend))
    return false;
  if (!enum_ok(phase, 1) || !enum_ok_unknown(dtype, 4) || !enum_ok_unknown(backend, 4) ||
      !enum_ok_unknown(mode, 3) || !enum_ok_unknown(dbackend, 4))
    return false;
  if (!r.u32(cmaj) || !r.u32(cmin) || !r.u64(mem)) return false;
  std::string shape, sampling;
  if (!r.string(shape) || !r.string(sampling)) return false;
  std::uint32_t n_ext;
  if (!r.u32(n_ext)) return false;
  if (n_ext > 1024) return false;
  std::vector<std::string> exts;
  exts.reserve(n_ext);
  for (std::uint32_t i = 0; i < n_ext; ++i) {
    std::string e;
    if (!r.string(e)) return false;
    exts.push_back(std::move(e));
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
  d.extensions = std::move(exts);
  return true;
}

}  // namespace

Result<void> PersistenceStore::save(const PersistedRuntime& rt, const std::string& path) const {
  ByteWriter w;
  w.u32(kMagic);
  w.u32(kFormatVersion);
  w.u64(rt.epoch.value);
  w.u64(rt.next_request);
  w.u64(rt.next_batch);
  w.u64(rt.next_attempt);
  w.u64(rt.next_generation);
  w.u64(static_cast<std::uint64_t>(rt.requests.size()));
  w.u64(static_cast<std::uint64_t>(rt.attempts.size()));
  w.u64(static_cast<std::uint64_t>(rt.batches.size()));

  for (const auto& r : rt.requests) {
    w.id(r.id);
    w.id(r.tenant);
    w.id(r.session);
    w.id(r.sequence);
    write_descriptor(w, r.descriptor);
    w.u64(r.tokens.input);
    w.u64(r.tokens.output);
    w.u64(r.work.value);
    w.u64(r.memory_bytes);
    w.i64(r.deadline.absolute_ns);
    w.u8(static_cast<std::uint8_t>(r.latency));
    w.u8(static_cast<std::uint8_t>(r.priority));
    w.string(r.payload);
    w.u8(static_cast<std::uint8_t>(r.state));
    w.i64(r.submitted_ns);
    w.id(r.batch);
    w.id(r.current_attempt);
    w.u64(static_cast<std::uint64_t>(r.attempt_ids.size()));
    for (auto a : r.attempt_ids) w.id(a);
  }

  for (const auto& a : rt.attempts) {
    w.id(a.id);
    w.u32(a.number);
    w.u8(static_cast<std::uint8_t>(a.state));
    w.id(a.batch);
    w.u8(static_cast<std::uint8_t>(a.outcome));
    w.string(a.result);
    w.i64(a.started);
    w.i64(a.completed);
  }

  for (const auto& b : rt.batches) {
    w.id(b.id);
    w.id(b.generation);
    w.id(b.epoch);
    w.u8(static_cast<std::uint8_t>(b.state));
    w.digest(b.key.digest());
    w.u64(static_cast<std::uint64_t>(b.members.size()));
    for (auto m : b.members) w.id(m);
    w.u32(b.used.count);
    w.u64(b.used.input_tokens);
    w.u64(b.used.output_tokens);
    w.u64(b.used.work);
    w.u64(b.used.memory);
    w.i64(b.formed_ns);
    w.i64(b.sealed_ns);
    w.i64(b.dispatched_ns);
    w.i64(b.completed_ns);
    w.u8(static_cast<std::uint8_t>(b.seal_reason));
    w.id(b.worker);
    w.id(b.boot);
    w.u64(static_cast<std::uint64_t>(b.parents.size()));
    for (auto p : b.parents) w.id(p);
    w.u64(static_cast<std::uint64_t>(b.children.size()));
    for (auto c : b.children) w.id(c);
  }

  w.u64(static_cast<std::uint64_t>(rt.fairness_served.size()));
  for (const auto& [k, v] : rt.fairness_served) {
    w.string(k);
    w.u64(v);
  }

  Digest hash = Sha256::digest(w.data().data(), w.data().size());
  w.digest(hash);

#ifdef _WIN32
  std::string tmp = path + ".tmp";
  HANDLE h = CreateFileA(tmp.c_str(), GENERIC_WRITE, 0, nullptr, CREATE_ALWAYS,
                         FILE_ATTRIBUTE_NORMAL, nullptr);
  if (h == INVALID_HANDLE_VALUE)
    return Result<void>::err(ErrorCode::persistence_failure, "cannot open temp state file");
  const std::uint8_t* p = w.data().data();
  DWORD total = static_cast<DWORD>(w.data().size());
  DWORD off = 0;
  while (off < total) {
    DWORD chunk = std::min<DWORD>(total - off, 1u << 30);
    DWORD written = 0;
    if (!WriteFile(h, p + off, chunk, &written, nullptr) || written != chunk) {
      CloseHandle(h);
      return Result<void>::err(ErrorCode::persistence_failure, "write failed during save");
    }
    off += chunk;
  }
  if (!FlushFileBuffers(h)) {
    CloseHandle(h);
    return Result<void>::err(ErrorCode::persistence_failure, "flush failed during save");
  }
  CloseHandle(h);
  if (!MoveFileExA(tmp.c_str(), path.c_str(), MOVEFILE_REPLACE_EXISTING))
    return Result<void>::err(ErrorCode::persistence_failure, "atomic replace failed during save");
#else
  std::string tmp = path + ".tmp";
  {
    std::ofstream out(tmp, std::ios::binary | std::ios::trunc);
    if (!out)
      return Result<void>::err(ErrorCode::persistence_failure, "cannot open temp state file");
    out.write(reinterpret_cast<const char*>(w.data().data()),
              static_cast<std::streamsize>(w.data().size()));
    if (!out) return Result<void>::err(ErrorCode::persistence_failure, "write failed during save");
    out.flush();
  }
  if (std::rename(tmp.c_str(), path.c_str()) != 0)
    return Result<void>::err(ErrorCode::persistence_failure, "rename failed during save");
#endif
  return Result<void>::success();
}

Result<PersistedRuntime> PersistenceStore::load(const std::string& path) const {
  std::ifstream in(path, std::ios::binary | std::ios::ate);
  if (!in)
    return Result<PersistedRuntime>::err(ErrorCode::persistence_failure, "cannot open state file");
  std::streamsize size = in.tellg();
  if (size < 0) return Result<PersistedRuntime>::err(ErrorCode::corruption, "cannot determine file size");
  if (static_cast<std::size_t>(size) > kMaxRecords * 8ull + 4096ull)
    return Result<PersistedRuntime>::err(ErrorCode::corruption, "state file impossibly large");
  std::vector<std::uint8_t> bytes(static_cast<std::size_t>(size));
  in.seekg(0, std::ios::beg);
  if (!in.read(reinterpret_cast<char*>(bytes.data()), size))
    return Result<PersistedRuntime>::err(ErrorCode::corruption, "read failed");
  return parse(bytes);
}

Result<PersistedRuntime> PersistenceStore::parse(const std::vector<std::uint8_t>& bytes) const {
  if (bytes.size() < 44) return Result<PersistedRuntime>::err(ErrorCode::corruption, "truncated header");

  {
    std::size_t body = bytes.size() - 32;
    Digest actual = Sha256::digest(bytes.data(), body);
    if (std::memcmp(actual.data(), bytes.data() + body, 32) != 0)
      return Result<PersistedRuntime>::err(ErrorCode::corruption, "checksum mismatch");
  }

  ByteReader r(bytes.data(), bytes.size());
  std::uint32_t magic, ver;
  std::uint64_t epoch, nr, nb, na, ng;
  std::uint64_t nreqc, nattc, nbatc;
  if (!r.u32(magic) || !r.u32(ver)) return Result<PersistedRuntime>::err(ErrorCode::corruption, "truncated header");
  if (magic != kMagic) return Result<PersistedRuntime>::err(ErrorCode::corruption, "bad magic");
  if (ver != kFormatVersion) return Result<PersistedRuntime>::err(ErrorCode::corruption, "bad version");
  if (!r.u64(epoch) || !r.u64(nr) || !r.u64(nb) || !r.u64(na) || !r.u64(ng) || !r.u64(nreqc) ||
      !r.u64(nattc) || !r.u64(nbatc))
    return Result<PersistedRuntime>::err(ErrorCode::corruption, "truncated counters");
  if (nreqc > kMaxRecords || nbatc > kMaxRecords || nattc > kMaxRecords)
    return Result<PersistedRuntime>::err(ErrorCode::corruption, "impossible counts");

  PersistedRuntime rt;
  rt.epoch = BatchEpoch(epoch);
  rt.next_request = nr;
  rt.next_batch = nb;
  rt.next_attempt = na;
  rt.next_generation = ng;

  std::set<RequestId> req_ids;
  std::set<AttemptId> att_ids;
  std::set<BatchId> bat_ids;

  rt.requests.reserve(static_cast<std::size_t>(nreqc));
  for (std::uint64_t i = 0; i < nreqc; ++i) {
    PersistedRequest pr;
    if (!r.id(pr.id) || !r.id(pr.tenant) || !r.id(pr.session) || !r.id(pr.sequence))
      return Result<PersistedRuntime>::err(ErrorCode::corruption, "truncated request ids");
    if (!read_descriptor(r, pr.descriptor)) return Result<PersistedRuntime>::err(ErrorCode::corruption, "bad descriptor");
    std::uint64_t in_t, out_t, work, mem;
    if (!r.u64(in_t) || !r.u64(out_t) || !r.u64(work) || !r.u64(mem))
      return Result<PersistedRuntime>::err(ErrorCode::corruption, "truncated request estimates");
    std::int64_t dl;
    if (!r.i64(dl)) return Result<PersistedRuntime>::err(ErrorCode::corruption, "truncated deadline");
    std::uint8_t lat, pri, state;
    if (!r.u8(lat) || !r.u8(pri)) return Result<PersistedRuntime>::err(ErrorCode::corruption, "truncated request enums");
    if (!enum_ok(lat, 4) || !enum_ok(pri, 3)) return Result<PersistedRuntime>::err(ErrorCode::corruption, "invalid request enum");
    if (!r.string(pr.payload)) return Result<PersistedRuntime>::err(ErrorCode::corruption, "bad payload");
    if (!r.u8(state)) return Result<PersistedRuntime>::err(ErrorCode::corruption, "truncated request state");
    if (!enum_ok(state, 12)) return Result<PersistedRuntime>::err(ErrorCode::corruption, "invalid request state");
    std::int64_t sub;
    if (!r.i64(sub)) return Result<PersistedRuntime>::err(ErrorCode::corruption, "truncated submit time");
    if (!r.id(pr.batch) || !r.id(pr.current_attempt)) return Result<PersistedRuntime>::err(ErrorCode::corruption, "truncated request batch");
    std::uint64_t n_att;
    if (!r.u64(n_att)) return Result<PersistedRuntime>::err(ErrorCode::corruption, "truncated attempt count");
    if (n_att > 1024) return Result<PersistedRuntime>::err(ErrorCode::corruption, "impossible attempt count");
    for (std::uint64_t k = 0; k < n_att; ++k) {
      AttemptId a;
      if (!r.id(a)) return Result<PersistedRuntime>::err(ErrorCode::corruption, "truncated attempt id");
      pr.attempt_ids.push_back(a);
    }
    pr.tokens.input = in_t;
    pr.tokens.output = out_t;
    pr.work.value = work;
    pr.memory_bytes = mem;
    pr.deadline.absolute_ns = dl;
    pr.latency = static_cast<LatencyClass>(lat);
    pr.priority = static_cast<PriorityClass>(pri);
    pr.state = static_cast<RequestState>(state);
    pr.submitted_ns = sub;
    if (pr.id.is_null() || !req_ids.insert(pr.id).second)
      return Result<PersistedRuntime>::err(ErrorCode::corruption, "duplicate request id");
    rt.requests.push_back(std::move(pr));
  }

  rt.attempts.reserve(static_cast<std::size_t>(nattc));
  for (std::uint64_t i = 0; i < nattc; ++i) {
    PersistedAttempt pa;
    std::uint32_t number;
    std::uint8_t state, outcome;
    if (!r.id(pa.id) || !r.u32(number) || !r.u8(state)) return Result<PersistedRuntime>::err(ErrorCode::corruption, "truncated attempt");
    if (!r.id(pa.batch) || !r.u8(outcome)) return Result<PersistedRuntime>::err(ErrorCode::corruption, "truncated attempt out");
    if (!r.string(pa.result)) return Result<PersistedRuntime>::err(ErrorCode::corruption, "bad attempt result");
    if (!r.i64(pa.started) || !r.i64(pa.completed)) return Result<PersistedRuntime>::err(ErrorCode::corruption, "truncated attempt times");
    if (!enum_ok(state, 12) || !enum_ok(outcome, 6)) return Result<PersistedRuntime>::err(ErrorCode::corruption, "invalid attempt enum");
    pa.number = number;
    pa.state = static_cast<RequestState>(state);
    pa.outcome = static_cast<CompletionStatus>(outcome);
    if (pa.id.is_null() || !att_ids.insert(pa.id).second)
      return Result<PersistedRuntime>::err(ErrorCode::corruption, "duplicate attempt id");
    rt.attempts.push_back(std::move(pa));
  }

  rt.batches.reserve(static_cast<std::size_t>(nbatc));
  for (std::uint64_t i = 0; i < nbatc; ++i) {
    PersistedBatch pb;
    std::uint8_t state;
    Digest keyd;
    if (!r.id(pb.id) || !r.id(pb.generation) || !r.id(pb.epoch) || !r.u8(state) || !r.digest(keyd))
      return Result<PersistedRuntime>::err(ErrorCode::corruption, "truncated batch");
    if (!enum_ok(state, 8)) return Result<PersistedRuntime>::err(ErrorCode::corruption, "invalid batch enum");
    std::uint64_t nmembers;
    if (!r.u64(nmembers)) return Result<PersistedRuntime>::err(ErrorCode::corruption, "truncated batch members");
    if (nmembers > kMaxRecords) return Result<PersistedRuntime>::err(ErrorCode::corruption, "impossible batch members");
    for (std::uint64_t k = 0; k < nmembers; ++k) {
      RequestId m;
      if (!r.id(m)) return Result<PersistedRuntime>::err(ErrorCode::corruption, "truncated member id");
      if (req_ids.find(m) == req_ids.end()) return Result<PersistedRuntime>::err(ErrorCode::corruption, "member references unknown request");
      pb.members.push_back(m);
    }
    std::uint32_t mr;
    std::uint64_t mi, mo, mw, mm;
    if (!r.u32(mr) || !r.u64(mi) || !r.u64(mo) || !r.u64(mw) || !r.u64(mm))
      return Result<PersistedRuntime>::err(ErrorCode::corruption, "truncated batch budget");
    std::int64_t fn, sn, dn, cn;
    if (!r.i64(fn) || !r.i64(sn) || !r.i64(dn) || !r.i64(cn)) return Result<PersistedRuntime>::err(ErrorCode::corruption, "truncated batch times");
    std::uint8_t s2;
    if (!r.u8(s2)) return Result<PersistedRuntime>::err(ErrorCode::corruption, "truncated seal reason");
    if (!enum_ok(s2, 10)) return Result<PersistedRuntime>::err(ErrorCode::corruption, "invalid seal reason");
    if (!r.id(pb.worker) || !r.id(pb.boot)) return Result<PersistedRuntime>::err(ErrorCode::corruption, "truncated worker");
    std::uint64_t np, nc;
    if (!r.u64(np) || !r.u64(nc)) return Result<PersistedRuntime>::err(ErrorCode::corruption, "truncated lineage");
    if (np > kMaxRecords || nc > kMaxRecords) return Result<PersistedRuntime>::err(ErrorCode::corruption, "impossible lineage");
    for (std::uint64_t k = 0; k < np; ++k) {
      BatchId p;
      if (!r.id(p)) return Result<PersistedRuntime>::err(ErrorCode::corruption, "truncated parent");
      pb.parents.push_back(p);
    }
    for (std::uint64_t k = 0; k < nc; ++k) {
      BatchId c;
      if (!r.id(c)) return Result<PersistedRuntime>::err(ErrorCode::corruption, "truncated child");
      pb.children.push_back(c);
    }
    pb.state = static_cast<BatchState>(state);
    pb.key = CompatibilityKey(keyd);
    pb.used.count = mr;
    pb.used.input_tokens = mi;
    pb.used.output_tokens = mo;
    pb.used.work = mw;
    pb.used.memory = mm;
    pb.formed_ns = fn;
    pb.sealed_ns = sn;
    pb.dispatched_ns = dn;
    pb.completed_ns = cn;
    pb.seal_reason = static_cast<SealReason>(s2);
    if (pb.id.is_null() || !bat_ids.insert(pb.id).second)
      return Result<PersistedRuntime>::err(ErrorCode::corruption, "duplicate batch id");
    rt.batches.push_back(std::move(pb));
  }

  std::uint64_t nfair;
  if (!r.u64(nfair)) return Result<PersistedRuntime>::err(ErrorCode::corruption, "truncated fairness");
  if (nfair > kMaxRecords) return Result<PersistedRuntime>::err(ErrorCode::corruption, "impossible fairness count");
  for (std::uint64_t i = 0; i < nfair; ++i) {
    std::string k;
    std::uint64_t v;
    if (!r.string(k) || !r.u64(v)) return Result<PersistedRuntime>::err(ErrorCode::corruption, "truncated fairness record");
    rt.fairness_served[k] = v;
  }

  std::map<RequestId, BatchId> active_member;
  for (const auto& b : rt.batches) {
    if (is_batch_terminal(b.state)) continue;
    for (auto m : b.members) {
      auto it = active_member.find(m);
      if (it != active_member.end() && it->second != b.id)
        return Result<PersistedRuntime>::err(ErrorCode::corruption, "request in two active batches");
      active_member[m] = b.id;
    }
  }

  if (r.remaining() != 32) return Result<PersistedRuntime>::err(ErrorCode::corruption, "trailing garbage");

  return Result<PersistedRuntime>::ok(std::move(rt));
}

}  // namespace batch_fabric