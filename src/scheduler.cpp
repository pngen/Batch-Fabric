#include "batch_fabric/scheduler.hpp"
#include "batch_fabric/persistence.hpp"
#include <algorithm>
#include <map>
#include <mutex>
#include <random>
#include <set>
#include <vector>

namespace batch_fabric {

namespace {

TimePoint resolve_now(const std::shared_ptr<Clock>& clock, TimePoint now) {
  return now < 0 ? clock->now() : now;
}

bool is_waiting_state(RequestState s) {
  return s == RequestState::eligible || s == RequestState::waiting;
}

}  // namespace

class BatchFabric::Impl {
 public:
  struct AttemptRec {
    AttemptId id;
    std::uint32_t number = 0;
    RequestState state = RequestState::created;
    BatchId batch;
    CompletionStatus outcome = CompletionStatus::success;
    std::string result;
    ErrorCode error = ErrorCode::ok;
    TimePoint started = 0;
    TimePoint completed = 0;
  };

  struct RequestRec {
    RequestId id;
    RequestMetadata meta;
    RequestState state = RequestState::created;
    AttemptId current_attempt;
    std::vector<AttemptId> attempts;
    BatchId batch;
    TimePoint submitted = 0;
    TimePoint sealed_ns = 0;
    CompatibilityKey key;
    bool in_waiting = false;
    bool authoritative_complete = false;
  };

  struct WorkerRec {
    ExecutorCapability cap;
    WorkerBootId boot;
    bool live = false;
    std::vector<BatchId> running;
  };

  struct BatchRec {
    BatchId id;
    Generation generation;
    BatchEpoch epoch;
    BatchState state = BatchState::forming;
    CompatibilityKey key;
    Phase phase = Phase::prefill;
    ModelDescriptor descriptor;
    std::vector<RequestId> members;
    BatchAccum accum;
    TimePoint formed_ns = 0;
    TimePoint sealed_ns = 0;
    TimePoint dispatched_ns = 0;
    TimePoint completed_ns = 0;
    SealReason seal_reason = SealReason::max_wait_elapsed;
    WorkerId worker;
    WorkerBootId boot;
    bool local = true;
    std::vector<BatchId> parents;
    std::vector<BatchId> children;
    TimePoint allowed_wait_ns = kNoDeadline;
    std::map<RequestId, CompletionStatus> outcomes;
    std::map<RequestId, std::string> member_results;
    std::vector<RequestId> cancelled_before_seal;
  };

  Impl(const BatchFabricConfig& cfg)
      : cfg_(cfg),
        clock_(cfg.clock ? cfg.clock : std::make_shared<RealMonotonicClock>()),
        events_(cfg.events ? cfg.events : std::make_shared<NullEventSink>()),
        fairness_(cfg.policy.fairness),
        former_(cfg.policy, clock_),
        epoch_(BatchEpoch(1)),
        rng_(cfg.seed) {
    policy_ = cfg.policy;
    ExecutorCapability local;
    local.worker = WorkerId(0);
    local.backend = Backend::cpu;
    workers_[WorkerId(0)] = WorkerRec{local, WorkerBootId(0), true, {}};
  }

  BatchFabricConfig cfg_;
  BatchPolicy policy_;
  std::shared_ptr<Clock> clock_;
  std::shared_ptr<EventSink> events_;
  FairnessController fairness_;
  BatchFormer former_;
  BatchEpoch epoch_;
  std::mt19937_64 rng_;
  std::uint64_t next_request_ = 1;
  std::uint64_t next_batch_ = 1;
  std::uint64_t next_attempt_ = 1;
  std::uint64_t next_generation_ = 1;

  std::map<RequestId, RequestRec> requests_;
  std::map<AttemptId, AttemptRec> attempts_;
  std::map<BatchId, BatchRec> batches_;
  std::map<WorkerId, WorkerRec> workers_;
  CompatibilityIndex waiting_index_;
  std::map<CompatibilityKey, BatchId> forming_by_key_;
  std::vector<BatchId> sealed_order_;
  BatchStats stats_;
  bool recovered_ = false;
  mutable std::mutex mu_;

  BatchId nb() { return BatchId(next_batch_++); }
  RequestId nr() { return RequestId(next_request_++); }
  AttemptId na_() { return AttemptId(next_attempt_++); }
  Generation ng() { return Generation(next_generation_++); }

  RequestRec* find_request(const RequestId& id) {
    auto it = requests_.find(id);
    return it == requests_.end() ? nullptr : &it->second;
  }
  BatchRec* find_batch(const BatchId& id) {
    auto it = batches_.find(id);
    return it == batches_.end() ? nullptr : &it->second;
  }
  AttemptRec* find_attempt(const AttemptId& id) {
    auto it = attempts_.find(id);
    return it == attempts_.end() ? nullptr : &it->second;
  }

  void emit(std::vector<BatchEvent>& ev, EventType type, const RequestId& req,
            const BatchId& bat, const AttemptId& att, const TenantId& tenant,
            const std::string& detail) {
    BatchEvent e;
    e.epoch = epoch_;
    e.time = clock_->now();
    e.type = type;
    e.request = req;
    e.batch = bat;
    e.attempt = att;
    e.tenant = tenant;
    e.detail = detail;
    ev.push_back(std::move(e));
  }

  // ---- submit ----
  Result<SubmitOutcome> do_submit(const RequestMetadata& meta, TimePoint now,
                                  std::vector<BatchEvent>& ev) {
    SubmitOutcome out;
    if (meta.descriptor.model.value.empty() || meta.descriptor.revision.value.empty()) {
      out.admission = AdmissionDecision::rejected;
      out.reason = "missing model/revision identity";
      stats_.rejected++;
      return Result<SubmitOutcome>::ok(std::move(out));
    }
    RequestId rid = nr();
    AttemptId aid = na_();
    RequestRec& r = requests_[rid];
    r.id = rid;
    r.meta = meta;
    r.submitted = now;
    r.key = CompatibilityKey::build(meta.descriptor);
    r.current_attempt = aid;
    r.attempts.push_back(aid);
    AttemptRec& a = attempts_[aid];
    a.id = aid;
    a.number = 1;
    a.state = RequestState::eligible;

    if (meta.deadline.has_deadline() && meta.deadline.expired(now)) {
      r.state = RequestState::expired;
      a.state = RequestState::expired;
      a.outcome = CompletionStatus::expired;
      stats_.submitted++;
      stats_.expired++;
      out.request = rid; out.attempt = aid; out.admission = AdmissionDecision::expired;
      out.reason = "deadline already expired";
      emit(ev, EventType::submit, rid, BatchId(), aid, meta.tenant, "expired");
      return Result<SubmitOutcome>::ok(std::move(out));
    }

    const auto& budget = policy_.constraints.budget;
    bool oversized = (budget.max_requests != 0 && budget.max_requests < 1) ||
        (budget.max_input_tokens != 0 && meta.tokens.input > budget.max_input_tokens) ||
        (budget.max_output_tokens != 0 && meta.tokens.output > budget.max_output_tokens) ||
        (budget.max_work != 0 && meta.work.value > budget.max_work) ||
        (budget.max_memory_bytes != 0 && meta.memory_bytes > budget.max_memory_bytes);
    if (oversized) {
      r.state = RequestState::rejected;
      a.state = RequestState::rejected;
      a.outcome = CompletionStatus::non_retryable_failure;
      stats_.submitted++;
      stats_.rejected++;
      out.request = rid; out.attempt = aid; out.admission = AdmissionDecision::oversized;
      out.reason = "request exceeds any batch budget";
      emit(ev, EventType::submit, rid, BatchId(), aid, meta.tenant, "oversized");
      return Result<SubmitOutcome>::ok(std::move(out));
    }

    if (policy_.fairness.enabled && fairness_.at_outstanding_cap(meta.tenant)) {
      r.state = RequestState::waiting;
      a.state = RequestState::waiting;
      stats_.submitted++;
      stats_.deferred++;
      out.request = rid; out.attempt = aid; out.admission = AdmissionDecision::deferred;
      out.reason = "tenant outstanding cap reached";
      emit(ev, EventType::submit, rid, BatchId(), aid, meta.tenant, "deferred");
      return Result<SubmitOutcome>::ok(std::move(out));
    }

    r.state = RequestState::eligible;
    a.state = RequestState::eligible;
    r.in_waiting = true;
    waiting_index_.add(r.key, rid);
    stats_.submitted++;
    stats_.admitted++;
    fairness_.note_submission(meta.tenant, meta.work.value);
    out.request = rid; out.attempt = aid; out.admission = AdmissionDecision::admitted;
    out.reason = "admitted";
    emit(ev, EventType::submit, rid, BatchId(), aid, meta.tenant, "submitted");

    // Formation and sealing are driven explicitly by tick(); submission only
    // admits and enqueues so max-wait / deadline semantics are preserved.
    return Result<SubmitOutcome>::ok(std::move(out));
  }

  // ---- formation ----
  BatchFormer::FormingBatch to_former_batch(const BatchRec& b) {
    BatchFormer::FormingBatch fb;
    fb.batch = b.id;
    fb.key = b.key;
    fb.phase = b.phase;
    fb.formed_ns = b.formed_ns;
    fb.count = b.accum.count;
    fb.input_tokens = b.accum.input_tokens;
    fb.output_tokens = b.accum.output_tokens;
    fb.work = b.accum.work;
    fb.memory = b.accum.memory;
    fb.allowed_wait_ns = b.allowed_wait_ns;
    return fb;
  }

  TimePoint compute_allowed_wait(const BatchRec& b, TimePoint now) {
    TimePoint allowed = kNoDeadline;
    if (policy_.constraints.global_max_wait_ns > 0)
      allowed = std::min(allowed, policy_.constraints.global_max_wait_ns);
    for (const auto& id : b.members) {
      auto* r = find_request(id);
      if (!r) continue;
      auto it = policy_.wait.latency_max_wait_ns.find(r->meta.latency);
      if (it != policy_.wait.latency_max_wait_ns.end()) allowed = std::min(allowed, it->second);
      if (r->meta.deadline.has_deadline()) {
        TimePoint rem = r->meta.deadline.remaining(now);
        if (rem >= 0) allowed = std::min(allowed, rem);
      }
    }
    if (policy_.wait.immediate_seal_when_solo && b.accum.count == 1) allowed = 0;
    return allowed;
  }

  void add_member_to_batch(BatchRec& b, const RequestId& id, TimePoint now,
                           std::vector<BatchEvent>& ev) {
    (void)now;
    auto* r = find_request(id);
    if (!r) return;
    b.members.push_back(id);
    b.accum.count = static_cast<std::uint32_t>(b.members.size());
    b.accum.input_tokens += r->meta.tokens.input;
    b.accum.output_tokens += r->meta.tokens.output;
    b.accum.work += r->meta.work.value;
    b.accum.memory += r->meta.memory_bytes;
    r->state = RequestState::reserved;
    r->batch = b.id;
    r->in_waiting = false;
    if (b.members.empty()) {
      // first member will set descriptor below
    }
    if (b.members.size() == 1) b.descriptor = r->meta.descriptor;
    fairness_.note_placed(r->meta.tenant, r->meta.work.value);
    stats_.reserved_now++;
    emit(ev, EventType::form, id, b.id, r->current_attempt, r->meta.tenant, "reserved");
  }

  std::uint32_t count_tenant(const BatchRec& b, TenantId t) {
    std::uint32_t n = 0;
    for (auto m : b.members) {
      auto* r = find_request(m);
      if (r && r->meta.tenant == t) n++;
    }
    return n;
  }

  BatchId make_forming(const CompatibilityKey& key, TimePoint now, std::vector<BatchEvent>& ev) {
    BatchId bid = nb();
    BatchRec& b = batches_[bid];
    b.id = bid;
    b.generation = ng();
    b.epoch = epoch_;
    b.state = BatchState::forming;
    b.key = key;
    b.formed_ns = now;
    b.seal_reason = SealReason::max_wait_elapsed;
    if (auto* bucket = waiting_index_.bucket(key); bucket && !bucket->empty()) {
      auto* r = find_request((*bucket)[0]);
      if (r) b.phase = r->meta.descriptor.phase;
    }
    forming_by_key_[key] = bid;
    stats_.forming_now++;
    fill_forming(bid, key, now, ev);
    b.allowed_wait_ns = compute_allowed_wait(b, now);
    // The max-wait clock starts when the batch began FORMING, i.e. the earliest
    // member arrival, not when the batch object was created this tick.
    TimePoint earliest = now;
    for (auto m : b.members) {
      auto* r = find_request(m);
      if (r && r->submitted < earliest) earliest = r->submitted;
    }
    b.formed_ns = earliest;
    return bid;
  }

  void fill_forming(const BatchId& bid, const CompatibilityKey& key, TimePoint now,
                    std::vector<BatchEvent>& ev) {
    auto* b = find_batch(bid);
    if (!b) return;
    auto* bucket = waiting_index_.bucket(key);
    if (!bucket) return;
    std::vector<RequestId> cands = *bucket;
    bool progress = true;
    while (progress) {
      progress = false;
      std::map<TenantId, std::vector<RequestId>> by_tenant;
      BatchFormer::FormingBatch fb = to_former_batch(*b);
      for (auto& id : cands) {
        auto* r = find_request(id);
        if (!r || !is_waiting_state(r->state)) continue;
        if (r->meta.deadline.expired(now)) continue;
        if (policy_.fairness.enabled && fairness_.at_outstanding_cap(r->meta.tenant)) continue;
        if (policy_.fairness.max_contribution_per_batch != 0 &&
            count_tenant(*b, r->meta.tenant) >= policy_.fairness.max_contribution_per_batch)
          continue;
        BatchFormer::Candidate c;
        c.request = id;
        c.tenant = r->meta.tenant;
        c.tokens = r->meta.tokens;
        c.work = r->meta.work;
        c.memory = r->meta.memory_bytes;
        c.deadline = r->meta.deadline;
        c.latency = r->meta.latency;
        c.arrived_ns = r->submitted;
        if (BatchFormer::fits(fb, c, policy_.constraints.budget)) by_tenant[r->meta.tenant].push_back(id);
      }
      if (by_tenant.empty()) break;
      std::vector<TenantId> tenants;
      for (auto& kv : by_tenant) tenants.push_back(kv.first);
      TenantId chosen = fairness_.select_next(tenants);
      auto& pool = by_tenant[chosen];
      std::sort(pool.begin(), pool.end(), [&](const RequestId& x, const RequestId& y) {
        auto* rx = find_request(x);
        auto* ry = find_request(y);
        if (!rx || !ry) return x.value < y.value;
        if (rx->submitted != ry->submitted) return rx->submitted < ry->submitted;
        return x.value < y.value;
      });
      RequestId pick = pool.front();
      add_member_to_batch(*b, pick, now, ev);
      waiting_index_.remove(key, pick);
      cands.erase(std::remove(cands.begin(), cands.end(), pick), cands.end());
      progress = true;
    }
  }

  bool do_seal(const BatchId& bid, SealReason reason, TimePoint now, std::vector<BatchEvent>& ev) {
    auto* b = find_batch(bid);
    if (!b || b->state != BatchState::forming) return false;
    b->state = BatchState::sealed;
    b->seal_reason = reason;
    b->sealed_ns = now;
    for (auto m : b->members) {
      auto* r = find_request(m);
      if (r && r->state == RequestState::reserved) r->state = RequestState::batched;
    }
    forming_by_key_.erase(b->key);
    sealed_order_.push_back(bid);
    stats_.forming_now--;
    stats_.batches_sealed++;
    emit(ev, EventType::seal, RequestId(), bid, AttemptId(), TenantId(), std::string(to_string(reason)));
    return true;
  }

  void recompute_accum(BatchRec& b) {
    b.accum = BatchAccum{};
    for (auto m : b.members) {
      auto* r = find_request(m);
      if (!r) continue;
      b.accum.count++;
      b.accum.input_tokens += r->meta.tokens.input;
      b.accum.output_tokens += r->meta.tokens.output;
      b.accum.work += r->meta.work.value;
      b.accum.memory += r->meta.memory_bytes;
    }
    if (!b.members.empty() && b.descriptor.model.value.empty()) {
      auto* r = find_request(b.members.front());
      if (r) b.descriptor = r->meta.descriptor;
    }
  }

  BatchId do_split(const BatchId& bid, SplitReason reason, TimePoint now, std::vector<BatchEvent>& ev) {
    (void)now;
    auto* b = find_batch(bid);
    if (!b) return BatchId();
    if (b->state == BatchState::dispatched || b->state == BatchState::running ||
        is_batch_terminal(b->state)) return BatchId();
    std::size_t n = b->members.size();
    std::size_t half = n / 2;
    if (half == 0) return BatchId();

    std::vector<RequestId> first(b->members.begin(), b->members.begin() + half);
    std::vector<RequestId> second(b->members.begin() + half, b->members.end());

    BatchId child = nb();
    BatchRec& c = batches_[child];
    c.id = child;
    c.generation = b->generation;
    c.epoch = b->epoch;
    c.state = b->state;
    c.key = b->key;
    c.phase = b->phase;
    c.descriptor = b->descriptor;
    c.formed_ns = b->formed_ns;
    c.sealed_ns = b->sealed_ns;
    c.allowed_wait_ns = b->allowed_wait_ns;
    c.parents.push_back(bid);
    c.worker = b->worker;
    c.boot = b->boot;

    for (auto m : second) {
      auto* r = find_request(m);
      if (r) r->batch = child;
      c.members.push_back(m);
    }
    b->members = first;
    b->children.push_back(child);
    for (auto m : first) {
      auto* r = find_request(m);
      if (r) r->batch = bid;
    }
    recompute_accum(*b);
    recompute_accum(c);

    if (b->state == BatchState::sealed) {
      auto pos = std::find(sealed_order_.begin(), sealed_order_.end(), bid);
      if (pos != sealed_order_.end()) sealed_order_.erase(pos);
      b->state = BatchState::superseded;
      // Re-add first-half as a new sealed batch.
      BatchId firstid = nb();
      BatchRec& f = batches_[firstid];
      f.id = firstid;
      f.generation = b->generation;
      f.epoch = b->epoch;
      f.state = BatchState::sealed;
      f.key = b->key;
      f.phase = b->phase;
      f.descriptor = b->descriptor;
      f.members = first;
      f.formed_ns = b->formed_ns;
      f.sealed_ns = b->sealed_ns;
      f.allowed_wait_ns = b->allowed_wait_ns;
      f.worker = b->worker;
      f.boot = b->boot;
      f.parents.push_back(bid);
      f.children.push_back(child);
      recompute_accum(f);
      firstid = firstid;
      sealed_order_.push_back(firstid);
      c.parents.push_back(firstid);
      b->children.push_back(firstid);
      emit(ev, EventType::split, RequestId(), firstid, AttemptId(), TenantId(),
           std::string(to_string(reason)) + " first");
      emit(ev, EventType::split, RequestId(), child, AttemptId(), TenantId(),
           std::string(to_string(reason)) + " second");
      stats_.forming_now += 0;
    } else {
      forming_by_key_[b->key] = bid;
      stats_.forming_now++;
      emit(ev, EventType::split, RequestId(), child, AttemptId(), TenantId(),
           std::string(to_string(reason)));
    }
    stats_.batches_split++;
    return child;
  }

  bool can_merge(const BatchRec& a, const BatchRec& b) {
    if (a.key != b.key) return false;
    if (a.phase != b.phase) return false;
    if (a.state != b.state) return false;
    if (a.state != BatchState::sealed && a.state != BatchState::forming) return false;
    if (!policy_.merge.enabled) return false;
    std::uint32_t total = a.accum.count + b.accum.count;
    return policy_.constraints.budget.allows_add(
        0, 0, 0, 0, 0, total,
        a.accum.input_tokens + b.accum.input_tokens,
        a.accum.output_tokens + b.accum.output_tokens,
        a.accum.work + b.accum.work, a.accum.memory + b.accum.memory);
  }

  BatchId do_merge(const BatchId& aid, const BatchId& bid, std::vector<BatchEvent>& ev) {
    auto* a = find_batch(aid);
    auto* b = find_batch(bid);
    if (!a || !b) return BatchId();
    if (!can_merge(*a, *b)) return BatchId();
    for (auto m : b->members) {
      auto* r = find_request(m);
      if (r) r->batch = aid;
      a->members.push_back(m);
    }
    for (auto p : b->parents) a->parents.push_back(p);
    a->parents.push_back(bid);
    a->children.insert(a->children.end(), b->children.begin(), b->children.end());
    recompute_accum(*a);
    b->state = BatchState::superseded;
    sealed_order_.erase(std::remove(sealed_order_.begin(), sealed_order_.end(), bid), sealed_order_.end());
    if (forming_by_key_.find(b->key) != forming_by_key_.end() && forming_by_key_[b->key] == bid)
      forming_by_key_[b->key] = aid;
    stats_.batches_merged++;
    emit(ev, EventType::merge, RequestId(), aid, AttemptId(), TenantId(), "merged");
    return aid;
  }

  void expire_locked(TimePoint now, std::vector<BatchEvent>& ev) {
    std::vector<RequestId> expired;
    for (auto& kv : waiting_index_.buckets()) {
      for (auto id : kv.second) {
        auto* r = find_request(id);
        if (r && r->meta.deadline.has_deadline() && r->meta.deadline.expired(now)) expired.push_back(id);
      }
    }
    for (auto id : expired) {
      auto* r = find_request(id);
      if (!r) continue;
      waiting_index_.remove(r->key, id);
      complete_request_terminal(id, CompletionStatus::expired, RequestState::expired, now, ev);
    }
    // Expire forming-batch members whose deadline passed: seal/cancel those batches.
    std::vector<RequestId> in_batch_expired;
    for (auto& kv : forming_by_key_) {
      auto* b = find_batch(kv.second);
      if (!b) continue;
      for (auto m : b->members) {
        auto* r = find_request(m);
        if (r && r->meta.deadline.has_deadline() && r->meta.deadline.expired(now))
          in_batch_expired.push_back(m);
      }
    }
    for (auto id : in_batch_expired) {
      auto* r = find_request(id);
      if (!r) continue;
      do_cancel(id, CancellationReason::deadline_expired, now, ev);
    }
  }

  void complete_request_terminal(const RequestId& id, CompletionStatus status, RequestState state,
                                 TimePoint now, std::vector<BatchEvent>& ev) {
    auto* r = find_request(id);
    if (!r) return;
    if (is_request_terminal(r->state)) return;
    r->state = state;
    r->in_waiting = false;
    auto* a = find_attempt(r->current_attempt);
    if (a) { a->state = state; a->outcome = status; a->completed = now; }
    if (status == CompletionStatus::expired) {
      stats_.expired++;
      emit(ev, EventType::expire, id, r->batch, r->current_attempt, r->meta.tenant, "expired");
    } else if (status == CompletionStatus::cancelled) {
      stats_.cancelled++;
      emit(ev, EventType::cancel, id, r->batch, r->current_attempt, r->meta.tenant, "cancelled");
    } else {
      stats_.requests_completed++;
      emit(ev, EventType::complete, id, r->batch, r->current_attempt, r->meta.tenant,
           std::string(to_string(status)));
    }
  }

  void do_tick(TimePoint now, std::vector<BatchEvent>& ev) {
    expire_locked(now, ev);

    std::vector<CompatibilityKey> keys;
    for (const auto& kv : waiting_index_.buckets()) keys.push_back(kv.first);
    for (auto& key : keys) {
      if (forming_by_key_.find(key) == forming_by_key_.end()) {
        make_forming(key, now, ev);
      }
    }

    std::vector<BatchId> to_seal;
    for (auto& kv : forming_by_key_) {
      BatchId bid = kv.second;
      auto* b = find_batch(bid);
      if (!b) continue;
      fill_forming(bid, b->key, now, ev);
      b->allowed_wait_ns = compute_allowed_wait(*b, now);
      std::vector<BatchFormer::Candidate> remaining;
      if (auto* bucket = waiting_index_.bucket(b->key)) {
        for (auto m : *bucket) {
          auto* r = find_request(m);
          if (!r || !is_waiting_state(r->state)) continue;
          BatchFormer::Candidate c;
          c.request = m;
          c.tenant = r->meta.tenant;
          c.tokens = r->meta.tokens;
          c.work = r->meta.work;
          c.memory = r->meta.memory_bytes;
          c.deadline = r->meta.deadline;
          c.latency = r->meta.latency;
          c.arrived_ns = r->submitted;
          remaining.push_back(c);
        }
      }
      auto decision = former_.decide(to_former_batch(*b), remaining, now);
      if (decision.action == BatchFormer::Action::seal) to_seal.push_back(bid);
    }
    for (auto bid : to_seal) {
      auto* b = find_batch(bid);
      if (b) do_seal(bid, sealed_reason_for(bid), now, ev);
    }

    if (policy_.merge.enabled) {
      std::map<CompatibilityKey, std::vector<BatchId>> by_key;
      for (auto bid : sealed_order_) {
        auto* b = find_batch(bid);
        if (b && b->state == BatchState::sealed) by_key[b->key].push_back(bid);
      }
      for (auto& kv : by_key) {
        auto& v = kv.second;
        for (std::size_t i = 0; i + 1 < v.size() && stats_.batches_merged < policy_.merge.max_merges_per_cycle; ++i)
          do_merge(v[i], v[i + 1], ev);
      }
    }

    for (std::size_t i = 0; i < sealed_order_.size(); ++i) {
      auto* b = find_batch(sealed_order_[i]);
      if (!b || b->state != BatchState::sealed) continue;
      std::uint32_t max_sz = max_worker_batch_size();
      if (max_sz != 0 && b->accum.count > max_sz) do_split(b->id, SplitReason::worker_batch_size_constraint, now, ev);
    }
  }

  SealReason sealed_reason_for(const BatchId& bid) {
    auto* b = find_batch(bid);
    return b ? b->seal_reason : SealReason::max_wait_elapsed;
  }

  std::uint32_t max_worker_batch_size() {
    std::uint32_t m = 0;
    for (auto& kv : workers_) {
      if (!kv.second.live) continue;
      auto c = kv.second.cap.max_batch_size;
      if (c != 0 && (m == 0 || c < m)) m = c;
    }
    return m;
  }

  // ---- cancellation ----
  Result<void> do_cancel(const RequestId& id, CancellationReason reason, TimePoint now,
                         std::vector<BatchEvent>& ev) {
    auto* r = find_request(id);
    if (!r) return Result<void>::err(ErrorCode::not_found, "request not found");
    if (is_request_terminal(r->state))
      return Result<void>::err(ErrorCode::invalid_state, "request already terminal");
    bool in_batch = r->state == RequestState::reserved || r->state == RequestState::batched ||
                    r->state == RequestState::dispatched || r->state == RequestState::running ||
                    r->state == RequestState::sealed;
    auto* a = find_attempt(r->current_attempt);
    if (!in_batch) {
      if (r->in_waiting) waiting_index_.remove(r->key, id);
      r->state = RequestState::cancelled;
      r->in_waiting = false;
      if (a) { a->state = RequestState::cancelled; a->outcome = CompletionStatus::cancelled; a->completed = now; }
      stats_.cancelled++;
      emit(ev, EventType::cancel, id, BatchId(), r->current_attempt, r->meta.tenant,
           std::string(to_string(reason)));
      return Result<void>::success();
    }
    auto* b = find_batch(r->batch);
    if (b && b->state == BatchState::forming) {
      auto& mem = b->members;
      mem.erase(std::remove(mem.begin(), mem.end(), id), mem.end());
      recompute_accum(*b);
      b->allowed_wait_ns = compute_allowed_wait(*b, now);
      r->state = RequestState::cancelled;
      r->batch = BatchId();
      if (a) { a->state = RequestState::cancelled; a->outcome = CompletionStatus::cancelled; a->completed = now; }
      stats_.cancelled++;
      emit(ev, EventType::cancel, id, b->id, r->current_attempt, r->meta.tenant,
           std::string(to_string(reason)) + " before seal");
      return Result<void>::success();
    }
    if (b && (b->state == BatchState::sealed || b->state == BatchState::dispatched ||
              b->state == BatchState::running)) {
      b->outcomes[id] = CompletionStatus::cancelled;
      r->state = RequestState::cancelled;
      if (a) { a->state = RequestState::cancelled; a->outcome = CompletionStatus::cancelled; a->completed = now; }
      stats_.cancelled++;
      emit(ev, EventType::cancel, id, b->id, r->current_attempt, r->meta.tenant,
           std::string(to_string(reason)) + " after seal");
      return Result<void>::success();
    }
    return Result<void>::err(ErrorCode::invalid_state, "cannot cancel request in this state");
  }

  // ---- retry ----
  Result<AttemptId> do_retry(const RequestId& id, TimePoint now, std::vector<BatchEvent>& ev) {
    auto* r = find_request(id);
    if (!r) return Result<AttemptId>::err(ErrorCode::not_found, "request not found");
    auto* a = find_attempt(r->current_attempt);
    std::uint32_t num = a ? a->number : 0;
    if (num >= policy_.retry.max_attempts)
      return Result<AttemptId>::err(ErrorCode::invalid_state, "retry budget exhausted");
    if (r->meta.deadline.has_deadline() && r->meta.deadline.expired(now))
      return Result<AttemptId>::err(ErrorCode::deadline_expired, "deadline impossible for retry");
    AttemptId na = na_();
    AttemptRec& na_rec = attempts_[na];
    na_rec.id = na;
    na_rec.number = num + 1;
    na_rec.state = RequestState::eligible;
    r->current_attempt = na;
    r->attempts.push_back(na);
    r->state = RequestState::eligible;
    r->batch = BatchId();
    r->in_waiting = true;
    waiting_index_.add(r->key, id);
    stats_.requests_retried++;
    emit(ev, EventType::retry, id, BatchId(), na, r->meta.tenant, "retry");
    do_tick(now, ev);
    return Result<AttemptId>::ok(na);
  }

  // ---- completion ----
  Result<void> do_complete(const BatchCompletion& comp, TimePoint now, std::vector<BatchEvent>& ev) {
    if (comp.epoch != epoch_) {
      stats_.stale_rejections++;
      emit(ev, EventType::stale_reject, RequestId(), comp.batch, AttemptId(), TenantId(), "stale epoch");
      return Result<void>::err(ErrorCode::stale_epoch, "completion epoch mismatch");
    }
    auto w = workers_.find(comp.worker);
    if (w == workers_.end() || !w->second.live || w->second.boot != comp.boot) {
      stats_.stale_rejections++;
      emit(ev, EventType::stale_reject, RequestId(), comp.batch, AttemptId(), TenantId(), "stale worker");
      return Result<void>::err(ErrorCode::stale_worker, "completion worker/boot not current");
    }
    auto* b = find_batch(comp.batch);
    if (!b) {
      stats_.stale_rejections++;
      return Result<void>::err(ErrorCode::not_found, "batch not found");
    }
    if (b->generation != comp.generation) {
      stats_.stale_rejections++;
      emit(ev, EventType::stale_reject, RequestId(), comp.batch, AttemptId(), TenantId(), "stale generation");
      return Result<void>::err(ErrorCode::stale_attempt, "completion generation mismatch");
    }
    if (b->state != BatchState::dispatched && b->state != BatchState::running)
      return Result<void>::err(ErrorCode::invalid_state, "batch not running");

    for (const auto& mc : comp.members) {
      auto* r = find_request(mc.request);
      if (!r) {
        emit(ev, EventType::stale_reject, mc.request, comp.batch, mc.attempt, TenantId(), "unknown request");
        continue;
      }
      if (r->current_attempt != mc.attempt) {
        stats_.stale_rejections++;
        emit(ev, EventType::stale_reject, mc.request, comp.batch, mc.attempt, r->meta.tenant, "stale attempt");
        continue;
      }
      auto* a = find_attempt(mc.attempt);
      if (!a) continue;
      if (a->state == RequestState::completed || a->state == RequestState::cancelled ||
          a->state == RequestState::expired || a->state == RequestState::failed)
        continue;
      apply_member_outcome(r, a, mc, now, ev);
    }

    bool all_terminal = true;
    for (auto m : b->members) {
      auto* r = find_request(m);
      if (!r || !is_request_terminal(r->state)) { all_terminal = false; }
    }
    if (all_terminal) {
      b->state = BatchState::completed;
      b->completed_ns = now;
      stats_.batches_completed++;
    } else {
      b->state = BatchState::partially_completed;
    }
    if (w != workers_.end()) {
      auto& run = w->second.running;
      run.erase(std::remove(run.begin(), run.end(), comp.batch), run.end());
    }
    return Result<void>::success();
  }

  void apply_member_outcome(RequestRec* r, AttemptRec* a, const MemberCompletion& mc,
                            TimePoint now, std::vector<BatchEvent>& ev) {
    auto* b = find_batch(r->batch);
    BatchId bid = r->batch;
    switch (mc.status) {
      case CompletionStatus::success: {
        r->state = RequestState::completed;
        r->authoritative_complete = true;
        a->state = RequestState::completed;
        a->outcome = CompletionStatus::success;
        a->result = mc.result;
        a->completed = now;
        if (b) { b->outcomes[mc.request] = CompletionStatus::success; b->member_results[mc.request] = mc.result; }
        stats_.requests_completed++;
        stats_.total_work_completed += r->meta.work.value;
        if (policy_.fairness.enabled) fairness_.note_completed(r->meta.tenant, r->meta.work.value);
        emit(ev, EventType::complete, r->id, bid, a->id, r->meta.tenant, "success");
        break;
      }
      case CompletionStatus::retryable_failure: {
        r->state = RequestState::failed;
        a->state = RequestState::failed;
        a->outcome = CompletionStatus::retryable_failure;
        a->error = mc.error_code;
        a->completed = now;
        if (b) { b->outcomes[mc.request] = CompletionStatus::retryable_failure; b->member_results[mc.request] = mc.error_message; }
        stats_.retryable_failures++;
        if (policy_.retry.enabled && a->number < policy_.retry.max_attempts) {
          AttemptId na = na_();
          AttemptRec& na_rec = attempts_[na];
          na_rec.id = na;
          na_rec.number = a->number + 1;
          na_rec.state = RequestState::eligible;
          r->current_attempt = na;
          r->attempts.push_back(na);
          r->state = RequestState::eligible;
          r->batch = BatchId();
          r->in_waiting = true;
          na_rec.batch = BatchId();
          waiting_index_.add(r->key, r->id);
          stats_.requests_retried++;
          emit(ev, EventType::retry, r->id, bid, na, r->meta.tenant, "retryable failure");
        } else {
          emit(ev, EventType::complete, r->id, bid, a->id, r->meta.tenant, "retryable failure (exhausted)");
        }
        break;
      }
      case CompletionStatus::non_retryable_failure: {
        r->state = RequestState::failed;
        a->state = RequestState::failed;
        a->outcome = CompletionStatus::non_retryable_failure;
        a->error = mc.error_code;
        a->completed = now;
        if (b) { b->outcomes[mc.request] = CompletionStatus::non_retryable_failure; b->member_results[mc.request] = mc.error_message; }
        emit(ev, EventType::complete, r->id, bid, a->id, r->meta.tenant, "non-retryable failure");
        break;
      }
      case CompletionStatus::cancelled: {
        r->state = RequestState::cancelled;
        a->state = RequestState::cancelled;
        a->outcome = CompletionStatus::cancelled;
        a->completed = now;
        if (b) b->outcomes[mc.request] = CompletionStatus::cancelled;
        stats_.cancelled++;
        emit(ev, EventType::cancel, r->id, bid, a->id, r->meta.tenant, "cancelled");
        break;
      }
      case CompletionStatus::expired: {
        r->state = RequestState::expired;
        a->state = RequestState::expired;
        a->outcome = CompletionStatus::expired;
        a->completed = now;
        if (b) b->outcomes[mc.request] = CompletionStatus::expired;
        stats_.expired++;
        emit(ev, EventType::expire, r->id, bid, a->id, r->meta.tenant, "expired");
        break;
      }
      case CompletionStatus::stale:
      case CompletionStatus::deferred: {
        stats_.stale_rejections++;
        emit(ev, EventType::stale_reject, r->id, bid, a->id, r->meta.tenant, std::string(to_string(mc.status)));
        break;
      }
    }
  }

  // ---- dispatch (in-process) ----
  Result<BatchId> do_dispatch_and_run(const BatchId& bid, IExecutor& executor, TimePoint now,
                                      std::vector<BatchEvent>& ev) {
    auto* b = find_batch(bid);
    if (!b) return Result<BatchId>::err(ErrorCode::not_found, "batch not found");
    if (b->state != BatchState::sealed) return Result<BatchId>::err(ErrorCode::invalid_state, "batch not sealed");
    b->state = BatchState::dispatched;
    b->dispatched_ns = now;
    b->worker = executor.capability().worker;
    b->boot = (workers_.find(b->worker) != workers_.end()) ? workers_[b->worker].boot : WorkerBootId(0);
    for (auto m : b->members) {
      auto* r = find_request(m);
      if (r && r->state == RequestState::batched) r->state = RequestState::dispatched;
    }
    emit(ev, EventType::dispatch, RequestId(), bid, AttemptId(), TenantId(), "dispatch");

    BatchExecution exec;
    exec.batch = b->id;
    exec.generation = b->generation;
    exec.epoch = b->epoch;
    exec.worker = b->worker;
    exec.descriptor = b->descriptor;
    for (auto m : b->members) {
      auto* r = find_request(m);
      if (!r) continue;
      MemberWork mw;
      mw.request = m;
      mw.attempt = r->current_attempt;
      mw.work = r->meta.work.value;
      mw.input_tokens = r->meta.tokens.input;
      mw.output_tokens = r->meta.tokens.output;
      mw.payload = r->meta.payload;
      exec.members.push_back(std::move(mw));
    }

    auto result = executor.execute(exec);
    b->state = BatchState::running;
    for (auto m : b->members) {
      auto* r = find_request(m);
      if (r && r->state == RequestState::dispatched) r->state = RequestState::running;
    }
    if (!result.ok()) {
      auto err = result.error();
      for (auto m : b->members) {
        auto* r = find_request(m);
        if (!r) continue;
        MemberCompletion mc;
        mc.request = m;
        mc.attempt = r->current_attempt;
        mc.status = CompletionStatus::retryable_failure;
        mc.error_code = err.code();
        mc.error_message = err.to_string();
        apply_member_outcome(r, find_attempt(r->current_attempt), mc, now, ev);
      }
      return Result<BatchId>::err(err.code(), "executor failed: " + err.message());
    }

    std::vector<MemberResult> results = result.move_value();
    BatchCompletion comp;
    comp.worker = b->worker;
    comp.boot = b->boot;
    comp.epoch = b->epoch;
    comp.generation = b->generation;
    comp.batch = b->id;
    for (auto& mr : results) {
      MemberCompletion mc;
      mc.request = mr.request;
      mc.attempt = mr.attempt;
      mc.status = CompletionStatus::success;
      mc.result = mr.output;
      comp.members.push_back(std::move(mc));
    }
    auto cret = do_complete(comp, now, ev);
    if (!cret.ok()) return Result<BatchId>::err(cret.error().code(), cret.error().message());
    return Result<BatchId>::ok(b->id);
  }

  // ---- introspection ----
  BatchStats do_stats() const {
    BatchStats s = stats_;
    std::uint64_t waiting = 0, reserved = 0, running = 0;
    for (auto& kv : requests_) {
      auto st = kv.second.state;
      if (st == RequestState::eligible || st == RequestState::waiting) waiting++;
      if (st == RequestState::reserved) reserved++;
      if (st == RequestState::dispatched || st == RequestState::running) running++;
    }
    s.waiting_now = waiting;
    s.reserved_now = reserved;
    s.running_now = running;
    s.forming_now = stats_.forming_now;
    return s;
  }

  PersistedRuntime build_persisted() const {
    PersistedRuntime rt;
    rt.epoch = epoch_;
    rt.next_request = next_request_;
    rt.next_batch = next_batch_;
    rt.next_attempt = next_attempt_;
    rt.next_generation = next_generation_;
    for (const auto& kv : requests_) {
      PersistedRequest pr;
      pr.id = kv.second.id;
      pr.tenant = kv.second.meta.tenant;
      pr.session = kv.second.meta.session;
      pr.sequence = kv.second.meta.sequence;
      pr.descriptor = kv.second.meta.descriptor;
      pr.tokens = kv.second.meta.tokens;
      pr.work = kv.second.meta.work;
      pr.memory_bytes = kv.second.meta.memory_bytes;
      pr.deadline = kv.second.meta.deadline;
      pr.latency = kv.second.meta.latency;
      pr.priority = kv.second.meta.priority;
      pr.payload = kv.second.meta.payload;
      pr.state = kv.second.state;
      pr.submitted_ns = kv.second.submitted;
      pr.batch = kv.second.batch;
      pr.current_attempt = kv.second.current_attempt;
      pr.attempt_ids = kv.second.attempts;
      rt.requests.push_back(std::move(pr));
    }
    for (const auto& kv : attempts_) {
      PersistedAttempt pa;
      pa.id = kv.second.id;
      pa.number = kv.second.number;
      pa.state = kv.second.state;
      pa.batch = kv.second.batch;
      pa.outcome = kv.second.outcome;
      pa.result = kv.second.result;
      pa.started = kv.second.started;
      pa.completed = kv.second.completed;
      rt.attempts.push_back(std::move(pa));
    }
    for (const auto& kv : batches_) {
      PersistedBatch pb;
      pb.id = kv.second.id;
      pb.generation = kv.second.generation;
      pb.epoch = kv.second.epoch;
      pb.state = kv.second.state;
      pb.key = kv.second.key;
      pb.members = kv.second.members;
      pb.used = kv.second.accum;
      pb.formed_ns = kv.second.formed_ns;
      pb.sealed_ns = kv.second.sealed_ns;
      pb.dispatched_ns = kv.second.dispatched_ns;
      pb.completed_ns = kv.second.completed_ns;
      pb.seal_reason = kv.second.seal_reason;
      pb.worker = kv.second.worker;
      pb.boot = kv.second.boot;
      pb.parents = kv.second.parents;
      pb.children = kv.second.children;
      rt.batches.push_back(std::move(pb));
    }
    for (const auto& kv : fairness_.served_all())
      rt.fairness_served[std::to_string(kv.first.value)] = kv.second;
    return rt;
  }

  void load_persisted(const PersistedRuntime& rt) {
    epoch_ = rt.epoch;
    next_request_ = rt.next_request;
    next_batch_ = rt.next_batch;
    next_attempt_ = rt.next_attempt;
    next_generation_ = rt.next_generation;
    requests_.clear();
    attempts_.clear();
    batches_.clear();
    forming_by_key_.clear();
    sealed_order_.clear();
    waiting_index_.clear();
    for (const auto& pr : rt.requests) {
      RequestRec rec;
      rec.id = pr.id;
      rec.meta.tenant = pr.tenant;
      rec.meta.session = pr.session;
      rec.meta.sequence = pr.sequence;
      rec.meta.descriptor = pr.descriptor;
      rec.meta.tokens = pr.tokens;
      rec.meta.work = pr.work;
      rec.meta.memory_bytes = pr.memory_bytes;
      rec.meta.deadline = pr.deadline;
      rec.meta.latency = pr.latency;
      rec.meta.priority = pr.priority;
      rec.meta.payload = pr.payload;
      rec.state = pr.state;
      rec.submitted = pr.submitted_ns;
      rec.batch = pr.batch;
      rec.current_attempt = pr.current_attempt;
      rec.attempts = pr.attempt_ids;
      rec.key = CompatibilityKey::build(pr.descriptor);
      rec.in_waiting = is_waiting_state(pr.state);
      if (rec.in_waiting) waiting_index_.add(rec.key, pr.id);
      requests_[pr.id] = std::move(rec);
    }
    for (const auto& pa : rt.attempts) {
      AttemptRec a;
      a.id = pa.id;
      a.number = pa.number;
      a.state = pa.state;
      a.batch = pa.batch;
      a.outcome = pa.outcome;
      a.result = pa.result;
      a.started = pa.started;
      a.completed = pa.completed;
      attempts_[pa.id] = std::move(a);
    }
    for (const auto& pb : rt.batches) {
      BatchRec b;
      b.id = pb.id;
      b.generation = pb.generation;
      b.epoch = pb.epoch;
      b.state = pb.state;
      b.key = pb.key;
      b.members = pb.members;
      b.accum = pb.used;
      b.formed_ns = pb.formed_ns;
      b.sealed_ns = pb.sealed_ns;
      b.dispatched_ns = pb.dispatched_ns;
      b.completed_ns = pb.completed_ns;
      b.seal_reason = pb.seal_reason;
      b.worker = pb.worker;
      b.boot = pb.boot;
      b.parents = pb.parents;
      b.children = pb.children;
      if (!b.members.empty()) {
        auto* r = find_request(b.members.front());
        if (r) {
          b.descriptor = r->meta.descriptor;
          b.phase = r->meta.descriptor.phase;
        }
      }
      if (b.state == BatchState::forming) {
        forming_by_key_[b.key] = b.id;
        stats_.forming_now++;
      }
      if (b.state == BatchState::sealed) sealed_order_.push_back(b.id);
      batches_[pb.id] = std::move(b);
    }
    fairness_.reset();
    for (const auto& kv : rt.fairness_served) {
      std::uint64_t v = 0;
      try {
        v = std::stoull(kv.first);
      } catch (...) {
        continue;
      }
      fairness_.restore_served(TenantId(v), kv.second);
    }
    recovered_ = true;
  }
};

// =====================================================================
// BatchFabric public methods
// =====================================================================

BatchFabric::BatchFabric(const BatchFabricConfig& config)
    : impl_(std::make_unique<Impl>(config)) {}
BatchFabric::~BatchFabric() = default;

Result<void> BatchFabric::register_worker(const WorkerId& worker, const WorkerBootId& boot,
                                          const ExecutorCapability& cap) {
  std::vector<BatchEvent> ev;
  {
    std::lock_guard<std::mutex> g(impl_->mu_);
    auto& w = impl_->workers_[worker];
    w.cap = cap;
    w.boot = boot;
    w.live = true;
    impl_->emit(ev, EventType::worker_up, RequestId(), BatchId(), AttemptId(), TenantId(),
                "worker up");
  }
  for (const auto& e : ev) impl_->events_->emit(e);
  return Result<void>::success();
}

Result<SubmitOutcome> BatchFabric::submit(const RequestMetadata& meta, TimePoint now) {
  TimePoint t = resolve_now(impl_->clock_, now);
  std::vector<BatchEvent> ev;
  Result<SubmitOutcome> r;
  {
    std::lock_guard<std::mutex> g(impl_->mu_);
    r = impl_->do_submit(meta, t, ev);
  }
  for (const auto& e : ev) impl_->events_->emit(e);
  return r;
}

Result<void> BatchFabric::cancel(const RequestId& request, CancellationReason reason, TimePoint now) {
  TimePoint t = resolve_now(impl_->clock_, now);
  std::vector<BatchEvent> ev;
  Result<void> r;
  {
    std::lock_guard<std::mutex> g(impl_->mu_);
    r = impl_->do_cancel(request, reason, t, ev);
  }
  for (const auto& e : ev) impl_->events_->emit(e);
  return r;
}

Result<void> BatchFabric::complete(const BatchCompletion& completion, TimePoint now) {
  TimePoint t = resolve_now(impl_->clock_, now);
  std::vector<BatchEvent> ev;
  Result<void> r;
  {
    std::lock_guard<std::mutex> g(impl_->mu_);
    r = impl_->do_complete(completion, t, ev);
  }
  for (const auto& e : ev) impl_->events_->emit(e);
  return r;
}

Result<void> BatchFabric::seal(const BatchId& batch, SealReason reason, TimePoint now) {
  TimePoint t = resolve_now(impl_->clock_, now);
  std::vector<BatchEvent> ev;
  bool ok = false;
  {
    std::lock_guard<std::mutex> g(impl_->mu_);
    auto* b = impl_->find_batch(batch);
    if (b && b->state == BatchState::forming) ok = impl_->do_seal(batch, reason, t, ev);
  }
  for (const auto& e : ev) impl_->events_->emit(e);
  if (!ok) return Result<void>::err(ErrorCode::invalid_state, "batch not forming or not found");
  return Result<void>::success();
}

Result<BatchId> BatchFabric::split(const BatchId& batch, SplitReason reason, TimePoint now) {
  TimePoint t = resolve_now(impl_->clock_, now);
  std::vector<BatchEvent> ev;
  BatchId child;
  {
    std::lock_guard<std::mutex> g(impl_->mu_);
    child = impl_->do_split(batch, reason, t, ev);
  }
  for (const auto& e : ev) impl_->events_->emit(e);
  if (child.is_null()) return Result<BatchId>::err(ErrorCode::split_required, "split not possible");
  return Result<BatchId>::ok(child);
}

Result<BatchId> BatchFabric::merge(const BatchId& a, const BatchId& b, MergeReason reason,
                                   TimePoint now) {
  (void)reason;
  (void)now;
  std::vector<BatchEvent> ev;
  BatchId merged;
  {
    std::lock_guard<std::mutex> g(impl_->mu_);
    merged = impl_->do_merge(a, b, ev);
  }
  for (const auto& e : ev) impl_->events_->emit(e);
  if (merged.is_null()) return Result<BatchId>::err(ErrorCode::merge_rejected, "merge not valid");
  return Result<BatchId>::ok(merged);
}

Result<AttemptId> BatchFabric::retry(const RequestId& request, TimePoint now) {
  TimePoint t = resolve_now(impl_->clock_, now);
  std::vector<BatchEvent> ev;
  Result<AttemptId> r;
  {
    std::lock_guard<std::mutex> g(impl_->mu_);
    r = impl_->do_retry(request, t, ev);
  }
  for (const auto& e : ev) impl_->events_->emit(e);
  return r;
}

FormationReport BatchFabric::tick(TimePoint now) {
  TimePoint t = resolve_now(impl_->clock_, now);
  std::vector<BatchEvent> ev;
  FormationReport rep;
  {
    std::lock_guard<std::mutex> g(impl_->mu_);
    std::vector<BatchId> before = impl_->sealed_order_;
    impl_->do_tick(t, ev);
    for (auto bid : impl_->sealed_order_) {
      if (std::find(before.begin(), before.end(), bid) == before.end())
        rep.sealed_batches.push_back(bid);
    }
    rep.forming = impl_->forming_by_key_.size();
  }
  for (const auto& e : ev) impl_->events_->emit(e);
  return rep;
}

Result<BatchId> BatchFabric::dispatch_and_run(const BatchId& batch, IExecutor& executor,
                                              TimePoint now) {
  TimePoint t = resolve_now(impl_->clock_, now);
  std::vector<BatchEvent> ev;
  Result<BatchId> r;
  {
    std::lock_guard<std::mutex> g(impl_->mu_);
    r = impl_->do_dispatch_and_run(batch, executor, t, ev);
  }
  for (const auto& e : ev) impl_->events_->emit(e);
  return r;
}

Result<BatchExecution> BatchFabric::prepare_dispatch(const BatchId& batch, const WorkerId& worker,
                                                     const WorkerBootId& boot, TimePoint now) {
  TimePoint t = resolve_now(impl_->clock_, now);
  std::vector<BatchEvent> ev;
  BatchExecution exec;
  {
    std::lock_guard<std::mutex> g(impl_->mu_);
    auto* b = impl_->find_batch(batch);
    if (!b) return Result<BatchExecution>::err(ErrorCode::not_found, "batch not found");
    if (b->state != BatchState::sealed)
      return Result<BatchExecution>::err(ErrorCode::invalid_state, "batch not sealed");
    auto w = impl_->workers_.find(worker);
    if (w == impl_->workers_.end() || !w->second.live || w->second.boot != boot)
      return Result<BatchExecution>::err(ErrorCode::stale_worker, "worker not current");
    b->state = BatchState::dispatched;
    b->dispatched_ns = t;
    b->worker = worker;
    b->boot = boot;
    for (auto m : b->members) {
      auto* r = impl_->find_request(m);
      if (r && r->state == RequestState::batched) r->state = RequestState::dispatched;
    }
    exec.batch = b->id;
    exec.generation = b->generation;
    exec.epoch = b->epoch;
    exec.worker = worker;
    exec.descriptor = b->descriptor;
    for (auto m : b->members) {
      auto* r = impl_->find_request(m);
      if (!r) continue;
      MemberWork mw;
      mw.request = m;
      mw.attempt = r->current_attempt;
      mw.work = r->meta.work.value;
      mw.input_tokens = r->meta.tokens.input;
      mw.output_tokens = r->meta.tokens.output;
      mw.payload = r->meta.payload;
      exec.members.push_back(std::move(mw));
    }
    impl_->emit(ev, EventType::dispatch, RequestId(), b->id, AttemptId(), TenantId(), "dispatch");
  }
  for (const auto& e : ev) impl_->events_->emit(e);
  return Result<BatchExecution>::ok(std::move(exec));
}

void BatchFabric::expire(TimePoint now) {
  TimePoint t = resolve_now(impl_->clock_, now);
  std::vector<BatchEvent> ev;
  {
    std::lock_guard<std::mutex> g(impl_->mu_);
    impl_->expire_locked(t, ev);
  }
  for (const auto& e : ev) impl_->events_->emit(e);
}

BatchStats BatchFabric::stats() const {
  std::lock_guard<std::mutex> g(impl_->mu_);
  return impl_->do_stats();
}

BatchSnapshot BatchFabric::snapshot() const {
  std::lock_guard<std::mutex> g(impl_->mu_);
  BatchSnapshot snap;
  snap.epoch = impl_->epoch_;
  snap.now = impl_->clock_->now();
  for (const auto& kv : impl_->batches_) {
    BatchSnapshotEntry e;
    e.batch = kv.second.id;
    e.generation = kv.second.generation;
    e.epoch = kv.second.epoch;
    e.state = kv.second.state;
    e.key = kv.second.key;
    e.member_count = kv.second.members.size();
    e.worker = kv.second.worker;
    e.boot = kv.second.boot;
    e.seal_reason = kv.second.seal_reason;
    snap.batches.push_back(std::move(e));
  }
  for (const auto& kv : impl_->requests_) {
    RequestSnapshotEntry e;
    e.request = kv.second.id;
    e.tenant = kv.second.meta.tenant;
    e.state = kv.second.state;
    e.batch = kv.second.batch;
    e.attempt = kv.second.current_attempt;
    e.attempt_number = 0;
    if (auto* a = impl_->find_attempt(kv.second.current_attempt)) e.attempt_number = a->number;
    e.expired = kv.second.meta.deadline.has_deadline() &&
                kv.second.meta.deadline.expired(impl_->clock_->now());
    snap.requests.push_back(std::move(e));
  }
  for (const auto& kv : impl_->workers_) {
    WorkerSnapshotEntry e;
    e.worker = kv.first;
    e.boot = kv.second.boot;
    e.live = kv.second.live;
    e.active_batch_count = static_cast<std::uint32_t>(kv.second.running.size());
    e.completed_batches = 0;
    snap.workers.push_back(std::move(e));
  }
  return snap;
}

ExplainResult BatchFabric::explain(const RequestId& id) const {
  std::lock_guard<std::mutex> g(impl_->mu_);
  ExplainResult r;
  r.request = id;
  auto* req = impl_->find_request(id);
  if (!req) {
    r.summary = "unknown request";
    return r;
  }
  r.summary = "request=" + id.string() + " state=" + std::string(to_string(req->state)) +
              " phase=" + std::string(to_string(req->meta.descriptor.phase));
  r.entries.push_back({"compatibility",
                       "key=" + req->key.hex() + " canonical=" + canonical_string(req->meta.descriptor)});
  if (req->batch.is_null()) {
    if (req->state == RequestState::waiting || req->state == RequestState::eligible)
      r.entries.push_back({"formation", "waiting for compatible work under current policy"});
    else
      r.entries.push_back({"formation", "not currently in a batch"});
  } else {
    auto* b = impl_->find_batch(req->batch);
    if (b) {
      r.entries.push_back({"formation",
                           "joined batch=" + req->batch.string() +
                               " because CompatibilityKey matches (model/revision/adapter/phase/"
                               "shape/executor contract)"});
      r.entries.push_back({"batch", "batch state=" + std::string(to_string(b->state)) +
                                        " members=" + std::to_string(b->members.size())});
    }
  }
  for (const auto& kv : impl_->batches_) {
    const auto& b = kv.second;
    if (b.id == req->batch) continue;
    if (b.state == BatchState::forming || b.state == BatchState::sealed) {
      if (b.key != req->key) {
        CompatibilityDecision d = evaluate_compatibility(req->meta.descriptor, b.descriptor);
        r.entries.push_back({"formation", "did not join batch=" + b.id.string() +
                                              " because " + std::string(to_string(d))});
      }
    }
  }
  return r;
}

std::vector<RequestId> BatchFabric::waiting() const {
  std::lock_guard<std::mutex> g(impl_->mu_);
  std::vector<RequestId> out;
  for (const auto& kv : impl_->requests_) {
    auto st = kv.second.state;
    if (st == RequestState::eligible || st == RequestState::waiting || st == RequestState::reserved)
      out.push_back(kv.first);
  }
  return out;
}

std::vector<BatchId> BatchFabric::batches() const {
  std::lock_guard<std::mutex> g(impl_->mu_);
  std::vector<BatchId> out;
  for (const auto& kv : impl_->batches_) out.push_back(kv.first);
  return out;
}

std::vector<WorkerSnapshotEntry> BatchFabric::worker_list() const {
  std::lock_guard<std::mutex> g(impl_->mu_);
  std::vector<WorkerSnapshotEntry> out;
  for (const auto& kv : impl_->workers_) {
    WorkerSnapshotEntry e;
    e.worker = kv.first;
    e.boot = kv.second.boot;
    e.live = kv.second.live;
    e.active_batch_count = static_cast<std::uint32_t>(kv.second.running.size());
    e.completed_batches = 0;
    out.push_back(std::move(e));
  }
  return out;
}

std::size_t BatchFabric::count_in_state(RequestState s) const {
  std::lock_guard<std::mutex> g(impl_->mu_);
  std::size_t n = 0;
  for (const auto& kv : impl_->requests_)
    if (kv.second.state == s) n++;
  return n;
}

void BatchFabric::roll_epoch() {
  std::vector<BatchEvent> ev;
  {
    std::lock_guard<std::mutex> g(impl_->mu_);
    impl_->epoch_ = BatchEpoch(impl_->epoch_.value + 1);
    impl_->emit(ev, EventType::epoch_roll, RequestId(), BatchId(), AttemptId(), TenantId(),
                "epoch roll");
  }
  for (const auto& e : ev) impl_->events_->emit(e);
}

BatchEpoch BatchFabric::epoch() const {
  std::lock_guard<std::mutex> g(impl_->mu_);
  return impl_->epoch_;
}

Result<void> BatchFabric::persist_to(const std::string& path) const {
  std::lock_guard<std::mutex> g(impl_->mu_);
  PersistenceStore store;
  auto rt = impl_->build_persisted();
  return store.save(rt, path);
}

Result<void> BatchFabric::recover_from(const std::string& path) {
  std::lock_guard<std::mutex> g(impl_->mu_);
  PersistenceStore store;
  auto rt = store.load(path);
  if (!rt.ok()) return Result<void>::err(rt.error().code(), rt.error().message());
  impl_->load_persisted(rt.move_value());
  return Result<void>::success();
}

std::shared_ptr<Clock> BatchFabric::clock() const { return impl_->clock_; }
const BatchPolicy& BatchFabric::policy() const { return impl_->policy_; }

}  // namespace batch_fabric
