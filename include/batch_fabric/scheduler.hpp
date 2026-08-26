#pragma once
#include "batch_fabric/batch.hpp"
#include "batch_fabric/compatibility.hpp"
#include "batch_fabric/event.hpp"
#include "batch_fabric/executor.hpp"
#include "batch_fabric/explain.hpp"
#include "batch_fabric/formation.hpp"
#include "batch_fabric/fairness.hpp"
#include "batch_fabric/id.hpp"
#include "batch_fabric/policy.hpp"
#include "batch_fabric/request.hpp"
#include "batch_fabric/result.hpp"
#include "batch_fabric/snapshot.hpp"
#include "batch_fabric/stats.hpp"
#include <memory>
#include <string>
#include <vector>

namespace batch_fabric {

struct BatchFabricConfig {
  BatchPolicy policy;
  std::shared_ptr<Clock> clock;        // default RealMonotonicClock
  std::uint64_t seed = 0;
  TimePoint start_time_ns = 0;
  bool enable_persistence = false;
  std::string persistence_path;
  std::shared_ptr<EventSink> events;   // default NullEventSink
};

struct SubmitOutcome {
  RequestId request;
  AttemptId attempt;
  AdmissionDecision admission;
  std::string reason;
};

class BatchFabric {
 public:
  explicit BatchFabric(const BatchFabricConfig& config);
  ~BatchFabric();
  BatchFabric(const BatchFabric&) = delete;
  BatchFabric& operator=(const BatchFabric&) = delete;

  Result<void> register_worker(const WorkerId& worker, const WorkerBootId& boot,
                               const ExecutorCapability& cap);
  Result<SubmitOutcome> submit(const RequestMetadata& meta, TimePoint now = -1);
  Result<void> cancel(const RequestId& request, CancellationReason reason, TimePoint now = -1);
  Result<void> complete(const BatchCompletion& completion, TimePoint now = -1);
  Result<void> seal(const BatchId& batch, SealReason reason, TimePoint now = -1);
  Result<BatchId> split(const BatchId& batch, SplitReason reason, TimePoint now = -1);
  Result<BatchId> merge(const BatchId& a, const BatchId& b, MergeReason reason, TimePoint now = -1);
  Result<AttemptId> retry(const RequestId& request, TimePoint now = -1);
  FormationReport tick(TimePoint now = -1);
  Result<BatchId> dispatch_and_run(const BatchId& batch, IExecutor& executor, TimePoint now = -1);

  // Distributed dispatch: mark a sealed batch dispatched to a worker and
  // return the BatchExecution to transmit over the control plane. The
  // completion is later applied via complete().
  Result<BatchExecution> prepare_dispatch(const BatchId& batch, const WorkerId& worker,
                                          const WorkerBootId& boot, TimePoint now = -1);
  void expire(TimePoint now = -1);

  BatchStats stats() const;
  BatchSnapshot snapshot() const;
  ExplainResult explain(const RequestId& request) const;
  std::vector<RequestId> waiting() const;
  std::vector<BatchId> batches() const;
  std::vector<WorkerSnapshotEntry> worker_list() const;
  std::size_t count_in_state(RequestState s) const;

  void roll_epoch();
  BatchEpoch epoch() const;

  Result<void> persist_to(const std::string& path) const;
  Result<void> recover_from(const std::string& path);

  std::shared_ptr<Clock> clock() const;
  const BatchPolicy& policy() const;

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace batch_fabric