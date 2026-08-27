# Batch Fabric

**Batch Fabric** is an open-source, vendor-neutral C++20 runtime for dynamic
inference batching across heterogeneous AI serving infrastructure. It is a real
standalone systems runtime: it owns dynamic inference *batch formation* and
*batch lifecycle*, and it emits sealed (and later split / merged) batch
decisions with complete, explainable justification.

Batch Fabric is not a model server, an inference scheduler, a tokenizer, a model
loader, a KV-cache runtime, a tensor cache, a memory allocator, a CUDA kernel
library, or a plugin to vLLM / SGLang / TensorRT-LLM / llama.cpp. It is a clean,
vendor-neutral batching layer that can accept a stream of eligible inference
work from an external scheduling layer, form legal batches under explicit
policy, and hand them to external executors/workers through a real control
plane.

## The governing systems question

> Which inference requests should execute together, when should a batch seal,
> and when is waiting for a larger batch no longer worth the latency cost?

Batch Fabric answers this with a deterministic batch former driven by explicit
policy, compatibility, budget, fairness, latency-class wait budgets, and
deadlines.

## Ownership boundary

Batch Fabric **owns**:

- which requests are compatible enough to batch (compatibility is a first-class
  correctness system; a false-positive compatibility decision is a bug)
- which queued requests are grouped together
- how long requests may wait for a larger batch
- maximum request count / token / work / memory budget per batch
- model, revision, adapter, phase, shape, dtype, backend, device, execution-mode,
  sampling, and opaque-extension compatibility
- latency-class and deadline feasibility
- per-tenant fairness during batch formation
- batch sealing and batch membership authority
- cancellation before and after seal
- batch splitting and merging where semantically valid
- partial failure handling and exactly-once per-member completion
- retry semantics with fresh attempt identity
- batch admission pressure / backpressure decisions
- determinism (equal inputs produce equal outputs)
- statistics, events, snapshots, explanation
- persistence and recovery of scheduler-owned batching metadata

Batch Fabric does **not** become a full inference scheduler, model server,
tokenizer, model loader, KV-cache runtime, tensor cache, memory allocator, GPU
kernel library, artifact cache, generic task scheduler, or a Kubernetes / Ray /
Slurm wrapper.

## Request lifecycle

`Created → Eligible → Waiting → Reserved → Batched → Sealed → Dispatched →
Running → Completed`, with terminal/error states `Cancelled`, `Expired`,
`Rejected`, `Failed`. Illegal transitions fail deterministically. A terminal
request never silently re-enters an active batch. Retries create a new attempt
identity; cancellation races are resolved at explicit commit points.

## Batch lifecycle

`Forming → Sealed → Dispatched → Running → PartiallyCompleted → Completed`, with
terminal/error states `Cancelled`, `Failed`, `Superseded`. A batch accepts
members only while `Forming`; once `Sealed` membership is immutable except
through explicitly modeled post-seal cancellation semantics. Batch identity is
stable; batch *generation* changes when a logically new batch replaces or
supersedes an older one.

## Compatibility model

Compatibility is modeled with a typed `ModelDescriptor` and a canonical
`CompatibilityKey` (SHA-256 over a deterministic field ordering). Two requests
share a key iff they are intensionally compatible across model, revision,
adapter, tokenizer, phase, dtype, backend, execution mode, device capability,
shape, sampling semantics, and opaque extensions. `evaluate_compatibility`
returns a precise `CompatibilityDecision` (e.g. `incompatible_revision`) so
the runtime never infers compatibility from scattered string equality. A
`CompatibilityIndex` buckets waiting requests by key for O(1) membership
lookup.

## Dynamic formation

The batch former is a policy engine that continuously decides: *wait for more
compatible work*, *seal the current batch*, *split candidates*, *form another
batch*, or *reject/defer* incompatible or infeasible work. It enforces maximum
request count, total input/output tokens, aggregate work, memory (when supplied),
minimum preferred batch size, and per-latency-class wait budgets. Formation uses
compatibility buckets so it does not scan the whole queue for every submission.

## Max-wait / latency protection

Batch Fabric never waits indefinitely to fill a batch. Max-wait semantics use a
monotonic clock (injectable, so tests use simulated virtual time and never rely
on wall-clock sleeps). It supports global max-wait, per-latency-class max-wait,
deadline-derived earlier seal, policy-derived earlier seal, and immediate seal
where required. A low-volume latency-sensitive request is sealed at its
latency-class budget rather than waiting for throughput.

## Fairness

Formation cannot become a monopoly mechanism. A `FairnessController` tracks
per-tenant service accounting (weight-normalized), tenant weights, per-tenant
outstanding caps, per-batch contribution limits, starvation prevention, and
deterministic tie-breaking. A flooding tenant and a sparse tenant continue to
make progress, and configured weight ratios are approximately honored over a
sustained workload without sacrificing compatibility correctness.

## Prefill / decode

Distinct phase semantics are modeled. `PREFILL` and `DECODE` are never batched
together unless an explicit policy intentionally allows cross-phase batching and
the executor contract proves it valid. Phase-specific limits, wait budgets, and
compatibility rules apply.

## Splitting

Explicit batch splitting supports resource capacity shrink, token/work
violation, deadline pressure, worker batch-size constraints, cancellation,
partial readiness, policy, and executor rejection. Split preserves request
identity, attempt identity, compatibility constraints, fairness accounting, and
ordering, creating new batch identities/generations as needed without silently
mutating a sealed batch's provenance.

## Merging

Batches merge only when compatibility identities match, aggregate budgets
remain valid, deadline/latency constraints remain valid, phase is compatible,
executor constraints allow it, and policy permits it. Merged batches preserve
lineage back to source batches; Batch Fabric never merges merely because two
batches are small.

## Cancellation

Cancellation is real: before eligibility, while waiting, while in a forming
batch, after seal but before dispatch, after dispatch, and during running
execution where the executor supports it. Before seal a member is removed
cleanly; after seal membership history remains and cancellation becomes an
explicit member outcome. Cancellation does not leak capacity or accounting.

## Partial completion & retry

Different members may complete differently (`success`, `retryable_failure`,
`non_retryable_failure`, `cancelled`, `expired`, `stale`). Per-member results
reconcile into one batch result with exactly one authoritative terminal outcome
per request attempt; duplicate completion reports never produce duplicate
success. Retries are explicit and bounded; each retry carries a new `AttemptId`,
and cancelled work or work whose deadline is already impossible is never
retried.

## Distributed authority

Batch Fabric provides a real framed-TCP multiprocess control plane: a batch
coordinator process, two or more worker/executor processes, and a validation
driver. Frames use a 4-byte length prefix with a hard maximum; malformed,
zero-length, oversized, truncated, and invalid frames are rejected. Completion
authority binds to the exact active identities (`BatchEpoch`, `WorkerBootId`,
`BatchId`, `AttemptId`, `Generation`). A restarted worker with a new boot
identity and a rolled epoch cannot carry stale authority: stale-epoch,
stale-worker, and stale-attempt/generation completions are rejected
deterministically over the real protocol, and fresh work completes under the new
authority with zero leaked accounting. The atomic end-to-end authority
transition is exercised by the `test_multiprocess` harness, which launches real
OS processes, terminates and restarts a worker, rolls the epoch, and confirms
stale-authority rejection and fresh success in the same scenario.

## Persistence / recovery

Scheduler-owned batching metadata is persisted atomically (temp file, flush,
atomic replace with `FlushFileBuffers` on Windows) with a SHA-256 checksum.
Loading rejects bad magic, bad version, truncation, checksum mismatch,
impossible counts, invalid enum values, duplicate ids, inconsistent membership,
invalid lineage, and oversized declared arrays. Forming batches may be
reconstructed or discarded per documented policy; sealed-but-undispatched
batches are recoverable; ambiguous dispatched/running batches are never invented
as completed.

## Threading & concurrency

`BatchFabric` serializes all mutating operations on an internal mutex. Callbacks
(events) are emitted only after the internal lock is released to avoid
callbacks-under-lock deadlocks. The control plane uses one reader thread per
worker channel. A proactive self-deadlock audit covered lock ordering,
condition-variable wakeups, iterator/reference invalidation, use-after-move,
dangling async captures, duplicate membership/completion, and generation/attempt
ABA-like bugs.

## CLI

A real CLI (`batchfabric`) ships with `serve`, `worker`, `submit`, `cancel`,
`status`, `waiting`, `batches`, `workers`, `stats`, `snapshot`, `explain`,
`seal`, `split`, `merge`, `recover`, and `bench`. Every command calls real
runtime functionality. `explain` reports why two requests are compatible /
incompatible, why a request joined a batch, why a batch sealed / split / merged,
and why a candidate worker was eligible / ineligible.

## Examples

Runnable examples build and run under `examples/`: `basic_batching`,
`compatibility`, `latency_bounded_batch`, `tenant_fairness`, `prefill_decode`,
`split_merge`, `cancellation`, `partial_failure_retry`, `persistence_recovery`,
`explain_batch`.

## Tests

`tests/` contains unit tests, lifecycle tests, seeded randomized property tests
(6000+ invariant checks across multiple seeds), adversarial hardening tests, a
real multiprocess atomic authority-transition proof, and a benchmark smoke test.
No test uses a wall-clock timeout; timing behavior uses the simulated monotonic
clock.

## CUDA proof

A real CUDA executor (`CudaExecutor`) performs bounded kernel-launched workloads
on the installed NVIDIA GPU: real `cudaMalloc`, host/device transfer, kernel
launch, `cudaDeviceSynchronize`, per-member verification with a real reduction
kernel, and `cudaFree`. `test_cuda` proves batch formation, sealing, dispatch,
execution, per-member verification, and cleanup across multiple batch sizes and
both phases on an RTX 5090-class Blackwell device.

## Installation & downstream CMake usage

```
cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=Release
cmake --build build
cmake --install build --prefix <prefix>
```

An independent downstream project can then use:

```cmake
find_package(BatchFabric CONFIG REQUIRED)
target_link_libraries(myapp PRIVATE BatchFabric::BatchFabric)
```

The exported target is namespaced `BatchFabric::BatchFabric`. A downstream
consumer outside the repository tree builds, links, and runs a real API call
against the installed package.

## Benchmarks

`benchmarks/bf_bench` measures completed runtime work (not async submission
alone), including submission, compatibility-key computation, batch-size scaling,
queue-depth scaling, multithreaded formation, persistence serialization and
recovery, and snapshot cost, reporting ops/s and total times.

## License

Apache License 2.0. Copyright 2026 Summon Software Labs. No telemetry transmission.
