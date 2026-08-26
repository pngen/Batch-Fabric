#pragma once
// Batch Fabric — open-source, vendor-neutral runtime for dynamic inference
// batching across heterogeneous AI serving infrastructure.
//
// Umbrella header. Include this to use the complete public API:
//   batch_fabric::BatchFabric, batch_fabric::BatchPolicy,
//   batch_fabric::CpuExecutor, and the strong typed domain model.

#include "batch_fabric/batch.hpp"
#include "batch_fabric/clock.hpp"
#include "batch_fabric/compatibility.hpp"
#include "batch_fabric/enums.hpp"
#include "batch_fabric/error.hpp"
#include "batch_fabric/event.hpp"
#include "batch_fabric/executor.hpp"
#include "batch_fabric/explain.hpp"
#include "batch_fabric/fairness.hpp"
#include "batch_fabric/formation.hpp"
#include "batch_fabric/hash.hpp"
#include "batch_fabric/id.hpp"
#include "batch_fabric/identity.hpp"
#include "batch_fabric/io.hpp"
#include "batch_fabric/persistence.hpp"
#include "batch_fabric/policy.hpp"
#include "batch_fabric/request.hpp"
#include "batch_fabric/result.hpp"
#include "batch_fabric/scheduler.hpp"
#include "batch_fabric/snapshot.hpp"
#include "batch_fabric/stats.hpp"
#include "batch_fabric/transport.hpp"
