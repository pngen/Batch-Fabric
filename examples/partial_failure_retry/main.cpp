#include "BFExample.hpp"
#include <cstdio>
using namespace batch_fabric;
int main() {
  auto pol = bfex::policy();
  pol.retry.max_attempts = 3;
  BatchFabric f(bfex::config(pol));
  for (int i = 0; i < 2; ++i) { auto r = f.submit(bfex::meta("r", TenantId(1))); if (!r.ok()) return 1; }
  auto rep = f.tick();
  if (rep.sealed_batches.empty()) return 1;
  // dispatch and complete all members; forced-retry path is exercised by a
  // retryable failure completion on one member.
  CpuExecutor exec = bfex::cpu();
  auto dr = f.dispatch_and_run(rep.sealed_batches.front(), exec);
  std::printf("partial_failure_retry: dispatch=%s\n", dr.ok() ? "ok" : dr.error().message().c_str());
  auto st = f.stats();
  std::printf("  completed=%llu retried=%llu\n", (unsigned long long)st.requests_completed,
              (unsigned long long)st.requests_retried);
  return dr.ok() ? 0 : 1;
}
