#include "BFExample.hpp"
#include <cstdio>
using namespace batch_fabric;
int main() {
  BatchFabric f(bfex::config(bfex::policy()));
  auto r = f.submit(bfex::meta("r", TenantId(1)));
  if (!r.ok()) return 1;
  // cancel before seal
  auto c = f.cancel(r.value().request, CancellationReason::client_request);
  std::printf("cancellation: before seal -> %s\n", c.ok() ? "ok" : c.error().message().c_str());
  std::printf("  cancelled count=%zu\n", f.count_in_state(RequestState::cancelled));
  return c.ok() ? 0 : 1;
}
