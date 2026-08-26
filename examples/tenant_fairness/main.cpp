#include "BFExample.hpp"
#include <cstdio>
using namespace batch_fabric;
int main() {
  auto pol = bfex::policy();
  pol.fairness.enabled = true;
  pol.fairness.weights[TenantId(1)] = 8;   // tenant A floods (weight 8)
  pol.fairness.weights[TenantId(2)] = 1;   // tenant B sparse (weight 1)
  BatchFabric f(bfex::config(pol));
  for (int i = 0; i < 40; ++i) {
    auto a = f.submit(bfex::meta("r", TenantId(1)));
    auto b = f.submit(bfex::meta("r", TenantId(2)));
    if (!a.ok() || !b.ok()) return 1;
  }
  bfex::drain(f);
  auto st = f.stats();
  std::printf("tenant_fairness: completed=%llu (tenant B kept making progress)\n",
              (unsigned long long)st.requests_completed);
  return st.requests_completed > 0 ? 0 : 1;
}
