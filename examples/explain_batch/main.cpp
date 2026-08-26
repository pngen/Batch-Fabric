#include "BFExample.hpp"
#include <cstdio>
using namespace batch_fabric;
int main() {
  BatchFabric f(bfex::config(bfex::policy()));
  auto r1 = f.submit(bfex::meta("r", TenantId(1)));
  auto r2 = f.submit(bfex::meta("r2", TenantId(1)));
  if (!r1.ok() || !r2.ok()) return 1;
  auto ex = f.explain(r1.value().request);
  std::printf("explain_batch:\n%s\n", ex.summary.c_str());
  for (auto& e : ex.entries) std::printf("  [%s] %s\n", e.category.c_str(), e.detail.c_str());
  return 0;
}
