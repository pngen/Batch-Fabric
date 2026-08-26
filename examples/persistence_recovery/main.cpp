#include "BFExample.hpp"
#include <cstdio>
using namespace batch_fabric;
int main() {
  const std::string path = "_bf_ex_persist.bfstate";
  {
    BatchFabric f(bfex::config(bfex::policy()));
    for (int i = 0; i < 3; ++i) { auto r = f.submit(bfex::meta("r", TenantId(1))); if (!r.ok()) return 1; }
    auto rep = f.tick();
    if (rep.sealed_batches.empty()) return 1;
    auto p = f.persist_to(path);
    std::printf("persistence_recovery: persisted=%s\n", p.ok() ? "ok" : p.error().message().c_str());
  }
  BatchFabric f(bfex::config(bfex::policy()));
  auto r = f.recover_from(path);
  std::printf("  recovered=%s requests=%zu\n", r.ok() ? "ok" : r.error().message().c_str(),
              f.snapshot().requests.size());
  std::remove(path.c_str());
  return r.ok() ? 0 : 1;
}
