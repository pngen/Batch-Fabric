#include "BFExample.hpp"
#include <cstdio>
using namespace batch_fabric;
int main() {
  BatchFabric f(bfex::config(bfex::policy()));
  for (int i = 0; i < 6; ++i) {
    auto r = f.submit(bfex::meta("r", TenantId(1)));
    if (!r.ok()) return 1;
  }
  bfex::drain(f);
  auto st = f.stats();
  std::printf("basic_batching: submitted=%llu completed=%llu waiting=%llu\n",
              (unsigned long long)st.submitted, (unsigned long long)st.requests_completed,
              (unsigned long long)st.waiting_now);
  std::printf("compatible requests formed into batches; all completed.\n");
  return st.requests_completed == 6 ? 0 : 1;
}
