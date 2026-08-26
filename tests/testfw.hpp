#pragma once
#include <cstdio>
#include <cstdlib>
#include <atomic>

namespace testfw {
inline std::atomic<int> g_failures{0};
inline std::atomic<int> g_checks{0};
inline std::atomic<int> g_run{0};

inline void report_fail(const char* file, int line, const char* msg) {
  std::printf("  [FAIL] %s:%d: %s\n", file, line, msg);
  g_failures.fetch_add(1);
}
inline int exit_code() {
  std::printf("testfw: checks=%d failures=%d\n", g_checks.load(), g_failures.load());
  return g_failures.load() == 0 ? 0 : 1;
}
}  // namespace testfw

#define CHECK(cond)                                                             \
  do {                                                                          \
    testfw::g_checks.fetch_add(1);                                              \
    if (!(cond)) testfw::report_fail(__FILE__, __LINE__, #cond);                 \
  } while (0)

#define CHECK_EQ(a, b)                                                          \
  do {                                                                          \
    testfw::g_checks.fetch_add(1);                                              \
    if (!((a) == (b))) testfw::report_fail(__FILE__, __LINE__, #a " == " #b);   \
  } while (0)

#define REQUIRE(cond)                                                           \
  do {                                                                          \
    testfw::g_checks.fetch_add(1);                                              \
    if (!(cond)) {                                                              \
      testfw::report_fail(__FILE__, __LINE__, #cond);                           \
      return testfw::exit_code();                                               \
    }                                                                           \
  } while (0)