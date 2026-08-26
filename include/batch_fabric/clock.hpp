#pragma once
#include <atomic>
#include <chrono>
#include <cstdint>

namespace batch_fabric {

// Time is expressed in monotonic nanoseconds. A Clock is injectable so tests
// can use simulated virtual time and never rely on wall-clock sleeps.
using TimePoint = std::int64_t;  // monotonic nanoseconds

class Clock {
 public:
  virtual ~Clock() = default;
  virtual TimePoint now() const noexcept = 0;
  virtual bool is_simulated() const noexcept = 0;
};

class RealMonotonicClock final : public Clock {
 public:
  TimePoint now() const noexcept override {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
  }
  bool is_simulated() const noexcept override { return false; }
};

// Deterministic, manually controlled monotonic clock for tests.
class SimulatedClock final : public Clock {
 public:
  explicit SimulatedClock(TimePoint start = 0) noexcept : now_(start) {}
  TimePoint now() const noexcept override { return now_.load(std::memory_order_relaxed); }
  bool is_simulated() const noexcept override { return true; }
  void advance(TimePoint delta) noexcept { now_.fetch_add(delta, std::memory_order_relaxed); }
  void advance_ms(std::int64_t ms) noexcept { now_.fetch_add(ms * 1000000LL, std::memory_order_relaxed); }
  void set(TimePoint absolute) noexcept { now_.store(absolute, std::memory_order_relaxed); }
  void reset(TimePoint start = 0) noexcept { now_.store(start, std::memory_order_relaxed); }

 private:
  std::atomic<TimePoint> now_;
};

}  // namespace batch_fabric
