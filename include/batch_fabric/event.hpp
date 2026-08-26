#pragma once
#include "batch_fabric/clock.hpp"
#include "batch_fabric/enums.hpp"
#include "batch_fabric/id.hpp"
#include <cstdint>
#include <string>
#include <vector>

namespace batch_fabric {

enum class EventType : std::uint8_t {
  submit = 0,
  admit = 1,
  form = 2,
  seal = 3,
  dispatch = 4,
  complete = 5,
  cancel = 6,
  split = 7,
  merge = 8,
  retry = 9,
  expire = 10,
  epoch_roll = 11,
  worker_up = 12,
  worker_down = 13,
  stale_reject = 14
};

std::string_view to_string(EventType t) noexcept;

struct BatchEvent {
  BatchEpoch epoch;
  TimePoint time = 0;
  EventType type = EventType::submit;
  RequestId request;
  BatchId batch;
  AttemptId attempt;
  TenantId tenant;
  std::string detail;
};

std::string format_event(const BatchEvent& e);

// A sink for events. All events are written locally or discarded; nothing is
// transmitted anywhere.
class EventSink {
 public:
  virtual ~EventSink() = default;
  virtual void emit(const BatchEvent& e) = 0;
};

class NullEventSink final : public EventSink {
 public:
  void emit(const BatchEvent&) override {}
};

// In-memory ring of recent events, used by the CLI and tests.
class VectorEventSink final : public EventSink {
 public:
  void emit(const BatchEvent& e) override;
  const std::vector<BatchEvent>& events() const { return events_; }
  void clear() { events_.clear(); }
  void set_capacity(std::size_t n) { capacity_ = n; }

 private:
  std::vector<BatchEvent> events_;
  std::size_t capacity_ = 4096;
};

}  // namespace batch_fabric