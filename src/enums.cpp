#include "batch_fabric/enums.hpp"
#include <string>

namespace batch_fabric {

std::optional<Phase> parse_phase(std::string_view s) noexcept {
  if (s == "prefill") return Phase::prefill;
  if (s == "decode") return Phase::decode;
  return std::nullopt;
}

std::optional<LatencyClass> parse_latency_class(std::string_view s) noexcept {
  if (s == "batch") return LatencyClass::batch;
  if (s == "low_latency") return LatencyClass::low_latency;
  if (s == "interactive") return LatencyClass::interactive;
  if (s == "deadline_bounded") return LatencyClass::deadline_bounded;
  if (s == "best_effort") return LatencyClass::best_effort;
  return std::nullopt;
}

std::optional<PriorityClass> parse_priority_class(std::string_view s) noexcept {
  if (s == "high") return PriorityClass::high;
  if (s == "normal") return PriorityClass::normal;
  if (s == "low") return PriorityClass::low;
  if (s == "background") return PriorityClass::background;
  return std::nullopt;
}

}  // namespace batch_fabric
