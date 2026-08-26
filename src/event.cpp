#include "batch_fabric/event.hpp"

namespace batch_fabric {

std::string_view to_string(EventType t) noexcept {
  switch (t) {
    case EventType::submit: return "submit";
    case EventType::admit: return "admit";
    case EventType::form: return "form";
    case EventType::seal: return "seal";
    case EventType::dispatch: return "dispatch";
    case EventType::complete: return "complete";
    case EventType::cancel: return "cancel";
    case EventType::split: return "split";
    case EventType::merge: return "merge";
    case EventType::retry: return "retry";
    case EventType::expire: return "expire";
    case EventType::epoch_roll: return "epoch_roll";
    case EventType::worker_up: return "worker_up";
    case EventType::worker_down: return "worker_down";
    case EventType::stale_reject: return "stale_reject";
  }
  return "unknown";
}

std::string format_event(const BatchEvent& e) {
  std::string out;
  out += std::to_string(e.time);
  out += " ";
  out += std::string(to_string(e.type));
  if (!e.batch.is_null()) {
    out += " batch=";
    out += e.batch.string();
  }
  if (!e.request.is_null()) {
    out += " req=";
    out += e.request.string();
  }
  if (!e.tenant.is_null()) {
    out += " tenant=";
    out += e.tenant.string();
  }
  if (!e.detail.empty()) {
    out += " ";
    out += e.detail;
  }
  return out;
}

void VectorEventSink::emit(const BatchEvent& e) {
  events_.push_back(e);
  if (events_.size() > capacity_) events_.erase(events_.begin(), events_.begin() + (events_.size() - capacity_));
}

}  // namespace batch_fabric
