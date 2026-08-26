#pragma once
#include <cstdint>
#include <functional>
#include <string>
#include <string_view>

namespace batch_fabric {

// Strong fixed-width identifiers. Each is a distinct type so a RequestId
// can never be passed where a BatchId or TenantId is expected. All are
// serializable, comparable, hashable, and deterministic across processes.

class RequestId {
 public:
  RequestId() = default;
  explicit RequestId(std::uint64_t v) noexcept : value(v) {}
  std::uint64_t value = 0;
  friend bool operator==(const RequestId& a, const RequestId& b) noexcept { return a.value == b.value; }
  friend bool operator!=(const RequestId& a, const RequestId& b) noexcept { return !(a == b); }
  friend bool operator<(const RequestId& a, const RequestId& b) noexcept { return a.value < b.value; }
  friend bool operator>(const RequestId& a, const RequestId& b) noexcept { return a.value > b.value; }
  friend bool operator<=(const RequestId& a, const RequestId& b) noexcept { return a.value <= b.value; }
  friend bool operator>=(const RequestId& a, const RequestId& b) noexcept { return a.value >= b.value; }
  bool is_null() const noexcept { return value == 0; }
  explicit operator bool() const noexcept { return value != 0; }
  std::string string() const { return std::string("RequestId") + ":" + std::to_string(value); }
};

class BatchId {
 public:
  BatchId() = default;
  explicit BatchId(std::uint64_t v) noexcept : value(v) {}
  std::uint64_t value = 0;
  friend bool operator==(const BatchId& a, const BatchId& b) noexcept { return a.value == b.value; }
  friend bool operator!=(const BatchId& a, const BatchId& b) noexcept { return !(a == b); }
  friend bool operator<(const BatchId& a, const BatchId& b) noexcept { return a.value < b.value; }
  friend bool operator>(const BatchId& a, const BatchId& b) noexcept { return a.value > b.value; }
  friend bool operator<=(const BatchId& a, const BatchId& b) noexcept { return a.value <= b.value; }
  friend bool operator>=(const BatchId& a, const BatchId& b) noexcept { return a.value >= b.value; }
  bool is_null() const noexcept { return value == 0; }
  explicit operator bool() const noexcept { return value != 0; }
  std::string string() const { return std::string("BatchId") + ":" + std::to_string(value); }
};

class TenantId {
 public:
  TenantId() = default;
  explicit TenantId(std::uint64_t v) noexcept : value(v) {}
  std::uint64_t value = 0;
  friend bool operator==(const TenantId& a, const TenantId& b) noexcept { return a.value == b.value; }
  friend bool operator!=(const TenantId& a, const TenantId& b) noexcept { return !(a == b); }
  friend bool operator<(const TenantId& a, const TenantId& b) noexcept { return a.value < b.value; }
  friend bool operator>(const TenantId& a, const TenantId& b) noexcept { return a.value > b.value; }
  friend bool operator<=(const TenantId& a, const TenantId& b) noexcept { return a.value <= b.value; }
  friend bool operator>=(const TenantId& a, const TenantId& b) noexcept { return a.value >= b.value; }
  bool is_null() const noexcept { return value == 0; }
  explicit operator bool() const noexcept { return value != 0; }
  std::string string() const { return std::string("TenantId") + ":" + std::to_string(value); }
};

class SessionId {
 public:
  SessionId() = default;
  explicit SessionId(std::uint64_t v) noexcept : value(v) {}
  std::uint64_t value = 0;
  friend bool operator==(const SessionId& a, const SessionId& b) noexcept { return a.value == b.value; }
  friend bool operator!=(const SessionId& a, const SessionId& b) noexcept { return !(a == b); }
  friend bool operator<(const SessionId& a, const SessionId& b) noexcept { return a.value < b.value; }
  friend bool operator>(const SessionId& a, const SessionId& b) noexcept { return a.value > b.value; }
  friend bool operator<=(const SessionId& a, const SessionId& b) noexcept { return a.value <= b.value; }
  friend bool operator>=(const SessionId& a, const SessionId& b) noexcept { return a.value >= b.value; }
  bool is_null() const noexcept { return value == 0; }
  explicit operator bool() const noexcept { return value != 0; }
  std::string string() const { return std::string("SessionId") + ":" + std::to_string(value); }
};

class SequenceId {
 public:
  SequenceId() = default;
  explicit SequenceId(std::uint64_t v) noexcept : value(v) {}
  std::uint64_t value = 0;
  friend bool operator==(const SequenceId& a, const SequenceId& b) noexcept { return a.value == b.value; }
  friend bool operator!=(const SequenceId& a, const SequenceId& b) noexcept { return !(a == b); }
  friend bool operator<(const SequenceId& a, const SequenceId& b) noexcept { return a.value < b.value; }
  friend bool operator>(const SequenceId& a, const SequenceId& b) noexcept { return a.value > b.value; }
  friend bool operator<=(const SequenceId& a, const SequenceId& b) noexcept { return a.value <= b.value; }
  friend bool operator>=(const SequenceId& a, const SequenceId& b) noexcept { return a.value >= b.value; }
  bool is_null() const noexcept { return value == 0; }
  explicit operator bool() const noexcept { return value != 0; }
  std::string string() const { return std::string("SequenceId") + ":" + std::to_string(value); }
};

class AttemptId {
 public:
  AttemptId() = default;
  explicit AttemptId(std::uint64_t v) noexcept : value(v) {}
  std::uint64_t value = 0;
  friend bool operator==(const AttemptId& a, const AttemptId& b) noexcept { return a.value == b.value; }
  friend bool operator!=(const AttemptId& a, const AttemptId& b) noexcept { return !(a == b); }
  friend bool operator<(const AttemptId& a, const AttemptId& b) noexcept { return a.value < b.value; }
  friend bool operator>(const AttemptId& a, const AttemptId& b) noexcept { return a.value > b.value; }
  friend bool operator<=(const AttemptId& a, const AttemptId& b) noexcept { return a.value <= b.value; }
  friend bool operator>=(const AttemptId& a, const AttemptId& b) noexcept { return a.value >= b.value; }
  bool is_null() const noexcept { return value == 0; }
  explicit operator bool() const noexcept { return value != 0; }
  std::string string() const { return std::string("AttemptId") + ":" + std::to_string(value); }
};

class Generation {
 public:
  Generation() = default;
  explicit Generation(std::uint64_t v) noexcept : value(v) {}
  std::uint64_t value = 0;
  friend bool operator==(const Generation& a, const Generation& b) noexcept { return a.value == b.value; }
  friend bool operator!=(const Generation& a, const Generation& b) noexcept { return !(a == b); }
  friend bool operator<(const Generation& a, const Generation& b) noexcept { return a.value < b.value; }
  friend bool operator>(const Generation& a, const Generation& b) noexcept { return a.value > b.value; }
  friend bool operator<=(const Generation& a, const Generation& b) noexcept { return a.value <= b.value; }
  friend bool operator>=(const Generation& a, const Generation& b) noexcept { return a.value >= b.value; }
  bool is_null() const noexcept { return value == 0; }
  explicit operator bool() const noexcept { return value != 0; }
  std::string string() const { return std::string("Generation") + ":" + std::to_string(value); }
};

class BatchEpoch {
 public:
  BatchEpoch() = default;
  explicit BatchEpoch(std::uint64_t v) noexcept : value(v) {}
  std::uint64_t value = 0;
  friend bool operator==(const BatchEpoch& a, const BatchEpoch& b) noexcept { return a.value == b.value; }
  friend bool operator!=(const BatchEpoch& a, const BatchEpoch& b) noexcept { return !(a == b); }
  friend bool operator<(const BatchEpoch& a, const BatchEpoch& b) noexcept { return a.value < b.value; }
  friend bool operator>(const BatchEpoch& a, const BatchEpoch& b) noexcept { return a.value > b.value; }
  friend bool operator<=(const BatchEpoch& a, const BatchEpoch& b) noexcept { return a.value <= b.value; }
  friend bool operator>=(const BatchEpoch& a, const BatchEpoch& b) noexcept { return a.value >= b.value; }
  bool is_null() const noexcept { return value == 0; }
  explicit operator bool() const noexcept { return value != 0; }
  std::string string() const { return std::string("BatchEpoch") + ":" + std::to_string(value); }
};

class WorkerBootId {
 public:
  WorkerBootId() = default;
  explicit WorkerBootId(std::uint64_t v) noexcept : value(v) {}
  std::uint64_t value = 0;
  friend bool operator==(const WorkerBootId& a, const WorkerBootId& b) noexcept { return a.value == b.value; }
  friend bool operator!=(const WorkerBootId& a, const WorkerBootId& b) noexcept { return !(a == b); }
  friend bool operator<(const WorkerBootId& a, const WorkerBootId& b) noexcept { return a.value < b.value; }
  friend bool operator>(const WorkerBootId& a, const WorkerBootId& b) noexcept { return a.value > b.value; }
  friend bool operator<=(const WorkerBootId& a, const WorkerBootId& b) noexcept { return a.value <= b.value; }
  friend bool operator>=(const WorkerBootId& a, const WorkerBootId& b) noexcept { return a.value >= b.value; }
  bool is_null() const noexcept { return value == 0; }
  explicit operator bool() const noexcept { return value != 0; }
  std::string string() const { return std::string("WorkerBootId") + ":" + std::to_string(value); }
};

class WorkerId {
 public:
  WorkerId() = default;
  explicit WorkerId(std::uint64_t v) noexcept : value(v) {}
  std::uint64_t value = 0;
  friend bool operator==(const WorkerId& a, const WorkerId& b) noexcept { return a.value == b.value; }
  friend bool operator!=(const WorkerId& a, const WorkerId& b) noexcept { return !(a == b); }
  friend bool operator<(const WorkerId& a, const WorkerId& b) noexcept { return a.value < b.value; }
  friend bool operator>(const WorkerId& a, const WorkerId& b) noexcept { return a.value > b.value; }
  friend bool operator<=(const WorkerId& a, const WorkerId& b) noexcept { return a.value <= b.value; }
  friend bool operator>=(const WorkerId& a, const WorkerId& b) noexcept { return a.value >= b.value; }
  bool is_null() const noexcept { return value == 0; }
  explicit operator bool() const noexcept { return value != 0; }
  std::string string() const { return std::string("WorkerId") + ":" + std::to_string(value); }
};

// Strong string-based identities (model / revision / adapter /
// tokenizer). These wrap canonical opaque strings but remain distinct,
// comparable, hashable types so the codebase never falls back to raw
// string equality.

class ModelIdentity {
 public:
  ModelIdentity() = default;
  explicit ModelIdentity(std::string v) : value(std::move(v)) {}
  std::string value;
  bool empty() const noexcept { return value.empty(); }
  const std::string& str() const noexcept { return value; }
  friend bool operator==(const ModelIdentity& a, const ModelIdentity& b) noexcept { return a.value == b.value; }
  friend bool operator!=(const ModelIdentity& a, const ModelIdentity& b) noexcept { return !(a == b); }
  friend bool operator<(const ModelIdentity& a, const ModelIdentity& b) noexcept { return a.value < b.value; }
  std::string string() const { return std::string("ModelIdentity") + ":" + value; }
};

class ModelRevision {
 public:
  ModelRevision() = default;
  explicit ModelRevision(std::string v) : value(std::move(v)) {}
  std::string value;
  bool empty() const noexcept { return value.empty(); }
  const std::string& str() const noexcept { return value; }
  friend bool operator==(const ModelRevision& a, const ModelRevision& b) noexcept { return a.value == b.value; }
  friend bool operator!=(const ModelRevision& a, const ModelRevision& b) noexcept { return !(a == b); }
  friend bool operator<(const ModelRevision& a, const ModelRevision& b) noexcept { return a.value < b.value; }
  std::string string() const { return std::string("ModelRevision") + ":" + value; }
};

class AdapterIdentity {
 public:
  AdapterIdentity() = default;
  explicit AdapterIdentity(std::string v) : value(std::move(v)) {}
  std::string value;
  bool empty() const noexcept { return value.empty(); }
  const std::string& str() const noexcept { return value; }
  friend bool operator==(const AdapterIdentity& a, const AdapterIdentity& b) noexcept { return a.value == b.value; }
  friend bool operator!=(const AdapterIdentity& a, const AdapterIdentity& b) noexcept { return !(a == b); }
  friend bool operator<(const AdapterIdentity& a, const AdapterIdentity& b) noexcept { return a.value < b.value; }
  std::string string() const { return std::string("AdapterIdentity") + ":" + value; }
};

class TokenizerIdentity {
 public:
  TokenizerIdentity() = default;
  explicit TokenizerIdentity(std::string v) : value(std::move(v)) {}
  std::string value;
  bool empty() const noexcept { return value.empty(); }
  const std::string& str() const noexcept { return value; }
  friend bool operator==(const TokenizerIdentity& a, const TokenizerIdentity& b) noexcept { return a.value == b.value; }
  friend bool operator!=(const TokenizerIdentity& a, const TokenizerIdentity& b) noexcept { return !(a == b); }
  friend bool operator<(const TokenizerIdentity& a, const TokenizerIdentity& b) noexcept { return a.value < b.value; }
  std::string string() const { return std::string("TokenizerIdentity") + ":" + value; }
};

}  // namespace batch_fabric

// std::hash specializations (in the global std namespace).
namespace std {
template <> struct hash<batch_fabric::RequestId> {
  std::size_t operator()(const batch_fabric::RequestId& id) const noexcept { return std::hash<std::uint64_t>{}(id.value); }
};
}
namespace std {
template <> struct hash<batch_fabric::BatchId> {
  std::size_t operator()(const batch_fabric::BatchId& id) const noexcept { return std::hash<std::uint64_t>{}(id.value); }
};
}
namespace std {
template <> struct hash<batch_fabric::TenantId> {
  std::size_t operator()(const batch_fabric::TenantId& id) const noexcept { return std::hash<std::uint64_t>{}(id.value); }
};
}
namespace std {
template <> struct hash<batch_fabric::SessionId> {
  std::size_t operator()(const batch_fabric::SessionId& id) const noexcept { return std::hash<std::uint64_t>{}(id.value); }
};
}
namespace std {
template <> struct hash<batch_fabric::SequenceId> {
  std::size_t operator()(const batch_fabric::SequenceId& id) const noexcept { return std::hash<std::uint64_t>{}(id.value); }
};
}
namespace std {
template <> struct hash<batch_fabric::AttemptId> {
  std::size_t operator()(const batch_fabric::AttemptId& id) const noexcept { return std::hash<std::uint64_t>{}(id.value); }
};
}
namespace std {
template <> struct hash<batch_fabric::Generation> {
  std::size_t operator()(const batch_fabric::Generation& id) const noexcept { return std::hash<std::uint64_t>{}(id.value); }
};
}
namespace std {
template <> struct hash<batch_fabric::BatchEpoch> {
  std::size_t operator()(const batch_fabric::BatchEpoch& id) const noexcept { return std::hash<std::uint64_t>{}(id.value); }
};
}
namespace std {
template <> struct hash<batch_fabric::WorkerBootId> {
  std::size_t operator()(const batch_fabric::WorkerBootId& id) const noexcept { return std::hash<std::uint64_t>{}(id.value); }
};
}
namespace std {
template <> struct hash<batch_fabric::WorkerId> {
  std::size_t operator()(const batch_fabric::WorkerId& id) const noexcept { return std::hash<std::uint64_t>{}(id.value); }
};
}
namespace std {
template <> struct hash<batch_fabric::ModelIdentity> {
  std::size_t operator()(const batch_fabric::ModelIdentity& id) const noexcept { return std::hash<std::string>{}(id.value); }
};
}
namespace std {
template <> struct hash<batch_fabric::ModelRevision> {
  std::size_t operator()(const batch_fabric::ModelRevision& id) const noexcept { return std::hash<std::string>{}(id.value); }
};
}
namespace std {
template <> struct hash<batch_fabric::AdapterIdentity> {
  std::size_t operator()(const batch_fabric::AdapterIdentity& id) const noexcept { return std::hash<std::string>{}(id.value); }
};
}
namespace std {
template <> struct hash<batch_fabric::TokenizerIdentity> {
  std::size_t operator()(const batch_fabric::TokenizerIdentity& id) const noexcept { return std::hash<std::string>{}(id.value); }
};
}
