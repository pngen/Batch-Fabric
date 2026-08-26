#pragma once
#include "batch_fabric/enums.hpp"
#include "batch_fabric/hash.hpp"
#include "batch_fabric/id.hpp"
#include <array>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

namespace batch_fabric {

enum class Dtype : std::uint8_t {
  float16 = 0,
  bfloat16 = 1,
  float32 = 2,
  int8 = 3,
  int4 = 4,
  unknown = 255
};

constexpr std::string_view to_string(Dtype d) noexcept {
  switch (d) {
    case Dtype::float16: return "float16";
    case Dtype::bfloat16: return "bfloat16";
    case Dtype::float32: return "float32";
    case Dtype::int8: return "int8";
    case Dtype::int4: return "int4";
    case Dtype::unknown: return "unknown";
  }
  return "unknown";
}

enum class Backend : std::uint8_t {
  cpu = 0,
  cuda = 1,
  rocm = 2,
  mps = 3,
  trt = 4,
  unknown = 255
};

constexpr std::string_view to_string(Backend b) noexcept {
  switch (b) {
    case Backend::cpu: return "cpu";
    case Backend::cuda: return "cuda";
    case Backend::rocm: return "rocm";
    case Backend::mps: return "mps";
    case Backend::trt: return "trt";
    case Backend::unknown: return "unknown";
  }
  return "unknown";
}

enum class ExecutionMode : std::uint8_t {
  eager = 0,
  graph = 1,
  compiled = 2,
  unknown = 255
};

constexpr std::string_view to_string(ExecutionMode m) noexcept {
  switch (m) {
    case ExecutionMode::eager: return "eager";
    case ExecutionMode::graph: return "graph";
    case ExecutionMode::compiled: return "compiled";
    case ExecutionMode::unknown: return "unknown";
  }
  return "unknown";
}

// Required device capability. A candidate capability "satisfies" a requirement
// when its backend matches (or is a superset) and its numeric capability is
// >= the requirement and its memory headroom is sufficient.
struct DeviceCapability {
  Backend backend = Backend::cpu;
  std::uint32_t compute_major = 0;
  std::uint32_t compute_minor = 0;
  std::uint64_t min_memory_bytes = 0;

  bool satisfies(const DeviceCapability& requirement) const noexcept {
    if (req_mismatch(requirement)) return false;
    if (compute_major < requirement.compute_major) return false;
    if (compute_major == requirement.compute_major && compute_minor < requirement.compute_minor)
      return false;
    return min_memory_bytes >= requirement.min_memory_bytes;
  }

  bool req_mismatch(const DeviceCapability&) const noexcept { return false; }

  friend bool operator==(const DeviceCapability& a, const DeviceCapability& b) noexcept {
    return a.backend == b.backend && a.compute_major == b.compute_major &&
           a.compute_minor == b.compute_minor && a.min_memory_bytes == b.min_memory_bytes;
  }
};

// Canonical shape/workload bucket. Strong string type; never compared as a raw
// string scattered through the codebase.
class ShapeKey {
 public:
  ShapeKey() = default;
  explicit ShapeKey(std::string v) : value(std::move(v)) {}
  std::string value;
  bool empty() const noexcept { return value.empty(); }
  const std::string& str() const noexcept { return value; }
  friend bool operator==(const ShapeKey& a, const ShapeKey& b) noexcept { return a.value == b.value; }
  friend bool operator!=(const ShapeKey& a, const ShapeKey& b) noexcept { return !(a == b); }
  friend bool operator<(const ShapeKey& a, const ShapeKey& b) noexcept { return a.value < b.value; }
  std::string string() const { return "ShapeKey:" + value; }
};

// ModelDescriptor is the full compatibility tuple describing an inference
// request's execution contract. Compatibility is evaluated as a first-class
// correctness system; a false-positive compatibility decision is a bug.
struct ModelDescriptor {
  ModelIdentity model;
  ModelRevision revision;
  AdapterIdentity adapter;      // empty means the base model
  TokenizerIdentity tokenizer;  // empty when not supplied by the caller
  Phase phase = Phase::prefill;
  Dtype dtype = Dtype::unknown;
  Backend backend = Backend::cpu;
  ExecutionMode execution_mode = ExecutionMode::eager;
  DeviceCapability device;
  ShapeKey shape;
  std::string sampling_semantics;  // signature of sampling/decoding semantics
  std::vector<std::string> extensions;  // sorted opaque compatibility extensions

  friend bool operator==(const ModelDescriptor& a, const ModelDescriptor& b) noexcept;
  friend bool operator!=(const ModelDescriptor& a, const ModelDescriptor& b) noexcept {
    return !(a == b);
  }
};

// Canonical deterministically-ordered serialization of a ModelDescriptor. Used
// exclusively to build the canonical CompatibilityKey digest.
std::string canonical_string(const ModelDescriptor& d);

// Human-readable deterministic description of a ModelDescriptor, used by
// explain and diagnostics. Same field order as canonical_string.
std::string describe(const ModelDescriptor& d);

// CompatibilityKey is the canonical, deterministic digest of a compatibility
// tuple. Two requests share a key iff they are intensionally compatible with
// respect to all modeled dimensions. It is SHA-256 over canonical_string.
class CompatibilityKey {
 public:
  CompatibilityKey() = default;
  explicit CompatibilityKey(Digest digest) : digest_(digest) {}
  static CompatibilityKey build(const ModelDescriptor& d);
  const Digest& digest() const noexcept { return digest_; }
  std::string hex() const;
  friend bool operator==(const CompatibilityKey& a, const CompatibilityKey& b) noexcept {
    return a.digest_ == b.digest_;
  }
  friend bool operator!=(const CompatibilityKey& a, const CompatibilityKey& b) noexcept {
    return !(a == b);
  }
  bool is_null() const noexcept {
    return digest_[0] == 0 && digest_[2] == 0;  // cheap non-colliding null check
  }

 private:
  Digest digest_{};
};

// Set a canonical extension signature; extensions are sorted before hashing so
// the key is order-independent (but set-sensitive).
void sort_extensions(ModelDescriptor& d);

// Deterministic ordering for CompatibilityKey (used as a map key).
inline bool operator<(const CompatibilityKey& a, const CompatibilityKey& b) noexcept {
  for (std::size_t i = 0; i < a.digest().size(); ++i) {
    if (a.digest()[i] != b.digest()[i]) return a.digest()[i] < b.digest()[i];
  }
  return false;
}

}  // namespace batch_fabric

namespace std {
template <>
struct hash<batch_fabric::ShapeKey> {
  std::size_t operator()(const batch_fabric::ShapeKey& k) const noexcept {
    return std::hash<std::string>{}(k.value);
  }
};
template <>
struct hash<batch_fabric::CompatibilityKey> {
  std::size_t operator()(const batch_fabric::CompatibilityKey& k) const noexcept {
    std::size_t h = 0;
    for (auto b : k.digest()) h = h * 131 + b;
    return h;
  }
};
}  // namespace std