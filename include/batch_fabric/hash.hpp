#pragma once
#include <array>
#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>

namespace batch_fabric {

using Digest = std::array<std::uint8_t, 32>;

// Minimal, self-contained SHA-256. Used for canonical CompatibilityKey
// digests and for persistence integrity checks. No external crypto dependency.
class Sha256 {
 public:
  Sha256();
  void update(const std::uint8_t* data, std::size_t len);
  void update(std::string_view s) {
    update(reinterpret_cast<const std::uint8_t*>(s.data()), s.size());
  }
  Digest final();
  static Digest digest(const std::uint8_t* data, std::size_t len);
  static Digest digest(std::string_view s);
  void reset();

 private:
  void transform(const std::uint8_t block[64]);

  std::uint32_t state_[8];
  std::uint8_t buffer_[64];
  std::uint64_t total_len_;
  std::size_t buffered_;
};

std::string to_hex(const Digest& d);
Digest from_hex(std::string_view s);  // returns zero digest on malformed input

inline std::uint64_t fnv1a64(std::string_view s) noexcept {
  std::uint64_t h = 1469598103934665603ULL;
  for (char c : s) {
    h ^= static_cast<std::uint64_t>(static_cast<std::uint8_t>(c));
    h *= 1099511628211ULL;
  }
  return h;
}

constexpr std::uint64_t kHashNull = static_cast<std::uint64_t>(0);

// SplitMix64 finalizer mixing a sequence of values into one 64-bit hash.
inline std::uint64_t hash_mix64(std::uint64_t x) noexcept {
  x += 0x9e3779b97f4a7c15ULL;
  x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9ULL;
  x = (x ^ (x >> 27)) * 0x94d049bb133111ebULL;
  x = x ^ (x >> 31);
  return x;
}

struct HashCombine {
  std::uint64_t state = kHashNull;
  void add(std::uint64_t v) noexcept { state = hash_mix64(state ^ v); }
  void add(std::string_view s) noexcept {
    state = hash_mix64(state ^ fnv1a64(s));
  }
  std::uint64_t value() const noexcept { return state; }
};

}  // namespace batch_fabric
