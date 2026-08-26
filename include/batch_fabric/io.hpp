#pragma once
#include "batch_fabric/clock.hpp"
#include "batch_fabric/enums.hpp"
#include "batch_fabric/hash.hpp"
#include "batch_fabric/id.hpp"
#include "batch_fabric/identity.hpp"
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string>
#include <string_view>
#include <vector>

namespace batch_fabric {

constexpr std::size_t kDefaultMaxStringLen = 1u << 20;   // 1 MiB
constexpr std::uint32_t kDefaultMaxFrameSize = 64u << 20;  // 64 MiB hard cap

// Big-endian (network order) serialization primitives. All multi-byte
// integers are written big-endian so frames and persistence files are
// byte-deterministic across hosts.
class ByteWriter {
 public:
  void u8(std::uint8_t v) { buf_.push_back(v); }
  void u16(std::uint16_t v) {
    buf_.push_back(static_cast<std::uint8_t>((v >> 8) & 0xff));
    buf_.push_back(static_cast<std::uint8_t>(v & 0xff));
  }
  void u32(std::uint32_t v) {
    buf_.push_back(static_cast<std::uint8_t>((v >> 24) & 0xff));
    buf_.push_back(static_cast<std::uint8_t>((v >> 16) & 0xff));
    buf_.push_back(static_cast<std::uint8_t>((v >> 8) & 0xff));
    buf_.push_back(static_cast<std::uint8_t>(v & 0xff));
  }
  void u64(std::uint64_t v) {
    for (int i = 7; i >= 0; --i) buf_.push_back(static_cast<std::uint8_t>((v >> (8 * i)) & 0xff));
  }
  void i64(std::int64_t v) { u64(static_cast<std::uint64_t>(v)); }
  void bytes(const std::uint8_t* p, std::size_t n) {
    if (n == 0) return;
    buf_.insert(buf_.end(), p, p + n);
  }
  template <typename It>
  void bytes_range(It begin, It end) {
    buf_.insert(buf_.end(), begin, end);
  }
  // Length prefix is a u64 followed by raw bytes.
  void string(std::string_view s) {
    u64(static_cast<std::uint64_t>(s.size()));
    buf_.insert(buf_.end(), s.begin(), s.end());
  }
  template <typename IdType>
  void id(const IdType& id) { u64(id.value); }
  void digest(const Digest& d) { bytes(d.data(), static_cast<std::size_t>(d.size())); }
  std::size_t size() const noexcept { return buf_.size(); }
  const std::vector<std::uint8_t>& data() const noexcept { return buf_; }
  std::vector<std::uint8_t> take() { return std::move(buf_); }

 private:
  std::vector<std::uint8_t> buf_;
};

// Bounds-checking reader. Each read returns false on overrun so corruption can
// be surfaced as a structured error rather than a crash.
class ByteReader {
 public:
  explicit ByteReader(const std::uint8_t* data, std::size_t size)
      : data_(data), size_(size) {}
  explicit ByteReader(const std::vector<std::uint8_t>& v)
      : data_(v.data()), size_(v.size()) {}

  bool u8(std::uint8_t& v) {
    if (pos_ + 1 > size_) return false;
    v = data_[pos_++];
    return true;
  }
  bool u16(std::uint16_t& v) {
    if (pos_ + 2 > size_) return false;
    v = static_cast<std::uint16_t>((data_[pos_] << 8) | data_[pos_ + 1]);
    pos_ += 2;
    return true;
  }
  bool u32(std::uint32_t& v) {
    if (pos_ + 4 > size_) return false;
    v = (static_cast<std::uint32_t>(data_[pos_]) << 24) |
        (static_cast<std::uint32_t>(data_[pos_ + 1]) << 16) |
        (static_cast<std::uint32_t>(data_[pos_ + 2]) << 8) |
        static_cast<std::uint32_t>(data_[pos_ + 3]);
    pos_ += 4;
    return true;
  }
  bool u64(std::uint64_t& v) {
    if (pos_ + 8 > size_) return false;
    v = 0;
    for (int i = 0; i < 8; ++i) v = (v << 8) | data_[pos_ + i];
    pos_ += 8;
    return true;
  }
  bool i64(std::int64_t& v) {
    std::uint64_t u;
    if (!u64(u)) return false;
    v = static_cast<std::int64_t>(u);
    return true;
  }
  bool string(std::string& v, std::size_t max_len = kDefaultMaxStringLen) {
    std::uint64_t len;
    if (!u64(len)) return false;
    if (len > max_len || len > size_ - pos_) return false;
    v.assign(reinterpret_cast<const char*>(data_ + pos_), static_cast<std::size_t>(len));
    pos_ += static_cast<std::size_t>(len);
    return true;
  }
  template <typename IdType>
  bool id(IdType& id) {
    std::uint64_t v;
    if (!u64(v)) return false;
    id = IdType(v);
    return true;
  }
  bool digest(Digest& d) {
    if (pos_ + d.size() > size_) return false;
    std::memcpy(d.data(), data_ + pos_, d.size());
    pos_ += d.size();
    return true;
  }
  std::size_t remaining() const noexcept { return size_ - pos_; }
  std::size_t offset() const noexcept { return pos_; }
  const std::uint8_t* peek() const noexcept { return data_ + pos_; }

 private:
  const std::uint8_t* data_;
  std::size_t size_;
  std::size_t pos_ = 0;
};

}  // namespace batch_fabric
