#include "batch_fabric/hash.hpp"
#include <cstring>

namespace batch_fabric {

namespace {
constexpr std::uint32_t kSha256K[64] = {
    0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1,
    0x923f82a4, 0xab1c5ed5, 0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3,
    0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174, 0xe49b69c1, 0xefbe4786,
    0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
    0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147,
    0x06ca6351, 0x14292967, 0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13,
    0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85, 0xa2bfe8a1, 0xa81a664b,
    0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
    0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a,
    0x5b9cca4f, 0x682e6ff3, 0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208,
    0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2};

inline std::uint32_t ror(std::uint32_t x, int n) {
  return (x >> n) | (x << (32 - n));
}
}  // namespace

Sha256::Sha256() { reset(); }

void Sha256::reset() {
  state_[0] = 0x6a09e667;
  state_[1] = 0xbb67ae85;
  state_[2] = 0x3c6ef372;
  state_[3] = 0xa54ff53a;
  state_[4] = 0x510e527f;
  state_[5] = 0x9b05688c;
  state_[6] = 0x1f83d9ab;
  state_[7] = 0x5be0cd19;
  buffered_ = 0;
  total_len_ = 0;
}

void Sha256::update(const std::uint8_t* data, std::size_t len) {
  total_len_ += len;
  while (len > 0) {
    std::size_t take = 64 - buffered_;
    if (take > len) take = len;
    std::memcpy(buffer_ + buffered_, data, take);
    buffered_ += take;
    data += take;
    len -= take;
    if (buffered_ == 64) {
      transform(buffer_);
      buffered_ = 0;
    }
  }
}

void Sha256::transform(const std::uint8_t block[64]) {
  std::uint32_t w[64];
  for (int i = 0; i < 16; ++i) {
    w[i] = (static_cast<std::uint32_t>(block[i * 4]) << 24) |
           (static_cast<std::uint32_t>(block[i * 4 + 1]) << 16) |
           (static_cast<std::uint32_t>(block[i * 4 + 2]) << 8) |
           static_cast<std::uint32_t>(block[i * 4 + 3]);
  }
  for (int i = 16; i < 64; ++i) {
    std::uint32_t s0 = ror(w[i - 15], 7) ^ ror(w[i - 15], 18) ^ (w[i - 15] >> 3);
    std::uint32_t s1 = ror(w[i - 2], 17) ^ ror(w[i - 2], 19) ^ (w[i - 2] >> 10);
    w[i] = w[i - 16] + s0 + w[i - 7] + s1;
  }
  std::uint32_t a = state_[0], b = state_[1], c = state_[2], d = state_[3];
  std::uint32_t e = state_[4], f = state_[5], g = state_[6], h = state_[7];
  for (int i = 0; i < 64; ++i) {
    std::uint32_t S1 = ror(e, 6) ^ ror(e, 11) ^ ror(e, 25);
    std::uint32_t ch = (e & f) ^ ((~e) & g);
    std::uint32_t temp1 = h + S1 + ch + kSha256K[i] + w[i];
    std::uint32_t S0 = ror(a, 2) ^ ror(a, 13) ^ ror(a, 22);
    std::uint32_t maj = (a & b) ^ (a & c) ^ (b & c);
    std::uint32_t temp2 = S0 + maj;
    h = g; g = f; f = e; e = d + temp1;
    d = c; c = b; b = a; a = temp1 + temp2;
  }
  state_[0] += a; state_[1] += b; state_[2] += c; state_[3] += d;
  state_[4] += e; state_[5] += f; state_[6] += g; state_[7] += h;
}

Digest Sha256::final() {
  std::uint64_t bitlen = total_len_ * 8;
  std::uint8_t pad = 0x80;
  update(&pad, 1);
  std::uint8_t zero = 0;
  while (buffered_ != 56) update(&zero, 1);
  std::uint8_t lenbuf[8];
  for (int i = 0; i < 8; ++i)
    lenbuf[i] = static_cast<std::uint8_t>((bitlen >> (8 * (7 - i))) & 0xff);
  update(lenbuf, 8);
  Digest out;
  for (int i = 0; i < 8; ++i) {
    out[i * 4] = static_cast<std::uint8_t>((state_[i] >> 24) & 0xff);
    out[i * 4 + 1] = static_cast<std::uint8_t>((state_[i] >> 16) & 0xff);
    out[i * 4 + 2] = static_cast<std::uint8_t>((state_[i] >> 8) & 0xff);
    out[i * 4 + 3] = static_cast<std::uint8_t>(state_[i] & 0xff);
  }
  return out;
}

Digest Sha256::digest(const std::uint8_t* data, std::size_t len) {
  Sha256 s;
  s.update(data, len);
  return s.final();
}

Digest Sha256::digest(std::string_view s) {
  return digest(reinterpret_cast<const std::uint8_t*>(s.data()), s.size());
}

std::string to_hex(const Digest& d) {
  static const char* kHex = "0123456789abcdef";
  std::string out;
  out.reserve(d.size() * 2);
  for (auto b : d) {
    out.push_back(kHex[(b >> 4) & 0xf]);
    out.push_back(kHex[b & 0xf]);
  }
  return out;
}

Digest from_hex(std::string_view s) {
  Digest d{};
  if (s.size() != d.size() * 2) return d;
  for (std::size_t i = 0; i < d.size(); ++i) {
    auto hi = std::string_view("0123456789abcdef").find(s[i * 2]);
    auto lo = std::string_view("0123456789abcdef").find(s[i * 2 + 1]);
    if (hi == std::string_view::npos || lo == std::string_view::npos) return Digest{};
    d[i] = static_cast<std::uint8_t>((hi << 4) | lo);
  }
  return d;
}

}  // namespace batch_fabric
