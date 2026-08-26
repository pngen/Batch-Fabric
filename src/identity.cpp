#include "batch_fabric/identity.hpp"
#include "batch_fabric/io.hpp"
#include <algorithm>

namespace batch_fabric {

namespace {

void append_escaped(std::string& out, std::string_view v) {
  for (char c : v) {
    switch (c) {
      case '\\': out += "\\\\"; break;
      case ';': out += "\\;"; break;
      case ':': out += "\\:"; break;
      case ',': out += "\\,"; break;
      case '[': out += "\\["; break;
      case ']': out += "\\]"; break;
      default:
        if (static_cast<unsigned char>(c) < 0x20) {
          char buf[8];
          auto n = std::snprintf(buf, sizeof(buf), "\\x%02x", static_cast<unsigned char>(c));
          out.append(buf, static_cast<std::size_t>(n));
        } else {
          out += c;
        }
    }
  }
}

}  // namespace

bool operator==(const ModelDescriptor& a, const ModelDescriptor& b) noexcept {
  return a.model == b.model && a.revision == b.revision && a.adapter == b.adapter &&
         a.tokenizer == b.tokenizer && a.phase == b.phase && a.dtype == b.dtype &&
         a.backend == b.backend && a.execution_mode == b.execution_mode &&
         a.device == b.device && a.shape == b.shape &&
         a.sampling_semantics == b.sampling_semantics && a.extensions == b.extensions;
}

void sort_extensions(ModelDescriptor& d) { std::sort(d.extensions.begin(), d.extensions.end()); }

std::string canonical_string(const ModelDescriptor& d) {
  std::string out;
  out.reserve(256);
  out += "model=";
  append_escaped(out, d.model.value);
  out += ";revision=";
  append_escaped(out, d.revision.value);
  out += ";adapter=";
  append_escaped(out, d.adapter.value);
  out += ";tokenizer=";
  append_escaped(out, d.tokenizer.value);
  out += ";phase=";
  out += std::string(to_string(d.phase));
  out += ";dtype=";
  out += std::string(to_string(d.dtype));
  out += ";backend=";
  out += std::string(to_string(d.backend));
  out += ";execution_mode=";
  out += std::string(to_string(d.execution_mode));
  out += ";device=backend";
  out += std::to_string(static_cast<unsigned>(d.device.backend));
  out += "/cc";
  out += std::to_string(d.device.compute_major);
  out += ".";
  out += std::to_string(d.device.compute_minor);
  out += "/mem";
  out += std::to_string(d.device.min_memory_bytes);
  out += ";shape=";
  append_escaped(out, d.shape.value);
  out += ";sampling=";
  append_escaped(out, d.sampling_semantics);
  out += ";extensions=[";
  bool first = true;
  for (const auto& e : d.extensions) {
    if (!first) out += ",";
    first = false;
    append_escaped(out, e);
  }
  out += "]";
  return out;
}

CompatibilityKey CompatibilityKey::build(const ModelDescriptor& d) {
  ByteWriter w;
  w.u8(1);  // canonical key format version
  w.string(d.model.value);
  w.string(d.revision.value);
  w.string(d.adapter.value);
  w.string(d.tokenizer.value);
  w.u8(static_cast<std::uint8_t>(d.phase));
  w.u8(static_cast<std::uint8_t>(d.dtype));
  w.u8(static_cast<std::uint8_t>(d.backend));
  w.u8(static_cast<std::uint8_t>(d.execution_mode));
  w.u8(static_cast<std::uint8_t>(d.device.backend));
  w.u32(d.device.compute_major);
  w.u32(d.device.compute_minor);
  w.u64(d.device.min_memory_bytes);
  w.string(d.shape.value);
  w.string(d.sampling_semantics);
  w.u32(static_cast<std::uint32_t>(d.extensions.size()));
  for (const auto& e : d.extensions) w.string(e);
  const auto& bytes = w.data();
  Digest dig = Sha256::digest(bytes.data(), bytes.size());
  return CompatibilityKey(dig);
}

std::string CompatibilityKey::hex() const { return to_hex(digest_); }

std::string describe(const ModelDescriptor& d) { return canonical_string(d); }

}  // namespace batch_fabric
