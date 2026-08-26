#pragma once
#include "batch_fabric/error.hpp"
#include "batch_fabric/io.hpp"
#include <cstdint>
#include <string>
#include <vector>

namespace batch_fabric {

enum class MessageType : std::uint8_t {
  hello = 0,
  register_worker = 1,
  submit = 2,
  cancel = 3,
  dispatch = 4,
  complete = 5,
  status = 6,
  status_response = 7,
  epoch = 8,
  ack = 9,
  ping = 10,
  pong = 11,
  error = 12,
  roll_epoch = 13,
  shutdown = 14
};

std::string_view to_string(MessageType t) noexcept;

// A single framed message: type byte + opaque body. The body's schema is
// owned by the protocol layer (see protocol.hpp).
struct FrameMessage {
  MessageType type = MessageType::hello;
  std::vector<std::uint8_t> body;
};

// Framed channel over a stream socket. Uses a 4-byte big-endian length prefix
// with a hard maximum frame size. Malformed / oversized / zero-length /
// truncated frames are rejected rather than parsed.
class Channel {
 public:
  Channel();
  ~Channel();
  Channel(const Channel&) = delete;
  Channel& operator=(const Channel&) = delete;

  bool connect(const std::string& host, std::uint16_t port, Error& err);
  bool send(const FrameMessage& msg, Error& err);
  bool recv(FrameMessage& msg, Error& err);  // blocking until a frame arrives
  void close();
  bool valid() const;

  friend class Server;

  static constexpr std::uint32_t kMaxFrameSize = kDefaultMaxFrameSize;

 private:
  void* handle_ = nullptr;  // SOCKET on Windows, int fd elsewhere
  bool valid_ = false;
};

// Stream socket server that accepts framed channels.
class Server {
 public:
  Server();
  ~Server();
  Server(const Server&) = delete;
  Server& operator=(const Server&) = delete;

  bool bind(std::uint16_t port, Error& err);
  Channel* accept(Error& err);  // returns a heap channel, or nullptr
  void close();
  std::uint16_t port() const { return port_; }

 private:
  void* handle_ = nullptr;
  bool valid_ = false;
  std::uint16_t port_ = 0;
};

}  // namespace batch_fabric