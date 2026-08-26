#include "batch_fabric/transport.hpp"
#include "batch_fabric/hash.hpp"
#include <cstring>

#ifdef _WIN32
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <winsock2.h>
#include <ws2tcpip.h>
#pragma comment(lib, "ws2_32.lib")
using SocketHandle = SOCKET;
constexpr SocketHandle kInvalidSocket = INVALID_SOCKET;
#else
#include <arpa/inet.h>
#include <cerrno>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
using SocketHandle = int;
constexpr SocketHandle kInvalidSocket = -1;
#endif

namespace batch_fabric {

namespace {

bool ensure_wsa() {
#ifdef _WIN32
  static bool init = false;
  if (init) return true;
  WSADATA d;
  if (WSAStartup(MAKEWORD(2, 2), &d) != 0) return false;
  init = true;
  return true;
#else
  return true;
#endif
}

void close_socket(SocketHandle s) {
#ifdef _WIN32
  closesocket(s);
#else
  ::close(s);
#endif
}

void set_last_error(Error& err, ErrorCode code, const std::string& msg) {
  err = Error(code, msg);
}

int recv_all(SocketHandle s, std::uint8_t* buf, int n) {
  int got = 0;
  while (got < n) {
    int r = ::recv(s, reinterpret_cast<char*>(buf + got), n - got, 0);
    if (r == 0) return 0;
    if (r < 0) {
#ifdef _WIN32
      if (WSAGetLastError() == WSAEINTR) continue;
#else
      if (errno == EINTR) continue;
#endif
      return -1;
    }
    got += r;
  }
  return got;
}

int send_all(SocketHandle s, const std::uint8_t* buf, int n) {
  int sent = 0;
  while (sent < n) {
    int r = ::send(s, reinterpret_cast<const char*>(buf + sent), n - sent, 0);
    if (r < 0) {
#ifdef _WIN32
      if (WSAGetLastError() == WSAEINTR) continue;
#else
      if (errno == EINTR) continue;
#endif
      return -1;
    }
    sent += r;
  }
  return sent;
}

}  // namespace

std::string_view to_string(MessageType t) noexcept {
  switch (t) {
    case MessageType::hello: return "hello";
    case MessageType::register_worker: return "register_worker";
    case MessageType::submit: return "submit";
    case MessageType::cancel: return "cancel";
    case MessageType::dispatch: return "dispatch";
    case MessageType::complete: return "complete";
    case MessageType::status: return "status";
    case MessageType::status_response: return "status_response";
    case MessageType::epoch: return "epoch";
    case MessageType::ack: return "ack";
    case MessageType::ping: return "ping";
    case MessageType::pong: return "pong";
    case MessageType::error: return "error";
    case MessageType::roll_epoch: return "roll_epoch";
    case MessageType::shutdown: return "shutdown";
  }
  return "unknown";
}

Channel::Channel() : handle_(nullptr), valid_(false) {}
Channel::~Channel() { close(); }

bool Channel::connect(const std::string& host, std::uint16_t port, Error& err) {
  if (!ensure_wsa()) {
    set_last_error(err, ErrorCode::transport_failure, "WSAStartup failed");
    return false;
  }
  SocketHandle s = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
  if (s == kInvalidSocket) {
    set_last_error(err, ErrorCode::transport_failure, "socket creation failed");
    return false;
  }
  sockaddr_in addr;
  std::memset(&addr, 0, sizeof(addr));
  addr.sin_family = AF_INET;
  addr.sin_port = htons(port);
  std::string h = (host == "localhost") ? std::string("127.0.0.1") : host;
  if (inet_pton(AF_INET, h.c_str(), &addr.sin_addr) != 1) {
    close_socket(s);
    set_last_error(err, ErrorCode::transport_failure, "invalid host address");
    return false;
  }
  if (::connect(s, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
    close_socket(s);
    set_last_error(err, ErrorCode::transport_failure, "connect failed");
    return false;
  }
  handle_ = reinterpret_cast<void*>(static_cast<std::intptr_t>(s));
  valid_ = true;
  return true;
}

bool Channel::send(const FrameMessage& msg, Error& err) {
  if (!valid_) {
    set_last_error(err, ErrorCode::transport_failure, "channel not connected");
    return false;
  }
  std::uint64_t len = 1 + static_cast<std::uint64_t>(msg.body.size());
  if (len == 0 || len > kMaxFrameSize) {
    set_last_error(err, ErrorCode::transport_failure, "frame too large (or zero)");
    return false;
  }
  std::vector<std::uint8_t> frame;
  frame.reserve(4 + static_cast<std::size_t>(len));
  frame.push_back(static_cast<std::uint8_t>((len >> 24) & 0xff));
  frame.push_back(static_cast<std::uint8_t>((len >> 16) & 0xff));
  frame.push_back(static_cast<std::uint8_t>((len >> 8) & 0xff));
  frame.push_back(static_cast<std::uint8_t>(len & 0xff));
  frame.push_back(static_cast<std::uint8_t>(msg.type));
  frame.insert(frame.end(), msg.body.begin(), msg.body.end());
  SocketHandle s = static_cast<SocketHandle>(reinterpret_cast<std::intptr_t>(handle_));
  if (send_all(s, frame.data(), static_cast<int>(frame.size())) < 0) {
    set_last_error(err, ErrorCode::transport_failure, "send failed");
    return false;
  }
  return true;
}

bool Channel::recv(FrameMessage& msg, Error& err) {
  if (!valid_) {
    set_last_error(err, ErrorCode::transport_failure, "channel not connected");
    return false;
  }
  std::uint8_t hdr[4];
  SocketHandle s = static_cast<SocketHandle>(reinterpret_cast<std::intptr_t>(handle_));
  int r = recv_all(s, hdr, 4);
  if (r == 0) {
    set_last_error(err, ErrorCode::transport_failure, "peer closed connection");
    return false;
  }
  if (r < 0) {
    set_last_error(err, ErrorCode::transport_failure, "recv failed");
    return false;
  }
  std::uint32_t len = (static_cast<std::uint32_t>(hdr[0]) << 24) |
                      (static_cast<std::uint32_t>(hdr[1]) << 16) |
                      (static_cast<std::uint32_t>(hdr[2]) << 8) |
                      static_cast<std::uint32_t>(hdr[3]);
  if (len == 0) {
    set_last_error(err, ErrorCode::transport_failure, "zero-length frame");
    return false;
  }
  if (len > kMaxFrameSize) {
    set_last_error(err, ErrorCode::transport_failure, "oversized frame");
    return false;
  }
  std::vector<std::uint8_t> payload(len);
  if (recv_all(s, payload.data(), static_cast<int>(len)) <= 0) {
    set_last_error(err, ErrorCode::transport_failure, "truncated frame");
    return false;
  }
  msg.type = static_cast<MessageType>(payload[0]);
  msg.body.assign(payload.begin() + 1, payload.end());
  return true;
}

void Channel::close() {
  if (valid_ && handle_ != nullptr) {
    SocketHandle s = static_cast<SocketHandle>(reinterpret_cast<std::intptr_t>(handle_));
    close_socket(s);
  }
  handle_ = nullptr;
  valid_ = false;
}

bool Channel::valid() const { return valid_ && handle_ != nullptr; }

Server::Server() : handle_(nullptr), valid_(false), port_(0) {}
Server::~Server() { close(); }

bool Server::bind(std::uint16_t port, Error& err) {
  if (!ensure_wsa()) {
    set_last_error(err, ErrorCode::transport_failure, "WSAStartup failed");
    return false;
  }
  SocketHandle s = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
  if (s == kInvalidSocket) {
    set_last_error(err, ErrorCode::transport_failure, "socket creation failed");
    return false;
  }
  int opt = 1;
  ::setsockopt(s, SOL_SOCKET, SO_REUSEADDR, reinterpret_cast<const char*>(&opt), sizeof(opt));
  sockaddr_in addr;
  std::memset(&addr, 0, sizeof(addr));
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_ANY);
  addr.sin_port = htons(port);
  if (::bind(s, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
    close_socket(s);
    set_last_error(err, ErrorCode::transport_failure, "bind failed");
    return false;
  }
  if (::listen(s, SOMAXCONN) != 0) {
    close_socket(s);
    set_last_error(err, ErrorCode::transport_failure, "listen failed");
    return false;
  }
  sockaddr_in actual;
  int alen = sizeof(actual);
  if (getsockname(s, reinterpret_cast<sockaddr*>(&actual), &alen) == 0) {
    port_ = ntohs(actual.sin_port);
  } else {
    port_ = port;
  }
  handle_ = reinterpret_cast<void*>(static_cast<std::intptr_t>(s));
  valid_ = true;
  return true;
}

Channel* Server::accept(Error& err) {
  if (!valid_) {
    set_last_error(err, ErrorCode::transport_failure, "server not bound");
    return nullptr;
  }
  SocketHandle ls = static_cast<SocketHandle>(reinterpret_cast<std::intptr_t>(handle_));
  sockaddr_in cli;
  int clen = sizeof(cli);
  SocketHandle s = ::accept(ls, reinterpret_cast<sockaddr*>(&cli), &clen);
  if (s == kInvalidSocket) {
    set_last_error(err, ErrorCode::transport_failure, "accept failed");
    return nullptr;
  }
  Channel* ch = new Channel();
  ch->handle_ = reinterpret_cast<void*>(static_cast<std::intptr_t>(s));
  ch->valid_ = true;
  return ch;
}

void Server::close() {
  if (valid_ && handle_ != nullptr) {
    SocketHandle s = static_cast<SocketHandle>(reinterpret_cast<std::intptr_t>(handle_));
    close_socket(s);
  }
  handle_ = nullptr;
  valid_ = false;
}

}  // namespace batch_fabric
