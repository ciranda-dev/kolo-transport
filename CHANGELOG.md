# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `Config.Datagrams` (default false) offers QUIC datagrams (RFC 9221); `Conn.SupportsDatagrams` reports whether both ends offered them.
- `Conn.SendDatagram`, which never blocks: a payload that does not fit in one packet on the current path is `ErrDatagramTooLarge`, and a full send queue (32) drops it with `ErrDatagramDropped`. `Conn.ReceiveDatagram` returns the next datagram (quic-go keeps up to 128). A datagram call on a connection without support is `ErrDatagramsUnsupported`.
- `Stream.ID`, the QUIC stream ID (RFC 9000 section 2.1).
- `internal/quicgo/PATCH` adds `Conn.TrySendDatagram`, a datagram send that drops instead of waiting for room in the queue.

### Fixed

- A path whose MTU drops after the connection found a larger packet size (a route change, a relayed or mobile path) no longer stalls every transfer that needs large packets (the large packets were all lost while the small ones kept the connection open): the loss of the large packets, or 3 probe timeouts in a row, brings the packet size back to 1200 bytes and path MTU discovery searches again (RFC 8899 section 4.3). `ConnStats.MaxDatagramSize` and the datagram size limit follow it. The detection is in `internal/quicgo/PATCH`.

## [0.1.0] - 2026-11-20

### Added

- First release of kolo-transport.
