# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.1] - 2026-11-24

### Changed

- **Breaking:** `Config.MaxConns` and `Config.MaxDialed` no longer have a range maximum of 10000: a value above 10000 that fits `MaxMemory` is now accepted, where it returned `ErrInvalidConfig`. The `MaxMemory` check of (`MaxConns` + `MaxDialed`) × `MaxConnReceiveWindow` is the upper bound, and a value below 1 is still invalid.
- **Breaking:** with `Config.Datagrams` on, the `MaxMemory` check also counts 240 KiB of datagram queues (128 received and 32 to send, 1536 bytes each) for each of `MaxConns` + `MaxDialed`, so a listener whose datagram queues do not fit `MaxMemory` now returns `ErrInvalidConfig`, where it was accepted. Raise `MaxMemory`, lower the counts, or turn `Datagrams` off. Datagrams stay opt-in, so a config without them is unchanged.
- **Breaking:** `Config.MaxMemory` and `Config.MaxGrownMemory` no longer have a range maximum of 64 GiB: a larger value is accepted, where it returned `ErrInvalidConfig`. The `MaxMemory` check is now computed without overflow at any count, and a configuration whose sum does not fit an `int64` is `ErrInvalidConfig`. The minimums (64 MiB and 1 MiB) stay.

### Added

- `DefaultMaxConns` (100), `DefaultMaxDialed` (32) and `DatagramQueueBytes` (240 KiB, what the `MaxMemory` check counts per connection with `Datagrams`), so libraries that size a listener do not copy them.
- `Listener.MaxConns` returns the connection cap of the listener, with the default applied.
- `Config.Datagrams` (default false) offers QUIC datagrams (RFC 9221); `Conn.SupportsDatagrams` reports whether both ends offered them.
- `Conn.SendDatagram`, which never blocks: a payload that does not fit in one packet on the current path is `ErrDatagramTooLarge`, and a full send queue (32) drops it with `ErrDatagramDropped`. `Conn.ReceiveDatagram` returns the next datagram (quic-go keeps up to 128). A datagram call on a connection without support is `ErrDatagramsUnsupported`.
- `Stream.ID`, the QUIC stream ID (RFC 9000 section 2.1).
- `internal/quicgo/PATCH` adds `Conn.TrySendDatagram`, a datagram send that drops instead of waiting for room in the queue.

### Fixed

- A path whose MTU drops after the connection found a larger packet size (a route change, a relayed or mobile path) no longer stalls every transfer that needs large packets (the large packets were all lost while the small ones kept the connection open): the loss of the large packets, or 3 probe timeouts in a row, brings the packet size back to 1200 bytes and path MTU discovery searches again (RFC 8899 section 4.3). `ConnStats.MaxDatagramSize` and the datagram size limit follow it. The detection is in `internal/quicgo/PATCH`.

## [0.1.0] - 2026-11-20

### Added

- First release of kolo-transport.
