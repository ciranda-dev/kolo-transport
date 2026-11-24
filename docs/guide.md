# Guide

How to use kolo-transport beyond the README quick start. Every default and bound is in [limits.md](limits.md); the design and the wire format are in [protocol.md](protocol.md).

## API

kolo-transport is IPv4 only. Sockets are `udp4`. An IPv6 address, the v4-mapped
form `::ffff:a.b.c.d` included, is refused with `ErrInvalidArgument`, and a
`Network` other than `"udp4"` with `ErrInvalidConfig`. A host name with no
IPv4 address fails to resolve.

- `Listen(ctx, addr, cfg) (*Listener, error)` opens a UDP socket on `addr`
  (such as `"127.0.0.1:0"`) and generates the listener's certificate.
  A host name binds one of its IPv4 addresses.
  `(*Listener).Accept(ctx)` returns the next connection whose handshake has
  completed. Keep-alive keeps the ones nobody accepts open until `Close`:
  up to 32, within `MaxConns` and `MaxConnsPerIP`, each holding up to
  `MaxConnReceiveWindow` unread. Call `Accept` even on a listener used only
  to dial. `Addr()` is the local address. `Close()` refuses new
  connections, closes every connection of the listener with code 0, then
  the socket.
- `ListenConn(ctx, pc, cfg) (*Listener, error)` is `Listen` on a
  `net.PacketConn` the caller supplies, such as a simulated network or a
  virtual conn. The Listener owns `pc` and closes it on `Close`, and also
  when `ListenConn` fails. Retry, the address check and the `*net.UDPAddr`
  requirement stay, so `pc` must report `*net.UDPAddr` addresses, and its
  own address must be IPv4 (`ErrInvalidArgument` otherwise). See
  "Caller-supplied sockets" below.
- `Dial(ctx, addr, cfg) (*Conn, error)` connects from a new `udp4` socket,
  which is released when the connection ends. The socket is bound to
  loopback when `addr` is a loopback address. A host name resolves to its
  first IPv4 address. A destination
  refused as for `Listener.Dial`, also one a name resolves to, is
  `ErrInvalidArgument` before any socket is opened.
- `(*Listener).Dial(ctx, addr netip.AddrPort) (*Conn, error)` connects from
  the listener's own socket, so the peer sees the listener's address and
  a NAT keeps one mapping for both directions, which hole punching needs.
  It uses the listener's `Config`; `Listener.Close` closes these
  connections too. They do not count against `MaxConns` or
  `MaxConnsPerIP`, which are for incoming connections; `MaxDialed` bounds
  them, and beyond it `Dial` returns `ErrMaxDialed`. A failed dial frees
  its slot. `AcceptFunc` does not filter the addresses it dials. A
  destination with port 0, or an IPv6 (v4-mapped included), unspecified,
  multicast, `0.0.0.0/8` or `240.0.0.0/4` address (which includes
  255.255.255.255), is `ErrInvalidArgument`. See "Destinations" below.
- `(*Listener).WritePacket(b, to)` sends one non-QUIC datagram from the
  listener's socket. It returns `ErrInvalidArgument` for an empty `b`, one
  over `MaxPacketSize` (1200) bytes, or one whose first byte has either of
  the two high bits set (`b[0]&0xC0 != 0`, which QUIC would parse), or a
  destination rejected as for `Listener.Dial`, and `ErrClosed` after
  `Close`. A failed write is `ErrTransport`, with no detail from the
  socket. It blocks as long as the socket's `WriteTo` does. `AcceptFunc`
  does not filter its destinations.
- `(*Listener).ReadPacket(ctx, b) (int, netip.AddrPort, error)` returns
  the next non-QUIC datagram on the socket and its sender. Datagrams over
  `MaxPacketSize` are dropped and reading goes on; a `b` shorter than the
  datagram gets its first `len(b)` bytes and the rest is dropped. It
  returns `ErrClosed` after `Close` and `ctx.Err()` when the context ends,
  also while another call is reading; calls take turns. A datagram already
  taken off quic-go's queue is returned even if the context ends at that
  moment. Senders that are not a valid UDP address are dropped. After the
  socket fails (see "Caller-supplied sockets"), every call returns
  `ErrTransport` for good; close the Listener. The sender is always a plain
  IPv4 address. The
  sender address is not validated and may be spoofed: do not dial it, and
  do not answer with more bytes than it sent, until it has proved it
  receives at that address (for example by echoing a token you sent
  there). See "Packets" below.
- `(*Conn).Context()` is canceled when the connection ends for any reason:
  `Close`, the peer closing it, an idle timeout, or `Listener.Close`.
  `context.Cause` gives the same error the connection's calls return (a
  sentinel or an `*Error`), never quic-go's error or peer text.
- `(*Conn).OpenStream(ctx)` and `OpenUniStream(ctx)` wait while the peer's
  stream limit is reached; the peer sees a stream once data or a `Close` is
  sent on it. `AcceptStream(ctx)` and `AcceptUniStream(ctx)` wait for the
  peer's next stream. `LocalAddr()` and `RemoteAddr()` are UDP addresses.
- `(*Conn).AllowWindowGrowth() bool` marks the connection as trusted so that
  its receive windows can grow (see "Faster transfers for trusted peers"
  below). It reports whether growth is on for the connection.
- `(*Conn).Stats() ConnStats` returns what the connection measured: round-trip times, bytes and packets, losses, the congestion
  window, the controller and the path (see "Connection statistics" below).
- `(*Conn).SupportsDatagrams()`, `SendDatagram(p)` and `ReceiveDatagram(ctx)` carry QUIC datagrams when both ends set
  `Config.Datagrams` (see "Datagrams" below).
- `(*Conn).ExportKey(label) ([32]byte, error)` derives 32 bytes from the
  TLS session with the RFC 9846 exporter. Both ends get the same bytes only
  if no one is in the middle, so a higher layer can sign them to tie its
  identity to this connection. The label must not be empty
  (`ErrInvalidArgument`); use one per purpose, such as
  `kolo-<lib>/<purpose>/v0`. Each side signs the label, its role (dialer or
  listener) and the bytes together, so a signature cannot be reflected back
  to its sender.
- `(*Conn).Close()` closes with code 0. `CloseWithError(code, msg)` sends
  `code` (at most `MaxCode`, a `uint64` equal to 2^62-1) and `msg` (at most 256 bytes) to the
  peer; out of range, it closes with code 0 and returns
  `ErrInvalidArgument`.
- `Stream` has `Read`, `Write`, `Close` (ends the send side), `CancelRead`,
  `CancelWrite`, `SetDeadline`, `SetReadDeadline` and `SetWriteDeadline`.
  `SendStream` is the write half and `ReceiveStream` the read half of a
  unidirectional stream. A code above `MaxCode` cancels with code 0 and
  returns `ErrInvalidArgument`. An expired deadline returns
  `os.ErrDeadlineExceeded`. `Stream.ID()` is the QUIC stream ID (RFC 9000
  section 2.1), the same on both ends: 0, 4, 8 and so on for the dialer's
  streams, 1, 5, 9 for the listener's.
- Every blocking call returns `ctx.Err()` as soon as its context is done.

## Errors

Errors are matched with `errors.Is`. Those with a code are an `*Error` (use
`errors.As` to read `Code`); none of them includes text from the peer.

- `ErrPeerClosed`: the peer closed the connection (its code), or sent a
  stateless reset.
- `ErrClosed`: this side closed the listener, connection or stream.
- `ErrRefused`: the peer refused the connection, for example because its
  listener is full.
- `ErrHandshake`: the TLS or QUIC handshake failed, for example on another
  ALPN or a peer certificate that is not one self-signed certificate.
- `ErrTimeout`: the handshake or idle timeout expired.
- `ErrStreamReset`: the peer reset the stream or asked this side to stop
  sending (its code). `ErrStreamCanceled`: this side canceled it.
- `ErrTransport`: a QUIC transport error (its code) or any other failure.
- `ErrMaxDialed`: `Listener.Dial` already has `MaxDialed` connections open.
- `ErrDatagramsUnsupported`: a datagram call on a connection whose ends did
  not both set `Config.Datagrams`. `ErrDatagramTooLarge` and
  `ErrDatagramDropped`: see "Datagrams" below.
- `ErrInvalidConfig`, `ErrInvalidArgument`.

## Packet size

A connection starts with packets of 1200 bytes of UDP payload, 1228 on the wire, the size QUIC requires every path to
carry; path MTU discovery then raises it, up to 1452, where the path allows. quic-go's own start is 1280 bytes, 1308 on
the wire, which a path with a smaller MTU drops (a tunnel, or a carrier that translates IPv4 to IPv6) and the handshake
then times out. A path that cannot carry 1228 bytes cannot run QUIC.

A path can also carry less later, after a route change or on a relayed or mobile path, and nothing tells the sender (ICMP
is not used). The sender then falls back to 1200 bytes when its losses show a black hole (RFC 8899 section 4.3): more
than 3 loss bursts (the packets one loss detection pass declares lost) made only of packets larger than 1200 bytes, with
no packet as large sent after them acknowledged, or 3 probe timeouts in a row with such a packet outstanding (the
full-stop timeout of RFC 4821 section 7.7). It then searches again from 1200 bytes: one probe at a time, sent with the
traffic at least 5 smoothed round trips after the previous one, 9 probes to find 1251 bytes on a path MTU of 1280. The
lost data is sent again in the smaller packets. An outage of 3 probe timeouts while such a packet is outstanding also
brings the size back to 1200 until the search raises it again. Once a search ends, the size is not probed again until
a black hole or a new path.

## Datagrams

`Config.Datagrams` offers QUIC datagrams (RFC 9221) on `Listen` and `Dial`. It is off by default. A connection carries
datagrams only when both ends set it, which `Conn.SupportsDatagrams` reports; otherwise `SendDatagram` and `ReceiveDatagram`
return `ErrDatagramsUnsupported`. A datagram is protected by the keys of the connection and counts against its congestion
window, but it is neither retransmitted nor flow controlled: it can be lost or arrive out of order.

- `SendDatagram(p)` copies `p` into the connection's send queue and never blocks. quic-go queues at most 32 datagrams per
  connection; while the queue is full, `p` is dropped with `ErrDatagramDropped`, as a UDP socket with a full buffer drops.
- A datagram must fit in one packet on the current path: at most `ConnStats.MaxDatagramSize` minus 37 bytes (quic-go's
  estimate of the packet overhead), 1163 bytes when a connection starts and up to 1415 once path MTU discovery has reached
  1452. A larger `p` is `ErrDatagramTooLarge`. The limit follows the path MTU, and starts over on a new path or after a
  black hole (see "Packet size").
- `ReceiveDatagram(ctx)` returns the next datagram, `ctx.Err()` when the context ends and the connection's error once it
  has ended. quic-go keeps at most 128 received datagrams per connection and drops the ones that arrive while 128 wait, so
  read promptly.
- The queues hold up to 240 KiB per connection, which `Listen` adds to its `MaxMemory` check for every connection when `Datagrams` is on (see [limits.md](limits.md)).

## Faster transfers for trusted peers

A stream moves at most window / round trip: with the default 256 KiB stream
window, about 21 Mbit/s at 100 ms. The windows are small because a hostile
peer can make an application hold up to 176 heap bytes per unread byte (see
[limits.md](limits.md)), and a larger window for everyone multiplies that. So
the windows start small for every connection and grow only for one the
application has marked as trusted, within a budget shared by all of them.

```go
cfg := &kolotransport.Config{WindowGrowth: true}
l, err := kolotransport.Listen(ctx, "127.0.0.1:0", cfg)
// ...
conn, err := l.Accept(ctx)
// The application learns who the peer is, for example from a handshake that proved its key,
// and decides it is one it wants to serve fast:
if peerIsAuthorized(conn) {
	conn.AllowWindowGrowth()
}
```

- `kolotransport` never learns who a peer is, so it cannot decide. The
  application calls `AllowWindowGrowth` once its own layer has authenticated
  the peer and its policy allows it, such as the admission function of a
  swarm. A connection nobody marked, including one that is already moving
  data before the application knows who it is, keeps the initial windows;
  marking it later starts the growth from there.
- `WindowGrowth` is off by default and then nothing changes: the windows are
  fixed at `MaxStreamReceiveWindow` and `MaxConnReceiveWindow`, and
  `AllowWindowGrowth` returns false.
- Growth is quic-go's receive window auto-tuning: a window doubles while the
  application reads it faster than a few round trips, up to
  `MaxGrownStreamReceiveWindow` (default 4 MiB) and
  `MaxGrownConnReceiveWindow` (default 8 MiB). It does not grow while the
  application reads slowly, and never shrinks.
- `MaxGrownMemory` (default 64 MiB) is the connection window growth that all
  trusted connections of one `Listener` can hold together, `Listener.Dial`
  connections included. Each step of growth takes its size from the budget,
  and is refused when the budget is not enough; the connection goes on with the
  window it has. The budget returns when the connection ends. A connection
  made by package `Dial` has a budget of its own. At the defaults, 64 MiB
  lets 8 connections reach 8 MiB.
- `Listen` also checks that `MaxMemory` covers the initial windows (and, with `Datagrams`, the datagram queues) of
  `MaxConns` + `MaxDialed` connections plus the growth they can hold at most.
- Keep `MaxConnReceiveWindow` equal to `MaxStreamReceiveWindow` (256 KiB
  each) when you turn growth on for a public socket: quic-go does not let the
  application refuse a stream window's growth, only the connection's, so a
  stream of a connection nobody marked can still reach the connection window
  (512 KiB by default, twice today's stream window); with equal values it
  stays at its start, exactly as without growth, and a marked connection
  grows from there.

Measured on a path with 100 Mbit/s each way, one stream read as fast as it
arrives, 3 s after a 2 s warm-up (`KOLO_TRANSPORT_GROWTH=1 go test -run
TestGrowthGoodputReport -v .`):

| Round trip | Default (no growth) | Growth on, not marked | Growth on, 256 KiB connection window, not marked | Marked |
|---|---|---|---|---|
| 20 ms | 10.10 MB/s | 12.24 | 10.19 | 12.24 |
| 50 ms | 4.43 | 8.29 | 4.72 | 12.24 |
| 100 ms | 2.01 | 3.98 | 2.00 | 12.24 |
| 200 ms | 1.22 | 2.45 | 1.22 | 11.25 |
| 300 ms | 0.81 | 1.62 | 0.81 | 8.59 |

12.24 MB/s is the 100 Mbit/s link itself. At 100 ms, `TestTrustedPeerGetsHigherGoodput` measured 2.40, 4.75, 2.34 and 12.24 MB/s in another run. The
third column is the setting recommended above: a connection nobody marked gets
what it got without growth.

## Congestion control

A connection paces what it sends with a congestion controller. The default, `"reno"`, is quic-go's own: Reno with hybrid slow start,
which treats every lost packet as a sign of a full queue and halves its window. On a path that also loses packets at random, as a
mobile link can, that caps the rate far below the link: one stream over 20 Mbit/s with 1 percent loss and 120 ms of round trip
reads at 1.4 to 1.8 Mbit/s, and more streams on the same connection do not help, since they share one window.

`"bbr"` estimates the link's rate and round trip instead and does not slow down for isolated losses.

```go
cfg := &kolotransport.Config{CongestionControl: "bbr"}
l, err := kolotransport.Listen(ctx, "127.0.0.1:0", cfg)
// ...
conn, err := kolotransport.Dial(ctx, addr, cfg)
```

- Each end paces its own sending, so set it on the end that sends the data: both ends for a transfer that goes both ways. It applies
  to every connection the `Config` makes: `Dial`, the ones `Listener.Accept` returns and the ones `Listener.Dial` returns. Any
  other value than `""`, `"reno"`, `"cubic"` and `"bbr"` is `ErrInvalidConfig`. `""` is `"reno"`, and with it nothing changes.
- A connection keeps its controller when the peer's address changes (a NAT gives the dialer a new port).
- `"cubic"` is the Cubic branch that quic-go ships but never turns on. It grows its window faster than Reno after a loss, and it
  reacts to loss the same way, so it does not help on a path with random loss; see the table below.
- BBR is only as fast as the receive window: with the default windows, 256 KiB per stream, a stream moves at most window / round
  trip, so raise the windows, or use `WindowGrowth` for trusted peers (above), together with BBR.

Measured with `internal/netsim` (20 Mbit/s each way, 60 ms of delay each way, 20 ms of jitter, 1 percent random loss each way,
8 MiB windows, one stream, goodput of the last 5 s of an 8 s run; `TestBBRFillsALossyPathThatRenoDoesNot`):

| Sender | Reno | BBR |
|---|---|---|
| the connection `Listener.Accept` returned | 1.83 Mbit/s | 18.86 |
| the connection `Dial` returned | not run | 18.91 |
| the connection `Listener.Dial` returned | not run | 18.71 |

All three controllers, from `TestImpairedBulkReport` (`KOLO_TRANSPORT_IMPAIR=15 go test -run TestImpairedBulkReport -v .`): one
stream for 15 s over a symmetric link with 60 ms of delay each way, 20 ms of jitter, random loss each way and 8 MiB windows. Each cell is
the goodput over the whole run, then over its last 5 s, in Mbit/s (one run per cell, on a computer shared with other work: the 100 Mbit/s
rows were limited by its CPU, which the simulator and both ends share, not by the link):

| Link | Loss | Reno | Cubic | BBR |
|---|---|---|---|---|
| 20 Mbit/s | 0 | 7.46 / 9.70 | 6.24 / 7.21 | 18.14 / 17.61 |
| 20 Mbit/s | 1% | 1.82 / 1.22 | 1.77 / 0.96 | 17.96 / 17.72 |
| 20 Mbit/s | 3% | 0.93 / 0.68 | 0.82 / 0.55 | 17.95 / 17.87 |
| 100 Mbit/s | 0 | 7.86 / 9.95 | 10.23 / 11.17 | 42.87 / 44.15 |
| 100 Mbit/s | 1% | 1.70 / 1.25 | 1.78 / 0.97 | 44.74 / 47.33 |
| 100 Mbit/s | 3% | 0.95 / 0.69 | 0.81 / 0.55 | 52.81 / 53.41 |

Without loss both Reno and Cubic take a long time to fill the link (hybrid slow start leaves slow start early), and BBR does not. With
loss Cubic is no faster than Reno, so choose `"cubic"` only if you want to compare it. With the default 256 KiB windows the same
report gives, at 3 Mbit/s up and 12 down with 1% loss, Reno 1.78, Cubic 1.74 and BBR 6.88 Mbit/s, and at 3% loss 0.93, 0.81 and 5.37:
the window then limits BBR.

Where the loss is not random, nothing here says BBR helps: the paths measured have independent random loss only, not the
bursts, reordering or policers of a real network.

When not to turn it on:

- Calls and live media stay on `"reno"`. BBR keeps a queue in the bottleneck (see "Queue" in [limits.md](limits.md)), and a call
  feels that delay. Do not set it on a connection that carries a call, or share a connection between a call and a bulk transfer
  that uses BBR.
- BBR is unfair to a Reno flow. On a simulated 20 Mbit/s link shared by two connections (a throwaway harness outside this
  repository, one 40 s run per cell, goodput of the last 30 s), a BBR connection leaves a Reno one almost nothing when the
  bottleneck queue is 100 ms or less:

  | Queue | BBR and Reno (Mbit/s) | BBR and BBR (Mbit/s) | Reno and Reno (Mbit/s) |
  |---|---|---|---|
  | 25 ms | 18.64 and 0.12 | 10.35 and 8.59 | 12.71 and 5.23 |
  | 100 ms | 18.42 and 0.43 | 11.07 and 7.93 | 12.93 and 6.67 |
  | 1000 ms | 12.11 and 7.23 | 8.46 and 10.35 | 16.09 and 3.50 |

  So a BBR transfer on a shared home or office link starves the other applications' flows on it. Turn it on for a transfer the
  user asked for and wants fast, not for background traffic. Fairness against real TCP flows and other stacks was not measured.
- The cause of a slow link may not be random loss. Measure first: on a 20 Mbit/s path without loss, BBR and Reno reach about the
  same rate (BBR 18.3 to 18.5 Mbit/s, Reno 15.0 with a 25 ms queue and 19.0 to 19.5 with a 100 ms or 1000 ms queue; one 12 s run
  each, goodput of the last 8 s).
- BBR is the standard profile of one implementation (BBRv1 shape, not BBRv3). It ignores loss as a signal, so a path that
  polices the rate with a token bucket can see heavy loss from it; that was not measured.

## Connection statistics

`Conn.Stats()` returns a `ConnStats` snapshot of what one connection measured. It is cheap (a few atomic reads), takes no lock a network read can hold (a path change swaps the
controller under a lock for microseconds), is safe to call from any goroutine, reads each field on its own (not one atomic snapshot, so two
fields can disagree for one sample), and returns the zero `ConnStats` once the connection has ended, so read it before you close the
connection if you want the final numbers. It is the transport's view of one end: what this end sent and what it learned from the
peer's acknowledgments.

```go
s := conn.Stats()
fmt.Printf("rtt %v (min %v) window %d in flight %d lost %d of %d packets via %s\n",
	s.SmoothedRTT, s.MinRTT, s.CongestionWindow, s.BytesInFlight, s.PacketsLost, s.PacketsSent, s.CongestionController)
```

| Field | Meaning | Unit |
|---|---|---|
| `SmoothedRTT` | Moving average of the round-trip samples (RFC 9002, section 5.3). 0 until the current path has a sample | duration |
| `MinRTT` | Lowest round trip seen on the current path, close to the propagation delay | duration |
| `LatestRTT` | The last round-trip sample | duration |
| `RTTVariation` | Mean deviation of the samples from `SmoothedRTT`, a measure of jitter | duration |
| `BytesSent`, `BytesReceived` | QUIC packet bytes over the life of the connection, including headers, retransmissions and duplicates, not UDP or IP headers | bytes |
| `PacketsSent`, `PacketsReceived` | Packets over the life of the connection, including packets later declared lost, duplicates and packets that could not be processed | packets |
| `PacketsLost`, `BytesLost` | Packets this end sent that were not acknowledged in time and their size. A packet that arrives late still counts | packets, bytes |
| `CongestionWindow` | How many bytes this end may have in flight, as set by the controller | bytes |
| `BytesInFlight` | Bytes sent and neither acknowledged nor declared lost | bytes |
| `CongestionController` | `"reno"`, `"cubic"` or `"bbr"`, from `Config.CongestionControl` | name |
| `BandwidthEstimate` | The bottleneck bandwidth BBR measured (the highest delivery rate of the last ten round trips). 0 for `"reno"` and `"cubic"`, which measure none, and before the first sample | bytes per second |
| `MaxDatagramSize` | Largest UDP payload this end sends on the current path; it grows as path MTU discovery succeeds and falls back to 1200 after a black hole (see "Packet size") | bytes |
| `PathChanges` | How often the connection moved to a new path, as when the peer's NAT gives it a new port. The RTT fields, `CongestionWindow` and `BandwidthEstimate` start over on a new path | count |

What the numbers say about a slow transfer:

- `BytesInFlight` near `CongestionWindow` means the controller limits the transfer. With `"reno"` or `"cubic"` a lot of
  `PacketsLost` explains a small window; with `"bbr"`, `BandwidthEstimate` is roughly the rate the link gave.
- A `SmoothedRTT` far above `MinRTT` means a queue on the path (see "Queue" in [limits.md](limits.md)); a high `RTTVariation`
  means a jittery link.
- `BytesInFlight` far below `CongestionWindow` while the transfer is slow means this end is not limited by the network: the
  application is not writing fast enough, or the peer's receive windows (see "Faster transfers for trusted peers") are full.
- `PacketsLost` over `PacketsSent` is an upper bound of the loss rate of this end's sending. A congestion signal of ECN counts as one lost packet, as in quic-go's own sender, so a peer that marks packets can raise it without any loss.
- `MaxDatagramSize` is not the path MTU: it is what this end sends after probing, at most 1452 bytes.

Retransmissions are not counted apart: QUIC resends the lost data in new packets, so `PacketsLost` is the number of packets
that had to be replaced.

## Maintenance

kolo-transport carries its own copy of quic-go (go.mod has no quic-go) because quic-go has no way to plug in a congestion
controller, and a BBR controller. Nothing else is vendored.

| Where | What |
|---|---|
| `internal/quicgo` | quic-go at the release named by `pin=` in `scripts/vendor-quicgo.sh` (also in `internal/quicgo/VERSION` with the module checksum), the non-test Go files of the packages the connection needs, plus `internal/quicgo/PATCH`. Generated: never edit it by hand |
| `internal/quicgo/PATCH` | The only change to quic-go: the hook for a congestion controller, the counters `Conn.Stats` reads, the datagram send that never blocks and the path MTU black hole detection (a header lists the files) |
| `internal/bbr` | The BBR controller, copied once, with its license file |
| `THIRD_PARTY_NOTICES` | The MIT notices of both and the BSD notice of the QUICHE code the BBR is a port of, which must stay |

To re-vendor, or to move to a new quic-go release:

1. Set `pin=` in `scripts/vendor-quicgo.sh` to the new tag. Nothing is needed for the same release.
2. Run `scripts/vendor-quicgo.sh` from the repository root. It downloads the release with `go mod download` (checked against the
   checksum database), copies the files, applies `PATCH`, rewrites the import paths of Go code only, writes `VERSION`,
   `CHECKSUMS` and `staticcheck.conf`, formats, builds and vets.
3. If `PATCH` does not apply to the new release: `scripts/vendor-quicgo.sh pristine /tmp/q` exports the release unchanged;
   `cp -R /tmp/q /tmp/q-edited`, make the change in `/tmp/q-edited` (the hook is small: see the header of `PATCH`), then
   `scripts/vendor-quicgo.sh diff /tmp/q-edited` rewrites `PATCH` (its header is kept) and step 2 vendors again.
4. Run the whole gate, also with `KOLO_TRANSPORT_TEST_CC=bbr go test ./...` and `go test -race ./...`, rerun the heap
   measurement of AGENTS.md, and record the new version in CHANGELOG and `THIRD_PARTY_NOTICES`.

Two checks guard the tree. `scripts/vendor-quicgo.sh verify` re-vendors the pinned release into a temporary directory and fails
if `internal/quicgo` differs from it in any file; the download is checked against the checksum database and against `pinsum=` in
the script. Run it before every tag. `TestVendoredQuicGoIsUpstreamPlusPatch` runs the same comparison on a copy of the tree,
offline, when the pinned release is in the module cache (it skips, saying so, when it is not), so `go test` proves the tree is
upstream plus `PATCH` on a machine that has downloaded the pinned release before (the script does it). `TestVendoredQuicGoMatchesThePin` needs no
cache: it fails on a file that disagrees with `CHECKSUMS`, a changed pin without a re-vendor, a file added or removed, but it
cannot see an edit that also rewrites `CHECKSUMS`. With the vendored copy a quic-go fix reaches kolo-transport only
when it is re-vendored: look at quic-go's releases and security advisories before every release of kolo-transport and at least once
a month.

`"cubic"` needs the Cubic code of quic-go's `internal/congestion`, which upstream is removing because it is unused: at a release that
has dropped it, `PATCH` must carry it or the value goes (`TestEveryConnectionRunsTheNamedController` fails first).

To drop BBR: delete `internal/bbr` and `cc.go`, make `Config.CongestionControl` refuse `"bbr"`; `internal/quicgo` can then be replaced
by the module again.

## Caller-supplied sockets

`ListenConn` runs on any `net.PacketConn` with an IPv4 address. If it is
not a `*net.UDPConn`, datagrams from senders `WritePacket` would refuse
are dropped, since quic-go would move a connection to such an address.
quic-go uses a conn that is not a `*net.UDPConn` through a basic wrapper (`sys_conn.go` `wrapConn`): no
ECN, no GSO, no batched reads, and, without `SyscallConn`, no
Don't-Fragment bit, so path MTU discovery is off and packets stay at 1200
bytes (the first packet size, which every connection starts with). The conn must carry datagrams of at least
1200 bytes of payload in both directions; quic-go reads into a 1452-byte buffer and
truncates anything longer. It should implement `SetReadBuffer` and
`SetWriteBuffer`, or quic-go logs a buffer warning once.

`ReadFrom` must return an error once a read deadline passes or `pc` is
closed: quic-go's `Close` sets a deadline in the past and waits for its
reader to stop (`transport.go` `Close`), and `Listener.Close` waits for
quic-go. If the reader has not stopped within 1s, `Listener.Close` closes
`pc` to unblock it, and then waits for that. For a read error, quic-go only checks
`Temporary()` (`transport.go` `listen`): a `net.Error` with `Temporary()`
true is retried at once, so a conn that keeps returning one makes quic-go
spin; any other error closes the transport and every connection on it,
for good. `ReadFrom` must report `*net.UDPAddr` senders: Retry tokens and
the listener's address checks rely on it.

## Packets

`WritePacket` and `ReadPacket` share the listener's socket with QUIC, for
hole punching and other small signals. quic-go treats a datagram whose
first byte has both high bits clear as non-QUIC (`transport.go`
`handlePacket`) and queues it for `ReadPacket`. Its queue holds at most 32
datagrams and drops the rest, so read promptly. quic-go only starts
queueing after the first read call, so `Listen` makes that call right
away and keeps any datagram it returns for the first `ReadPacket`;
datagrams are queued from the moment the listener exists, so a listener
that never calls `ReadPacket` still holds up to 32 of them (about 45 KiB
of packet buffers). They are not filtered by `AcceptFunc` and carry no
protection: anyone can send one from any address, and the sender address
may be spoofed.

## Destinations

`Dial`, `Listener.Dial` and `WritePacket` share one destination policy:
the addresses listed for `Listener.Dial` above are refused before anything
is sent. Loopback, private, link-local and subnet broadcast addresses
(such as 192.168.1.255) are not: without the netmask, a subnet broadcast
address looks like a host. Filter any address a peer supplies before you
dial or write to it, also with `ListenConn`. On the sockets `Listen` and
`Dial` create, the library clears `SO_BROADCAST` (Go sets it on every UDP
socket), so the kernel refuses the broadcast addresses it knows on Linux,
macOS, the BSDs, Solaris, AIX and Windows; on js, wasip1 and plan9 it is
left as is. `ListenConn` leaves `SO_BROADCAST` as is; quic-go may set
buffer sizes, the DF bit and packet-info options.
`ReadPacket` drops datagrams from senders that `WritePacket` would refuse.
