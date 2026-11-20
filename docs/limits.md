# Limits

Every option, default and bound of kolo-transport, with the reason for it. A value out of range returns `ErrInvalidConfig`.

## Config

`Config` (nil or a zero field means the default; out of range is
`ErrInvalidConfig`):

| Option | Default | Range | Why |
|---|---|---|---|
| `MaxConns` (per listener, handshaking or open) | 100 | 1 to 10000 | Security: bounds the state a flood from many addresses makes a public node hold |
| `MaxConnsPerIP` (per listener and source address) | 4, or `MaxConns` if lower | 1 to `MaxConns` | Security: stops one address from holding every slot |
| `MaxDialed` (connections open from `Listener.Dial`) | 32 | 1 to 10000 | Memory: bounds the unread data dialed connections can make this node hold |
| `MaxIncomingStreams` (concurrent, per connection, bidirectional and unidirectional each) | 16 | 1 to 1000 | Measured: quic-go keeps about 96 heap bytes per byte read on an open stream, so the worst case per connection is about 856 MiB at 16 and 4.8 GiB at 100 (see below) |
| `HandshakeTimeout` (also the life of a Retry token) | 10s | 1s to 60s | Security: a stalled handshake holds a `MaxConns` slot until it expires |
| `IdleTimeout` | 30s | 1s to 10m | Memory: ends a connection, and frees its slot, once the peer or the path is gone |
| `KeepAlivePeriod` (always on) | 5s, or `IdleTimeout`/2 if lower | 500ms to below `IdleTimeout` | Protocol: keeps a connection open while the peer answers; the minimum is 500ms so that the default fits a 1s `IdleTimeout` |
| `MaxMemory` (must cover (`MaxConns` + `MaxDialed`) × `MaxConnReceiveWindow`, bytes; unread data, not heap) | 1 GiB (`DefaultMaxMemory`) | 64 MiB to 64 GiB | Memory: checks that the windows of every connection fit |
| `AcceptFunc` (source IP filter) | nil (allow all) | any `func(netip.AddrPort) bool` | Security: refuses source IPs before an attempt takes a slot or any connection state |
| `MaxStreamReceiveWindow` (advanced; unread bytes per stream) | 256 KiB (`DefaultMaxStreamReceiveWindow`) | 64 KiB to 16 MiB | Measured: about 176 heap bytes per unread byte and 96 per byte read on an open stream, with 1-byte frames (see below) |
| `MaxConnReceiveWindow` (advanced; unread bytes per connection) | 512 KiB (`DefaultMaxConnReceiveWindow`) | 64 KiB to 64 MiB, at least `MaxStreamReceiveWindow` | Measured: the same heap factors, for the unread data of one connection |
| `WindowGrowth` (lets `Conn.AllowWindowGrowth` raise the windows of a trusted connection) | false | true or false | Memory: off means every window stays fixed, exactly as before |
| `MaxGrownStreamReceiveWindow` (growth ceiling of a trusted stream; `WindowGrowth` only) | 4 MiB (`DefaultMaxGrownStreamReceiveWindow`), or `MaxStreamReceiveWindow` if higher | `MaxStreamReceiveWindow` to 16 MiB | Measured: with 100 ms of round trip a 4 MiB window carries 335 Mbit/s |
| `MaxGrownConnReceiveWindow` (growth ceiling of a trusted connection; `WindowGrowth` only) | 8 MiB (`DefaultMaxGrownConnReceiveWindow`), or `MaxConnReceiveWindow` and `MaxGrownStreamReceiveWindow` if higher | the larger of `MaxConnReceiveWindow` and `MaxGrownStreamReceiveWindow`, to 64 MiB | Memory: quic-go keeps the connection window at 1.5 times a growing stream window |
| `MaxGrownMemory` (window growth shared by the trusted connections of a `Listener`; `WindowGrowth` only) | 64 MiB (`DefaultMaxGrownMemory`) | 1 MiB to 64 GiB | Memory: bounds how many connections can hold a grown window |
| `Network` (UDP network of the socket `Listen` opens) | `"udp4"` | `"udp4"` only | Protocol: v0.1.x is IPv4 only |
| `CongestionControl` (pacing of what this end sends) | `"reno"` | `"reno"`, `"cubic"` or `"bbr"` | Protocol: Reno is quic-go's own and `"cubic"` is the Cubic branch of the same code; BBR fills a link that loses packets at random, but holds a queue and takes bandwidth from Reno flows (see "Queue" below and the [guide](guide.md#congestion-control)) |
| Retry token life (fixed, follows `HandshakeTimeout`) | 10s | 1s to 60s | Security: quic-go sets it to the handshake timeout (quic-go config.go:17-23); a longer life also lengthens the window in which a token can be replayed from one IP |
| NEW_TOKEN token life (fixed) | expired at any age, even after the wall clock steps back (`MaxTokenAge` is the smallest duration) | fixed | Security: every new attempt goes through Retry |
| Accept queue (fixed in quic-go) | 32 handshaked connections | fixed | Protocol: fixed by quic-go (internal/protocol/params.go:47); `MaxConns` bounds the total |
| Non-QUIC packet queue (fixed in quic-go) | 32 datagrams | fixed | Protocol: fixed by quic-go (transport.go:709) |
| `MaxPacketSize` (non-QUIC datagram) | 1200 bytes | fixed | Protocol: the 1200 bytes QUIC starts with, so it fits any path QUIC works on |
| Read buffer (fixed in quic-go) | 1452 bytes | fixed | Protocol: fixed by quic-go (internal/protocol/protocol.go:111) |
| `CloseWithError` message | 256 bytes | 0 to 256 | Protocol: 256 bytes always fit one packet; peers drop the text (see Errors in [guide.md](guide.md#errors)) |
| `Listener.Close` fallback (fixed) | 1s | fixed | Protocol: closes a `PacketConn` that ignores read deadlines (see [Caller-supplied sockets](guide.md#caller-supplied-sockets)) |

quic-go file and line references are at quic-go v0.65.0.

`MaxConns`, `MaxConnsPerIP`, `MaxDialed`, `MaxMemory` and `AcceptFunc`
apply to `Listen` and `ListenConn` only; `CongestionControl` applies to `Listen`,
`ListenConn` and `Dial`, and so to every connection they make; `Network` applies to `Listen`,
`ListenConn` checks it but keeps the caller's socket as it is, and `Dial`
ignores it; the windows apply to `Listen` and `Dial`, and quic-go gets each as
both its initial and its maximum window, so it never grows them, unless
`WindowGrowth` is on (see "Grown windows" below). `Config` is not
comparable: it has a func field. The three `MaxGrown` options without
`WindowGrowth` are `ErrInvalidConfig`.

`AcceptFunc` filters by address, not identity: an address says nothing
about who the peer is. It runs after Retry and before the attempt takes a
slot or any connection state, and gets the source as a plain IPv4 address
with its port. A peer can move its connection to another allowed IPv4 address
after the handshake; `AcceptFunc` and `MaxConnsPerIP` see only the address
it started from. Retry shows only that the IP
received a packet from the listener within the last `HandshakeTimeout`
(quic-go binds the token to the IP only, in
`internal/handshake/token_generator.go`): the port is whatever the sender
wrote, and anyone who saw a Retry token on the path can replay it for that
IP until it expires. Filter by IP, never by port.

If `AcceptFunc` returns false or panics, the attempt is refused and
nothing is counted; the panic value is dropped. quic-go answers with
CONNECTION_REFUSED (`ErrRefused` on the dialer). It drops the answer when
its 4-slot refusal queue is full; the dialer then retransmits its Initial,
the hook runs again, and a later refusal still gives `ErrRefused`. Other
errors come only if every refusal is dropped: once the listener's
`HandshakeTimeout` (the life of the Retry token) has passed, its
retransmits get CONNECTION_CLOSE with INVALID_TOKEN, which the dialer
reports as `ErrTransport` with code 0x0b; and if nothing reaches it for
its handshake idle timeout (half its `HandshakeTimeout` after the last
packet it received), it gives `ErrTimeout`, or its context error first.
The hook runs on quic-go's server goroutine, which handles
every packet that does not belong to an open connection: while it runs,
new attempts wait and then are dropped, open connections keep working,
and `Listener.Close` waits for it to return. Keep it fast and
non-blocking, and never call `runtime.Goexit` or `t.Fatal` in it, which
stops the listener from accepting. It runs for each Initial packet of an
attempt that has no connection yet: twice for a refused attempt with Go's
default client, and also more than once for an attempt it allowed that
`MaxConns` or `MaxConnsPerIP` then refused.

A peer whose round
trip is longer than about half of `HandshakeTimeout` cannot connect: the
Retry round trip and the TLS handshake must both fit in the token's life.

`MaxStreamReceiveWindow` and `MaxConnReceiveWindow` cap the unread data a
peer can make one stream and one connection buffer. At run time, the
unread data of a listener is bounded by (`MaxConns` + `MaxDialed`) ×
`MaxConnReceiveWindow`; `MaxMemory` is a check on that product, made by
`Listen`:

    (MaxConns + MaxDialed) × MaxConnReceiveWindow ≤ MaxMemory

With `WindowGrowth` on, the check adds the growth the connections can hold,
the smaller of `MaxGrownMemory` and (`MaxConns` + `MaxDialed`) ×
(`MaxGrownConnReceiveWindow` - `MaxConnReceiveWindow`).

The defaults need 66 MiB of the 1 GiB: the default `MaxMemory` leaves
headroom for raising `MaxConns` or the windows, and does not by itself
bound the heap. Otherwise `Listen` returns `ErrInvalidConfig` with a
message naming the ways out, such as "(MaxConns 1000 + MaxDialed 32) ×
MaxConnReceiveWindow 512 KiB needs 516 MiB of receive window, above
MaxMemory 256 MiB; raise MaxMemory or lower MaxConns, MaxDialed or
MaxConnReceiveWindow". The 64 GiB maximum is a sanity limit.

Small windows limit throughput to about window / RTT: at 100 ms, the
defaults allow about 40 Mbit/s per connection and 20 Mbit/s per stream.

The heap that unread data takes depends on how the peer sends it. quic-go
keeps every received STREAM frame until it is read. A frame under 128
bytes becomes a new 48-byte `StreamFrame` with its own data allocation, a
16-byte release closure and an entry in the stream's frame map; a frame of
128 bytes or more is copied into a pooled `StreamFrame` with a 1452-byte
buffer. Measured with `go test -run '^$' -bench
'UnreadHeapPerByte|RetainedHeapAfterRead' -benchtime 1x` (a quic-go peer
over loopback, heap after GC; Apple M5, Go 1.27):

- Unread data: 1.2 heap bytes per unread byte for one 64 KiB write, 176
  for 1-byte frames one per packet, and about 163 for 1-byte frames from
  64 streams sharing packets.
- Retained after reading: a stream filled with 1-byte frames and then read
  to the end still holds about 96 heap bytes per byte received while it
  stays open, the same for 64 KiB and 256 KiB, and it adds up across
  streams (8 streams hold 8 times as much). Closing the stream in both
  directions and reading it to EOF releases it.

The retention is quic-go behavior: each stream keeps its frames in a map
(`frameSorter.queue` in `frame_sorter.go`, created per stream in
`receive_stream.go`), and a Go map keeps its buckets after delete. The map
grows with the most frames held at once, so it is bounded by the stream
window. Worst case per connection, from the measured factors:

    2 × MaxIncomingStreams × MaxStreamReceiveWindow × 96   retained
      + MaxConnReceiveWindow × 176                         unread

The 2 counts bidirectional and unidirectional streams, which are capped
separately. At the defaults that is 2 × 16 × 256 KiB × 96 = 768 MiB plus
512 KiB × 176 = 88 MiB, about 856 MiB per connection, about 3.3 GiB per
source address (4 connections) and about 84 GiB for all 100 connections
(25 addresses), plus about 27 GiB for the 32 connections `Listener.Dial`
may open. With the default windows, one connection's worst case is
856 MiB with 16 streams, about 1.6 GiB with 32, about 3.1 GiB with 64 and
about 4.8 GiB with 100. Streams this side opens add their own retention. To bound
the heap, lower `MaxConns`, `MaxDialed`, `MaxIncomingStreams`, `MaxStreamReceiveWindow`
or `MaxConnReceiveWindow`. Also set `GOMEMLIMIT` (or
`debug.SetMemoryLimit`) a little below the memory the process may use, so
the Go runtime collects harder before the system runs out. It is a soft
limit: heap that peers keep live is not freed, so size the options so that
the formula fits under it. An application that does not read its streams
promptly keeps that data in memory, and a stream keeps its retained heap
until it is closed in both directions, so read promptly and close streams
when done.

For a fast link, raise the windows and size the rest to match. With
`MaxStreamReceiveWindow` 4 MiB and `MaxConnReceiveWindow` 8 MiB (about 335
and 670 Mbit/s at 100 ms), the default 100 connections plus 32 dialed need 1056 MiB of
window, above the default `MaxMemory`, and the heap worst case
per connection is about 12 GiB retained (2 × 16 × 4 MiB × 96) plus 1.4 GiB
unread. Lower `MaxIncomingStreams`, `MaxConns` and `MaxDialed` with it: with
`MaxIncomingStreams` 4, `MaxConns` 8, `MaxDialed` 8 and `MaxMemory` 128 MiB, the worst
case is about 4.4 GiB per connection (3 GiB retained plus 1.4 GiB unread)
and about 70 GiB in total.

## Grown windows

With `WindowGrowth` off, everything above stands: no window ever grows. With
it on, a connection starts with the same windows as without (256 KiB per
stream, 512 KiB per connection by default) and quic-go's auto-tuning can
double them while the application reads fast. Two rules bound what that costs:

- The connection window only grows for a connection the application marked
  with `Conn.AllowWindowGrowth`, by `AllowConnectionWindowIncrease`, and only
  while the budget `MaxGrownMemory` has room: each increase takes its size from
  it and is refused when the budget is not enough. A connection nobody marked,
  one that opened before the application knew the peer, a marked connection
  after the budget ran out, and a connection that ended (its share is returned
  at once, and a grant that races with the end is refused) never grow. The budget
  counts window credit, so a marked connection that stops reading keeps its share
  until it ends, which `IdleTimeout` does once the peer or the path is gone.
- quic-go lets a stream window grow with no veto, only the connection window
  has one. The data a stream holds is still bounded by the connection window, so
  for a connection that is not marked a stream holds at most
  min(`MaxGrownStreamReceiveWindow`, `MaxConnReceiveWindow`) bytes. With the
  defaults that is 512 KiB instead of 256 KiB; with `MaxConnReceiveWindow` equal
  to `MaxStreamReceiveWindow` it is what it was.

Worst case of heap for one connection nobody marked, from the measured
factors above:

    2 × MaxIncomingStreams × min(MaxGrownStreamReceiveWindow, MaxConnReceiveWindow) × 96   retained
      + MaxConnReceiveWindow × 176                                                         unread

At the defaults that is 2 × 16 × 512 KiB × 96 = 1536 MiB plus 88 MiB, about
1.6 GiB, against about 856 MiB with growth off. With `MaxConnReceiveWindow`
256 KiB, equal to `MaxStreamReceiveWindow`, it is 768 MiB plus 44 MiB, about
812 MiB, below the 856 MiB of today, and the per-address and total figures
above scale the same way. The retained part follows from the measured factors
and the connection window; it was not measured with a grown stream window.
`TestFloodOfSmallFramesStaysWithinTheInitialWindow` floods one-byte frames at a
connection nobody marked and checks the bytes it holds against the window and its
heap against the 176 factor.

A marked connection adds, at most, its growth. Let G be the growth the budget
lets all marked connections of a listener hold, the smaller of `MaxGrownMemory`
and (`MaxConns` + `MaxDialed`) × (`MaxGrownConnReceiveWindow` -
`MaxConnReceiveWindow`). A marked connection that reaches its ceiling can
hold, in the worst case, the same sum with the grown windows:

    2 × MaxIncomingStreams × min(MaxGrownStreamReceiveWindow, MaxGrownConnReceiveWindow) × 96
      + MaxGrownConnReceiveWindow × 176

and the number of such connections is at most G / (`MaxGrownConnReceiveWindow`
- `MaxConnReceiveWindow`). At the defaults one such connection is 2 × 16 ×
4 MiB × 96 = 12 GiB plus 8 MiB × 176 = 1.4 GiB, about 13.4 GiB, and the 64 MiB
of budget lets 8 of them exist: about 107 GiB of heap in the theoretical worst
case, for peers the application has authorized (against 856 MiB for each
connection nobody marked, in the same listener). Mark only peers you would trust
with that, lower `MaxIncomingStreams` for a listener that marks (with 4 the
figure is a quarter), and lower `MaxGrownMemory` to allow fewer. A marked peer
that sends large frames, as an honest one does, costs about 1.2 heap bytes per
unread byte: 64 MiB of grown window is about 80 MiB of heap.

## Queue (BBR)

Reno with random loss keeps the bottleneck's queue nearly empty, but only because it does not use the link. BBR fills the link and
keeps a standing queue in the bottleneck, up to the queue the bottleneck has. Measured with `internal/netsim` (20 Mbit/s each way,
60 ms of delay each way, 20 ms of jitter, 8 MiB windows, one stream, 12 s, goodput of the last 8 s, one run per cell). The delay is
the largest queueing delay of the data direction over the whole run, start-up included:

| Bottleneck queue | Loss | Reno | BBR |
|---|---|---|---|
| 25 ms | none | 14.97 Mbit/s, 25 ms | 18.50 Mbit/s, 26 ms |
| 25 ms | 1% | 1.50 Mbit/s, 6 ms | 18.67 Mbit/s, 26 ms |
| 100 ms | none | 19.00 Mbit/s, 33 ms | 18.53 Mbit/s, 101 ms |
| 100 ms | 1% | 1.44 Mbit/s, 6 ms | 19.00 Mbit/s, 101 ms |
| 1000 ms | none | 19.48 Mbit/s, 36 ms | 18.34 Mbit/s, 149 ms |
| 1000 ms | 1% | 1.44 Mbit/s, 8 ms | 18.75 Mbit/s, 151 ms |

So a BBR connection adds up to 100 ms or more of delay to everything else that crosses the same bottleneck (a bufferbloated router:
about 150 ms here), which a call feels. Keep calls and other latency-sensitive traffic on `"reno"`, and never mix them with a BBR
transfer on one connection. The receive window also bounds the queue: a window near the bandwidth-delay product holds less in flight.

A BBR connection also keeps one 88-byte sample for every packet number between the oldest packet it still waits for and the
newest it sent, because the packet numbers of ACK-only packets take a slot too. A peer that never acknowledges and sends
ack-eliciting packets, which makes this end fill the numbers in between with ACK-only packets, could make that span grow by
hundreds of thousands of slots (about 20 numbers per tracked packet, 87 MiB for a gap of a million). The span is capped at
40000 packet numbers (4 times quic-go's `MaxCongestionWindowPackets`, 10000: a full window, twice for the ACK-only packets
between its packets, twice again for margin); when a new packet falls further from the oldest one, the oldest samples are
dropped and an acknowledgment of such a packet gives no bandwidth sample, so only the estimate suffers, on a connection that
was not acknowledging anyway. The cap bounds the memory: 88 bytes × 65536 slots (the ring grows by doubling and does not
shrink) = 5.5 MiB per BBR connection, on top of the figures above, for the connections whose end chose `"bbr"`. `"reno"` and
`"cubic"` have no such structure.
