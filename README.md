# kolo-transport

One connection between two Kolo peers, with multiplexed streams over QUIC.

[![status: experimental](https://img.shields.io/badge/status-experimental-orange)](#status)
[![license: MIT OR Apache-2.0](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue)](#license)
[![go: 1.26+](https://img.shields.io/badge/go-1.26%2B-00ADD8)](go.mod)

## What it is

kolo-transport opens [QUIC version 1 (RFC 9000)](https://www.rfc-editor.org/info/rfc9000) connections secured by [TLS 1.3 (RFC 9001)](https://www.rfc-editor.org/info/rfc9001), with the loss recovery and Reno congestion control of [RFC 9002](https://www.rfc-editor.org/info/rfc9002), and an optional BBR controller after [Cardwell et al.](https://doi.org/10.1145/3012426.3022184). It builds on [quic-go](https://github.com/quic-go/quic-go) and gives each peer bidirectional and unidirectional streams, with hard limits on unread data, streams and connections.

It does not authenticate the peer. The listener presents an ephemeral self-signed certificate and the dialer accepts any such certificate, so TLS 1.3 protects against passive observers but gives no identity, and an active man in the middle is not detected. Identity comes from a higher layer, for example kolo-id signatures over `ExportKey`; kolo-secure does this for you.

It is Go only and builds without cgo. It is IPv4 only: sockets are `udp4`, and an IPv6 address, the v4-mapped form included, is refused with `ErrInvalidArgument`.

## Install

```sh
go get github.com/ciranda-dev/kolo-transport
```

## Quick start

An echo over loopback: a listener accepts one stream and copies it back.

```go
package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"time"

	kolotransport "github.com/ciranda-dev/kolo-transport"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	l, err := kolotransport.Listen(ctx, "127.0.0.1:0", nil)
	if err != nil {
		log.Fatal(err)
	}
	defer l.Close()
	go func() {
		c, err := l.Accept(ctx)
		if err != nil {
			return
		}
		s, err := c.AcceptStream(ctx)
		if err != nil {
			return
		}
		io.Copy(s, s)
		s.Close()
	}()

	c, err := kolotransport.Dial(ctx, l.Addr().String(), nil)
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()
	s, err := c.OpenStream(ctx)
	if err != nil {
		log.Fatal(err)
	}
	if _, err := s.Write([]byte("hello kolo")); err != nil {
		log.Fatal(err)
	}
	s.Close()
	reply, err := io.ReadAll(s)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("%s\n", reply)
}
```

```text
$ go run .
hello kolo
```

## API

| Symbol | What it does |
|---|---|
| `Listen`, `ListenConn` | Open a listener on an address, or on a `net.PacketConn` you supply |
| `Listener.Accept` | Return the next connection whose handshake has completed |
| `Dial`, `Listener.Dial` | Connect from a new socket, or from the listener's own socket so a NAT keeps one mapping |
| `Conn.OpenStream`, `Conn.OpenUniStream` | Open a bidirectional or unidirectional stream |
| `Conn.AcceptStream`, `Conn.AcceptUniStream` | Wait for the peer's next stream |
| `Conn.Stats` | Round-trip times, bytes, losses, congestion window, controller and path of the connection (zero after it ends) |
| `Conn.ExportKey` | Derive 32 bytes from the TLS session (RFC 9846 exporter) for a higher layer to sign |
| `Conn.Close`, `Conn.CloseWithError` | Close with code 0, or with a code and a message |
| `Listener.WritePacket`, `Listener.ReadPacket` | Send and receive small non-QUIC datagrams on the listener's socket, for hole punching |
| `Config` | Limits on connections, streams, memory and timeouts, and the congestion controller; a nil or zero field means the default |

Errors are sentinels for `errors.Is` (`ErrPeerClosed`, `ErrClosed`, `ErrRefused`, `ErrHandshake`, `ErrTimeout`, `ErrStreamReset`, `ErrStreamCanceled`, `ErrTransport`, `ErrMaxDialed`, `ErrInvalidConfig`, `ErrInvalidArgument`); those with a code are an `*Error`.

## Documentation

- [Guide](docs/guide.md): every call, errors, caller-supplied sockets, packets and destinations.
- [Limits](docs/limits.md): every option with its default, range and reason, and how memory is bounded.
- [Protocol and design](docs/protocol.md): the handshake, Retry, keep-alive, ALPN, the wire format and references.

## Status

Experimental. The API may change before v1.0.0.

## License

Licensed under either of the [Apache License, Version 2.0](LICENSE) or the [MIT license](LICENSE-MIT), at your option. The QUIC code in `internal/quicgo` and the congestion controller in `internal/bbr` are MIT-licensed (the controller is also a port of BSD-3-Clause code) and keep their notices: see [THIRD_PARTY_NOTICES](THIRD_PARTY_NOTICES).
