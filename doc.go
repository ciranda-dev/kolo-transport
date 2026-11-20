// Package kolotransport carries multiplexed streams over one QUIC (RFC 9000)
// connection between two Kolo peers.
//
// It does not authenticate the peer. Each Listener presents an ephemeral
// self-signed certificate and the dialer accepts any such certificate, so
// TLS 1.3 gives confidentiality and integrity against passive observers but
// no identity, and an active man in the middle is not detected. Identity
// must come from a higher layer, for example kolo-id signatures over
// ExportKey.
//
// Experimental: the API may change before v1.0.0.
package kolotransport
