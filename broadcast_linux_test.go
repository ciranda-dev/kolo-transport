package kolotransport

import (
	"net/netip"
	"testing"
)

func TestWritePacketRefusesBroadcast(t *testing.T) {
	checkGoroutines(t)
	l := listen(t, nil)
	if err := l.WritePacket([]byte{1}, netip.MustParseAddrPort("127.255.255.255:9")); err == nil {
		t.Fatal("sent to the loopback broadcast address")
	}
}
