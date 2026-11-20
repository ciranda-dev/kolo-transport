//go:build unix

package kolotransport

import (
	"net/netip"
	"syscall"
	"testing"
)

func TestDialSocketNoBroadcast(t *testing.T) {
	pc, err := dialConn(netip.MustParseAddrPort("127.0.0.1:9"))
	if err != nil {
		t.Fatal(err)
	}
	defer pc.Close()
	rc, err := pc.SyscallConn()
	if err != nil {
		t.Fatal(err)
	}
	var v int
	var gerr error
	if err := rc.Control(func(fd uintptr) {
		v, gerr = syscall.GetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_BROADCAST)
	}); err != nil || gerr != nil {
		t.Fatal(err, gerr)
	}
	if v != 0 {
		t.Fatalf("SO_BROADCAST is %d", v)
	}
}
