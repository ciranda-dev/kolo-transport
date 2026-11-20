//go:build unix

package sockopt

import (
	"net"
	"syscall"
	"testing"
)

func broadcast(t *testing.T, c *net.UDPConn) int {
	t.Helper()
	rc, err := c.SyscallConn()
	if err != nil {
		t.Fatal(err)
	}
	var v int
	var gerr error
	if err := rc.Control(func(fd uintptr) {
		v, gerr = syscall.GetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_BROADCAST)
	}); err != nil {
		t.Fatal(err)
	}
	if gerr != nil {
		t.Fatal(gerr)
	}
	return v
}

func TestNoBroadcast(t *testing.T) {
	c, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if broadcast(t, c) == 0 {
		t.Fatal("Go no longer sets SO_BROADCAST; this test needs a new baseline")
	}
	if err := NoBroadcast(c); err != nil {
		t.Fatal(err)
	}
	if v := broadcast(t, c); v != 0 {
		t.Fatalf("SO_BROADCAST is %d", v)
	}
}
