package sockopt

import (
	"net"
	"testing"
)

type plainConn struct{ net.PacketConn }

func TestNoBroadcastWithoutSyscallConn(t *testing.T) {
	if err := NoBroadcast(plainConn{}); err != nil {
		t.Fatal(err)
	}
}
