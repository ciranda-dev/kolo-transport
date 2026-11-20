// Package sockopt sets the socket options kolotransport needs on the sockets it creates.
package sockopt

import (
	"net"
	"syscall"
)

// NoBroadcast clears SO_BROADCAST on pc, which Go sets on every UDP socket; it does nothing without SyscallConn.
func NoBroadcast(pc net.PacketConn) error {
	sc, ok := pc.(syscall.Conn)
	if !ok {
		return nil
	}
	rc, err := sc.SyscallConn()
	if err != nil {
		return err
	}
	var serr error
	if err := rc.Control(func(fd uintptr) { serr = clearBroadcast(fd) }); err != nil {
		return err
	}
	return serr
}
