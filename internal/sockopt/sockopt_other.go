//go:build !unix && !windows

package sockopt

func clearBroadcast(uintptr) error { return nil }
