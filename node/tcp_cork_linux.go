//go:build linux

package node

import (
	"syscall"

	"golang.org/x/sys/unix"
)

func setTCPCork(raw syscall.RawConn, v int) {
	_ = raw.Control(func(fd uintptr) { _ = unix.SetsockoptInt(int(fd), unix.IPPROTO_TCP, unix.TCP_CORK, v) })
}
