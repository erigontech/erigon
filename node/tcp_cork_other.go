//go:build !linux

package node

import "syscall"

func setTCPCork(syscall.RawConn, int) {}
