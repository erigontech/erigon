// Copyright 2026 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

//go:build darwin || freebsd || netbsd || openbsd || dragonfly

package p2p

import (
	"errors"
	"os"
	"sync"

	"golang.org/x/sys/unix"

	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/log/v3"
)

// routeNotifier signals on any message from a PF_ROUTE socket, which the kernel
// emits on address, link and route changes.
type routeNotifier struct {
	file      *os.File
	events    chan struct{}
	stopped   chan struct{}
	closeOnce sync.Once
	closeErr  error
}

func newNetChangeNotifier(logger log.Logger) netChangeNotifier {
	fd, err := unix.Socket(unix.AF_ROUTE, unix.SOCK_RAW, unix.AF_UNSPEC)
	if err != nil {
		logger.Debug(notifierUnavailableMsg, "err", err)
		return noopNotifier{}
	}
	unix.CloseOnExec(fd)
	// Nonblocking so os.NewFile registers the socket with the runtime netpoller:
	// the read parks until a message arrives and Close unblocks it at once,
	// instead of a receive-timeout loop that wakes on a timer to observe Close.
	if err := unix.SetNonblock(fd, true); err != nil {
		_ = unix.Close(fd)
		logger.Debug(notifierUnavailableMsg, "err", err)
		return noopNotifier{}
	}

	n := &routeNotifier{
		file:    os.NewFile(uintptr(fd), "pf_route"),
		events:  make(chan struct{}, 1),
		stopped: make(chan struct{}),
	}
	go n.loop(logger)
	return n
}

func (n *routeNotifier) Events() <-chan struct{} { return n.events }

func (n *routeNotifier) Close() error {
	n.closeOnce.Do(func() {
		n.closeErr = n.file.Close()
		<-n.stopped
	})
	return n.closeErr
}

func (n *routeNotifier) loop(logger log.Logger) {
	defer dbg.LogPanic()
	defer close(n.stopped)

	buf := make([]byte, 4096)
	for {
		nr, err := n.file.Read(buf)
		if err != nil {
			if errors.Is(err, os.ErrClosed) {
				return
			}
			if errors.Is(err, unix.ENOBUFS) {
				// The kernel dropped route messages because we fell behind. A
				// drop means state we didn't see changed, so signal a refresh
				// and keep watching rather than reverting to poll-only.
				select {
				case n.events <- struct{}{}:
				default:
				}
				continue
			}
			logger.Debug(notifierReadFailedMsg, "err", err)
			return
		}
		if nr <= 0 {
			continue
		}
		select {
		case n.events <- struct{}{}:
		default:
		}
	}
}
