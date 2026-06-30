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
	"sync"

	"golang.org/x/sys/unix"

	"github.com/erigontech/erigon/common/log/v3"
)

// routeNotifier signals on any message from a PF_ROUTE socket, which the kernel
// emits on address, link and route changes.
type routeNotifier struct {
	fd        int
	events    chan struct{}
	done      chan struct{}
	stopped   chan struct{}
	closeOnce sync.Once
}

func newNetChangeNotifier(logger log.Logger) netChangeNotifier {
	fd, err := unix.Socket(unix.AF_ROUTE, unix.SOCK_RAW, unix.AF_UNSPEC)
	if err != nil {
		logger.Debug("p2p: route notifier unavailable, using periodic external IP refresh only", "err", err)
		return noopNotifier{}
	}
	unix.CloseOnExec(fd)
	// A receive timeout lets the read wake periodically to observe Close;
	// without it Close could block forever on a blocked read.
	if err := unix.SetsockoptTimeval(fd, unix.SOL_SOCKET, unix.SO_RCVTIMEO, &unix.Timeval{Sec: 1}); err != nil {
		_ = unix.Close(fd)
		logger.Debug("p2p: route receive timeout setup failed, using periodic external IP refresh only", "err", err)
		return noopNotifier{}
	}

	n := &routeNotifier{
		fd:      fd,
		events:  make(chan struct{}, 1),
		done:    make(chan struct{}),
		stopped: make(chan struct{}),
	}
	go n.loop()
	return n
}

func (n *routeNotifier) Events() <-chan struct{} { return n.events }

func (n *routeNotifier) Close() error {
	n.closeOnce.Do(func() {
		close(n.done)
		<-n.stopped
	})
	return nil
}

func (n *routeNotifier) loop() {
	defer close(n.stopped)
	defer func() { _ = unix.Close(n.fd) }()

	buf := make([]byte, 4096)
	for {
		select {
		case <-n.done:
			return
		default:
		}

		nr, err := unix.Read(n.fd, buf)
		if err != nil {
			if errors.Is(err, unix.EAGAIN) || errors.Is(err, unix.EINTR) {
				continue
			}
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
