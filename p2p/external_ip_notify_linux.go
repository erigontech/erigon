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

//go:build linux

package p2p

import (
	"errors"
	"sync"

	"golang.org/x/sys/unix"

	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/log/v3"
)

// netlinkNotifier signals on interface address and link changes via an
// RTNETLINK multicast socket.
type netlinkNotifier struct {
	fd        int
	events    chan struct{}
	done      chan struct{}
	stopped   chan struct{}
	closeOnce sync.Once
}

func newNetChangeNotifier(logger log.Logger) netChangeNotifier {
	fd, err := unix.Socket(unix.AF_NETLINK, unix.SOCK_RAW|unix.SOCK_CLOEXEC, unix.NETLINK_ROUTE)
	if err != nil {
		logger.Debug(notifierUnavailableMsg, "err", err)
		return noopNotifier{}
	}
	addr := &unix.SockaddrNetlink{
		Family: unix.AF_NETLINK,
		Groups: unix.RTMGRP_IPV4_IFADDR | unix.RTMGRP_IPV6_IFADDR | unix.RTMGRP_LINK,
	}
	if err := unix.Bind(fd, addr); err != nil {
		_ = unix.Close(fd)
		logger.Debug(notifierUnavailableMsg, "err", err)
		return noopNotifier{}
	}
	// A receive timeout lets the read wake periodically to observe Close;
	// without it Close could block forever on a blocked read.
	if err := unix.SetsockoptTimeval(fd, unix.SOL_SOCKET, unix.SO_RCVTIMEO, &unix.Timeval{Sec: 1}); err != nil {
		_ = unix.Close(fd)
		logger.Debug(notifierUnavailableMsg, "err", err)
		return noopNotifier{}
	}

	n := &netlinkNotifier{
		fd:      fd,
		events:  make(chan struct{}, 1),
		done:    make(chan struct{}),
		stopped: make(chan struct{}),
	}
	go n.loop(logger)
	return n
}

func (n *netlinkNotifier) Events() <-chan struct{} { return n.events }

func (n *netlinkNotifier) Close() error {
	n.closeOnce.Do(func() {
		close(n.done)
		<-n.stopped
	})
	return nil
}

func (n *netlinkNotifier) loop(logger log.Logger) {
	defer dbg.LogPanic()
	defer close(n.stopped)
	defer func() { _ = unix.Close(n.fd) }()

	buf := make([]byte, 8192)
	for {
		select {
		case <-n.done:
			return
		default:
		}

		nr, _, err := unix.Recvfrom(n.fd, buf, 0)
		if err != nil {
			if errors.Is(err, unix.EAGAIN) || errors.Is(err, unix.EINTR) {
				continue
			}
			if errors.Is(err, unix.ENOBUFS) {
				// The kernel dropped multicast messages because we fell behind.
				// A drop means state we didn't see changed, so signal a refresh
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
		if nr < unix.NLMSG_HDRLEN {
			continue
		}
		select {
		case n.events <- struct{}{}:
		default:
		}
	}
}
