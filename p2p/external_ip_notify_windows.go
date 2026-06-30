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

//go:build windows

package p2p

import (
	"sync"
	"unsafe"

	"golang.org/x/sys/windows"

	"github.com/erigontech/erigon/common/log/v3"
)

var (
	modiphlpapi          = windows.NewLazySystemDLL("iphlpapi.dll")
	procNotifyAddrChange = modiphlpapi.NewProc("NotifyAddrChange")
	procCancelIPChange   = modiphlpapi.NewProc("CancelIPChangeNotify")
)

// addrChangeNotifier signals on IP address changes via the IP Helper
// NotifyAddrChange API, cancelled through a manual-reset event.
type addrChangeNotifier struct {
	events      chan struct{}
	cancelEvent windows.Handle
	stopped     chan struct{}
	closeOnce   sync.Once
}

func newNetChangeNotifier(logger log.Logger) netChangeNotifier {
	cancelEvent, err := windows.CreateEvent(nil, 1, 0, nil) // manual-reset, nonsignaled
	if err != nil {
		logger.Debug("p2p: address-change notifier unavailable, using periodic external IP refresh only", "err", err)
		return noopNotifier{}
	}
	n := &addrChangeNotifier{
		events:      make(chan struct{}, 1),
		cancelEvent: cancelEvent,
		stopped:     make(chan struct{}),
	}
	go n.loop(logger)
	return n
}

func (n *addrChangeNotifier) Events() <-chan struct{} { return n.events }

func (n *addrChangeNotifier) Close() error {
	n.closeOnce.Do(func() {
		windows.SetEvent(n.cancelEvent)
		<-n.stopped
		windows.CloseHandle(n.cancelEvent)
	})
	return nil
}

func (n *addrChangeNotifier) loop(logger log.Logger) {
	defer close(n.stopped)

	notifyEvent, err := windows.CreateEvent(nil, 0, 0, nil) // auto-reset, nonsignaled
	if err != nil {
		logger.Debug("p2p: address-change event creation failed, using periodic external IP refresh only", "err", err)
		return
	}
	defer windows.CloseHandle(notifyEvent)

	for {
		var handle windows.Handle
		overlapped := &windows.Overlapped{HEvent: notifyEvent}
		r, _, _ := procNotifyAddrChange.Call(uintptr(unsafe.Pointer(&handle)), uintptr(unsafe.Pointer(overlapped)))
		if errno := windows.Errno(r); errno != 0 && errno != windows.ERROR_IO_PENDING {
			logger.Debug("p2p: NotifyAddrChange failed, using periodic external IP refresh only", "err", errno)
			return
		}

		s, err := windows.WaitForMultipleObjects([]windows.Handle{n.cancelEvent, notifyEvent}, false, windows.INFINITE)
		if err != nil {
			logger.Debug("p2p: address-change wait failed", "err", err)
			procCancelIPChange.Call(uintptr(unsafe.Pointer(overlapped)))
			return
		}
		switch s {
		case windows.WAIT_OBJECT_0: // cancelled by Close
			procCancelIPChange.Call(uintptr(unsafe.Pointer(overlapped)))
			return
		case windows.WAIT_OBJECT_0 + 1: // address changed
			select {
			case n.events <- struct{}{}:
			default:
			}
		default:
			procCancelIPChange.Call(uintptr(unsafe.Pointer(overlapped)))
			return
		}
	}
}
