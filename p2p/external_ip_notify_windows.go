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
	"runtime"
	"sync"
	"unsafe"

	"golang.org/x/sys/windows"

	"github.com/erigontech/erigon/common/log/v3"
)

// addrChangeCallback is registered once for the whole process. A
// windows.NewCallback trampoline is never freed, so it must not be allocated
// per notifier; the owning notifier is recovered from the caller context.
var addrChangeCallback = windows.NewCallback(onUnicastIPAddressChange)

// addrChangeNotifier signals on unicast IP address changes via the IP Helper
// NotifyUnicastIpAddressChange API. Cancellation is synchronous —
// CancelMibChangeNotify2 waits for any in-flight callback to return and
// guarantees none fires afterwards — so no OVERLAPPED buffer or manual event
// handshake is needed.
type addrChangeNotifier struct {
	events    chan struct{}
	handle    windows.Handle
	pinner    runtime.Pinner
	closeOnce sync.Once
}

func newNetChangeNotifier(logger log.Logger) netChangeNotifier {
	n := &addrChangeNotifier{events: make(chan struct{}, 1)}
	// The OS retains the caller context until CancelMibChangeNotify2, so pin n
	// to keep it alive and unmoved for the callback to dereference.
	n.pinner.Pin(n)
	err := windows.NotifyUnicastIpAddressChange(windows.AF_UNSPEC, addrChangeCallback, unsafe.Pointer(n), false, &n.handle)
	if err != nil {
		n.pinner.Unpin()
		logger.Debug(notifierUnavailableMsg, "err", err)
		return noopNotifier{}
	}
	return n
}

func (n *addrChangeNotifier) Events() <-chan struct{} { return n.events }

func (n *addrChangeNotifier) Close() error {
	n.closeOnce.Do(func() {
		windows.CancelMibChangeNotify2(n.handle)
		n.pinner.Unpin()
	})
	return nil
}

// onUnicastIPAddressChange is the process-wide notify callback. It runs on an OS
// thread-pool thread, so it does nothing but a non-blocking send to the owning
// notifier's channel; the debounced refresh loop does the real work.
func onUnicastIPAddressChange(callerContext, row, notificationType uintptr) uintptr {
	n := (*addrChangeNotifier)(unsafe.Pointer(callerContext))
	select {
	case n.events <- struct{}{}:
	default:
	}
	return 0
}
