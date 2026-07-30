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

//go:build linux || darwin || freebsd || netbsd || openbsd || dragonfly

package p2p

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"github.com/erigontech/erigon/common/log/v3"
)

type msgCaptureHandler struct {
	mu   sync.Mutex
	msgs []string
}

func (h *msgCaptureHandler) Log(r *log.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.msgs = append(h.msgs, r.Msg)
	return nil
}
func (h *msgCaptureHandler) Enabled(context.Context, log.Lvl) bool { return true }

func (h *msgCaptureHandler) contains(sub string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, m := range h.msgs {
		if strings.Contains(m, sub) {
			return true
		}
	}
	return false
}

func (h *msgCaptureHandler) snapshot() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]string(nil), h.msgs...)
}

func captureLogger() (log.Logger, *msgCaptureHandler) {
	logger := log.New()
	h := &msgCaptureHandler{}
	logger.SetHandler(h)
	return logger, h
}

// closeWithin closes n in a goroutine bounded by a timeout, so a poller
// regression that makes Close block forever fails the test promptly instead of
// hanging until the package timeout. It returns how long Close took.
func closeWithin(t *testing.T, n netChangeNotifier, timeout time.Duration) time.Duration {
	t.Helper()
	start := time.Now()
	done := make(chan error, 1)
	go func() { done <- n.Close() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	case <-time.After(timeout):
		t.Fatal("Close hung: read did not unblock via the poller")
	}
	return time.Since(start)
}

func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.After(timeout)
	for {
		if cond() {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for %s", what)
		case <-time.After(5 * time.Millisecond):
		}
	}
}

// TestFdNotifierParksAndCloses drives the notifier from a raw, initially
// blocking pipe descriptor so it exercises the SetNonblock + os.NewFile
// transition the fix relies on, hermetically (no privileged socket, never
// skips). It asserts the three properties of that transition: an idle read
// parks (no spurious event, no read-failure), a write wakes the parked read,
// and Close unblocks the parked read at once.
func TestFdNotifierParksAndCloses(t *testing.T) {
	var fds [2]int
	if err := unix.Pipe(fds[:]); err != nil {
		t.Fatalf("unix.Pipe: %v", err)
	}
	readFD, writeFD := fds[0], fds[1]
	defer unix.Close(writeFD)

	logger, h := captureLogger()
	n, err := newNetChangeNotifierFromFD(readFD, logger)
	if err != nil {
		_ = unix.Close(readFD)
		t.Fatalf("newNetChangeNotifierFromFD: %v", err)
	}

	select {
	case <-n.Events():
		t.Fatal("notifier fired without any input")
	case <-time.After(100 * time.Millisecond):
	}
	if h.contains(notifierReadFailedMsg) {
		t.Fatalf("read exited early instead of parking in the poller: %v", h.snapshot())
	}

	// Large enough to clear the netlink loop's NLMSG_HDRLEN (16-byte) minimum,
	// which would otherwise discard the message without firing an event.
	if _, err := unix.Write(writeFD, make([]byte, 128)); err != nil {
		t.Fatalf("write: %v", err)
	}
	select {
	case <-n.Events():
	case <-time.After(2 * time.Second):
		t.Fatal("write did not wake the parked read")
	}

	if d := closeWithin(t, n, 2*time.Second); d > 500*time.Millisecond {
		t.Fatalf("Close took %v; read did not park in the poller", d)
	}
}

// TestFdNotifierClosesDescriptorOnReadError asserts the loop releases the
// descriptor as soon as it exits on a terminal read error, rather than leaving
// it open until the tracker's deferred Close at shutdown. Closing the pipe's
// write end makes the read return io.EOF, which exits the loop.
func TestFdNotifierClosesDescriptorOnReadError(t *testing.T) {
	var fds [2]int
	if err := unix.Pipe(fds[:]); err != nil {
		t.Fatalf("unix.Pipe: %v", err)
	}
	readFD, writeFD := fds[0], fds[1]

	logger, h := captureLogger()
	n, err := newNetChangeNotifierFromFD(readFD, logger)
	if err != nil {
		_ = unix.Close(readFD)
		_ = unix.Close(writeFD)
		t.Fatalf("newNetChangeNotifierFromFD: %v", err)
	}

	if err := unix.Close(writeFD); err != nil {
		t.Fatalf("close write end: %v", err)
	}

	waitFor(t, 2*time.Second, "read-failure log", func() bool {
		return h.contains(notifierReadFailedMsg)
	})
	waitFor(t, 2*time.Second, "descriptor release", func() bool {
		_, err := unix.FcntlInt(uintptr(readFD), unix.F_GETFD, 0)
		return errors.Is(err, unix.EBADF)
	})

	if err := n.Close(); err != nil {
		t.Fatalf("Close after loop exit: %v", err)
	}
}
