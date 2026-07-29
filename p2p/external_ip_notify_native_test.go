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
	"os"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

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

// TestNativeNotifierConstructs asserts that a platform with a native notifier
// actually builds one: a noopNotifier here means socket setup broke, which must
// fail loudly rather than silently degrade to poll-only. Closing the idle
// notifier is near-instant because the read parks in the poller and Close
// unblocks it at once; the pre-fix receive-timeout loop took up to a second.
func TestNativeNotifierConstructs(t *testing.T) {
	logger, h := captureLogger()

	n := newNetChangeNotifier(logger)
	if _, ok := n.(noopNotifier); ok {
		t.Fatalf("native notifier expected on %s, got noop fallback: %v", runtime.GOOS, h.snapshot())
	}

	start := time.Now()
	if err := n.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if d := time.Since(start); d > 500*time.Millisecond {
		t.Fatalf("Close took %v; read did not park in the poller", d)
	}
}

// TestFdNotifierParksAndCloses exercises the poller integration hermetically via
// an os.Pipe, so it needs no privileged socket and never skips. It asserts the
// three properties the fix relies on: an idle read parks (no spurious event, no
// read-failure), a write wakes the parked read (event delivered), and Close
// unblocks the parked read at once.
func TestFdNotifierParksAndCloses(t *testing.T) {
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe: %v", err)
	}
	defer w.Close()

	logger, h := captureLogger()
	n := notifierFromFile(r, logger)
	defer n.Close()

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
	if _, err := w.Write(make([]byte, 128)); err != nil {
		t.Fatalf("write: %v", err)
	}
	select {
	case <-n.Events():
	case <-time.After(2 * time.Second):
		t.Fatal("write to the pipe did not wake the parked read")
	}

	start := time.Now()
	if err := n.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if d := time.Since(start); d > 500*time.Millisecond {
		t.Fatalf("Close took %v; read did not park in the poller", d)
	}
}
