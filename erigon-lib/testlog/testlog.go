// Copyright 2019 The go-ethereum Authors
// (original work)
// Copyright 2024 The Erigon Authors
// (modifications)
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

// Package testlog provides a log handler for unit tests.
package testlog

import (
	"context"
	"sync"
	"testing"

	"github.com/erigontech/erigon-lib/log/v3"
)

// Handler returns a log handler which logs to the unit test log of t.
func Handler(t *testing.T, level log.Lvl) log.Handler {
	t.Helper()
	return log.LvlFilterHandler(level, &handler{t, log.TerminalFormat()})
}

type handler struct {
	t   *testing.T
	fmt log.Format
}

func (h *handler) Log(r *log.Record) error {
	h.t.Logf("%s", h.fmt.Format(r))
	return nil
}

func (h *handler) Enabled(ctx context.Context, lvl log.Lvl) bool {
	return true
}

// logger implements log.Logger such that all output goes to the unit test log via
// t.Logf(). All methods in between logger.Trace, logger.Debug, etc. are marked as test
// helpers, so the file and line number in unit test output correspond to the call site
// which emitted the log message.
type logger struct {
	t   *testing.T
	log log.Logger
	mu  *sync.Mutex
	h   *bufHandler
}

type bufHandler struct {
	buf []*log.Record
	fmt log.Format
}

func (h *bufHandler) Log(r *log.Record) error {
	h.buf = append(h.buf, r)
	return nil
}

func (h *bufHandler) Enabled(ctx context.Context, lvl log.Lvl) bool {
	return true
}

// Logger returns a logger which logs to the unit test log of t.
func Logger(t *testing.T, level log.Lvl) log.Logger {
	t.Helper()

	l := &logger{
		t:   t,
		log: log.New(),
		mu:  new(sync.Mutex),
		h:   &bufHandler{fmt: log.TerminalFormat()},
	}
	l.log.SetHandler(log.LvlFilterHandler(level, l.h))
	return l
}

func (l *logger) Trace(msg string, ctx ...interface{}) {
	l.t.Helper()
	l.mu.Lock()
	defer l.mu.Unlock()
	l.log.Trace(msg, ctx...)
	l.flush()
}

func (l *logger) Debug(msg string, ctx ...interface{}) {
	l.t.Helper()
	l.mu.Lock()
	defer l.mu.Unlock()
	l.log.Debug(msg, ctx...)
	l.flush()
}

func (l *logger) Info(msg string, ctx ...interface{}) {
	l.t.Helper()
	l.mu.Lock()
	defer l.mu.Unlock()
	l.log.Info(msg, ctx...)
	l.flush()
}

func (l *logger) Warn(msg string, ctx ...interface{}) {
	l.t.Helper()
	l.mu.Lock()
	defer l.mu.Unlock()
	l.log.Warn(msg, ctx...)
	l.flush()
}

func (l *logger) Error(msg string, ctx ...interface{}) {
	l.t.Helper()
	l.mu.Lock()
	defer l.mu.Unlock()
	l.log.Error(msg, ctx...)
	l.flush()
}

func (l *logger) Crit(msg string, ctx ...interface{}) {
	l.t.Helper()
	l.mu.Lock()
	defer l.mu.Unlock()
	l.log.Crit(msg, ctx...)
	l.flush()
}

func (l *logger) Log(level log.Lvl, msg string, ctx ...interface{}) {
	l.t.Helper()
	l.mu.Lock()
	defer l.mu.Unlock()
	l.log.Log(level, msg, ctx...)
	l.flush()
}

func (l *logger) Enabled(ctx context.Context, lvl log.Lvl) bool {
	return l.log.Enabled(ctx, lvl)
}

func (l *logger) New(ctx ...interface{}) log.Logger {
	return &logger{l.t, l.log.New(ctx...), l.mu, l.h}
}

func (l *logger) GetHandler() log.Handler {
	return l.log.GetHandler()
}

func (l *logger) SetHandler(h log.Handler) {
	l.log.SetHandler(h)
}

// flush writes all buffered messages and clears the buffer.
func (l *logger) flush() {
	l.t.Helper()
	for _, r := range l.h.buf {
		l.t.Logf("%s", l.h.fmt.Format(r))
	}
	l.h.buf = nil
}
