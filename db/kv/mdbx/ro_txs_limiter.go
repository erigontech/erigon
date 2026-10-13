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

package mdbx

import (
	"context"
	"runtime"
	"sync/atomic"
)

// RoTxsLimiter bounds concurrent read txs without a lock: below the bound Acquire
// and Release are one atomic add each.
type RoTxsLimiter struct {
	size  int64
	avail atomic.Int64  // free permits, or minus the waiters not yet handed one
	grant chan struct{} // permits handed from Release to waiters; never more than size
}

func NewRoTxsLimiter(size int64) *RoTxsLimiter {
	l := &RoTxsLimiter{size: size, grant: make(chan struct{}, size)}
	l.avail.Store(size)
	return l
}

// TryAcquire takes a permit without waiting. It fails while others wait.
func (l *RoTxsLimiter) TryAcquire() bool {
	for n := l.avail.Load(); n > 0; n = l.avail.Load() {
		if l.avail.CompareAndSwap(n, n-1) {
			return true
		}
	}
	return false
}

func (l *RoTxsLimiter) Acquire(ctx context.Context) error {
	if l.avail.Add(-1) >= 0 {
		return nil
	}
	select {
	case <-l.grant:
		return nil
	case <-ctx.Done():
	}
	for {
		if n := l.avail.Load(); n < 0 && l.avail.CompareAndSwap(n, n+1) {
			return ctx.Err()
		}
		// avail >= 0: a Release already handed out a permit for every waiter, us included.
		select {
		case <-l.grant:
			l.Release()
			return ctx.Err()
		default:
			runtime.Gosched()
		}
	}
}

func (l *RoTxsLimiter) Release() {
	n := l.avail.Add(1)
	if n > l.size {
		panic("mdbx: RoTxsLimiter released more permits than it holds")
	}
	if n <= 0 {
		l.grant <- struct{}{}
	}
}
