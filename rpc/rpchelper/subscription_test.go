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

package rpchelper

import (
	"testing"
	"time"
)

// An unbuffered channel can hold no latest value: with no reader ready,
// SendLatest must drop and return instead of spinning forever under the lock.
func TestSendLatestUnbufferedChannelReturns(t *testing.T) {
	s := &chan_sub[int]{ch: make(chan int)}

	done := make(chan struct{})
	go func() {
		s.SendLatest(1)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("SendLatest must return on an unbuffered channel with no ready reader")
	}
}
