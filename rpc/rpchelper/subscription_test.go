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
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
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

func TestSharedEncodesOnce(t *testing.T) {
	ev := &Shared[int]{Value: 7}
	var calls atomic.Int32
	encode := func(v int) ([]byte, error) {
		calls.Add(1)
		return []byte(strconv.Itoa(v)), nil
	}
	var wg sync.WaitGroup
	for range 16 {
		wg.Go(func() {
			b, err := ev.Encode(encode)
			require.NoError(t, err)
			require.Equal(t, "7", string(b))
		})
	}
	wg.Wait()
	require.Equal(t, int32(1), calls.Load())
}
