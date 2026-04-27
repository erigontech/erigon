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

package libsentry_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/p2p/sentry/libsentry"
)

func TestEvictOldestIfHalfFull_NoopBelowThreshold(t *testing.T) {
	ch := make(chan int, 1024)
	for i := range 512 {
		ch <- i
	}
	libsentry.EvictOldestIfHalfFull(ch)
	assert.Equal(t, 512, len(ch), "at exactly half capacity eviction must not run")
}

func TestEvictOldestIfHalfFull_DropsQuarterFromOldest(t *testing.T) {
	ch := make(chan int, 1024)
	for i := range 600 {
		ch <- i
	}
	libsentry.EvictOldestIfHalfFull(ch)
	// cap/4 = 256 drained; 600 - 256 = 344 remain.
	assert.Equal(t, 344, len(ch))
	// Drop is from the front of the FIFO, so the oldest (0..255) are gone and
	// the freshest (599) is still queued.
	first := <-ch
	assert.Equal(t, 256, first)
	var last int
	for len(ch) > 0 {
		last = <-ch
	}
	assert.Equal(t, 599, last)
}

func TestEvictOldestIfHalfFull_DrainStopsWhenEmpty(t *testing.T) {
	// Small cap to make eviction target larger than buffer contents.
	ch := make(chan int, 4)
	ch <- 0
	ch <- 1
	ch <- 2
	// len=3, cap=4, cap/2=2, len>cap/2 → eviction runs; cap/4=1 drain target.
	libsentry.EvictOldestIfHalfFull(ch)
	require.Equal(t, 2, len(ch))
	assert.Equal(t, 1, <-ch)
	assert.Equal(t, 2, <-ch)
}
