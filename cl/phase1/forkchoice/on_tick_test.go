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

package forkchoice

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	"github.com/erigontech/erigon/cl/cltypes/solid"
)

// A stalled /eth/v1/events subscriber must not wedge fork choice: the finalized
// checkpoint event send has to run outside f.mu.
func TestFinalizedCheckpointEmittedOutsideLock(t *testing.T) {
	f := buildExAnteStore(t)

	delivered := make(chan *beaconevents.EventStream, 1)
	stalled := make(chan *beaconevents.EventStream)
	subA := f.emitters.State().Subscribe(delivered)
	subB := f.emitters.State().Subscribe(stalled)
	defer subA.Unsubscribe()
	defer subB.Unsubscribe()

	_, root := decodeDiffBlock(t, diffBlockc2Enc)
	promoted := solid.Checkpoint{Epoch: 1, Root: root}
	f.unrealizedJustifiedCheckpoint.Store(promoted)
	f.unrealizedFinalizedCheckpoint.Store(promoted)

	tickDone := make(chan struct{})
	go func() {
		f.OnTick(f.beaconCfg.SlotsPerEpoch * f.beaconCfg.SecondsPerSlot)
		close(tickDone)
	}()

	var ev *beaconevents.EventStream
	select {
	case ev = <-delivered:
	case <-time.After(10 * time.Second):
		t.Fatal("finalized checkpoint event was not sent")
	}
	require.Equal(t, beaconevents.StateFinalizedCheckpoint, ev.Event)

	// subB has not accepted the event, so the send is still in flight.
	lockFree := make(chan struct{})
	go func() {
		f.mu.Lock()
		_ = f.headHash
		f.mu.Unlock()
		close(lockFree)
	}()
	select {
	case <-lockFree:
	case <-time.After(10 * time.Second):
		t.Fatal("f.mu is held while the event send is blocked on a stalled subscriber")
	}

	select {
	case <-stalled:
	case <-time.After(10 * time.Second):
		t.Fatal("stalled subscriber never received the event")
	}
	select {
	case <-tickDone:
	case <-time.After(10 * time.Second):
		t.Fatal("OnTick did not finish")
	}
}
