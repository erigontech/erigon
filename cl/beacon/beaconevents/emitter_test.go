// Copyright 2024 The Erigon Authors
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

package beaconevents

import (
	"testing"
	"time"
)

func TestEmitterSet(t *testing.T) {
	emitter := NewEventEmitter()
	done := make(chan struct{})
	go func() {
		ch := make(chan *EventStream, 10)
		t.Logf("Subscribing to emitter")
		sub := emitter.Operation().Subscribe(ch)
		defer sub.Unsubscribe()
		t.Logf("Subscribed to emitter")
		event := <-ch
		t.Logf("Received event: %v", event)
		close(done)
	}()
	time.Sleep(100 * time.Millisecond)
	emitter.Operation().SendAttestation(&AttestationData{})
	<-done
}
