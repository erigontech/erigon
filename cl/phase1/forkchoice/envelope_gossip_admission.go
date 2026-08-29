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
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/erigontech/erigon/common"
)

const (
	maxSeenExecutionPayloadEnvelopes      = 4096
	maxInflightExecutionPayloadEnvelopes  = 1024
	maxWaitersPerExecutionPayloadEnvelope = 1
)

var ErrExecutionPayloadEnvelopeAdmissionBusy = errors.New("execution payload envelope admission busy")

type executionPayloadEnvelopeIdentity struct {
	beaconBlockRoot common.Hash
	builderIndex    uint64
}

type ExecutionPayloadEnvelopeAdmissionToken struct {
	identity executionPayloadEnvelopeIdentity
	id       uint64
}

type ExecutionPayloadEnvelopeAdmissions struct {
	mu       sync.Mutex
	nextID   uint64
	inflight map[executionPayloadEnvelopeIdentity]executionPayloadEnvelopeAdmission
	seen     map[executionPayloadEnvelopeIdentity]struct{}
	seenFIFO []executionPayloadEnvelopeIdentity
	seenNext int
}

type executionPayloadEnvelopeAdmission struct {
	id      uint64
	done    chan struct{}
	waiters uint8
}

func (a *ExecutionPayloadEnvelopeAdmissions) Claim(
	ctx context.Context,
	beaconBlockRoot common.Hash,
	builderIndex uint64,
) (ExecutionPayloadEnvelopeAdmissionToken, error) {
	identity := executionPayloadEnvelopeIdentity{beaconBlockRoot: beaconBlockRoot, builderIndex: builderIndex}
	for {
		if err := ctx.Err(); err != nil {
			return ExecutionPayloadEnvelopeAdmissionToken{}, err
		}
		a.mu.Lock()
		if _, ok := a.seen[identity]; ok {
			a.mu.Unlock()
			return ExecutionPayloadEnvelopeAdmissionToken{}, errors.New("execution payload envelope already seen")
		}
		if admission, ok := a.inflight[identity]; ok {
			if admission.waiters >= maxWaitersPerExecutionPayloadEnvelope {
				a.mu.Unlock()
				return ExecutionPayloadEnvelopeAdmissionToken{}, fmt.Errorf("%w: execution payload envelope already being published", ErrExecutionPayloadEnvelopeAdmissionBusy)
			}
			admission.waiters++
			a.inflight[identity] = admission
			a.mu.Unlock()
			select {
			case <-admission.done:
				continue
			case <-ctx.Done():
				a.removeWaiter(identity, admission.id)
				return ExecutionPayloadEnvelopeAdmissionToken{}, ctx.Err()
			}
		}
		if len(a.inflight) >= maxInflightExecutionPayloadEnvelopes {
			a.mu.Unlock()
			return ExecutionPayloadEnvelopeAdmissionToken{}, fmt.Errorf("%w: too many execution payload envelopes are being published", ErrExecutionPayloadEnvelopeAdmissionBusy)
		}
		if a.inflight == nil {
			a.inflight = make(map[executionPayloadEnvelopeIdentity]executionPayloadEnvelopeAdmission)
		}
		a.nextID++
		id := a.nextID
		a.inflight[identity] = executionPayloadEnvelopeAdmission{id: id, done: make(chan struct{})}
		a.mu.Unlock()
		return ExecutionPayloadEnvelopeAdmissionToken{identity: identity, id: id}, nil
	}
}

func (a *ExecutionPayloadEnvelopeAdmissions) removeWaiter(identity executionPayloadEnvelopeIdentity, admissionID uint64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	admission, ok := a.inflight[identity]
	if !ok || admission.id != admissionID || admission.waiters == 0 {
		return
	}
	admission.waiters--
	a.inflight[identity] = admission
}

func (a *ExecutionPayloadEnvelopeAdmissions) Finish(token ExecutionPayloadEnvelopeAdmissionToken, seen bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	admission, ok := a.inflight[token.identity]
	if !ok || admission.id != token.id {
		return
	}
	delete(a.inflight, token.identity)
	defer close(admission.done)
	if !seen {
		return
	}
	if a.seen == nil {
		a.seen = make(map[executionPayloadEnvelopeIdentity]struct{})
	}
	if _, ok := a.seen[token.identity]; ok {
		return
	}
	if len(a.seenFIFO) >= maxSeenExecutionPayloadEnvelopes {
		delete(a.seen, a.seenFIFO[a.seenNext])
		a.seenFIFO[a.seenNext] = token.identity
		a.seenNext = (a.seenNext + 1) % maxSeenExecutionPayloadEnvelopes
	} else {
		a.seenFIFO = append(a.seenFIFO, token.identity)
	}
	a.seen[token.identity] = struct{}{}
}
