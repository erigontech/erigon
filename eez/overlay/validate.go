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

package overlay

import (
	"fmt"

	pubsub "github.com/libp2p/go-libp2p-pubsub"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/eez/seqsig"
)

// validator applies the relay rule. Gossipsub forwards a message only once
// the rule accepts it:
//   - Accept: a head of the authorized signer above its source's latest.
//   - Ignore: a head at or below its source's latest, however encoded, or
//     one whose epoch is ahead of local time.
//   - Reject: an undecodable message, a head of another rollup or signer, a
//     block that is not the head's, or an equivocation.
type validator struct {
	domain  seqsig.Domain
	signer  common.Address
	tracker seqsig.Tracker
	now     func() uint64
}

// check returns the verdict on data and, on Accept, the verified head; err is
// why it rejects.
func (v *validator) check(data []byte) (h *seqsig.VerifiedHead, verdict pubsub.ValidationResult, err error) {
	e, err := seqsig.Decode(data)
	if err != nil {
		return nil, pubsub.ValidationReject, err
	}
	if h, err = e.Verify(v.domain, v.signer); err != nil {
		return nil, pubsub.ValidationReject, err
	}
	switch obs, kept := v.tracker.Observe(h, v.now()); obs {
	case seqsig.ObservedNew:
		return h, pubsub.ValidationAccept, nil
	case seqsig.ObservedEquivocation:
		return nil, pubsub.ValidationReject, fmt.Errorf("equivocation at %d, kept %x", h.Head.Counter, kept)
	default:
		// Snappy has many encodings of one payload: relaying each would let
		// a peer flood the mesh with copies of one valid head.
		return nil, pubsub.ValidationIgnore, nil
	}
}
