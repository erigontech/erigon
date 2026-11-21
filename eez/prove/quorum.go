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

package prove

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/eez/l1"
	"github.com/erigontech/erigon/eez/protocol"
)

// DefaultGrace is how long a quorum keeps collecting once it has enough
// attestations: more proofs than the threshold cost little gas and keep a
// batch valid should a member's key rotate before it lands.
const DefaultGrace = 250 * time.Millisecond

// ErrQuorumUnreachable reports a threshold the members the manager keys
// cannot reach.
var ErrQuorumUnreachable = errors.New("attesters cannot reach the manager's threshold")

// Member is an attester of a quorum: the proof system it attests for, the
// address its attestations recover to, and its prover.
type Member struct {
	ProofSystem, Attester common.Address
	Client                *Client
}

// Attestation is a member's proof of a batch.
type Attestation struct {
	ProofSystem common.Address
	Proof       []byte
}

// Quorum gathers the attestations a rollup's manager requires of a batch.
type Quorum struct {
	Members []*Member
	// Register reads the manager's registration of the members' proof
	// systems.
	Register func(ctx context.Context) (l1.Registration, error)
	Grace    time.Duration
}

// ProveBy returns the attestations of w's batch by the members the manager
// keys with the signer each is configured with, at least its threshold, in
// ascending proof-system order. Each member proves the batch listing its
// proof system alone, under the manager's key: the hash it signs is the one
// its proof system verifies in the assembled batch.
func (q *Quorum) ProveBy(ctx context.Context, w *Window, start time.Time, per time.Duration) ([]Attestation, error) {
	reg, err := q.Register(ctx)
	if err != nil {
		return nil, fmt.Errorf("manager registration: %w", err)
	}
	threshold := max(reg.Threshold, 1)
	var usable []*Member
	for _, m := range q.Members {
		if k, ok := reg.Keys[m.ProofSystem]; ok && k.Signer == m.Attester {
			usable = append(usable, m)
		}
	}
	if uint64(len(usable)) < threshold {
		return nil, fmt.Errorf("%w: %d usable of %d, threshold %d", ErrQuorumUnreachable, len(usable), len(q.Members), threshold)
	}
	type answer struct {
		m     *Member
		proof []byte
		err   error
	}
	actx, cancel := context.WithCancel(ctx)
	defer cancel()
	answers := make(chan answer, len(usable))
	for _, m := range usable {
		mw := alone(w, m.ProofSystem, reg.Keys[m.ProofSystem].VKey)
		go func() {
			proof, err := m.Client.ProveBy(actx, mw, start, per)
			answers <- answer{m, proof, err}
		}()
	}
	var atts []Attestation
	failed := &QuorumError{Threshold: threshold}
	var grace <-chan time.Time
	for pending := len(usable); pending > 0; {
		select {
		case a := <-answers:
			pending--
			if a.err != nil {
				failed.Failures = append(failed.Failures, MemberFailure{ProofSystem: a.m.ProofSystem, Err: a.err})
				continue
			}
			atts = append(atts, Attestation{ProofSystem: a.m.ProofSystem, Proof: a.proof})
			if uint64(len(atts)) == threshold && pending > 0 {
				timer := time.NewTimer(q.Grace)
				defer timer.Stop()
				grace = timer.C
			}
		case <-grace:
			pending = 0
		}
	}
	if uint64(len(atts)) < threshold {
		return nil, failed
	}
	slices.SortFunc(atts, func(x, y Attestation) int { return x.ProofSystem.Cmp(y.ProofSystem) })
	return atts, nil
}

// alone returns w with its batch listing proofSystem alone, keyed vkey.
func alone(w *Window, proofSystem common.Address, vkey common.Hash) *Window {
	b := *w.Batch
	b.ProofSystems = []common.Address{proofSystem}
	b.RollupIDsWithProofSystems = make([]protocol.RollupIDWithProofSystems, len(w.Batch.RollupIDsWithProofSystems))
	for i, r := range w.Batch.RollupIDsWithProofSystems {
		b.RollupIDsWithProofSystems[i] = protocol.RollupIDWithProofSystems{RollupID: r.RollupID, ProofSystemIndexes: []uint64{0}}
	}
	mw := *w
	mw.Batch, mw.VKey = &b, vkey
	return &mw
}

// MemberFailure is a member's failure to attest.
type MemberFailure struct {
	ProofSystem common.Address
	Err         error
}

// QuorumError reports a batch fewer members than the threshold attested.
type QuorumError struct {
	Threshold uint64
	Failures  []MemberFailure
}

func (e *QuorumError) Error() string {
	parts := make([]string, len(e.Failures))
	for i, f := range e.Failures {
		parts[i] = fmt.Sprintf("%x: %v", f.ProofSystem, f.Err)
	}
	return fmt.Sprintf("quorum of %d not reached: %s", e.Threshold, strings.Join(parts, "; "))
}

func (e *QuorumError) Unwrap() []error {
	errs := make([]error, len(e.Failures))
	for i, f := range e.Failures {
		errs[i] = f.Err
	}
	return errs
}

// GRPCStatus is the provers' answer when every failed member answered with
// a status, a refusal before a transient failure; else Unknown, a failure
// that lies with the node.
func (e *QuorumError) GRPCStatus() *status.Status {
	var transient *status.Status
	for _, f := range e.Failures {
		s, ok := status.FromError(f.Err)
		switch {
		case !ok || s.Code() == codes.Unknown || s.Code() == codes.Canceled:
			return status.New(codes.Unknown, e.Error())
		case !Retryable(f.Err):
			return status.New(s.Code(), e.Error())
		case transient == nil:
			transient = status.New(s.Code(), e.Error())
		}
	}
	if transient == nil {
		return status.New(codes.Unknown, e.Error())
	}
	return transient
}
