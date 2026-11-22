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
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/eez/l1"
	"github.com/erigontech/erigon/eez/protocol"
)

// DefaultGrace is how long a quorum keeps collecting once it has enough
// attestations, so members answering together all count.
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
	// Logger, when set, takes the members left out of a batch.
	Logger log.Logger
}

// ProveBy returns the attestations of w's batch by the members the manager
// keys with the signer each is configured with, at least its threshold, in
// ascending proof-system order. Each member proves the batch listing its
// proof system alone, under the manager's key: the hash it signs is the one
// its proof system verifies in the assembled batch. A failed quorum refuses
// an effect when as many members name it as it takes to block the
// threshold, waiting for the members that could still make up that many.
func (q *Quorum) ProveBy(ctx context.Context, w *Window, start time.Time, per time.Duration) ([]Attestation, error) {
	reg, err := q.Register(ctx)
	if err != nil {
		return nil, fmt.Errorf("manager registration: %w", err)
	}
	threshold := max(reg.Threshold, 1)
	var usable []*Member
	for _, m := range q.Members {
		k, ok := reg.Keys[m.ProofSystem]
		switch {
		case !ok:
			q.skipped(m, "its proof system is not registered on the rollup manager")
		case k.Signer != m.Attester:
			q.skipped(m, "its proof system accepts another signer", "signer", k.Signer)
		default:
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
	var wg sync.WaitGroup
	// Members cut off by the grace or a lost quorum stop reading the window
	// before it returns.
	defer wg.Wait()
	defer cancel()
	answers := make(chan answer, len(usable))
	for _, m := range usable {
		mw := alone(w, m.ProofSystem, reg.Keys[m.ProofSystem].VKey)
		wg.Go(func() {
			proof, err := m.Client.ProveBy(actx, mw, start, per)
			answers <- answer{m, proof, err}
		})
	}
	var atts []Attestation
	failed := &QuorumError{Threshold: threshold, Usable: len(usable)}
	var grace <-chan time.Time
	var timer *time.Timer
	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()
	for pending := len(usable); pending > 0; {
		select {
		case a := <-answers:
			pending--
			if a.err != nil {
				failed.Failures = append(failed.Failures, MemberFailure{ProofSystem: a.m.ProofSystem, Err: a.err})
				if failed.lost(len(atts), pending) {
					pending = 0
				}
				continue
			}
			atts = append(atts, Attestation{ProofSystem: a.m.ProofSystem, Proof: a.proof})
			switch {
			case uint64(len(atts)) == threshold && pending > 0:
				timer = time.NewTimer(q.Grace)
				grace = timer.C
			case failed.lost(len(atts), pending):
				pending = 0
			}
		case <-grace:
			pending = 0
		}
	}
	if uint64(len(atts)) < threshold {
		if r, n := failed.named(); n >= failed.blocking() {
			failed.Refused = &r
		}
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
	// Usable is the number of members asked.
	Usable   int
	Failures []MemberFailure
	// Refused is the effect enough members name to block the threshold.
	Refused *Refused
}

func (e *QuorumError) Error() string {
	parts := make([]string, len(e.Failures))
	for i, f := range e.Failures {
		parts[i] = fmt.Sprintf("%x: %v", f.ProofSystem, f.Err)
	}
	msg := fmt.Sprintf("quorum of %d not reached: %s", e.Threshold, strings.Join(parts, "; "))
	if e.Refused != nil {
		msg += fmt.Sprintf("; refusing %v", *e.Refused)
	}
	return msg
}

// skipped logs m, left out of a batch for reason.
func (q *Quorum) skipped(m *Member, reason string, ctx ...any) {
	if q.Logger == nil {
		return
	}
	q.Logger.Warn("[eez] attester not asked for this batch", append([]any{
		"event", "eez.composer.attestation.member_skipped",
		"proofSystem", m.ProofSystem, "attester", m.Attester, "reason", reason,
	}, ctx...)...)
}

// blocking is how many of the members it takes to keep the threshold out
// of reach.
func (e *QuorumError) blocking() int { return e.Usable - int(min(e.Threshold, uint64(e.Usable))) + 1 }

// named returns the effect the most members refused the window for, and
// how many; on a tie, the one the lowest proof system names.
func (e *QuorumError) named() (Refused, int) {
	failures := slices.SortedFunc(slices.Values(e.Failures), func(x, y MemberFailure) int { return x.ProofSystem.Cmp(y.ProofSystem) })
	counts := map[Refused]int{}
	for _, f := range failures {
		if r, ok := Refusal(f.Err); ok {
			counts[r]++
		}
	}
	var most Refused
	n := 0
	for _, f := range failures {
		if r, ok := Refusal(f.Err); ok && counts[r] > n {
			most, n = r, counts[r]
		}
	}
	return most, n
}

// lost reports a threshold out of reach whose verdict the pending members
// cannot change: enough members name one effect to block the threshold, or
// the pending ones are too few to make up that many.
func (e *QuorumError) lost(attested, pending int) bool {
	if uint64(attested+pending) >= e.Threshold {
		return false
	}
	_, n := e.named()
	return n >= e.blocking() || n+pending < e.blocking()
}

func (e *QuorumError) Unwrap() []error {
	errs := make([]error, len(e.Failures))
	for i, f := range e.Failures {
		errs[i] = f.Err
	}
	return errs
}

// GRPCStatus is FAILED_PRECONDITION when the members refuse one effect; else
// a refusal's status when the refusals alone keep the threshold out of
// reach; else Unknown, a failure that lies with the node, when a member's
// failure has no prover status; else a transient failure's.
func (e *QuorumError) GRPCStatus() *status.Status {
	if e.Refused != nil {
		return status.New(codes.FailedPrecondition, e.Error())
	}
	failures := slices.SortedFunc(slices.Values(e.Failures), func(x, y MemberFailure) int { return x.ProofSystem.Cmp(y.ProofSystem) })
	var refusal, transient *status.Status
	refusals, node := 0, false
	for _, f := range failures {
		s, ok := status.FromError(f.Err)
		switch {
		case !ok || s.Code() == codes.Unknown || s.Code() == codes.Canceled:
			node = true
		case !Retryable(f.Err):
			refusals++
			if refusal == nil {
				refusal = status.New(s.Code(), e.Error())
			}
		case transient == nil:
			transient = status.New(s.Code(), e.Error())
		}
	}
	switch {
	case uint64(refusals) > uint64(e.Usable)-min(e.Threshold, uint64(e.Usable)):
		return refusal
	case node || transient == nil:
		return status.New(codes.Unknown, e.Error())
	}
	return transient
}
