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
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/eez/l1"
	"github.com/erigontech/erigon/eez/protocol"
	"github.com/erigontech/erigon/eez/prove/provepb"
)

// attester is a quorum member's prover: it signs the public inputs hash of
// the batch it is sent under vkey, after delay, unless refusal is set.
type attester struct {
	ps, signer common.Address
	vkey       common.Hash
	p          *prover
	member     *Member
}

func newAttester(t *testing.T, ps byte, delay time.Duration, refusal error) *attester {
	t.Helper()
	return newRefusingAttester(t, ps, delay, refusal, nil)
}

// newRefusingAttester is newAttester whose refusal carries details.
func newRefusingAttester(t *testing.T, ps byte, delay time.Duration, refusal error, details []byte) *attester {
	t.Helper()
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	a := &attester{ps: common.Address{ps}, signer: crypto.PubkeyToAddress(key.PublicKey), vkey: common.Hash{ps, 0x77}}
	a.p = &prover{}
	a.p.answer = func(int) (*provepb.Proof, *provepb.Rejected, error) {
		if delay > 0 {
			time.Sleep(delay)
		}
		if refusal != nil {
			if details != nil {
				return nil, &provepb.Rejected{Code: int32(status.Code(refusal)), Message: status.Convert(refusal).Message(), Details: details}, nil
			}
			return nil, nil, refusal
		}
		b, err := protocol.DecodePostAndVerifyBatch(a.p.finalized().GetSettlement().GetAbiCalldata())
		if err != nil {
			return nil, nil, err
		}
		hash, err := publicInputsHash(b, a.vkey, nil)
		if err != nil {
			return nil, nil, err
		}
		return &provepb.Proof{PublicInputsHash: hash[:], Proof: sign(t, key, hash[:])}, nil, nil
	}
	c, err := Dial(serve(t, a.p), a.signer, common.Hash{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })
	a.member = &Member{ProofSystem: a.ps, Attester: a.signer, Client: c}
	return a
}

// registered is the manager's registration of attesters with threshold.
func registered(threshold uint64, attesters ...*attester) func(context.Context) (l1.Registration, error) {
	reg := l1.Registration{Threshold: threshold, Keys: map[common.Address]l1.Key{}}
	for _, a := range attesters {
		reg.Keys[a.ps] = l1.Key{VKey: a.vkey, Signer: a.signer}
	}
	return func(context.Context) (l1.Registration, error) { return reg, nil }
}

func members(attesters ...*attester) []*Member {
	var out []*Member
	for _, a := range attesters {
		out = append(out, a.member)
	}
	return out
}

// quorumWindow is anchor402's window, its batch listing proof systems.
func quorumWindow(t *testing.T, proofSystems ...common.Address) *Window {
	t.Helper()
	w := anchor402(t).window()
	b := *w.Batch
	b.ProofSystems = proofSystems
	b.RollupIDsWithProofSystems = []protocol.RollupIDWithProofSystems{{RollupID: w.RollupID, ProofSystemIndexes: []uint64{}}}
	for i := range proofSystems {
		b.RollupIDsWithProofSystems[0].ProofSystemIndexes = append(b.RollupIDsWithProofSystems[0].ProofSystemIndexes, uint64(i))
	}
	w.Batch = &b
	return w
}

func proofSystemsOf(atts []Attestation) []common.Address {
	var out []common.Address
	for _, a := range atts {
		out = append(out, a.ProofSystem)
	}
	return out
}

// TestQuorumAssembles: every keyed member is asked with the batch listing
// its proof system alone, under the manager's key, and the attestations
// come back in ascending proof-system order whatever the members' order;
// each one verifies in the assembled batch.
func TestQuorumAssembles(t *testing.T) {
	a, b, c := newAttester(t, 0xa, 0, nil), newAttester(t, 0xb, 0, nil), newAttester(t, 0xc, 0, nil)
	q := &Quorum{Members: members(c, a, b), Register: registered(2, a, b, c), Grace: time.Second}
	w := quorumWindow(t, a.ps, b.ps, c.ps)
	atts, err := q.ProveBy(t.Context(), w, time.Now().Add(5*time.Second), 0)
	require.NoError(t, err)
	require.Equal(t, []common.Address{a.ps, b.ps, c.ps}, proofSystemsOf(atts))
	for _, x := range []*attester{a, b, c} {
		sent, err := protocol.DecodePostAndVerifyBatch(x.p.finalized().GetSettlement().GetAbiCalldata())
		require.NoError(t, err)
		require.Equal(t, []common.Address{x.ps}, sent.ProofSystems)
	}
	assembled := *w.Batch
	p, err := protocol.NewPublicInputs(&assembled, nil, [][]byte{nil}, [][]common.Hash{{a.vkey, b.vkey, c.vkey}}, common.Address{})
	require.NoError(t, err)
	hashes, err := p.ProofSystemHashes(3)
	require.NoError(t, err)
	for i, x := range []*attester{a, b, c} {
		_, err := attest(atts[i].Proof, hashes[i][:], x.signer)
		require.NoError(t, err, "proof system %x", x.ps)
	}
}

// TestQuorumThreshold: a quorum reached without a refusing member; a
// member the manager does not key, or whose proof system signs for another
// address, is not asked; a threshold no usable members reach is refused
// before asking any.
func TestQuorumThreshold(t *testing.T) {
	a, b := newAttester(t, 0xa, 0, nil), newAttester(t, 0xb, 0, nil)
	bad := newAttester(t, 0xc, 0, status.Error(codes.InvalidArgument, "window refused"))
	atts, err := (&Quorum{Members: members(a, b, bad), Register: registered(2, a, b, bad), Grace: time.Millisecond}).
		ProveBy(t.Context(), quorumWindow(t, a.ps, b.ps, bad.ps), time.Now().Add(5*time.Second), 0)
	require.NoError(t, err)
	require.Equal(t, []common.Address{a.ps, b.ps}, proofSystemsOf(atts))

	a = newAttester(t, 0xa, 0, nil)
	unkeyed, rotated := newAttester(t, 0xd, 0, nil), newAttester(t, 0xe, 0, nil)
	reg := registered(2, a, rotated)
	register := func(ctx context.Context) (l1.Registration, error) {
		r, err := reg(ctx)
		r.Keys[rotated.ps] = l1.Key{VKey: rotated.vkey, Signer: common.Address{0xff}}
		return r, err
	}
	_, err = (&Quorum{Members: members(a, unkeyed, rotated), Register: register}).
		ProveBy(t.Context(), quorumWindow(t, a.ps, unkeyed.ps, rotated.ps), time.Now().Add(5*time.Second), 0)
	require.ErrorIs(t, err, ErrQuorumUnreachable)
	for _, x := range []*attester{a, unkeyed, rotated} {
		calls, _ := x.p.last()
		require.Zero(t, calls, "nobody asked")
	}
}

// TestQuorumKeysEachMember: a member signing under any key but the
// manager's, as its address padded, does not count.
func TestQuorumKeysEachMember(t *testing.T) {
	a := newAttester(t, 0xa, 0, nil)
	padded := l1.Key{VKey: common.BytesToHash(a.signer[:]), Signer: a.signer}
	register := func(context.Context) (l1.Registration, error) {
		return l1.Registration{Threshold: 1, Keys: map[common.Address]l1.Key{a.ps: padded}}, nil
	}
	_, err := (&Quorum{Members: members(a), Register: register}).
		ProveBy(t.Context(), quorumWindow(t, a.ps), time.Now().Add(5*time.Second), 0)
	require.ErrorIs(t, err, ErrAttestation)
	require.Equal(t, codes.Unknown, status.Code(err), "the node's to fix")
}

// TestQuorumFailure: a quorum that is not reached reports the provers'
// status when every failed member answered with one, a refusal before a
// transient failure; the registration failing is the node's.
func TestQuorumFailure(t *testing.T) {
	down := func(ps byte) *attester { return newAttester(t, ps, 0, status.Error(codes.Unavailable, "down")) }
	a, b := down(0xa), down(0xb)
	_, err := (&Quorum{Members: members(a, b), Register: registered(2, a, b)}).
		ProveBy(t.Context(), quorumWindow(t, a.ps, b.ps), time.Now().Add(300*time.Millisecond), 0)
	// A member's last attempt may meet the deadline instead of the outage.
	require.True(t, Retryable(err), "%v", err)

	refusing := newAttester(t, 0xc, 0, status.Error(codes.InvalidArgument, "window refused"))
	_, err = (&Quorum{Members: members(a, refusing), Register: registered(2, a, refusing)}).
		ProveBy(t.Context(), quorumWindow(t, a.ps, refusing.ps), time.Now().Add(300*time.Millisecond), 0)
	require.Equal(t, codes.InvalidArgument, status.Code(err))

	unreadable := func(context.Context) (l1.Registration, error) { return l1.Registration{}, errors.New("L1 down") }
	_, err = (&Quorum{Members: members(a), Register: unreadable}).
		ProveBy(t.Context(), quorumWindow(t, a.ps), time.Now().Add(time.Second), 0)
	require.ErrorContains(t, err, "L1 down")
	require.Equal(t, codes.Unknown, status.Code(err))
}

// TestQuorumGrace: once the threshold is met, a late answer within the
// grace still counts and a member past it is cancelled.
func TestQuorumGrace(t *testing.T) {
	a, b := newAttester(t, 0xa, 0, nil), newAttester(t, 0xb, 0, nil)
	late, stuck := newAttester(t, 0xc, 100*time.Millisecond, nil), newAttester(t, 0xd, 5*time.Second, nil)
	q := &Quorum{Members: members(a, b, late, stuck), Register: registered(2, a, b, late, stuck), Grace: 400 * time.Millisecond}
	begin := time.Now()
	atts, err := q.ProveBy(t.Context(), quorumWindow(t, a.ps, b.ps, late.ps, stuck.ps), time.Now().Add(10*time.Second), 0)
	require.NoError(t, err)
	require.Equal(t, []common.Address{a.ps, b.ps, late.ps}, proofSystemsOf(atts))
	require.Less(t, time.Since(begin), 2*time.Second, "the stuck member is cancelled")
	require.True(t, slices.IsSortedFunc(atts, func(x, y Attestation) int { return x.ProofSystem.Cmp(y.ProofSystem) }))
}

// TestQuorumErrorStatus: the status of a failed quorum, whatever order the
// members failed in: a refusal only when the refusals alone keep the
// threshold out of reach; else the node's when a member's failure is the
// node's; else the transient failure's.
func TestQuorumErrorStatus(t *testing.T) {
	refused := status.Error(codes.InvalidArgument, "window refused")
	down := status.Error(codes.Unavailable, "down")
	local := errors.New("window block 9: no witness")
	for _, c := range []struct {
		name      string
		usable    int
		threshold uint64
		errs      []error
		want      codes.Code
	}{
		{"refusals blocking", 2, 2, []error{refused, local}, codes.InvalidArgument},
		{"refusals blocking, other order", 2, 2, []error{local, refused}, codes.InvalidArgument},
		{"a refusal not blocking, a node failure", 3, 2, []error{refused, local}, codes.Unknown},
		{"a refusal not blocking, other order", 3, 2, []error{local, refused}, codes.Unknown},
		{"a refusal not blocking, an outage", 3, 2, []error{refused, down}, codes.Unavailable},
		{"an outage, then a blocking refusal", 2, 2, []error{down, refused}, codes.InvalidArgument},
		{"outages", 2, 2, []error{down, down}, codes.Unavailable},
		{"cancelled", 2, 2, []error{status.Error(codes.Canceled, "stopping"), down}, codes.Unknown},
	} {
		t.Run(c.name, func(t *testing.T) {
			e := &QuorumError{Threshold: c.threshold, Usable: c.usable}
			for i, err := range c.errs {
				e.Failures = append(e.Failures, MemberFailure{ProofSystem: common.Address{byte(i + 1)}, Err: err})
			}
			require.Equal(t, c.want, status.Code(e))
		})
	}
}

// TestQuorumGivesUp: once the members left can neither reach the threshold
// nor name an effect with enough of the rest to block it, the quorum fails
// without waiting for them.
func TestQuorumGivesUp(t *testing.T) {
	refusing := newAttester(t, 0xa, 0, status.Error(codes.InvalidArgument, "window refused"))
	failing := newAttester(t, 0xb, 0, status.Error(codes.Internal, "prover fault"))
	stuck := newAttester(t, 0xc, 3*time.Second, nil)
	begin := time.Now()
	_, err := (&Quorum{Members: members(refusing, failing, stuck), Register: registered(2, refusing, failing, stuck)}).
		ProveBy(t.Context(), quorumWindow(t, refusing.ps, failing.ps, stuck.ps), time.Now().Add(10*time.Second), 0)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	require.Less(t, time.Since(begin), 1500*time.Millisecond)

	// An attestation can leave the threshold out of reach as a failure does.
	refusing2 := newAttester(t, 0xd, 0, status.Error(codes.InvalidArgument, "window refused"))
	late := newAttester(t, 0xe, 200*time.Millisecond, nil)
	stuck2 := newAttester(t, 0xf, 3*time.Second, nil)
	all := []*attester{refusing, refusing2, late, stuck2}
	begin = time.Now()
	_, err = (&Quorum{Members: members(all...), Register: registered(3, all...)}).
		ProveBy(t.Context(), quorumWindow(t, refusing.ps, refusing2.ps, late.ps, stuck2.ps), time.Now().Add(10*time.Second), 0)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	require.Less(t, time.Since(begin), 1500*time.Millisecond)
}

// refusalDetails encodes r as a prover's refusal details.
func refusalDetails(t *testing.T, r Refused) []byte {
	t.Helper()
	pf := &provepb.ProveFailure{ActionableFailure: &provepb.ProveFailure_Inbound{Inbound: &provepb.InboundFailure{EntryIndex: r.Index, EntryHash: r.Hash[:]}}}
	if r.Outbound {
		pf.ActionableFailure = &provepb.ProveFailure_Outbound{Outbound: &provepb.OutboundFailure{TransactionIndex: r.Index, TransactionHash: r.Hash[:]}}
	}
	raw, err := proto.Marshal(pf)
	require.NoError(t, err)
	return raw
}

// TestQuorumNamesTheRefusedEffect: a failed quorum refuses an effect only
// when as many members name it as it takes to block the threshold, waiting
// for the members that could still do so.
func TestQuorumNamesTheRefusedEffect(t *testing.T) {
	poison := Refused{Index: 1, Hash: common.Hash{0x11}}
	other := Refused{Outbound: true, Index: 3, Hash: common.Hash{0x22}}
	type member struct {
		delay     time.Duration
		named     *Refused // nil signs, unless rejecting
		rejecting bool
	}
	for _, c := range []struct {
		name      string
		threshold uint64
		members   []member
		want      *Refused
	}{
		{"a blocking set names it", 2, []member{{named: &poison}, {delay: 10 * time.Millisecond, named: &poison}, {}}, &poison},
		{"one alone cannot", 2, []member{{named: &poison}, {rejecting: true}, {}}, nil},
		{"members disagree", 2, []member{{named: &poison}, {named: &other}, {}}, nil},
		{"waits for a member that could still name it", 3, []member{{named: &poison}, {rejecting: true}, {delay: 100 * time.Millisecond, named: &poison}, {}}, &poison},
		{"waits for a first name", 2, []member{{rejecting: true}, {delay: 100 * time.Millisecond, named: &poison}}, &poison},
		{"a lone member", 1, []member{{named: &other}}, &other},
	} {
		t.Run(c.name, func(t *testing.T) {
			var as []*attester
			for i, m := range c.members {
				var refusal error
				var details []byte
				switch {
				case m.named != nil:
					refusal, details = status.Error(codes.FailedPrecondition, "effect refused"), refusalDetails(t, *m.named)
				case m.rejecting:
					refusal = status.Error(codes.InvalidArgument, "window refused")
				}
				as = append(as, newRefusingAttester(t, byte(0xa+i), m.delay, refusal, details))
			}
			var pss []common.Address
			for _, a := range as {
				pss = append(pss, a.ps)
			}
			_, err := (&Quorum{Members: members(as...), Register: registered(c.threshold, as...)}).
				ProveBy(t.Context(), quorumWindow(t, pss...), time.Now().Add(3*time.Second), 0)
			require.Error(t, err)
			r, ok := Refusal(err)
			if c.want == nil {
				require.False(t, ok, "%v", r)
				return
			}
			require.True(t, ok, "%v", err)
			require.Equal(t, *c.want, r)
			require.Equal(t, codes.FailedPrecondition, status.Code(err))
		})
	}
}

// TestQuorumGraceEndsAtTheDeadline: the grace never runs past the time the
// proof must be ready by; a threshold of zero needs one attestation.
func TestQuorumGraceEndsAtTheDeadline(t *testing.T) {
	a, slow := newAttester(t, 0xa, 0, nil), newAttester(t, 0xb, 2*time.Second, nil)
	begin := time.Now()
	atts, err := (&Quorum{Members: members(a, slow), Register: registered(0, a, slow), Grace: 5 * time.Second}).
		ProveBy(t.Context(), quorumWindow(t, a.ps, slow.ps), time.Now().Add(300*time.Millisecond), 0)
	require.NoError(t, err)
	require.Equal(t, []common.Address{a.ps}, proofSystemsOf(atts))
	require.Less(t, time.Since(begin), time.Second)
}
