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

package parlia

import (
	"errors"
	"fmt"
	"math/bits"
	"math/rand"
	"time"

	"github.com/erigontech/erigon/cl/utils/bls"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/types"
)

const (
	blockInterval             = 450
	wiggleTime                = 1000
	lorentzInitialBackOffTime = 2000
	ancestorGenerationDepth   = 3
	maxAttestationExtraLength = 256
	diffInTurn                = 2
	diffNoTurn                = 1
)

var (
	ErrFutureBlock      = errors.New("parlia: block in the future")
	ErrBlockTooEarly    = errors.New("parlia: block time before parent time plus interval and back-off")
	ErrInvalidHeader    = errors.New("parlia: invalid header field")
	ErrWrongDifficulty  = errors.New("parlia: wrong difficulty")
	ErrCoinbaseMismatch = errors.New("parlia: coinbase does not match the signer")
)

type Chain interface {
	HeaderReader
	Snapshot(hash common.Hash, number uint64) (*Snapshot, error)
}

type Verifier struct {
	config *chain.Config
	now    func() time.Time
}

func NewVerifier(config *chain.Config) *Verifier {
	return &Verifier{config: config, now: time.Now}
}

// VerifyHeader checks the header's seal, authorization, recents, difficulty, block time and
// vote attestation. Gas, base fee and blob fields are left to execution.
func (v *Verifier) VerifyHeader(c Chain, h *types.Header) error {
	if h.Time > uint64(v.now().Unix()) {
		return ErrFutureBlock
	}
	number := h.Number.Uint64()
	if number == 0 || !v.config.IsFermi(number, h.Time) {
		return ErrNotFermi
	}
	if err := v.verifyFields(h); err != nil {
		return err
	}
	parent := c.GetHeader(h.ParentHash, number-1)
	if parent == nil {
		return fmt.Errorf("%w: %d", ErrUnknownAncestor, number-1)
	}
	snap, err := c.Snapshot(parent.Hash(), number-1)
	if err != nil {
		return err
	}
	if err := v.verifyCascadingFields(snap, h, parent); err != nil {
		return err
	}
	if err := v.verifyVoteAttestation(c, snap, h, parent); err != nil {
		return err
	}
	signer, err := RecoverSigner(v.config, h)
	if err != nil {
		return err
	}
	if signer != h.Coinbase {
		return fmt.Errorf("%w: signer %x, coinbase %x", ErrCoinbaseMismatch, signer, h.Coinbase)
	}
	return nil
}

func (v *Verifier) verifyFields(h *types.Header) error {
	if len(h.Extra) < extraVanity+extraSeal {
		return fmt.Errorf("%w: extra too short", ErrInvalidHeader)
	}
	if h.Number.Uint64()%epochLength == 0 {
		if _, err := epochValidatorsEnd(h, epochLength); err != nil {
			return err
		}
	}
	if MilliTimestamp(h)/1000 != h.Time {
		return fmt.Errorf("%w: mix digest %x does not hold milliseconds", ErrInvalidHeader, h.MixDigest)
	}
	if h.UncleHash != empty.UncleHash {
		return fmt.Errorf("%w: uncle hash", ErrInvalidHeader)
	}
	if h.ParentBeaconBlockRoot == nil || *h.ParentBeaconBlockRoot != (common.Hash{}) {
		return fmt.Errorf("%w: parent beacon block root", ErrInvalidHeader)
	}
	if v.config.IsPrague(h.Time) != (h.RequestsHash != nil) {
		return fmt.Errorf("%w: requests hash", ErrInvalidHeader)
	}
	amsterdam := v.config.IsAmsterdam(h.Time)
	if amsterdam != (h.BlockAccessListHash != nil) || amsterdam != (h.SlotNumber != nil) {
		return fmt.Errorf("%w: block access list hash or slot number", ErrInvalidHeader)
	}
	return nil
}

func (v *Verifier) verifyCascadingFields(snap *Snapshot, h, parent *types.Header) error {
	if _, ok := snap.Validators[h.Coinbase]; !ok {
		return fmt.Errorf("%w: %x", ErrUnauthorizedValidator, h.Coinbase)
	}
	if snap.SignRecently(h.Coinbase) {
		return fmt.Errorf("%w: %x", ErrRecentlySigned, h.Coinbase)
	}
	want := uint64(diffNoTurn)
	if snap.inturnValidator() == h.Coinbase {
		want = diffInTurn
	}
	if !h.Difficulty.IsUint64() || h.Difficulty.Uint64() != want {
		return fmt.Errorf("%w: have %d, want %d", ErrWrongDifficulty, &h.Difficulty, want)
	}
	if MilliTimestamp(h) < MilliTimestamp(parent)+blockInterval+snap.backOffTime(h, h.Coinbase) {
		return ErrBlockTooEarly
	}
	return nil
}

func (v *Verifier) verifyVoteAttestation(c Chain, snap *Snapshot, h, parent *types.Header) error {
	att, err := ParseVoteAttestation(h, epochLength)
	if err != nil || att == nil {
		return err
	}
	if len(att.Extra) > maxAttestationExtraLength {
		return fmt.Errorf("%w: extra too long: %d", ErrInvalidAttestation, len(att.Extra))
	}
	if att.Data.SourceNumber >= att.Data.TargetNumber {
		return fmt.Errorf("%w: source %d not below target %d", ErrInvalidAttestation, att.Data.SourceNumber, att.Data.TargetNumber)
	}
	justified, justifiedHash := snap.Justified()
	if snap.Attestation == nil || att.Data.SourceNumber != justified || att.Data.SourceHash != justifiedHash {
		return fmt.Errorf("%w: source mismatch: want %d %x, have %d %x", ErrInvalidAttestation,
			justified, justifiedHash, att.Data.SourceNumber, att.Data.SourceHash)
	}

	ancestor, match := parent, false
	for range ancestorGenerationDepth {
		if att.Data.TargetNumber == ancestor.Number.Uint64() && att.Data.TargetHash == ancestor.Hash() {
			match = true
			break
		}
		next := c.GetHeader(ancestor.ParentHash, ancestor.Number.Uint64()-1)
		if next == nil {
			return fmt.Errorf("%w: %d", ErrUnknownAncestor, ancestor.Number.Uint64()-1)
		}
		ancestor = next
	}
	if !match {
		return fmt.Errorf("%w: target mismatch: %d %x", ErrInvalidAttestation, att.Data.TargetNumber, att.Data.TargetHash)
	}

	targetSnap, err := c.Snapshot(ancestor.ParentHash, ancestor.Number.Uint64()-1)
	if err != nil {
		return err
	}
	validators := sortedValidators(targetSnap.Validators)
	if bits.OnesCount64(att.VoteAddressSet) > len(validators) {
		return fmt.Errorf("%w: more votes than validators", ErrInvalidAttestation)
	}
	voteAddrs := make([][]byte, 0, len(validators))
	for i, val := range validators {
		if i < 64 && att.VoteAddressSet&(1<<i) != 0 {
			voteAddrs = append(voteAddrs, targetSnap.Validators[val].VoteAddress[:])
		}
	}
	if len(voteAddrs) < (2*len(validators)+2)/3 {
		return fmt.Errorf("%w: not enough votes: %d of %d", ErrInvalidAttestation, len(voteAddrs), len(validators))
	}
	dataHash := att.Data.Hash()
	ok, err := bls.VerifyAggregate(att.AggSignature[:], dataHash[:], voteAddrs)
	if err != nil {
		return fmt.Errorf("%w: aggregate signature: %w", ErrInvalidAttestation, err)
	}
	if !ok {
		return fmt.Errorf("%w: aggregate signature does not verify", ErrInvalidAttestation)
	}
	return nil
}

func (s *Snapshot) inturnValidator() common.Address {
	validators := sortedValidators(s.Validators)
	return validators[(s.Number+1)/uint64(s.TurnLength)%uint64(len(validators))]
}

func (s *Snapshot) backOffTime(h *types.Header, val common.Address) uint64 {
	inTurn := s.inturnValidator()
	if val == inTurn {
		return 0
	}
	counts := s.countRecents()
	signedRecently := func(addr common.Address) bool { return counts[addr] >= s.TurnLength }
	if signedRecently(val) {
		return 0
	}
	delay := uint64(lorentzInitialBackOffTime)
	if signedRecently(inTurn) {
		delay = 0
	}
	var candidates []common.Address
	for _, addr := range sortedValidators(s.Validators) {
		if !signedRecently(addr) && addr != inTurn {
			candidates = append(candidates, addr)
		}
	}
	idx := -1
	for i, addr := range candidates {
		if addr == val {
			idx = i
		}
	}
	if idx < 0 {
		return 0
	}

	// The shuffle is part of the consensus rules; math/rand keeps it deterministic per seed.
	r := rand.New(rand.NewSource(int64(h.Number.Uint64() / uint64(s.TurnLength)))) //nolint:gosec
	steps := make([]uint64, len(candidates))
	for i := range steps {
		steps[i] = uint64(i)
	}
	r.Shuffle(len(steps), func(i, j int) { steps[i], steps[j] = steps[j], steps[i] })

	if delay == 0 {
		if steps[idx] == 0 {
			return 0
		}
		return lorentzInitialBackOffTime + (steps[idx]-1)*wiggleTime
	}
	return delay + steps[idx]*wiggleTime
}
