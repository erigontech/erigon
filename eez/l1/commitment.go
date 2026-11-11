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

package l1

import (
	"context"
	"errors"
	"fmt"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/eez/protocol"
)

var (
	// ErrNoStateReads reports an L1 source that is not a StateReader.
	ErrNoStateReads = errors.New("L1 source serves no state reads")
	// ErrZeroRoot reports a rollup registered with root zero, a misconfigured
	// registration.
	ErrZeroRoot = errors.New("rollup registered with root zero")
	// ErrNotRollups reports an answer to rollups(rollupId) that does not
	// decode as its return, as from a contract at a wrong registry address.
	ErrNotRollups = errors.New("registry answer is not a rollups return")
)

// CommitmentOutcome is what L1's finalized block F says of a rollup's
// history.
type CommitmentOutcome uint8

const (
	// CommitmentWait: L1 cannot name the root yet, having no finalized block
	// or failing a read that a retry can mend.
	CommitmentWait CommitmentOutcome = iota
	// CommitmentSeedGenesis: the rollup is not registered at F, so no post
	// precedes F and its history starts at L2 genesis.
	CommitmentSeedGenesis
	// CommitmentAnchor: the registry holds the rollup's root H_F at F.
	CommitmentAnchor
)

// Commitment is the rollup's root as L1's finalized block F holds it.
type Commitment struct {
	Outcome CommitmentOutcome
	// Finalized is F, unless Outcome is CommitmentWait.
	Finalized Header
	// Root is H_F when Outcome is CommitmentAnchor.
	Root common.Hash
	// Cause is why Outcome is CommitmentWait.
	Cause error
}

// FinalizedCommitment reads L1's finalized block F and the root the
// registry holds for rollupID at F. The registry's code and record are read
// at F's hash, so that a reorg cannot mix two blocks. It errs only where
// retrying cannot change the answer: r is not a StateReader
// (ErrNoStateReads), the rollup is registered with root zero (ErrZeroRoot),
// the answer is not a rollups return (ErrNotRollups), or ctx ended.
func FinalizedCommitment(ctx context.Context, r ChainReader, registry common.Address, rollupID uint64) (Commitment, error) {
	sr, ok := r.(StateReader)
	if !ok {
		return Commitment{}, fmt.Errorf("%w: %T", ErrNoStateReads, r)
	}
	wait := func(cause error) (Commitment, error) {
		if err := ctx.Err(); err != nil {
			return Commitment{}, err
		}
		return Commitment{Outcome: CommitmentWait, Cause: cause}, nil
	}
	f, err := r.Finalized(ctx)
	if err != nil {
		return wait(fmt.Errorf("L1 finalized block: %w", err))
	}
	code, err := sr.CodeAt(ctx, registry, f.Hash)
	if err != nil {
		return wait(fmt.Errorf("registry code at L1 block %d: %w", f.Number, err))
	}
	if len(code) == 0 {
		return Commitment{Outcome: CommitmentSeedGenesis, Finalized: *f}, nil
	}
	out, err := sr.CallAt(ctx, Call{To: registry, Data: protocol.EncodeRollups(rollupID)}, f.Hash)
	if err != nil {
		return wait(fmt.Errorf("rollups(%d) at L1 block %d: %w", rollupID, f.Number, err))
	}
	record, err := protocol.DecodeRollupsReturn(out)
	if err != nil {
		return Commitment{}, fmt.Errorf("%w: %x answered rollups(%d) at L1 block %d: %w", ErrNotRollups, registry, rollupID, f.Number, err)
	}
	if record.RollupContract == (common.Address{}) {
		return Commitment{Outcome: CommitmentSeedGenesis, Finalized: *f}, nil
	}
	if record.Root == (common.Hash{}) {
		return Commitment{}, fmt.Errorf("%w: rollup %d at L1 block %d", ErrZeroRoot, rollupID, f.Number)
	}
	return Commitment{Outcome: CommitmentAnchor, Finalized: *f, Root: record.Root}, nil
}
