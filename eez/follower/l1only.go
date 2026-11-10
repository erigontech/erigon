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

package follower

import (
	"context"
	"errors"
	"fmt"

	"github.com/erigontech/erigon/eez/l1"
)

// historyMissingDepth is the depth below the L1 head past which a block L1
// does not serve is pruned rather than not yet served: two Ethereum epochs,
// past finality.
const historyMissingDepth = 64

// l1Only reports whether the follower derives the whole history of an R0
// chain from L1. RollupX chains keep the boot checks they had.
func (f *Follower) l1Only() bool { return f.cfg.Peers == nil && f.l2.RollupX == nil }

// finalizedRoot returns the root L1's finalized block holds before the boot
// scan, which must end holding it; nil when L1 cannot name one.
func (f *Follower) finalizedRoot(ctx context.Context) (*l1.Commitment, error) {
	c, err := l1.FinalizedCommitment(ctx, f.reader, f.l2.Registry, f.l2.RollupID)
	switch {
	case errors.Is(err, l1.ErrNoStateReads):
		f.logger.Debug("[eez] L1 serves no state reads; boot does not check that the local chain holds its finalized root")
		return nil, nil
	case err != nil:
		return nil, err
	case c.Outcome == l1.CommitmentWait:
		f.logger.Warn("[eez] L1's finalized root is unreadable; boot does not check that the local chain holds it",
			"event", "eez.node.sync.history_check_skipped", "cause", c.Cause)
		return nil, nil
	}
	return &c, nil
}

// historyIncomplete turns err into ErrL1HistoryIncomplete in L1-only mode
// when it names a block L1 no longer serves, more than historyMissingDepth
// below its head; L1 has nowhere else to read that history from.
func (f *Follower) historyIncomplete(ctx context.Context, err error) error {
	var u *l1.UnservedError
	if !f.l1Only() || !errors.As(err, &u) {
		return err
	}
	head, herr := f.reader.Head(ctx)
	if herr != nil || u.Block+historyMissingDepth >= head.Number {
		return err
	}
	f.logger.Error("[eez] L1 no longer serves a block L1-only derivation needs", "event", "eez.node.deriver.boot_catch_up.history_incomplete",
		"block", u.Block, "l1Head", head.Number, "err", err)
	return fmt.Errorf("%w: L1 no longer serves block %d, %d blocks below its head: %w", ErrL1HistoryIncomplete, u.Block, head.Number-u.Block, err)
}

// checkRoot checks that the local chain holds the root before names.
func (f *Follower) checkRoot(ctx context.Context, before *l1.Commitment) error {
	if before == nil || before.Outcome != l1.CommitmentAnchor {
		return nil
	}
	if _, ok, err := f.chain.CanonicalNumber(ctx, before.Root); err != nil || ok {
		return err
	}
	return fmt.Errorf("%w: L1-only derivation passed L1 block %d but the local chain lacks its finalized root %x", ErrL1HistoryIncomplete, before.Finalized.Number, before.Root)
}
