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

package deriver

import (
	"errors"
	"fmt"
	"math/bits"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/eez/config"
	"github.com/erigontech/erigon/execution/types"
)

// ErrSchedule reports a block or candidate off the rollupX schedule.
var ErrSchedule = errors.New("off the rollupX schedule")

// AnchorKind classifies an accepted endpoint of a rollupX candidate.
type AnchorKind int

const (
	// LiveAnchor is an endpoint whose timestamp is its L1 block's: it
	// reseeds prevRandao for the blocks after it.
	LiveAnchor AnchorKind = iota
	// CatchUpAnchor is a historical endpoint; the seed carries over.
	CatchUpAnchor
)

// Schedule is the block grid of a rollupX chain: block n is at genesis time
// plus n intervals, and Sync blocks are every SyncIntervalBlocks blocks.
type Schedule struct {
	cfg         config.RollupXConfig
	genesisHash common.Hash
	genesisTime uint64
	slot        uint64
}

// NewSchedule returns the schedule of x pinned to genesis.
func NewSchedule(x *config.RollupXConfig, genesis *types.Header) (*Schedule, error) {
	hi, slot := bits.Mul64(x.BlockIntervalSeconds, x.SyncIntervalBlocks)
	if hi != 0 {
		return nil, fmt.Errorf("%w: settlement interval overflows", ErrSchedule)
	}
	if err := x.CheckGenesis(genesis); err != nil {
		return nil, err
	}
	return &Schedule{cfg: *x, genesisHash: genesis.Hash(), genesisTime: genesis.Time, slot: slot}, nil
}

// Timestamp returns the timestamp of block number.
func (s *Schedule) Timestamp(number uint64) (uint64, error) {
	hi, offset := bits.Mul64(number, s.cfg.BlockIntervalSeconds)
	t, carry := bits.Add64(s.genesisTime, offset, 0)
	if hi != 0 || carry != 0 {
		return 0, fmt.Errorf("%w: block %d timestamp overflows", ErrSchedule, number)
	}
	return t, nil
}

// CheckPosition checks that h sits at its number's timestamp and, for block
// 0, is the genesis.
func (s *Schedule) CheckPosition(h *types.Header) error {
	if !h.Number.IsUint64() {
		return fmt.Errorf("%w: block %d", ErrSchedule, &h.Number)
	}
	n := h.Number.Uint64()
	t, err := s.Timestamp(n)
	if err != nil {
		return err
	}
	if h.Time != t || (n == 0 && h.Hash() != s.genesisHash) {
		return fmt.Errorf("%w: block %d at time %d", ErrSchedule, n, h.Time)
	}
	return nil
}

// Candidate checks a candidate of blocks blocks on parent, settled toward
// targetTime, and returns its endpoint and anchor kind. A candidate starts
// at a Sync block and covers whole intervals; a catch-up one carries no
// cross-chain actions.
func (s *Schedule) Candidate(parent *types.Header, blocks, targetTime uint64, hasActions bool) (uint64, AnchorKind, error) {
	if err := s.CheckPosition(parent); err != nil {
		return 0, 0, err
	}
	interval := s.cfg.SyncIntervalBlocks
	pn := parent.Number.Uint64()
	if blocks == 0 || pn%interval != 0 || blocks%interval != 0 {
		return 0, 0, fmt.Errorf("%w: %d blocks on block %d are not whole Sync intervals", ErrSchedule, blocks, pn)
	}
	end, carry := bits.Add64(pn, blocks, 0)
	if carry != 0 {
		return 0, 0, fmt.Errorf("%w: endpoint overflows", ErrSchedule)
	}
	endTime, err := s.Timestamp(end)
	if err != nil {
		return 0, 0, err
	}
	kind, err := s.AnchorKind(end, endTime, targetTime)
	if err != nil {
		return 0, 0, err
	}
	if kind == CatchUpAnchor && hasActions {
		return 0, 0, fmt.Errorf("%w: catch-up candidate ending at %d carries cross-chain actions", ErrSchedule, end)
	}
	return end, kind, nil
}

// AnchorKind classifies the endpoint number at timestamp, accepted in an L1
// block at l1Time: live when the two timestamps are equal, catch-up when the
// endpoint is older. The endpoint must be a Sync block on the grid and l1Time
// a settlement slot boundary.
func (s *Schedule) AnchorKind(number, timestamp, l1Time uint64) (AnchorKind, error) {
	t, err := s.Timestamp(number)
	if err != nil {
		return 0, err
	}
	ref := s.cfg.L1Reference.Timestamp
	if number%s.cfg.SyncIntervalBlocks != 0 || timestamp != t || l1Time < ref || (l1Time-ref)%s.slot != 0 || timestamp > l1Time {
		return 0, fmt.Errorf("%w: endpoint %d at %d in an L1 block at %d", ErrSchedule, number, timestamp, l1Time)
	}
	if timestamp == l1Time {
		return LiveAnchor, nil
	}
	return CatchUpAnchor, nil
}
