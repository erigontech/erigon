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
	"context"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/eez/config"
	"github.com/erigontech/erigon/eez/l1"
	"github.com/erigontech/erigon/eez/protocol"
)

// ErrHistory reports L1 history updates, or an L1 history state, that
// contradict the canonical L1 chain or the update schedule.
var ErrHistory = errors.New("invalid L1 history")

// historyEndpointSlot holds the number of the latest L1 block the history
// predeploy imported; slot 0 maps imported numbers to their hashes.
const historyEndpointSlot = 1

// historyHashSlot is the slot of the hash of L1 block number.
func historyHashSlot(number uint64) common.Hash {
	var key [64]byte
	binary.BigEndian.PutUint64(key[24:32], number)
	return crypto.Keccak256Hash(key[:])
}

// historyState is the L1 origin a rollupX v0.1 block inherits: the latest
// L1 block its history imported, or the genesis L1 reference before the
// first import.
type historyState struct {
	imported bool
	endpoint uint64
	hash     common.Hash
}

// historyAt reads the history state after L2 block number.
func (d *Deriver) historyAt(ctx context.Context, number uint64) (historyState, error) {
	word, err := d.chain.Storage(ctx, number, protocol.HistoryAddress, common.Hash{31: historyEndpointSlot})
	if err != nil {
		return historyState{}, err
	}
	n := new(uint256.Int).SetBytes32(word[:])
	if !n.IsUint64() {
		return historyState{}, fmt.Errorf("%w: block %d: endpoint %v", ErrHistory, number, n)
	}
	endpoint := n.Uint64()
	hash, err := d.chain.Storage(ctx, number, protocol.HistoryAddress, historyHashSlot(endpoint))
	if err != nil {
		return historyState{}, err
	}
	ref := d.l2.RollupX.L1Reference
	switch {
	case endpoint == 0 && hash != (common.Hash{}):
		return historyState{}, fmt.Errorf("%w: block %d: L1 block 0 imported", ErrHistory, number)
	case endpoint == 0:
		return historyState{endpoint: ref.Number, hash: ref.Hash}, nil
	case endpoint < ref.Number || hash == (common.Hash{}):
		return historyState{}, fmt.Errorf("%w: block %d: endpoint %d with hash %x", ErrHistory, number, endpoint, hash)
	}
	return historyState{imported: true, endpoint: endpoint, hash: hash}, nil
}

// bridge is the canonical L1 headers from a history endpoint to a batch's
// anchor, linked by their parent hashes.
type bridge struct {
	first   uint64
	headers []*l1.Header
}

// at returns the header of number, which the bridge must hold.
func (b *bridge) at(number uint64) (*l1.Header, error) {
	if number < b.first || number-b.first >= uint64(len(b.headers)) {
		return nil, fmt.Errorf("%w: L1 block %d outside %d to %d", ErrHistory, number, b.first, b.first+uint64(len(b.headers))-1)
	}
	return b.headers[number-b.first], nil
}

// maxBridge bounds the L1 headers one batch's history reads: four days of
// Chiado blocks.
const maxBridge = 131_072

// readBridge reads the canonical L1 headers from to through, and checks
// they link.
func (d *Deriver) readBridge(ctx context.Context, from, through uint64) (*bridge, error) {
	if from > through || through-from >= maxBridge {
		return nil, fmt.Errorf("%w: endpoint %d against anchor %d", ErrHistory, from, through)
	}
	b := &bridge{first: from, headers: make([]*l1.Header, 0, through-from+1)}
	for n := from; n <= through; n++ {
		h, err := d.l1.HeaderByNumber(ctx, n)
		if err != nil {
			return nil, fmt.Errorf("L1 history: block %d: %w", n, err)
		}
		if k := len(b.headers); k > 0 && (h.ParentHash != b.headers[k-1].Hash || h.Time <= b.headers[k-1].Time) {
			return nil, fmt.Errorf("%w: L1 block %d does not follow %d", l1.ErrInconsistent, n, n-1)
		}
		b.headers = append(b.headers, h)
	}
	return b, nil
}

// historySchedule places the history updates of a rollupX v0.1 chain: the
// L1 blocks an update may import are those up to its source cutoff, the
// start of the Sync interval it is HistoryUpdateOffset+1 blocks into.
type historySchedule struct {
	genesis  uint64
	block    uint64
	interval uint64
	delay    uint64
}

func newHistorySchedule(x *config.RollupXConfig, genesisTime uint64) historySchedule {
	return historySchedule{
		genesis:  genesisTime,
		block:    x.BlockIntervalSeconds,
		interval: x.BlockIntervalSeconds * x.SyncIntervalBlocks,
		delay:    x.BlockIntervalSeconds * (*x.HistoryUpdateOffset + 1),
	}
}

// cutoff is the source cutoff of the block at time t: the latest L1 time
// its origin may have. Before the first update it is a Sync interval
// before genesis.
func (s historySchedule) cutoff(t uint64) uint64 {
	elapsed := t - s.genesis
	if elapsed < s.delay {
		return s.genesis - s.interval
	}
	return s.genesis + (elapsed-s.delay)/s.interval*s.interval
}

// transition checks the history update of the block at time t on state:
// only in an update block, importing increasing L1 blocks of the bridge, no
// older than the endpoint, and none past the cutoff. It returns the block's
// origin, the latest L1 block it imported or else the endpoint.
func (s historySchedule) transition(t uint64, state historyState, b *bridge, update *protocol.HistoryTx) (*l1.Header, error) {
	if t <= s.genesis || (t-s.genesis)%s.block != 0 {
		return nil, fmt.Errorf("%w: time %d off the schedule", ErrHistory, t)
	}
	cutoff := s.cutoff(t)
	origin, err := b.at(state.endpoint)
	if err != nil {
		return nil, err
	}
	if origin.Hash != state.hash {
		return nil, fmt.Errorf("%w: endpoint %d is %x, L1 has %x", ErrHistory, state.endpoint, state.hash, origin.Hash)
	}
	if update != nil {
		if cutoff+s.delay != t {
			return nil, fmt.Errorf("%w: update at time %d, off its delay after cutoff %d", ErrHistory, t, cutoff)
		}
		var previous uint64
		if state.imported {
			previous = state.endpoint
		}
		for _, block := range update.Blocks {
			if block.Number <= previous || block.Number < state.endpoint {
				return nil, fmt.Errorf("%w: import of L1 block %d after %d, endpoint %d", ErrHistory, block.Number, previous, state.endpoint)
			}
			if origin, err = b.at(block.Number); err != nil {
				return nil, err
			}
			if origin.Hash != block.Hash {
				return nil, fmt.Errorf("%w: import of L1 block %d as %x, L1 has %x", ErrHistory, block.Number, block.Hash, origin.Hash)
			}
			previous = block.Number
		}
	}
	if origin.Time > cutoff {
		return nil, fmt.Errorf("%w: origin L1 block %d at %d, after cutoff %d", ErrHistory, origin.Number, origin.Time, cutoff)
	}
	return origin, nil
}

// apply returns the history state after a block of origin.
func (s historyState) apply(origin *l1.Header, update *protocol.HistoryTx) historyState {
	return historyState{imported: s.imported || update != nil, endpoint: origin.Number, hash: origin.Hash}
}
