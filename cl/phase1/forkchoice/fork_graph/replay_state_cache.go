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

package fork_graph

import (
	"sync"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/clonable"
)

const replayStateCacheSize = 4

type replayStateCache struct {
	mu     sync.Mutex
	states map[common.Hash]*state.CachingBeaconState
	order  []common.Hash
}

type replayStateListItem interface {
	solid.EncodableHashableSSZ
	clonable.Clonable
	DecodeSSZ([]byte, int) error
}

func copyReplayState(source *state.CachingBeaconState) (*state.CachingBeaconState, error) {
	copied, err := source.Copy()
	if err != nil {
		return nil, err
	}
	version := source.Version()
	copiedCurrentAttestations, err := copyReplayStateList(source.CurrentEpochAttestations(), int(version))
	if err != nil {
		return nil, err
	}
	copiedPreviousAttestations, err := copyReplayStateList(source.PreviousEpochAttestations(), int(version))
	if err != nil {
		return nil, err
	}
	copied.SetCurrentEpochAttestations(copiedCurrentAttestations)
	copied.SetPreviousEpochAttestations(copiedPreviousAttestations)
	if version >= clparams.ElectraVersion {
		pendingDeposits, err := copyReplayStateList(source.GetPendingDeposits(), int(version))
		if err != nil {
			return nil, err
		}
		partialWithdrawals, err := copyReplayStateList(source.GetPendingPartialWithdrawals(), int(version))
		if err != nil {
			return nil, err
		}
		pendingConsolidations, err := copyReplayStateList(source.GetPendingConsolidations(), int(version))
		if err != nil {
			return nil, err
		}
		copied.SetPendingDeposits(pendingDeposits)
		copied.SetPendingPartialWithdrawals(partialWithdrawals)
		copied.SetPendingConsolidations(pendingConsolidations)
	}
	if version >= clparams.GloasVersion {
		builders, err := copyReplayStateList(source.GetBuilders(), int(version))
		if err != nil {
			return nil, err
		}
		builderWithdrawals, err := copyReplayStateList(source.GetBuilderPendingWithdrawals(), int(version))
		if err != nil {
			return nil, err
		}
		expectedWithdrawals, err := copyReplayStateList(source.GetPayloadExpectedWithdrawals(), int(version))
		if err != nil {
			return nil, err
		}
		copied.SetBuilders(builders)
		copied.SetBuilderPendingWithdrawals(builderWithdrawals)
		copied.SetPayloadExpectedWithdrawals(expectedWithdrawals)
	}
	return copied, nil
}

func copyReplayStateList[T replayStateListItem](source *solid.ListSSZ[T], version int) (*solid.ListSSZ[T], error) {
	if source == nil {
		return nil, nil
	}
	copied := source.Clone().(*solid.ListSSZ[T])
	for index := range source.Len() {
		encoded, err := source.Get(index).EncodeSSZ(nil)
		if err != nil {
			return nil, err
		}
		item := source.Get(index).Clone().(T)
		if err := item.DecodeSSZ(encoded, version); err != nil {
			return nil, err
		}
		copied.Append(item)
	}
	return copied, nil
}

func (c *replayStateCache) get(root common.Hash) (*state.CachingBeaconState, bool) {
	c.mu.Lock()
	cached, ok := c.states[root]
	c.mu.Unlock()
	if !ok {
		return nil, false
	}
	copy, err := copyReplayState(cached)
	if err != nil {
		return nil, false
	}
	return copy, true
}

func (c *replayStateCache) add(root common.Hash, bs *state.CachingBeaconState) {
	copy, err := copyReplayState(bs)
	if err != nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.states == nil {
		c.states = make(map[common.Hash]*state.CachingBeaconState, replayStateCacheSize)
	}
	if _, ok := c.states[root]; ok {
		for i, cachedRoot := range c.order {
			if cachedRoot == root {
				c.order = append(c.order[:i], c.order[i+1:]...)
				break
			}
		}
	} else if len(c.order) == replayStateCacheSize {
		delete(c.states, c.order[0])
		c.order = c.order[1:]
	}
	c.states[root] = copy
	c.order = append(c.order, root)
}

func (c *replayStateCache) clear() {
	c.mu.Lock()
	c.states = nil
	c.order = nil
	c.mu.Unlock()
}
