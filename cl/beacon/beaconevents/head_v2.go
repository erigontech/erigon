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

package beaconevents

import (
	"errors"
	"fmt"

	"github.com/erigontech/erigon/cl/abstract"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/common"
)

// BuildHeadV2Data derives a head_v2 event from one head-state snapshot.
func BuildHeadV2Data(
	beaconCfg *clparams.BeaconChainConfig,
	headState abstract.BeaconState,
	headSlot uint64,
	headRoot common.Hash,
	stateRoot common.Hash,
	payloadStatus string,
	executionOptimistic bool,
) (*HeadV2Data, error) {
	if beaconCfg == nil || beaconCfg.SlotsPerEpoch == 0 {
		return nil, errors.New("invalid beacon configuration")
	}
	if headState == nil {
		return nil, errors.New("nil head state")
	}

	genesisRoot := headRoot
	var err error
	if headSlot > 0 {
		genesisRoot, err = headState.GetBlockRootAtSlot(0)
		if err != nil {
			return nil, fmt.Errorf("get genesis block root: %w", err)
		}
	}
	headEpoch := headSlot / beaconCfg.SlotsPerEpoch
	currentDependentRoot := genesisRoot
	nextDependentRoot := genesisRoot
	if headEpoch > 1 {
		currentDependentRoot, err = headState.GetBlockRootAtSlot((headEpoch-1)*beaconCfg.SlotsPerEpoch - 1)
		if err != nil {
			return nil, fmt.Errorf("get current epoch dependent root: %w", err)
		}
	}
	if headEpoch > 0 {
		nextDependentRoot, err = headState.GetBlockRootAtSlot(headEpoch*beaconCfg.SlotsPerEpoch - 1)
		if err != nil {
			return nil, fmt.Errorf("get next epoch dependent root: %w", err)
		}
	}

	return &HeadV2Data{
		Version: headState.Version().String(),
		Data: HeadV2Content{
			Slot:                      headSlot,
			Block:                     headRoot,
			State:                     stateRoot,
			PayloadStatus:             payloadStatus,
			EpochTransition:           headSlot%beaconCfg.SlotsPerEpoch == 0,
			CurrentEpochDependentRoot: currentDependentRoot,
			NextEpochDependentRoot:    nextDependentRoot,
			ExecutionOptimistic:       executionOptimistic,
		},
	}, nil
}
