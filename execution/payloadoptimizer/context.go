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

package payloadoptimizer

import (
	"encoding/binary"
	"errors"
	"math"
	"reflect"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/execution/builder"
	"github.com/erigontech/erigon/execution/builder/buildercfg"
	protocolparams "github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/types"
)

var ErrCustomTxnProvider = errors.New("payload optimizer build context cannot contain a custom transaction provider")

type BuildContext struct {
	params            *builder.Parameters
	stateVersion      clparams.StateVersion
	forkVersion       [4]byte
	executionRequests types.FlatRequests
	parentGasLimit    uint64
	targetGasLimit    uint64
}

type BuildDefaults struct {
	TargetGasLimit   *uint64
	ExtraData        []byte
	MaxBlobsPerBlock *uint64
}

func BuildDefaultsFromConfig(config buildercfg.BuilderConfig) BuildDefaults {
	return BuildDefaults{
		TargetGasLimit:   copyUint64(config.GasLimit),
		ExtraData:        append([]byte(nil), config.ExtraData...),
		MaxBlobsPerBlock: copyUint64(config.MaxBlobsPerBlock),
	}
}

func NewBuildContext(params *builder.Parameters, beaconConfig *clparams.BeaconChainConfig, proposalSlot uint64, executionRequests types.FlatRequests, parentGasLimit uint64, configured ...BuildDefaults) (BuildContext, error) {
	if params == nil {
		return BuildContext{}, errors.New("payload optimizer build context requires parameters")
	}
	if beaconConfig == nil {
		return BuildContext{}, errors.New("payload optimizer build context requires beacon config")
	}
	if beaconConfig.SlotsPerEpoch == 0 {
		return BuildContext{}, errors.New("payload optimizer beacon config requires slots per epoch")
	}
	if params.CustomTxnProvider != nil {
		return BuildContext{}, ErrCustomTxnProvider
	}
	stateVersion := beaconConfig.GetCurrentStateVersion(proposalSlot / beaconConfig.SlotsPerEpoch)
	var forkVersion [4]byte
	binary.BigEndian.PutUint32(forkVersion[:], beaconConfig.GetForkVersionByVersion(stateVersion))
	if parentGasLimit < protocolparams.MinBlockGasLimit || parentGasLimit > protocolparams.MaxBlockGasLimit {
		return BuildContext{}, errors.New("payload optimizer build context has an invalid parent gas limit")
	}
	for _, withdrawal := range params.Withdrawals {
		if withdrawal == nil {
			return BuildContext{}, errors.New("payload optimizer build context contains a nil withdrawal")
		}
	}
	if len(configured) > 1 {
		return BuildContext{}, errors.New("payload optimizer build context accepts one defaults value")
	}
	var defaults BuildDefaults
	if len(configured) == 1 {
		defaults = configured[0]
	}
	var targetGasLimit uint64
	if stateVersion < clparams.GloasVersion {
		if params.SlotNumber != nil {
			return BuildContext{}, errors.New("payload optimizer pre-Gloas build context contains a slot number")
		}
		if params.TargetGasLimit != nil {
			return BuildContext{}, errors.New("payload optimizer pre-Gloas build context contains a target gas limit")
		}
		targetGasLimit = parentGasLimit
		if defaults.TargetGasLimit != nil {
			targetGasLimit = *defaults.TargetGasLimit
		}
	} else {
		if params.SlotNumber == nil {
			return BuildContext{}, errors.New("payload optimizer Gloas build context requires a slot number")
		}
		if *params.SlotNumber != proposalSlot {
			return BuildContext{}, errors.New("payload optimizer Gloas build context slot does not match proposal slot")
		}
		if params.TargetGasLimit == nil {
			return BuildContext{}, errors.New("payload optimizer Gloas build context requires a target gas limit")
		}
		targetGasLimit = *params.TargetGasLimit
	}
	extraData := params.ExtraData
	if extraData == nil {
		extraData = defaults.ExtraData
	}
	if uint64(len(extraData)) > protocolparams.MaximumExtraDataSize {
		return BuildContext{}, errors.New("payload optimizer build context has invalid extra data")
	}
	owned := params.Copy()
	owned.PayloadId = 0
	if owned.ExtraData == nil {
		owned.ExtraData = append([]byte{}, extraData...)
	}
	if owned.MaxBlobsPerBlock == nil {
		owned.MaxBlobsPerBlock = copyUint64(defaults.MaxBlobsPerBlock)
		if owned.MaxBlobsPerBlock == nil {
			unlimited := uint64(math.MaxUint64)
			owned.MaxBlobsPerBlock = &unlimited
		}
	}
	return BuildContext{
		params:            owned,
		stateVersion:      stateVersion,
		forkVersion:       forkVersion,
		executionRequests: copyRequests(executionRequests),
		parentGasLimit:    parentGasLimit,
		targetGasLimit:    targetGasLimit,
	}, nil
}

func (c BuildContext) StateVersion() clparams.StateVersion {
	return c.stateVersion
}

func copyUint64(value *uint64) *uint64 {
	if value == nil {
		return nil
	}
	owned := *value
	return &owned
}

func (c BuildContext) Parameters() *builder.Parameters {
	if c.params == nil {
		return nil
	}
	return c.params.Copy()
}

func (c BuildContext) ForkVersion() [4]byte {
	return c.forkVersion
}

func (c BuildContext) ExecutionRequests() types.FlatRequests {
	return copyRequests(c.executionRequests)
}

func (c BuildContext) ParentGasLimit() uint64 {
	return c.parentGasLimit
}

func (c BuildContext) Equal(other BuildContext) bool {
	return c.params != nil && other.params != nil &&
		c.stateVersion == other.stateVersion &&
		c.forkVersion == other.forkVersion &&
		c.parentGasLimit == other.parentGasLimit &&
		c.targetGasLimit == other.targetGasLimit &&
		reflect.DeepEqual(c.params, other.params) &&
		reflect.DeepEqual(c.executionRequests, other.executionRequests)
}

func (c BuildContext) clone() BuildContext {
	return BuildContext{
		params:            c.Parameters(),
		stateVersion:      c.stateVersion,
		forkVersion:       c.forkVersion,
		executionRequests: copyRequests(c.executionRequests),
		parentGasLimit:    c.parentGasLimit,
		targetGasLimit:    c.targetGasLimit,
	}
}

func copyRequests(requests types.FlatRequests) types.FlatRequests {
	if requests == nil {
		return nil
	}
	owned := make(types.FlatRequests, len(requests))
	for i := range requests {
		owned[i].Type = requests[i].Type
		owned[i].RequestData = append([]byte(nil), requests[i].RequestData...)
	}
	return owned
}
