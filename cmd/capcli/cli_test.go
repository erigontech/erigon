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

package main

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	state2 "github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/common"
)

func TestCheckpointBackwardAdmissionRejectsHistoricalLookahead(t *testing.T) {
	admissionState := state2.New(&clparams.MainnetBeaconConfig)
	admissionState.SetVersion(clparams.GloasVersion)
	admissionState.SetSlot(100)
	anchor := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.GloasVersion)
	anchor.Block.Slot = 99
	child := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.GloasVersion)
	child.Block.Slot = 101
	_, validateLookahead := checkpointBackwardAdmissionValidators(admissionState, common.HexToHash("0x1234"))

	err := validateLookahead(anchor, child)

	require.ErrorContains(t, err, "anchor state unavailable")
}
