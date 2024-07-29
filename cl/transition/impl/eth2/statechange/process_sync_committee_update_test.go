// Copyright 2024 The Erigon Authors
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

package statechange_test

import (
	"encoding/binary"
	"testing"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/cl/transition/impl/eth2/statechange"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/stretchr/testify/require"
)

func TestProcessSyncCommittee(t *testing.T) {
	pkBytes := common.Hex2Bytes("88c141df77cd9d8d7a71a75c826c41a9c9f03c6ee1b180f3e7852f6a280099ded351b58d66e653af8e42816a4d8f532e")
	var pk [48]byte
	copy(pk[:], pkBytes)
	validatorNum := 10_000
	s := state.New(&clparams.MainnetBeaconConfig)
	currentCommittee := &solid.SyncCommittee{}
	nextCommittee := &solid.SyncCommittee{}
	for i := 0; i < validatorNum; i++ {
		var pubKey [48]byte
		binary.BigEndian.PutUint64(pubKey[:], uint64(i))
		v := solid.NewValidator()
		v.SetExitEpoch(clparams.MainnetBeaconConfig.FarFutureEpoch)
		v.SetPublicKey(pk)
		v.SetEffectiveBalance(2000000000)
		s.AddValidator(v, 2000000000)
	}
	s.SetCurrentSyncCommittee(currentCommittee)
	s.SetNextSyncCommittee(nextCommittee)
	prevNextSyncCommittee := s.NextSyncCommittee()
	s.SetSlot(8160)
	require.NoError(t, statechange.ProcessSyncCommitteeUpdate(s))
	require.Equal(t, s.CurrentSyncCommittee(), prevNextSyncCommittee)
	require.NotEqual(t, s.NextSyncCommittee(), prevNextSyncCommittee)
}
