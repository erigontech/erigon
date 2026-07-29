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

package handler

import (
	"encoding/binary"
	"testing"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/pool"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/stretchr/testify/require"
)

// A change is only valid for a validator that still carries BLS_WITHDRAWAL_PREFIX, and a block
// may carry at most MaxBlsToExecutionChanges of them.
func TestGetBlockOperationsBLSToExecutionChanges(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	candidates := int(cfg.MaxBlsToExecutionChanges) + 4

	s := state.New(&cfg)
	opPool := pool.NewOperationsPool(&cfg)

	for i := range candidates {
		var from common.Bytes48
		binary.LittleEndian.PutUint64(from[:], uint64(i+1))

		hashedFrom := crypto.Sha256(from[:])
		var creds common.Hash
		creds[0] = byte(cfg.BLSWithdrawalPrefixByte)
		copy(creds[1:], hashedFrom[1:])

		s.AddValidator(solid.NewValidatorFromParameters(
			from, creds, cfg.MaxEffectiveBalance, false, 0, 0, cfg.FarFutureEpoch, cfg.FarFutureEpoch,
		), cfg.MaxEffectiveBalance)

		var sig common.Bytes96
		binary.LittleEndian.PutUint64(sig[:], uint64(i+1))
		opPool.BLSToExecutionChangesPool.Insert(sig, &cltypes.SignedBLSToExecutionChange{
			Message: &cltypes.BLSToExecutionChange{
				ValidatorIndex: uint64(i),
				From:           from,
			},
			Signature: sig,
		})
	}

	a := &ApiHandler{beaconChainCfg: &cfg, operationsPool: opPool}
	_, _, _, changes := a.getBlockOperations(s, 0)

	require.Equal(t, int(cfg.MaxBlsToExecutionChanges), changes.Len())
}
