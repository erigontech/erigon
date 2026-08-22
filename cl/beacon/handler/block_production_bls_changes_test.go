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

// blsChangeFor builds a validator whose credentials satisfy the spec's
// withdrawal_credentials[1:] == hash(from_bls_pubkey)[1:] under the given prefix, plus a change
// for it. Signatures are not verified here: the pool only ever admits changes whose signature
// already verified, so getBlockOperations does not re-check them.
func blsChangeFor(cfg *clparams.BeaconChainConfig, seed int, prefix byte) (solid.Validator, common.Bytes48) {
	var from common.Bytes48
	binary.LittleEndian.PutUint64(from[:], uint64(seed))

	hashedFrom := crypto.Sha256(from[:])
	var creds common.Hash
	creds[0] = prefix
	copy(creds[1:], hashedFrom[1:])

	return solid.NewValidatorFromParameters(
		from, creds, cfg.MaxEffectiveBalance, false, 0, 0, cfg.FarFutureEpoch, cfg.FarFutureEpoch,
	), from
}

func sig96(i int) common.Bytes96 {
	var sig common.Bytes96
	binary.LittleEndian.PutUint64(sig[:], uint64(i))
	return sig
}

func TestGetBlockOperationsBLSToExecutionChanges(t *testing.T) {
	// A block may carry at most MaxBlsToExecutionChanges. ListSSZ.Append does not enforce the
	// limit and HashSSZ merkleizes to the limit's depth regardless of length, so an over-long
	// list is published with a wrong body root instead of failing locally.
	t.Run("caps at MaxBlsToExecutionChanges", func(t *testing.T) {
		cfg := clparams.MainnetBeaconConfig
		s := state.New(&cfg)
		opPool := pool.NewOperationsPool(&cfg)

		for i := range int(cfg.MaxBlsToExecutionChanges) + 4 {
			v, from := blsChangeFor(&cfg, i+1, byte(cfg.BLSWithdrawalPrefixByte))
			s.AddValidator(v, cfg.MaxEffectiveBalance)
			opPool.BLSToExecutionChangesPool.Insert(sig96(i+1), &cltypes.SignedBLSToExecutionChange{
				Message:   &cltypes.BLSToExecutionChange{ValidatorIndex: uint64(i), From: from},
				Signature: sig96(i + 1),
			})
		}

		a := &ApiHandler{beaconChainCfg: &cfg, operationsPool: opPool}
		_, _, _, changes := a.getBlockOperations(s, 0)
		require.Equal(t, int(cfg.MaxBlsToExecutionChanges), changes.Len())
	})

	// A change is only valid while the validator still carries BLS_WITHDRAWAL_PREFIX. Packing one
	// for an already-migrated validator would fail ProcessBlsToExecutionChange and cost the slot.
	t.Run("skips validators that already migrated", func(t *testing.T) {
		cfg := clparams.MainnetBeaconConfig
		s := state.New(&cfg)
		opPool := pool.NewOperationsPool(&cfg)

		v, from := blsChangeFor(&cfg, 1, byte(cfg.ETH1AddressWithdrawalPrefixByte))
		s.AddValidator(v, cfg.MaxEffectiveBalance)
		opPool.BLSToExecutionChangesPool.Insert(sig96(1), &cltypes.SignedBLSToExecutionChange{
			Message:   &cltypes.BLSToExecutionChange{ValidatorIndex: 0, From: from},
			Signature: sig96(1),
		})

		a := &ApiHandler{beaconChainCfg: &cfg, operationsPool: opPool}
		_, _, _, changes := a.getBlockOperations(s, 0)
		require.Zero(t, changes.Len())
	})

	// The pool is keyed by signature, so one validator can hold several changes. Only the first
	// may be packed: processing one flips the credentials to 0x01, so a second in the same block
	// fails the prefix assert and takes the whole block down with it.
	t.Run("packs at most one change per validator", func(t *testing.T) {
		cfg := clparams.MainnetBeaconConfig
		s := state.New(&cfg)
		opPool := pool.NewOperationsPool(&cfg)

		v, from := blsChangeFor(&cfg, 1, byte(cfg.BLSWithdrawalPrefixByte))
		s.AddValidator(v, cfg.MaxEffectiveBalance)
		for i := range 3 {
			opPool.BLSToExecutionChangesPool.Insert(sig96(i+1), &cltypes.SignedBLSToExecutionChange{
				Message:   &cltypes.BLSToExecutionChange{ValidatorIndex: 0, From: from},
				Signature: sig96(i + 1),
			})
		}

		a := &ApiHandler{beaconChainCfg: &cfg, operationsPool: opPool}
		_, _, _, changes := a.getBlockOperations(s, 0)
		require.Equal(t, 1, changes.Len())
	})
}
