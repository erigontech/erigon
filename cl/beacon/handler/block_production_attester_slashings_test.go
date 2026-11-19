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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/pool"
	"github.com/erigontech/erigon/common"
)

func TestGetBlockOperationsAttesterSlashingsFork(t *testing.T) {
	for _, tc := range []struct {
		name                 string
		submittedVersion     clparams.StateVersion
		blockVersion         clparams.StateVersion
		epoch                uint64
		maxCommitteesPerSlot uint64
	}{
		{"Deneb into Electra", clparams.DenebVersion, clparams.ElectraVersion, 1, 0},
		{"Deneb into Fulu", clparams.DenebVersion, clparams.FuluVersion, 2, 0},
		{"Electra into Gloas", clparams.ElectraVersion, clparams.GloasVersion, 3, 0},
		{"Deneb into Electra with configured limits", clparams.DenebVersion, clparams.ElectraVersion, 1, 4},
		{"Deneb into Fulu with configured limits", clparams.DenebVersion, clparams.FuluVersion, 2, 4},
		{"Electra into Gloas with configured limits", clparams.ElectraVersion, clparams.GloasVersion, 3, 4},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := clparams.MainnetBeaconConfig
			if tc.maxCommitteesPerSlot != 0 {
				cfg.MaxCommitteesPerSlot = tc.maxCommitteesPerSlot
			}
			cfg.AltairForkEpoch = 0
			cfg.BellatrixForkEpoch = 0
			cfg.CapellaForkEpoch = 0
			cfg.DenebForkEpoch = 0
			cfg.ElectraForkEpoch = 1
			cfg.FuluForkEpoch = 2
			cfg.GloasForkEpoch = 3
			s := state.New(&cfg)
			s.SetVersion(tc.blockVersion)
			validator := solid.NewValidatorFromParameters(common.Bytes48{}, common.Hash{}, cfg.MaxEffectiveBalance, false, 0, 0, cfg.FarFutureEpoch, cfg.FarFutureEpoch)
			require.NoError(t, s.AddValidator(validator, cfg.MaxEffectiveBalance))
			opPool := pool.NewOperationsPool(&cfg)
			submitted := cltypes.NewAttesterSlashing(tc.submittedVersion)
			submitted.SetVersionWithConfig(tc.submittedVersion, &cfg)
			submitted.Attestation_1.AttestingIndices.Append(0)
			submitted.Attestation_2.AttestingIndices.Append(0)
			submitted.Attestation_1.Data.BeaconBlockRoot = common.Hash{1}
			submitted.Attestation_2.Data.BeaconBlockRoot = common.Hash{2}
			originalCap := submitted.Attestation_1.AttestingIndices.Cap()
			originalRoot, err := submitted.HashSSZ()
			require.NoError(t, err)
			opPool.AttesterSlashingsPool.Insert(pool.ComputeKeyForAttesterSlashing(submitted), submitted)
			handler := &ApiHandler{beaconChainCfg: &cfg, operationsPool: opPool}

			slashings, _, _, _ := handler.getBlockOperations(s, tc.epoch*cfg.SlotsPerEpoch)
			require.Equal(t, 1, slashings.Len())
			body := cltypes.NewBeaconBody(&cfg, tc.blockVersion)
			body.AttesterSlashings = slashings
			encoded, err := body.EncodeSSZ(nil)
			require.NoError(t, err)
			producedRoot, err := body.HashSSZ()
			require.NoError(t, err)
			decoded := cltypes.NewBeaconBody(&cfg, tc.blockVersion)
			require.NoError(t, decoded.DecodeSSZ(encoded, int(tc.blockVersion)))
			decodedRoot, err := decoded.HashSSZ()
			require.NoError(t, err)
			require.Equal(t, decodedRoot, producedRoot, "produced body root must match decoding at the block fork")
			expectedSlashingRoot, err := decoded.AttesterSlashings.Get(0).HashSSZ()
			require.NoError(t, err)
			actualSlashingRoot, err := slashings.Get(0).HashSSZ()
			require.NoError(t, err)
			require.Equal(t, expectedSlashingRoot, actualSlashingRoot)
			expectedCap := int(cfg.MaxValidatorsPerCommittee * cfg.MaxCommitteesPerSlot)
			require.Equal(t, expectedCap, slashings.Get(0).Attestation_1.AttestingIndices.Cap())
			require.Equal(t, expectedCap, slashings.Get(0).Attestation_2.AttestingIndices.Cap())

			require.Equal(t, originalCap, submitted.Attestation_1.AttestingIndices.Cap())
			require.Equal(t, originalCap, submitted.Attestation_2.AttestingIndices.Cap())
			pooledRoot, err := submitted.HashSSZ()
			require.NoError(t, err)
			require.Equal(t, originalRoot, pooledRoot)
			slashings.Get(0).Attestation_1.AttestingIndices.Set(0, 1)
			require.Zero(t, submitted.Attestation_1.AttestingIndices.Get(0))
		})
	}
}
