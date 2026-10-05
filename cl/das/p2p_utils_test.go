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

package das

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
)

func gloasColumnsForBatchVerification(t *testing.T) ([]*cltypes.DataColumnSidecar, *solid.ListSSZ[*cltypes.KZGCommitment]) {
	t.Helper()
	cfg := clparams.MainnetBeaconConfig
	cfg.AltairForkEpoch = 0
	cfg.BellatrixForkEpoch = 0
	cfg.CapellaForkEpoch = 0
	cfg.DenebForkEpoch = 0
	cfg.ElectraForkEpoch = 0
	cfg.FuluForkEpoch = 1
	cfg.GloasForkEpoch = 2
	cfg.InitializeForkSchedule()
	initTestBeaconConfig(&cfg)
	block, _, columns := recoverableGloasColumns(t, &cfg, 2*cfg.SlotsPerEpoch)
	commitments := block.GetBlobKzgCommitments()
	require.Equal(t, 2, commitments.Len())
	return columns, commitments
}

func TestVerifyDataColumnSidecarsKZGProofsWithCommitments(t *testing.T) {
	columns, commitments := gloasColumnsForBatchVerification(t)
	require.True(t, VerifyDataColumnSidecarsKZGProofsWithCommitments(columns, commitments))
	require.True(t, VerifyDataColumnSidecarsKZGProofsWithCommitments(columns[:1], commitments))
}

func TestVerifyDataColumnSidecarsKZGProofsWithCommitmentsRejectsInvalidBatch(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func([]*cltypes.DataColumnSidecar, *solid.ListSSZ[*cltypes.KZGCommitment]) ([]*cltypes.DataColumnSidecar, *solid.ListSSZ[*cltypes.KZGCommitment])
	}{
		{name: "tampered proof", mutate: func(columns []*cltypes.DataColumnSidecar, commitments *solid.ListSSZ[*cltypes.KZGCommitment]) ([]*cltypes.DataColumnSidecar, *solid.ListSSZ[*cltypes.KZGCommitment]) {
			columns[len(columns)-1].KzgProofs.Get(1)[0] ^= 0xff
			return columns, commitments
		}},
		{name: "nil sidecar", mutate: func(columns []*cltypes.DataColumnSidecar, commitments *solid.ListSSZ[*cltypes.KZGCommitment]) ([]*cltypes.DataColumnSidecar, *solid.ListSSZ[*cltypes.KZGCommitment]) {
			return append(columns[:1], nil), commitments
		}},
		{name: "nil column", mutate: func(columns []*cltypes.DataColumnSidecar, commitments *solid.ListSSZ[*cltypes.KZGCommitment]) ([]*cltypes.DataColumnSidecar, *solid.ListSSZ[*cltypes.KZGCommitment]) {
			columns[0].Column = nil
			return columns, commitments
		}},
		{name: "more commitments than cells", mutate: func(columns []*cltypes.DataColumnSidecar, commitments *solid.ListSSZ[*cltypes.KZGCommitment]) ([]*cltypes.DataColumnSidecar, *solid.ListSSZ[*cltypes.KZGCommitment]) {
			commitments.Append(commitments.Get(0))
			return columns, commitments
		}},
		{name: "fewer proofs than cells", mutate: func(columns []*cltypes.DataColumnSidecar, commitments *solid.ListSSZ[*cltypes.KZGCommitment]) ([]*cltypes.DataColumnSidecar, *solid.ListSSZ[*cltypes.KZGCommitment]) {
			proofs := solid.NewStaticListSSZ[*cltypes.KZGProof](int(clparams.MainnetBeaconConfig.MaxBlobCommittmentsPerBlock), 48)
			proofs.Append(columns[0].KzgProofs.Get(0))
			columns[0].KzgProofs = proofs
			return columns, commitments
		}},
		{name: "no sidecars", mutate: func(_ []*cltypes.DataColumnSidecar, commitments *solid.ListSSZ[*cltypes.KZGCommitment]) ([]*cltypes.DataColumnSidecar, *solid.ListSSZ[*cltypes.KZGCommitment]) {
			return nil, commitments
		}},
		{name: "no commitments", mutate: func(columns []*cltypes.DataColumnSidecar, _ *solid.ListSSZ[*cltypes.KZGCommitment]) ([]*cltypes.DataColumnSidecar, *solid.ListSSZ[*cltypes.KZGCommitment]) {
			return columns, solid.NewStaticListSSZ[*cltypes.KZGCommitment](int(clparams.MainnetBeaconConfig.MaxBlobCommittmentsPerBlock), 48)
		}},
		{name: "nil commitments", mutate: func(columns []*cltypes.DataColumnSidecar, _ *solid.ListSSZ[*cltypes.KZGCommitment]) ([]*cltypes.DataColumnSidecar, *solid.ListSSZ[*cltypes.KZGCommitment]) {
			return columns, nil
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			columns, commitments := gloasColumnsForBatchVerification(t)
			columns, commitments = test.mutate(columns, commitments)
			require.False(t, VerifyDataColumnSidecarsKZGProofsWithCommitments(columns, commitments))
		})
	}
}
