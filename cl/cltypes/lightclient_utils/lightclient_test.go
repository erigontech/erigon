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

package lightclient_utils

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/common"
)

var lcTestCfg = &clparams.MainnetBeaconConfig

func lcTestBlock(version clparams.StateVersion, slot uint64, parent common.Hash) *cltypes.SignedBeaconBlock {
	b := cltypes.NewSignedBeaconBlock(lcTestCfg, version)
	b.Block.Slot = slot
	b.Block.ParentRoot = parent
	b.Block.Body.SyncAggregate = cltypes.NewSyncAggregateWithSize(int(lcTestCfg.SyncCommitteeSize) / 8)
	for i := range b.Block.Body.SyncAggregate.SyncCommiteeBits {
		b.Block.Body.SyncAggregate.SyncCommiteeBits[i] = 0xff
	}
	if b.Block.Body.ExecutionPayload != nil {
		b.Block.Body.ExecutionPayload.BlockHash = common.Hash{byte(version), 1}
		b.Block.Body.ExecutionPayload.BlobGasUsed = 7
	}
	return b
}

func createTestUpdate(t *testing.T, signature, attested, finalized *cltypes.SignedBeaconBlock) *cltypes.LightClientUpdate {
	t.Helper()
	finalizedRoot, err := finalized.Block.HashSSZ()
	require.NoError(t, err)
	zero := cltypes.NewLightClientUpdate(attested.Version(), lcTestCfg)
	update, err := CreateLightClientUpdate(lcTestCfg, signature, finalized, attested, attested.Block.Slot,
		zero.NextSyncCommittee, solid.Checkpoint{Root: finalizedRoot}, zero.NextSyncCommitteeBranch, zero.FinalityBranch)
	require.NoError(t, err)
	return update
}

// requireEncodesAs checks that the update round-trips through SSZ in the attested header's fork, the fork its
// req/resp context bytes name.
func requireEncodesAs(t *testing.T, update *cltypes.LightClientUpdate, version clparams.StateVersion) {
	t.Helper()
	encoded, err := update.EncodeSSZ(nil)
	require.NoError(t, err)
	decoded := cltypes.NewLightClientUpdate(version, lcTestCfg)
	require.NoError(t, decoded.DecodeSSZ(encoded, int(version)))
	reencoded, err := decoded.EncodeSSZ(nil)
	require.NoError(t, err)
	require.Equal(t, encoded, reencoded)
}

// Light client data takes the fork of its attested header; a finalized header from an older fork is brought into that
// fork's layout, as block_to_light_client_header does.
func TestCreateLightClientUpdateTakesTheAttestedFork(t *testing.T) {
	period := lcTestCfg.SlotsPerEpoch * lcTestCfg.EpochsPerSyncCommitteePeriod
	finalized := lcTestBlock(clparams.CapellaVersion, period+1, common.Hash{})
	attested := lcTestBlock(clparams.DenebVersion, period+64, common.Hash{1})
	attestedRoot, err := attested.Block.HashSSZ()
	require.NoError(t, err)
	signature := lcTestBlock(clparams.ElectraVersion, period+65, attestedRoot)

	update := createTestUpdate(t, signature, attested, finalized)

	require.Equal(t, clparams.DenebVersion, update.AttestedHeader.Version())
	require.Equal(t, clparams.DenebVersion, update.FinalizedHeader.Version())
	require.Equal(t, finalized.Block.Body.ExecutionPayload.BlockHash, update.FinalizedHeader.ExecutionPayloadHeader.BlockHash)
	require.Zero(t, update.FinalizedHeader.ExecutionPayloadHeader.BlobGasUsed, "a Capella payload has no blob gas")
	requireEncodesAs(t, update, clparams.DenebVersion)
}

// When the signature is in the next period, the update has no next sync committee, and its empty branch still takes
// the attested fork's length.
func TestCreateLightClientUpdateAtAPeriodAndForkBoundary(t *testing.T) {
	period := lcTestCfg.SlotsPerEpoch * lcTestCfg.EpochsPerSyncCommitteePeriod
	finalized := lcTestBlock(clparams.DenebVersion, 2*period-64, common.Hash{})
	attested := lcTestBlock(clparams.DenebVersion, 2*period-1, common.Hash{1})
	attestedRoot, err := attested.Block.HashSSZ()
	require.NoError(t, err)
	signature := lcTestBlock(clparams.ElectraVersion, 2*period, attestedRoot)

	update := createTestUpdate(t, signature, attested, finalized)

	require.Equal(t, solid.NewHashVector(update.NextSyncCommitteeBranch.Length()), update.NextSyncCommitteeBranch)
	requireEncodesAs(t, update, clparams.DenebVersion)
}

// Before Capella the light client header carries no execution data, even for a Bellatrix block that has a payload.
func TestCreateLightClientUpdateLeavesPreCapellaExecutionOut(t *testing.T) {
	period := lcTestCfg.SlotsPerEpoch * lcTestCfg.EpochsPerSyncCommitteePeriod
	finalized := lcTestBlock(clparams.BellatrixVersion, period+1, common.Hash{})
	attested := lcTestBlock(clparams.CapellaVersion, period+64, common.Hash{1})
	attestedRoot, err := attested.Block.HashSSZ()
	require.NoError(t, err)
	signature := lcTestBlock(clparams.CapellaVersion, period+65, attestedRoot)

	update := createTestUpdate(t, signature, attested, finalized)

	require.Equal(t, clparams.CapellaVersion, update.FinalizedHeader.Version())
	require.True(t, update.FinalizedHeader.ExecutionPayloadHeader.IsZero())
	require.Equal(t, solid.NewHashVector(update.FinalizedHeader.ExecutionBranch.Length()), update.FinalizedHeader.ExecutionBranch)
	requireEncodesAs(t, update, clparams.CapellaVersion)
}

type rankedUpdate struct {
	participants  int
	attestedSlot  uint64
	signatureSlot uint64
	syncCommittee bool // next sync committee branch is set
	finalizedSlot uint64
	finality      bool // finality branch is set
}

func (r rankedUpdate) build() *cltypes.LightClientUpdate {
	u := cltypes.NewLightClientUpdate(clparams.DenebVersion, lcTestCfg)
	for i := range r.participants {
		u.SyncAggregate.SyncCommiteeBits[i/8] |= 1 << (i % 8)
	}
	u.AttestedHeader.Beacon.Slot = r.attestedSlot
	u.SignatureSlot = r.signatureSlot
	u.FinalizedHeader.Beacon.Slot = r.finalizedSlot
	if r.syncCommittee {
		u.NextSyncCommitteeBranch.Set(0, common.Hash{1})
	}
	if r.finality {
		u.FinalityBranch.Set(0, common.Hash{1})
	}
	return u
}

// TestIsBetterUpdate walks the rules of is_better_update in the spec's order. A case named after a rule differs from
// its rival only in the property that rule compares; an "X before Y" case makes two rules disagree, to pin their order.
func TestIsBetterUpdate(t *testing.T) {
	period := lcTestCfg.SlotsPerEpoch * lcTestCfg.EpochsPerSyncCommitteePeriod
	size := int(lcTestCfg.SyncCommitteeSize)
	supermajority := (size*2 + 2) / 3
	base := rankedUpdate{participants: size, attestedSlot: period + 10, signatureSlot: period + 11, syncCommittee: true, finalizedSlot: period + 1, finality: true}
	with := func(f func(*rankedUpdate)) rankedUpdate { r := base; f(&r); return r }

	tests := []struct {
		name          string
		better, worse rankedUpdate
	}{
		{"supermajority", with(func(r *rankedUpdate) { r.participants = supermajority; r.finality = false }), with(func(r *rankedUpdate) { r.participants = supermajority - 1 })},
		{"more participants below supermajority", with(func(r *rankedUpdate) { r.participants = 10; r.finality = false }), with(func(r *rankedUpdate) { r.participants = 9 })},
		{"supermajority before relevant sync committee", with(func(r *rankedUpdate) { r.syncCommittee = false }), with(func(r *rankedUpdate) { r.participants = supermajority - 1 })},
		{"participants below supermajority before relevant sync committee", with(func(r *rankedUpdate) { r.participants = 10; r.syncCommittee = false }), with(func(r *rankedUpdate) { r.participants = 9 })},
		{"relevant sync committee", base, with(func(r *rankedUpdate) { r.syncCommittee = false })},
		{"sync committee signed in another period", base, with(func(r *rankedUpdate) { r.attestedSlot = period + 5; r.signatureSlot = 2*period + 1 })},
		{"relevant sync committee before finality", with(func(r *rankedUpdate) { r.finality = false }), with(func(r *rankedUpdate) { r.syncCommittee = false })},
		{"finality", base, with(func(r *rankedUpdate) { r.finality = false })},
		{"sync committee finality", base, with(func(r *rankedUpdate) { r.finalizedSlot = period - 1 })},
		{"sync committee finality before participants", with(func(r *rankedUpdate) { r.participants = size - 1 }), with(func(r *rankedUpdate) { r.finalizedSlot = period - 1 })},
		{"more participants above supermajority", base, with(func(r *rankedUpdate) { r.participants = size - 1 })},
		{"participants before older attested header", with(func(r *rankedUpdate) { r.attestedSlot = period + 11; r.signatureSlot = period + 12 }), with(func(r *rankedUpdate) { r.participants = size - 1 })},
		{"older attested header", with(func(r *rankedUpdate) { r.signatureSlot = period + 13 }), with(func(r *rankedUpdate) { r.attestedSlot = period + 11; r.signatureSlot = period + 12 })},
		{"earlier signature", base, with(func(r *rankedUpdate) { r.signatureSlot = period + 12 })},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			better, worse := tt.better.build(), tt.worse.build()
			require.True(t, IsBetterUpdate(lcTestCfg, better, worse))
			require.False(t, IsBetterUpdate(lcTestCfg, worse, better))
		})
	}
	require.False(t, IsBetterUpdate(lcTestCfg, base.build(), base.build()), "an equal update is not better")
}
