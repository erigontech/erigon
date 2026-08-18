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

package snaptype_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/snapshotsync"
	"github.com/erigontech/erigon/db/snaptype"
	"github.com/erigontech/erigon/db/version"
)

type caplinStateTypeExpectation struct {
	typ        snaptype.Type
	name       string
	introduced clparams.StateVersion
}

func caplinStateTypeExpectations() []caplinStateTypeExpectation {
	return []caplinStateTypeExpectation{
		{snaptype.ValidatorEffectiveBalance, kv.ValidatorEffectiveBalance, clparams.Phase0Version},
		{snaptype.ValidatorSlashings, kv.ValidatorSlashings, clparams.Phase0Version},
		{snaptype.ValidatorBalance, kv.ValidatorBalance, clparams.Phase0Version},
		{snaptype.StateEvents, kv.StateEvents, clparams.Phase0Version},
		{snaptype.ActiveValidatorIndicies, kv.ActiveValidatorIndicies, clparams.Phase0Version},
		{snaptype.StateRoot, kv.StateRoot, clparams.Phase0Version},
		{snaptype.BlockRoot, kv.BlockRoot, clparams.Phase0Version},
		{snaptype.SlotData, kv.SlotData, clparams.Phase0Version},
		{snaptype.EpochData, kv.EpochData, clparams.Phase0Version},
		{snaptype.InactivityScores, kv.InactivityScores, clparams.AltairVersion},
		{snaptype.NextSyncCommittee, kv.NextSyncCommittee, clparams.AltairVersion},
		{snaptype.CurrentSyncCommittee, kv.CurrentSyncCommittee, clparams.AltairVersion},
		{snaptype.Eth1DataVotes, kv.Eth1DataVotes, clparams.Phase0Version},
		{snaptype.IntraRandaoMixes, kv.IntraRandaoMixes, clparams.Phase0Version},
		{snaptype.RandaoMixes, kv.RandaoMixes, clparams.Phase0Version},
		{snaptype.BalancesDump, kv.BalancesDump, clparams.Phase0Version},
		{snaptype.EffectiveBalancesDump, kv.EffectiveBalancesDump, clparams.Phase0Version},
		{snaptype.PendingConsolidations, kv.PendingConsolidations, clparams.ElectraVersion},
		{snaptype.PendingPartialWithdrawals, kv.PendingPartialWithdrawals, clparams.ElectraVersion},
		{snaptype.PendingDeposits, kv.PendingDeposits, clparams.ElectraVersion},
		{snaptype.PendingConsolidationsDump, kv.PendingConsolidationsDump, clparams.ElectraVersion},
		{snaptype.PendingPartialWithdrawalsDump, kv.PendingPartialWithdrawalsDump, clparams.ElectraVersion},
		{snaptype.PendingDepositsDump, kv.PendingDepositsDump, clparams.ElectraVersion},
		{snaptype.Builders, kv.Builders, clparams.GloasVersion},
		{snaptype.BuildersDump, kv.BuildersDump, clparams.GloasVersion},
		{snaptype.BuilderPendingWithdrawals, kv.BuilderPendingWithdrawals, clparams.GloasVersion},
		{snaptype.BuilderPendingWithdrawalsDump, kv.BuilderPendingWithdrawalsDump, clparams.GloasVersion},
		{snaptype.PayloadExpectedWithdrawals, kv.PayloadExpectedWithdrawals, clparams.GloasVersion},
		{snaptype.PayloadExpectedWithdrawalsDump, kv.PayloadExpectedWithdrawalsDump, clparams.GloasVersion},
		{snaptype.ExecutionPayloadAvailabilityTable, kv.ExecutionPayloadAvailabilityTable, clparams.GloasVersion},
		{snaptype.BuilderPendingPaymentsTable, kv.BuilderPendingPaymentsTable, clparams.GloasVersion},
		{snaptype.PtcWindowTable, kv.PtcWindowTable, clparams.GloasVersion},
		{snaptype.LatestExecutionPayloadBidTable, kv.LatestExecutionPayloadBidTable, clparams.GloasVersion},
	}
}

func TestCaplinStateSnapshotTypes(t *testing.T) {
	want := caplinStateTypeExpectations()
	require.Len(t, snaptype.CaplinStateSnapshotTypes, len(want))

	wantNames := make(map[string]struct{}, len(want))
	seenEnums := make(map[snaptype.Enum]string, len(want))
	for i, expected := range want {
		typ := snaptype.CaplinStateSnapshotTypes[i]
		require.NotNil(t, typ)
		require.Equal(t, expected.typ, typ)
		require.Equal(t, snaptype.MinCaplinEnum+2+snaptype.Enum(i), typ.Enum())
		require.Equal(t, expected.name, typ.Name())
		require.Equal(t, version.V1_1_standart, typ.Versions())
		require.Equal(t, expected.introduced, snaptype.CaplinStateIntroducedIn(typ.Enum()))

		indexes := typ.Indexes()
		require.Len(t, indexes, 1)
		require.Equal(t, expected.name, indexes[0].Name)
		require.Equal(t, version.V1_1_standart, indexes[0].Version)

		if previous, ok := seenEnums[typ.Enum()]; ok {
			t.Fatalf("enum %d is used by %q and %q", typ.Enum(), previous, typ.Name())
		}
		seenEnums[typ.Enum()] = typ.Name()
		wantNames[expected.name] = struct{}{}
	}

	gotNames := make(map[string]struct{}, len(snapshotsync.MakeCaplinStateSnapshotsTypes(nil).KeyValueGetters))
	for name := range snapshotsync.MakeCaplinStateSnapshotsTypes(nil).KeyValueGetters {
		gotNames[name] = struct{}{}
	}
	require.Equal(t, gotNames, wantNames)
}

func TestCaplinStateSnapshotTypeRange(t *testing.T) {
	require.LessOrEqual(t, len(snaptype.CaplinSnapshotTypes)+len(snaptype.CaplinStateSnapshotTypes), snaptype.MinBorEnum-snaptype.MinCaplinEnum)
	for i, typ := range snaptype.CaplinStateSnapshotTypes {
		require.Equal(t, snaptype.MinCaplinEnum+2+snaptype.Enum(i), typ.Enum())
		require.True(t, snaptype.IsCaplinType(typ.Enum()))
	}
	require.True(t, snaptype.IsCaplinType(snaptype.CaplinEnums.BeaconBlocks))
	require.True(t, snaptype.IsCaplinType(snaptype.CaplinEnums.BlobSidecars))
	require.False(t, snaptype.IsCaplinType(snaptype.MinCaplinEnum-1))
	require.False(t, snaptype.IsCaplinType(snaptype.MinBorEnum))
}

func TestCaplinSnapshotTypesRemainBlockTypes(t *testing.T) {
	require.Len(t, snaptype.CaplinSnapshotTypes, 2)
}

func TestCaplinStateIntroducedInDefaultsToGenesis(t *testing.T) {
	require.Equal(t, clparams.Phase0Version, snaptype.CaplinStateIntroducedIn(snaptype.Unknown))
}
