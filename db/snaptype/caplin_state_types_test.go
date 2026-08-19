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

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/snapshotsync"
	"github.com/erigontech/erigon/db/snaptype"
	"github.com/erigontech/erigon/db/version"
)

type caplinStateTypeExpectation struct {
	typ  snaptype.Type
	name string
}

func caplinStateTypeExpectations() []caplinStateTypeExpectation {
	return []caplinStateTypeExpectation{
		{snaptype.ValidatorEffectiveBalance, kv.ValidatorEffectiveBalance},
		{snaptype.ValidatorSlashings, kv.ValidatorSlashings},
		{snaptype.ValidatorBalance, kv.ValidatorBalance},
		{snaptype.StateEvents, kv.StateEvents},
		{snaptype.ActiveValidatorIndicies, kv.ActiveValidatorIndicies},
		{snaptype.StateRoot, kv.StateRoot},
		{snaptype.BlockRoot, kv.BlockRoot},
		{snaptype.SlotData, kv.SlotData},
		{snaptype.EpochData, kv.EpochData},
		{snaptype.InactivityScores, kv.InactivityScores},
		{snaptype.NextSyncCommittee, kv.NextSyncCommittee},
		{snaptype.CurrentSyncCommittee, kv.CurrentSyncCommittee},
		{snaptype.Eth1DataVotes, kv.Eth1DataVotes},
		{snaptype.IntraRandaoMixes, kv.IntraRandaoMixes},
		{snaptype.RandaoMixes, kv.RandaoMixes},
		{snaptype.BalancesDump, kv.BalancesDump},
		{snaptype.EffectiveBalancesDump, kv.EffectiveBalancesDump},
		{snaptype.PendingConsolidations, kv.PendingConsolidations},
		{snaptype.PendingPartialWithdrawals, kv.PendingPartialWithdrawals},
		{snaptype.PendingDeposits, kv.PendingDeposits},
		{snaptype.PendingConsolidationsDump, kv.PendingConsolidationsDump},
		{snaptype.PendingPartialWithdrawalsDump, kv.PendingPartialWithdrawalsDump},
		{snaptype.PendingDepositsDump, kv.PendingDepositsDump},
		{snaptype.Builders, kv.Builders},
		{snaptype.BuildersDump, kv.BuildersDump},
		{snaptype.BuilderPendingWithdrawals, kv.BuilderPendingWithdrawals},
		{snaptype.BuilderPendingWithdrawalsDump, kv.BuilderPendingWithdrawalsDump},
		{snaptype.PayloadExpectedWithdrawals, kv.PayloadExpectedWithdrawals},
		{snaptype.PayloadExpectedWithdrawalsDump, kv.PayloadExpectedWithdrawalsDump},
		{snaptype.ExecutionPayloadAvailabilityTable, kv.ExecutionPayloadAvailabilityTable},
		{snaptype.BuilderPendingPaymentsTable, kv.BuilderPendingPaymentsTable},
		{snaptype.PtcWindowTable, kv.PtcWindowTable},
		{snaptype.LatestExecutionPayloadBidTable, kv.LatestExecutionPayloadBidTable},
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
