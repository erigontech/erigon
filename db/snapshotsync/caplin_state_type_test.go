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

package snapshotsync

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/kv"
)

// wantCaplinStateNames is an independent oracle: each enum's on-disk name is the
// kv.* table constant embedded in v1.1-<from>-<to>-<Name>.seg. String() must match
// it exactly or OpenFolder can't find existing files on an already-populated datadir.
var wantCaplinStateNames = map[CaplinStateType]string{
	CaplinValidatorEffectiveBalance:      kv.ValidatorEffectiveBalance,
	CaplinValidatorSlashings:             kv.ValidatorSlashings,
	CaplinValidatorBalance:               kv.ValidatorBalance,
	CaplinStateEvents:                    kv.StateEvents,
	CaplinActiveValidatorIndicies:        kv.ActiveValidatorIndicies,
	CaplinStateRoot:                      kv.StateRoot,
	CaplinBlockRoot:                      kv.BlockRoot,
	CaplinSlotData:                       kv.SlotData,
	CaplinEpochData:                      kv.EpochData,
	CaplinInactivityScores:               kv.InactivityScores,
	CaplinNextSyncCommittee:              kv.NextSyncCommittee,
	CaplinCurrentSyncCommittee:           kv.CurrentSyncCommittee,
	CaplinEth1DataVotes:                  kv.Eth1DataVotes,
	CaplinIntraRandaoMixes:               kv.IntraRandaoMixes,
	CaplinRandaoMixes:                    kv.RandaoMixes,
	CaplinBalancesDump:                   kv.BalancesDump,
	CaplinEffectiveBalancesDump:          kv.EffectiveBalancesDump,
	CaplinPendingConsolidations:          kv.PendingConsolidations,
	CaplinPendingPartialWithdrawals:      kv.PendingPartialWithdrawals,
	CaplinPendingDeposits:                kv.PendingDeposits,
	CaplinPendingConsolidationsDump:      kv.PendingConsolidationsDump,
	CaplinPendingPartialWithdrawalsDump:  kv.PendingPartialWithdrawalsDump,
	CaplinPendingDepositsDump:            kv.PendingDepositsDump,
	CaplinBuilders:                       kv.Builders,
	CaplinBuildersDump:                   kv.BuildersDump,
	CaplinBuilderPendingWithdrawals:      kv.BuilderPendingWithdrawals,
	CaplinBuilderPendingWithdrawalsDump:  kv.BuilderPendingWithdrawalsDump,
	CaplinPayloadExpectedWithdrawals:     kv.PayloadExpectedWithdrawals,
	CaplinPayloadExpectedWithdrawalsDump: kv.PayloadExpectedWithdrawalsDump,
	CaplinExecutionPayloadAvailability:   kv.ExecutionPayloadAvailabilityTable,
	CaplinBuilderPendingPayments:         kv.BuilderPendingPaymentsTable,
	CaplinPtcWindow:                      kv.PtcWindowTable,
	CaplinLatestExecutionPayloadBid:      kv.LatestExecutionPayloadBidTable,
}

func TestCaplinStateTypeNamesMatchKvConstants(t *testing.T) {
	require.Equal(t, int(caplinStateTypeCount), len(wantCaplinStateNames),
		"every CaplinStateType must have an expected kv name (and vice versa)")

	seen := make(map[string]CaplinStateType, caplinStateTypeCount)
	for typ := CaplinStateType(0); typ < caplinStateTypeCount; typ++ {
		want, ok := wantCaplinStateNames[typ]
		require.Truef(t, ok, "no expected name for CaplinStateType(%d)", int(typ))
		require.Equalf(t, want, typ.String(), "String() must equal the kv table constant for %d", int(typ))

		prev, dup := seen[want]
		require.Falsef(t, dup, "duplicate name %q for types %d and %d", want, int(prev), int(typ))
		seen[want] = typ
	}
}

func TestCaplinStateTypeParseRoundTrip(t *testing.T) {
	for typ := CaplinStateType(0); typ < caplinStateTypeCount; typ++ {
		got, ok := ParseCaplinStateType(typ.String())
		require.Truef(t, ok, "ParseCaplinStateType(%q) must resolve", typ.String())
		require.Equal(t, typ, got)
	}

	_, ok := ParseCaplinStateType("NotAStateTable")
	require.False(t, ok, "unknown name must not resolve")
	_, ok = ParseCaplinStateType("")
	require.False(t, ok, "empty name must not resolve")
}
