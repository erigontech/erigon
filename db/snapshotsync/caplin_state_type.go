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
	"fmt"

	"github.com/erigontech/erigon/db/kv"
)

// CaplinStateType enumerates the Caplin beacon-state snapshot types. String()
// returns the exact on-disk type name (the kv.* table constant) embedded in
// v1.1-<from>-<to>-<Name>.seg, which is also the DB table name — so file
// discovery and DB access stay name-compatible after enum-indexing.
type CaplinStateType int

const (
	CaplinValidatorEffectiveBalance CaplinStateType = iota
	CaplinValidatorSlashings
	CaplinValidatorBalance
	CaplinStateEvents
	CaplinActiveValidatorIndicies
	CaplinStateRoot
	CaplinBlockRoot
	CaplinSlotData
	CaplinEpochData
	CaplinInactivityScores
	CaplinNextSyncCommittee
	CaplinCurrentSyncCommittee
	CaplinEth1DataVotes
	CaplinIntraRandaoMixes
	CaplinRandaoMixes
	CaplinBalancesDump
	CaplinEffectiveBalancesDump
	CaplinPendingConsolidations
	CaplinPendingPartialWithdrawals
	CaplinPendingDeposits
	CaplinPendingConsolidationsDump
	CaplinPendingPartialWithdrawalsDump
	CaplinPendingDepositsDump
	CaplinBuilders
	CaplinBuildersDump
	CaplinBuilderPendingWithdrawals
	CaplinBuilderPendingWithdrawalsDump
	CaplinPayloadExpectedWithdrawals
	CaplinPayloadExpectedWithdrawalsDump
	CaplinExecutionPayloadAvailability
	CaplinBuilderPendingPayments
	CaplinPtcWindow
	CaplinLatestExecutionPayloadBid

	caplinStateTypeCount
)

var caplinStateTypeName = [caplinStateTypeCount]string{
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

var caplinStateTypeByName = func() map[string]CaplinStateType {
	m := make(map[string]CaplinStateType, caplinStateTypeCount)
	for t := CaplinStateType(0); t < caplinStateTypeCount; t++ {
		m[caplinStateTypeName[t]] = t
	}
	return m
}()

func (t CaplinStateType) String() string {
	if t < 0 || t >= caplinStateTypeCount {
		return fmt.Sprintf("CaplinStateType(%d)", int(t))
	}
	return caplinStateTypeName[t]
}

// ParseCaplinStateType maps a type/table name back to its enum. ok is false for
// any name that is not a Caplin state snapshot type, letting reader bridges fall
// through to the DB unchanged.
func ParseCaplinStateType(name string) (typ CaplinStateType, ok bool) {
	typ, ok = caplinStateTypeByName[name]
	return typ, ok
}
