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

package snaptype

import (
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/version"
)

func newCaplinStateType(enum Enum, name string) SnapType {
	return SnapType{
		enum:     enum,
		name:     name,
		versions: version.V1_1_standart,
		indexes:  []Index{{Name: name, Version: version.V1_1_standart}},
	}
}

var (
	ValidatorEffectiveBalance         = newCaplinStateType(MinCaplinEnum+2, kv.ValidatorEffectiveBalance)
	ValidatorSlashings                = newCaplinStateType(MinCaplinEnum+3, kv.ValidatorSlashings)
	ValidatorBalance                  = newCaplinStateType(MinCaplinEnum+4, kv.ValidatorBalance)
	StateEvents                       = newCaplinStateType(MinCaplinEnum+5, kv.StateEvents)
	ActiveValidatorIndicies           = newCaplinStateType(MinCaplinEnum+6, kv.ActiveValidatorIndicies)
	StateRoot                         = newCaplinStateType(MinCaplinEnum+7, kv.StateRoot)
	BlockRoot                         = newCaplinStateType(MinCaplinEnum+8, kv.BlockRoot)
	SlotData                          = newCaplinStateType(MinCaplinEnum+9, kv.SlotData)
	EpochData                         = newCaplinStateType(MinCaplinEnum+10, kv.EpochData)
	InactivityScores                  = newCaplinStateType(MinCaplinEnum+11, kv.InactivityScores)
	NextSyncCommittee                 = newCaplinStateType(MinCaplinEnum+12, kv.NextSyncCommittee)
	CurrentSyncCommittee              = newCaplinStateType(MinCaplinEnum+13, kv.CurrentSyncCommittee)
	Eth1DataVotes                     = newCaplinStateType(MinCaplinEnum+14, kv.Eth1DataVotes)
	IntraRandaoMixes                  = newCaplinStateType(MinCaplinEnum+15, kv.IntraRandaoMixes)
	RandaoMixes                       = newCaplinStateType(MinCaplinEnum+16, kv.RandaoMixes)
	BalancesDump                      = newCaplinStateType(MinCaplinEnum+17, kv.BalancesDump)
	EffectiveBalancesDump             = newCaplinStateType(MinCaplinEnum+18, kv.EffectiveBalancesDump)
	PendingConsolidations             = newCaplinStateType(MinCaplinEnum+19, kv.PendingConsolidations)
	PendingPartialWithdrawals         = newCaplinStateType(MinCaplinEnum+20, kv.PendingPartialWithdrawals)
	PendingDeposits                   = newCaplinStateType(MinCaplinEnum+21, kv.PendingDeposits)
	PendingConsolidationsDump         = newCaplinStateType(MinCaplinEnum+22, kv.PendingConsolidationsDump)
	PendingPartialWithdrawalsDump     = newCaplinStateType(MinCaplinEnum+23, kv.PendingPartialWithdrawalsDump)
	PendingDepositsDump               = newCaplinStateType(MinCaplinEnum+24, kv.PendingDepositsDump)
	Builders                          = newCaplinStateType(MinCaplinEnum+25, kv.Builders)
	BuildersDump                      = newCaplinStateType(MinCaplinEnum+26, kv.BuildersDump)
	BuilderPendingWithdrawals         = newCaplinStateType(MinCaplinEnum+27, kv.BuilderPendingWithdrawals)
	BuilderPendingWithdrawalsDump     = newCaplinStateType(MinCaplinEnum+28, kv.BuilderPendingWithdrawalsDump)
	PayloadExpectedWithdrawals        = newCaplinStateType(MinCaplinEnum+29, kv.PayloadExpectedWithdrawals)
	PayloadExpectedWithdrawalsDump    = newCaplinStateType(MinCaplinEnum+30, kv.PayloadExpectedWithdrawalsDump)
	ExecutionPayloadAvailabilityTable = newCaplinStateType(MinCaplinEnum+31, kv.ExecutionPayloadAvailabilityTable)
	BuilderPendingPaymentsTable       = newCaplinStateType(MinCaplinEnum+32, kv.BuilderPendingPaymentsTable)
	PtcWindowTable                    = newCaplinStateType(MinCaplinEnum+33, kv.PtcWindowTable)
	LatestExecutionPayloadBidTable    = newCaplinStateType(MinCaplinEnum+34, kv.LatestExecutionPayloadBidTable)

	CaplinStateSnapshotTypes = []Type{
		ValidatorEffectiveBalance,
		ValidatorSlashings,
		ValidatorBalance,
		StateEvents,
		ActiveValidatorIndicies,
		StateRoot,
		BlockRoot,
		SlotData,
		EpochData,
		InactivityScores,
		NextSyncCommittee,
		CurrentSyncCommittee,
		Eth1DataVotes,
		IntraRandaoMixes,
		RandaoMixes,
		BalancesDump,
		EffectiveBalancesDump,
		PendingConsolidations,
		PendingPartialWithdrawals,
		PendingDeposits,
		PendingConsolidationsDump,
		PendingPartialWithdrawalsDump,
		PendingDepositsDump,
		Builders,
		BuildersDump,
		BuilderPendingWithdrawals,
		BuilderPendingWithdrawalsDump,
		PayloadExpectedWithdrawals,
		PayloadExpectedWithdrawalsDump,
		ExecutionPayloadAvailabilityTable,
		BuilderPendingPaymentsTable,
		PtcWindowTable,
		LatestExecutionPayloadBidTable,
	}
)
