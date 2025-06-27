// Copyright 2024 The Erigon Authors
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

package raw

type StateLeafIndex uint

// All position of all the leaves of the state merkle tree.
const (
	GenesisTimeLeafIndex                 StateLeafIndex = 0
	GenesisValidatorsRootLeafIndex       StateLeafIndex = 1
	SlotLeafIndex                        StateLeafIndex = 2
	ForkLeafIndex                        StateLeafIndex = 3
	LatestBlockHeaderLeafIndex           StateLeafIndex = 4
	BlockRootsLeafIndex                  StateLeafIndex = 5
	StateRootsLeafIndex                  StateLeafIndex = 6
	HistoricalRootsLeafIndex             StateLeafIndex = 7
	Eth1DataLeafIndex                    StateLeafIndex = 8
	Eth1DataVotesLeafIndex               StateLeafIndex = 9
	Eth1DepositIndexLeafIndex            StateLeafIndex = 10
	ValidatorsLeafIndex                  StateLeafIndex = 11
	BalancesLeafIndex                    StateLeafIndex = 12
	RandaoMixesLeafIndex                 StateLeafIndex = 13
	SlashingsLeafIndex                   StateLeafIndex = 14
	PreviousEpochParticipationLeafIndex  StateLeafIndex = 15
	CurrentEpochParticipationLeafIndex   StateLeafIndex = 16
	JustificationBitsLeafIndex           StateLeafIndex = 17
	PreviousJustifiedCheckpointLeafIndex StateLeafIndex = 18
	CurrentJustifiedCheckpointLeafIndex  StateLeafIndex = 19
	FinalizedCheckpointLeafIndex         StateLeafIndex = 20
	// Altair
	InactivityScoresLeafIndex     StateLeafIndex = 21
	CurrentSyncCommitteeLeafIndex StateLeafIndex = 22
	NextSyncCommitteeLeafIndex    StateLeafIndex = 23
	// Bellatrix
	LatestExecutionPayloadHeaderLeafIndex StateLeafIndex = 24
	// Capella
	NextWithdrawalIndexLeafIndex          StateLeafIndex = 25
	NextWithdrawalValidatorIndexLeafIndex StateLeafIndex = 26
	HistoricalSummariesLeafIndex          StateLeafIndex = 27
	// Electra
	DepositRequestsStartIndexLeafIndex     StateLeafIndex = 28
	DepositBalanceToConsumeLeafIndex       StateLeafIndex = 29
	ExitBalanceToConsumeLeafIndex          StateLeafIndex = 30
	EarliestExitEpochLeafIndex             StateLeafIndex = 31
	ConsolidationBalanceToConsumeLeafIndex StateLeafIndex = 32
	EarliestConsolidationEpochLeafIndex    StateLeafIndex = 33
	PendingDepositsLeafIndex               StateLeafIndex = 34
	PendingPartialWithdrawalsLeafIndex     StateLeafIndex = 35
	PendingConsolidationsLeafIndex         StateLeafIndex = 36
	// Fulu
	ProposerLookaheadLeafIndex StateLeafIndex = 37
)

const (
	StateLeafSizeDeneb   = 32
	StateLeafSizeElectra = 37
	StateLeafSizeFulu    = 38

	StateLeafSizeLatest = StateLeafSizeFulu

	LeafInitValue  = 0
	LeafCleanValue = 1
	LeafDirtyValue = 2
)
