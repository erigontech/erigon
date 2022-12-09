package state

type StateLeafIndex uint

// All position of all the leaves of the state merkle tree.
const (
	GenesisTimeLeafIndex                  StateLeafIndex = 0
	GenesisValidatorsRootLeafIndex        StateLeafIndex = 1
	SlotLeafIndex                         StateLeafIndex = 2
	ForkLeafIndex                         StateLeafIndex = 3
	LatestBlockHeaderLeafIndex            StateLeafIndex = 4
	BlockRootsLeafIndex                   StateLeafIndex = 5
	StateRootsLeafIndex                   StateLeafIndex = 6
	HistoricalRootsLeafIndex              StateLeafIndex = 7
	Eth1DataLeafIndex                     StateLeafIndex = 8
	Eth1DataVotesLeafIndex                StateLeafIndex = 9
	Eth1DepositIndexLeafIndex             StateLeafIndex = 10
	ValidatorsLeafIndex                   StateLeafIndex = 11
	BalancesLeafIndex                     StateLeafIndex = 12
	RandaoMixesLeafIndex                  StateLeafIndex = 13
	SlashingsLeafIndex                    StateLeafIndex = 14
	PreviousEpochParticipationLeafIndex   StateLeafIndex = 15
	CurrentEpochParticipationLeafIndex    StateLeafIndex = 16
	JustificationBitsLeafIndex            StateLeafIndex = 17
	PreviousJustifiedCheckpointLeafIndex  StateLeafIndex = 18
	CurrentJustifiedCheckpointLeafIndex   StateLeafIndex = 19
	FinalizedCheckpointLeafIndex          StateLeafIndex = 20
	InactivityScoresLeafIndex             StateLeafIndex = 21
	CurrentSyncCommitteeLeafIndex         StateLeafIndex = 22
	NextSyncCommitteeLeafIndex            StateLeafIndex = 23
	LatestExecutionPayloadHeaderLeafIndex StateLeafIndex = 24

	// Leaves sizes
	BellatrixLeavesSize = 25
)
