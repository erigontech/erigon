package state

// All position of all the leaves of the state merkle tree.
const (
	GenesisTimeLeafIndex                  = 0
	GenesisValidatorsRootLeafIndex        = 1
	SlotLeafIndex                         = 2
	ForkLeafIndex                         = 3
	LatestBlockHeaderLeafIndex            = 4
	BlockRootsLeafIndex                   = 5
	StateRootsLeafIndex                   = 6
	HistoricalRootsLeafIndex              = 7
	Eth1DataLeafIndex                     = 8
	Eth1DataVotesLeafIndex                = 9
	Eth1DepositIndexLeafIndex             = 10
	ValidatorsLeafIndex                   = 11
	BalancesLeafIndex                     = 12
	RandaoMixesLeafIndex                  = 13
	SlashingsLeafIndex                    = 14
	PreviousEpochParticipationLeafIndex   = 15
	CurrentEpochParticipationLeafIndex    = 16
	JustificationBitsLeafIndex            = 17
	PreviousJustifiedCheckpointLeafIndex  = 18
	CurrentJustifiedCheckpointLeafIndex   = 19
	FinalizedCheckpointLeafIndex          = 20
	InactivityScoresLeafIndex             = 21
	CurrentSyncCommitteeLeafIndex         = 22
	NextSyncCommitteeLeafIndex            = 23
	LatestExecutionPayloadHeaderLeafIndex = 24

	// Leaves sizes
	BellatrixLeavesSize = 25
)
