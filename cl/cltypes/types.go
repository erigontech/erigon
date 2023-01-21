package cltypes

import (
	"github.com/ledgerwatch/erigon/core/types"
)

// BellatrixBeaconState is the bellatrix beacon state.
type BeaconStateBellatrix struct {
	GenesisTime                  uint64
	GenesisValidatorsRoot        [32]byte `ssz-size:"32"`
	Slot                         uint64
	Fork                         *Fork
	LatestBlockHeader            *BeaconBlockHeader
	BlockRoots                   [][32]byte `ssz-size:"8192,32"`
	StateRoots                   [][32]byte `ssz-size:"8192,32"`
	HistoricalRoots              [][32]byte `ssz-max:"16777216" ssz-size:"?,32"`
	Eth1Data                     *Eth1Data
	Eth1DataVotes                []*Eth1Data `ssz-max:"2048"`
	Eth1DepositIndex             uint64
	Validators                   []*Validator `ssz-max:"1099511627776"`
	Balances                     []uint64     `ssz-max:"1099511627776"`
	RandaoMixes                  [][32]byte   `ssz-size:"65536,32"`
	Slashings                    []uint64     `ssz-size:"8192"`
	PreviousEpochParticipation   []byte       `ssz-max:"1099511627776"`
	CurrentEpochParticipation    []byte       `ssz-max:"1099511627776"`
	JustificationBits            []byte       `ssz-size:"1"`
	PreviousJustifiedCheckpoint  *Checkpoint
	CurrentJustifiedCheckpoint   *Checkpoint
	FinalizedCheckpoint          *Checkpoint
	InactivityScores             []uint64 `ssz-max:"1099511627776"`
	CurrentSyncCommittee         *SyncCommittee
	NextSyncCommittee            *SyncCommittee
	LatestExecutionPayloadHeader *types.Header
}
