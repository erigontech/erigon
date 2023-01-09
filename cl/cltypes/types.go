package cltypes

import (
	"bytes"

	"github.com/ledgerwatch/erigon/cl/utils"
	"github.com/ledgerwatch/erigon/core/types"
)

// Slashing requires 2 blocks with the same signer as proof
type ProposerSlashing struct {
	Header1 *SignedBeaconBlockHeader
	Header2 *SignedBeaconBlockHeader
}

/*
 * AttesterSlashing, slashing data for attester, needs to provide valid duplicates as proof.
 */
type AttesterSlashing struct {
	Attestation_1 *IndexedAttestation
	Attestation_2 *IndexedAttestation
}

// we will send this to Erigon once validation is done.
type ExecutionPayload struct {
	ParentHash    [32]byte `ssz-size:"32"`
	FeeRecipient  [20]byte `ssz-size:"20"`
	StateRoot     [32]byte `ssz-size:"32"`
	ReceiptsRoot  [32]byte `ssz-size:"32"`
	LogsBloom     []byte   `ssz-size:"256"`
	PrevRandao    [32]byte `ssz-size:"32"`
	BlockNumber   uint64
	GasLimit      uint64
	GasUsed       uint64
	Timestamp     uint64
	ExtraData     []byte   `ssz-max:"32"`
	BaseFeePerGas []byte   `ssz-size:"32"`
	BlockHash     [32]byte `ssz-size:"32"`
	Transactions  [][]byte `ssz-size:"?,?" ssz-max:"1048576,1073741824"`
}

/*
 * Block body for Consensus Layer, we only care about its hash and execution payload.
 */
type BeaconBodyBellatrix struct {
	RandaoReveal      [96]byte `ssz-size:"96"`
	Eth1Data          *Eth1Data
	Graffiti          []byte                 `ssz-size:"32"`
	ProposerSlashings []*ProposerSlashing    `ssz-max:"16"`
	AttesterSlashings []*AttesterSlashing    `ssz-max:"2"`
	Attestations      []*Attestation         `ssz-max:"128"`
	Deposits          []*Deposit             `ssz-max:"16"`
	VoluntaryExits    []*SignedVoluntaryExit `ssz-max:"16"`
	SyncAggregate     *SyncAggregate
	ExecutionPayload  *ExecutionPayload
}

/*
 * Block body for Consensus Layer to be stored internally (payload and attestations are stored separatedly).
 */
type BeaconBlockForStorage struct {
	// Non-body fields
	Signature     [96]byte `ssz-size:"96"`
	Slot          uint64
	ProposerIndex uint64
	ParentRoot    [32]byte `ssz-size:"32"`
	StateRoot     [32]byte `ssz-size:"32"`
	// Body fields
	RandaoReveal      [96]byte `ssz-size:"96"`
	Eth1Data          *Eth1Data
	Graffiti          []byte                 `ssz-size:"32"`
	ProposerSlashings []*ProposerSlashing    `ssz-max:"16"`
	AttesterSlashings []*AttesterSlashing    `ssz-max:"2"`
	Deposits          []*Deposit             `ssz-max:"16"`
	VoluntaryExits    []*SignedVoluntaryExit `ssz-max:"16"`
	SyncAggregate     *SyncAggregate
	// Metadatas
	Eth1Number    uint64
	Eth1BlockHash [32]byte `ssz-size:"32"`
	Eth2BlockRoot [32]byte `ssz-size:"32"`
	// Version type
	Version uint8
}

/*
 * Bellatrix block structure.
 */
type BeaconBlockBellatrix struct {
	Slot          uint64
	ProposerIndex uint64
	ParentRoot    [32]byte `ssz-size:"32"`
	StateRoot     [32]byte `ssz-size:"32"`
	Body          *BeaconBodyBellatrix
}

/*
 * We get this object with gossip so we need to do proper decoding.
 */
type SignedBeaconBlockBellatrix struct {
	Block     *BeaconBlockBellatrix
	Signature [96]byte `ssz-size:"96"`
}

type SignedBeaconBlockAltair struct {
	Block     *BeaconBlockAltair
	Signature [96]byte `ssz-size:"96"`
}

type BeaconBlockAltair struct {
	Slot          uint64
	ProposerIndex uint64
	ParentRoot    [32]byte `ssz-size:"32"`
	StateRoot     [32]byte `ssz-size:"32"`
	Body          *BeaconBodyAltair
}

type BeaconBodyAltair struct {
	RandaoReveal      [96]byte `ssz-size:"96"`
	Eth1Data          *Eth1Data
	Graffiti          []byte                 `ssz-size:"32"`
	ProposerSlashings []*ProposerSlashing    `ssz-max:"16"`
	AttesterSlashings []*AttesterSlashing    `ssz-max:"2"`
	Attestations      []*Attestation         `ssz-max:"128"`
	Deposits          []*Deposit             `ssz-max:"16"`
	VoluntaryExits    []*SignedVoluntaryExit `ssz-max:"16"`
	SyncAggregate     *SyncAggregate
}

type SignedBeaconBlockPhase0 struct {
	Block     *BeaconBlockPhase0
	Signature [96]byte `ssz-size:"96"`
}

type BeaconBlockPhase0 struct {
	Slot          uint64
	ProposerIndex uint64
	ParentRoot    [32]byte `ssz-size:"32"`
	StateRoot     [32]byte `ssz-size:"32"`
	Body          *BeaconBodyPhase0
}

type BeaconBodyPhase0 struct {
	RandaoReveal      [96]byte `ssz-size:"96"`
	Eth1Data          *Eth1Data
	Graffiti          []byte                 `ssz-size:"32"`
	ProposerSlashings []*ProposerSlashing    `ssz-max:"16"`
	AttesterSlashings []*AttesterSlashing    `ssz-max:"2"`
	Attestations      []*Attestation         `ssz-max:"128"`
	Deposits          []*Deposit             `ssz-max:"16"`
	VoluntaryExits    []*SignedVoluntaryExit `ssz-max:"16"`
}

/*
 * Sync committe public keys and their aggregate public keys, we use array of pubKeys.
 */
type SyncCommittee struct {
	PubKeys            [][48]byte `ssz-size:"512,48"`
	AggregatePublicKey [48]byte   `ssz-size:"48"`
}

func (s *SyncCommittee) Equal(s2 *SyncCommittee) bool {
	if !bytes.Equal(s.AggregatePublicKey[:], s2.AggregatePublicKey[:]) {
		return false
	}
	if len(s.PubKeys) != len(s2.PubKeys) {
		return false
	}
	for i := range s.PubKeys {
		if !bytes.Equal(s.PubKeys[i][:], s2.PubKeys[i][:]) {
			return false
		}
	}
	return true
}

// LightClientBootstrap is used to bootstrap the lightclient from checkpoint sync.
type LightClientBootstrap struct {
	Header                     *BeaconBlockHeader
	CurrentSyncCommittee       *SyncCommittee
	CurrentSyncCommitteeBranch [][]byte `ssz-size:"5,32"`
}

// LightClientUpdate is used to update the sync committee every 27 hours.
type LightClientUpdate struct {
	AttestedHeader          *BeaconBlockHeader
	NextSyncCommitee        *SyncCommittee
	NextSyncCommitteeBranch [][]byte `ssz-size:"5,32"`
	FinalizedHeader         *BeaconBlockHeader
	FinalityBranch          [][]byte `ssz-size:"6,32"`
	SyncAggregate           *SyncAggregate
	SignatureSlot           uint64
}

func (l *LightClientUpdate) HasNextSyncCommittee() bool {
	return l.NextSyncCommitee != nil
}

func (l *LightClientUpdate) IsFinalityUpdate() bool {
	return l.FinalityBranch != nil
}

func (l *LightClientUpdate) HasSyncFinality() bool {
	return l.FinalizedHeader != nil &&
		utils.SlotToPeriod(l.AttestedHeader.Slot) == utils.SlotToPeriod(l.FinalizedHeader.Slot)
}

// LightClientFinalityUpdate is used to update the sync aggreggate every 6 minutes.
type LightClientFinalityUpdate struct {
	AttestedHeader  *BeaconBlockHeader
	FinalizedHeader *BeaconBlockHeader
	FinalityBranch  [][]byte `ssz-size:"6,32"`
	SyncAggregate   *SyncAggregate
	SignatureSlot   uint64
}

// LightClientOptimisticUpdate is used for verifying N-1 block.
type LightClientOptimisticUpdate struct {
	AttestedHeader *BeaconBlockHeader
	SyncAggregate  *SyncAggregate
	SignatureSlot  uint64
}

// Fork data, contains if we were on bellatrix/alteir/phase0 and transition epoch. NOT USED.
type Fork struct {
	PreviousVersion [4]byte `ssz-size:"4" `
	CurrentVersion  [4]byte `ssz-size:"4" `
	Epoch           uint64
}

// Validator, contains if we were on bellatrix/alteir/phase0 and transition epoch.
// NOT USED but necessary for decoding Checkpoint sync.
type Validator struct {
	PublicKey                  [48]byte `ssz-size:"48"`
	WithdrawalCredentials      []byte   `ssz-size:"32"`
	EffectiveBalance           uint64
	Slashed                    bool
	ActivationEligibilityEpoch uint64
	ActivationEpoch            uint64
	ExitEpoch                  uint64
	WithdrawableEpoch          uint64
}

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

// BlockRoot retrieves a the state block root from the state.
func (b *BeaconStateBellatrix) BlockRoot() ([32]byte, error) {
	stateRoot, err := b.HashTreeRoot()
	if err != nil {
		return [32]byte{}, nil
	}
	// We make a temporary header for block root computation
	tempHeader := &BeaconBlockHeader{
		Slot:          b.LatestBlockHeader.Slot,
		ProposerIndex: b.LatestBlockHeader.ProposerIndex,
		ParentRoot:    b.LatestBlockHeader.ParentRoot,
		BodyRoot:      b.LatestBlockHeader.BodyRoot,
		Root:          stateRoot,
	}
	return tempHeader.HashTreeRoot()
}
