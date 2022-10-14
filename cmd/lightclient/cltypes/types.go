package cltypes

import (
	"bytes"

	ssz "github.com/ferranbt/fastssz"
	"github.com/ledgerwatch/erigon/cmd/lightclient/utils"
)

type Eth1Data struct {
	Root         [32]byte `ssz-size:"32"`
	DepositCount uint64
	BlockHash    [32]byte `ssz-size:"32"`
}

// Attestation Metadatas
type AttestationData struct {
	Slot            uint64
	Index           uint64
	BeaconBlockHash [32]byte `ssz-size:"32"`
}

type BeaconBlockHeader struct {
	Slot          uint64
	ProposerIndex uint64
	ParentRoot    [32]byte `ssz-size:"32"`
	Root          [32]byte `ssz-size:"32"`
	BodyRoot      [32]byte `ssz-size:"32"`
}

type SignedBeaconBlockHeader struct {
	Header    *BeaconBlockHeader
	Signature [96]byte `ssz-size:"96"`
}

// Slashing requires 2 blocks with the same signer as proof
type Slashing struct {
	Header1 *SignedBeaconBlockHeader
	Header2 *SignedBeaconBlockHeader
}

// Full signed attestation
type Attestation struct {
	AggregationBits []byte `ssz-max:"2048" ssz:"bitlist"`
	Data            *AttestationData
	Signature       [96]byte `ssz-size:"96"`
}

type DepositData struct {
	PubKey                [48]byte `ssz-size:"48"`
	WithdrawalCredentials []byte   `ssz-size:"32"`
	Amount                uint64
	Signature             [96]byte `ssz-size:"96"`
	Root                  [32]byte `ssz:"-"`
}

type Deposit struct {
	// Merkle proof is used for deposits
	Proof [][]byte `ssz-size:"33,32"`
	Data  *DepositData
}

type VoluntaryExit struct {
	Epoch          uint64
	ValidatorIndex uint64
}

type SignedVoluntaryExit struct {
	VolunaryExit *VoluntaryExit
	Signature    [96]byte `ssz-size:"96"`
}

type SyncAggregate struct {
	SyncCommiteeBits      []byte   `ssz-size:"64"` // @gotags: ssz-size:"64"
	SyncCommiteeSignature [96]byte `ssz-size:"96"` // @gotags: ssz-size:"96"
}

// return sum of the committee bits
func (agg *SyncAggregate) Sum() int {
	ret := 0
	for i := range agg.SyncCommiteeBits {
		for bit := 1; bit <= 128; bit *= 2 {
			if agg.SyncCommiteeBits[i]&byte(bit) > 0 {
				ret++
			}
		}
	}
	return ret
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

// we will send this to Erigon once validation is done.
type ExecutionHeader struct {
	ParentHash      [32]byte `ssz-size:"32"`
	FeeRecipient    [20]byte `ssz-size:"20"`
	StateRoot       [32]byte `ssz-size:"32"`
	ReceiptsRoot    [32]byte `ssz-size:"32"`
	LogsBloom       []byte   `ssz-size:"256"`
	PrevRandao      [32]byte `ssz-size:"32"`
	BlockNumber     uint64
	GasLimit        uint64
	GasUsed         uint64
	Timestamp       uint64
	ExtraData       []byte   `ssz-max:"32"`
	BaseFeePerGas   []byte   `ssz-size:"32"`
	BlockHash       [32]byte `ssz-size:"32"`
	TransactionRoot [32]byte `ssz-size:"32"`
}

type BeaconBodyBellatrix struct {
	RandaoReveal      [96]byte `ssz-size:"96"`
	Eth1Data          *Eth1Data
	Graffiti          []byte                 `ssz-size:"32"`
	ProposerSlashings []*Slashing            `ssz-max:"16"`
	AttesterSlashings []*Slashing            `ssz-max:"2"`
	Attestations      []*Attestation         `ssz-max:"128"`
	Deposits          []*Deposit             `ssz-max:"16"`
	VoluntaryExits    []*SignedVoluntaryExit `ssz-max:"16"`
	SyncAggregate     *SyncAggregate
	ExecutionPayload  *ExecutionPayload
}

type BeaconBlockBellatrix struct {
	Slot          uint64
	ProposerIndex uint64
	ParentRoot    [32]byte `ssz-size:"32"`
	StateRoot     [32]byte `ssz-size:"32"`
	Body          *BeaconBodyBellatrix
}

type SignedBeaconBlockBellatrix struct {
	Block     *BeaconBlockBellatrix
	Signature [96]byte `ssz-size:"96"`
}

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

type LightClientBootstrap struct {
	Header                     *BeaconBlockHeader
	CurrentSyncCommittee       *SyncCommittee
	CurrentSyncCommitteeBranch [][]byte `ssz-size:"5,32"`
}

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

type LightClientFinalityUpdate struct {
	AttestedHeader  *BeaconBlockHeader
	FinalizedHeader *BeaconBlockHeader
	FinalityBranch  [][]byte `ssz-size:"6,32"`
	SyncAggregate   *SyncAggregate
	SignatureSlot   uint64
}

type LightClientOptimisticUpdate struct {
	AttestedHeader *BeaconBlockHeader
	SyncAggregate  *SyncAggregate
	SignatureSlot  uint64
}

type Fork struct {
	PreviousVersion [4]byte `ssz-size:"4" `
	CurrentVersion  [4]byte `ssz-size:"4" `
	Epoch           uint64
}

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

type PendingAttestation struct {
	AggregationBits []byte `ssz-max:"2048"`
	Data            *AttestationData
	InclusionDelay  uint64
	ProposerIndex   uint64
}

type Checkpoint struct {
	Epoch uint64
	Root  [32]byte `ssz-size:"32"`
}

type BeaconState struct {
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
	LatestExecutionPayloadHeader *ExecutionHeader
}

type ObjectSSZ interface {
	ssz.Marshaler
	ssz.Unmarshaler

	HashTreeRoot() ([32]byte, error)
}
