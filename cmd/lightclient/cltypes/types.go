package cltypes

import ssz "github.com/ferranbt/fastssz"

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
	PubKeys [][48]byte `ssz-size:"512,48"`
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

type ObjectSSZ interface {
	ssz.Marshaler
	ssz.Unmarshaler
}
