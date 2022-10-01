package p2p

//go:generate go run github.com/ferranbt/fastssz/sszgen -path generated.go -exclude-objs Bitvector4,Bitvector64,Bytea,Epoch,Root,Signature,Slot,Ignore

import (
	"github.com/ledgerwatch/erigon/cmd/lightclient/sentinel/proto"
)

type Bitvector4 uint8

type Bitvector64 uint64

type Bytea []byte

type Epoch uint64

type Root [32]byte

type Signature [96]byte

type Slot uint64

type Attestation struct {
	AggregationBits Bytea `json:"aggregation_bits" ssz:"bitlist" ssz-max:"2048" `

	Data AttestationData `json:"data" `

	Signature Signature `json:"signature" ssz-size:"96" `
}

func (typ *Attestation) Clone() proto.Packet {
	return &Attestation{}
}

type AttestationData struct {
	Slot uint64 `json:"slot" `

	Index uint64 `json:"index" `

	BeaconBlockHash [32]byte `ssz-size:"32" json:"beacon_block_root" `

	Source Checkpoint `json:"source" `

	Target Checkpoint `json:"target" `
}

func (typ *AttestationData) Clone() proto.Packet {
	return &AttestationData{}
}

type AttesterSlashing struct {
	Header1 SignedBeaconBlockHeader `json:"signed_header_1" `

	Header2 SignedBeaconBlockHeader `json:"signed_header_2" `
}

func (typ *AttesterSlashing) Clone() proto.Packet {
	return &AttesterSlashing{}
}

type BeaconBlockAltair struct {
	Slot Slot `json:"slot" `

	ProposerIndex uint64 `json:"proposer_index" `

	ParentRoot Root `json:"parent_root" ssz-size:"32" `

	StateRoot Root `ssz-size:"32" json:"state_root" `

	Body BeaconBlockBodyAltair `json:"body" `
}

func (typ *BeaconBlockAltair) Clone() proto.Packet {
	return &BeaconBlockAltair{}
}

type BeaconBlockBellatrix struct {
	Slot Slot `json:"slot" `

	ProposerIndex uint64 `json:"proposer_index" `

	ParentRoot Root `json:"parent_root" ssz-size:"32" `

	StateRoot Root `json:"state_root" ssz-size:"32" `

	Body BeaconBlockBodyBellatrix `json:"body" `
}

func (typ *BeaconBlockBellatrix) Clone() proto.Packet {
	return &BeaconBlockBellatrix{}
}

type BeaconBlockBodyAltair struct {
	RandaoReveal Signature `json:"randao_reveal" ssz-size:"96" `

	Eth1Data Eth1Data `json:"eth1_data" `

	Graffiti [32]byte `json:"graffiti" ssz-size:"32" `

	ProposerSlashings []*ProposerSlashing `json:"proposer_slashings" ssz-max:"16" `

	AttesterSlashings []*AttesterSlashing `json:"attester_slashings" ssz-max:"2" `

	Attestations []*Attestation `json:"attestations" ssz-max:"128" `

	Deposits []*Deposit `json:"deposits" ssz-max:"16" `

	VoluntaryExits []*SignedVoluntaryExit `json:"voluntary_exits" ssz-max:"16" `

	SyncAggregate SyncAggregate `json:"sync_aggregate" `
}

func (typ *BeaconBlockBodyAltair) Clone() proto.Packet {
	return &BeaconBlockBodyAltair{}
}

type BeaconBlockBodyBellatrix struct {
	RandaoReveal Signature `json:"randao_reveal" ssz-size:"96" `

	Eth1Data Eth1Data `json:"eth1_data" `

	Graffiti [32]byte `json:"graffiti" ssz-size:"32" `

	ProposerSlashings []*ProposerSlashing `json:"proposer_slashings" ssz-max:"16" `

	AttesterSlashings []*AttesterSlashing `json:"attester_slashings" ssz-max:"2" `

	Attestations []*Attestation `json:"attestations" ssz-max:"128" `

	Deposits []*Deposit `json:"deposits" ssz-max:"16" `

	VoluntaryExits []*SignedVoluntaryExit `json:"voluntary_exits" ssz-max:"16" `

	SyncAggregate SyncAggregate `json:"sync_aggregate" `

	ExecutionPayload ExecutionPayload `json:"execution_payload" `
}

func (typ *BeaconBlockBodyBellatrix) Clone() proto.Packet {
	return &BeaconBlockBodyBellatrix{}
}

type BeaconBlockHeader struct {
	Slot Slot `json:"slot" `

	ProposerIndex uint64 `json:"proposer_index" `

	ParentRoot Root `json:"parent_root" ssz-size:"32" `

	StateRoot Root `json:"state_root" ssz-size:"32" `

	BodyRoot Root `json:"body_root" ssz-size:"32" `
}

func (typ *BeaconBlockHeader) Clone() proto.Packet {
	return &BeaconBlockHeader{}
}

type Checkpoint struct {
	Epoch uint64 `json:"epoch" `

	Root Root `json:"root" ssz-size:"32" `
}

func (typ *Checkpoint) Clone() proto.Packet {
	return &Checkpoint{}
}

type Deposit struct {
	Proof [33][32]byte `ssz-size:"33,32" `

	Data DepositData ``
}

func (typ *Deposit) Clone() proto.Packet {
	return &Deposit{}
}

type DepositData struct {
	Pubkey [48]byte `json:"pubkey" ssz-size:"48" `

	WithdrawalCredentials [32]byte `json:"withdrawal_credentials" ssz-size:"32" `

	Amount uint64 `json:"amount" `

	Signature Signature `ssz-size:"96" json:"signature" `

	Root [32]byte `ssz:"-" `
}

func (typ *DepositData) Clone() proto.Packet {
	return &DepositData{}
}

type ENRForkID struct {
	CurrentForkDigest Bytea `json:"current_fork_digest,omitempty" ssz-size:"4" `

	NextForkVersion Bytea `json:"next_fork_version,omitempty" ssz-size:"4" `

	NextForkEpoch Epoch `json:"next_fork_epoch,omitempty" `
}

func (typ *ENRForkID) Clone() proto.Packet {
	return &ENRForkID{}
}

type Eth1Data struct {
	DepositRoot Root `json:"deposit_root" ssz-size:"32" `

	DepositCount uint64 `json:"deposit_count" `

	BlockHash [32]byte `json:"block_hash" ssz-size:"32" `
}

func (typ *Eth1Data) Clone() proto.Packet {
	return &Eth1Data{}
}

type ExecutionPayload struct {
	ParentHash [32]byte `ssz-size:"32" json:"parent_hash" `

	FeeRecipient [20]byte `ssz-size:"20" json:"fee_recipient" `

	StateRoot [32]byte `json:"state_root" ssz-size:"32" `

	ReceiptsRoot [32]byte `ssz-size:"32" json:"receipts_root" `

	LogsBloom [256]byte `ssz-size:"256" json:"logs_bloom" `

	PrevRandao [32]byte `ssz-size:"32" `

	BlockNumber uint64 `json:"block_number" `

	GasLimit uint64 `json:"gas_limit" `

	GasUsed uint64 `json:"gas_used" `

	Timestamp uint64 `json:"timestamp" `

	ExtraData []byte `ssz-max:"32" json:"extra_data" `

	BaseFeePerGas [32]byte `ssz-size:"32" json:"base_fee_per_gas" `

	BlockHash [32]byte `ssz-size:"32" json:"block_hash" `

	Transactions [][]byte `ssz-max:"1048576,1073741824" ssz-size:"?,?" json:"transactions" `
}

func (typ *ExecutionPayload) Clone() proto.Packet {
	return &ExecutionPayload{}
}

type ForkData struct {
	CurrentVersion [4]byte `json:"current_version" ssz-size:"4" `

	GenesisValidatorsRoot Root `json:"genesis_validators_root" ssz-size:"32" `
}

func (typ *ForkData) Clone() proto.Packet {
	return &ForkData{}
}

type Goodbye struct {
	Reason uint64 `json:"reason" `
}

func (typ *Goodbye) Clone() proto.Packet {
	return &Goodbye{}
}

type LightClientBootstrap struct {
	Header BeaconBlockHeader `json:"header" `

	CurrentSyncCommittee SyncCommittee `json:"current_sync_committee" `

	CurrentSyncCommitteeBranch [][32]byte `json:"current_sync_committee_branch" ssz-max:"5" ssz-size:",32" `
}

func (typ *LightClientBootstrap) Clone() proto.Packet {
	return &LightClientBootstrap{}
}

type LightClientFinalityUpdate struct {
	AttestedHeader BeaconBlockHeader `json:"attested_header" `

	FinalizedHeader BeaconBlockHeader `json:"finalized_header" `

	FinalityBranch [][32]byte `json:"finality_branch" ssz-max:"6" ssz-size:",32" `

	SyncAggregate SyncAggregate `json:"sync_aggregate" `

	SignatureSlot Slot `json:"signature_slot" `
}

func (typ *LightClientFinalityUpdate) Clone() proto.Packet {
	return &LightClientFinalityUpdate{}
}

type LightClientOptimisticUpdate struct {
	AttestedHeader BeaconBlockHeader `json:"attested_header" `

	SyncAggregate SyncAggregate `json:"sync_aggregate" `

	SignatureSlot Slot `json:"signature_slot" `
}

func (typ *LightClientOptimisticUpdate) Clone() proto.Packet {
	return &LightClientOptimisticUpdate{}
}

type LightClientUpdate struct {
	AttestedHeader BeaconBlockHeader `json:"attested_header" `

	NextSyncCommittee SyncCommittee `json:"next_sync_committee" `

	NextSyncCommitteeBranch [][32]byte `json:"next_sync_committee_branch" ssz-max:"5" ssz-size:",32" `

	FinalizedHeader BeaconBlockHeader `json:"finalized_header" `

	FinalityBranch [][32]byte `json:"finality_branch" ssz-max:"6" ssz-size:",32" `

	SyncAggregate SyncAggregate `json:"sync_aggregate" `

	SignatureSlot Slot `json:"signature_slot" `
}

func (typ *LightClientUpdate) Clone() proto.Packet {
	return &LightClientUpdate{}
}

type MetadataV1 struct {
	SeqNumber uint64 `json:"seq_number,omitempty" `

	Attnets Bitvector64 `json:"attnets,omitempty" ssz-size:"8" `

	Syncnets Bitvector64 `json:"syncnets,omitempty" ssz-size:"1" `
}

func (typ *MetadataV1) Clone() proto.Packet {
	return &MetadataV1{}
}

type MetadataV0 struct {
	SeqNumber uint64 `json:"seq_number,omitempty" `

	Attnets Bitvector64 `json:"attnets,omitempty" ssz-size:"8" `
}

func (typ *MetadataV0) Clone() proto.Packet {
	return &MetadataV0{}
}

type Ping struct {
	Id uint64 `json:"id" `
}

func (typ *Ping) Clone() proto.Packet {
	return &Ping{}
}

type ProposerSlashing struct {
	Header1 SignedBeaconBlockHeader `json:"signed_header_1" `

	Header2 SignedBeaconBlockHeader `json:"signed_header_2" `
}

func (typ *ProposerSlashing) Clone() proto.Packet {
	return &ProposerSlashing{}
}

type SignedBeaconBlockAltair struct {
	Block BeaconBlockAltair `json:"message" `

	Signature Signature `ssz-size:"96" json:"signature" `
}

func (typ *SignedBeaconBlockAltair) Clone() proto.Packet {
	return &SignedBeaconBlockAltair{}
}

type SignedBeaconBlockBellatrix struct {
	Block BeaconBlockBellatrix `json:"message" `

	Signature Signature `ssz-size:"96" json:"signature" `
}

func (typ *SignedBeaconBlockBellatrix) Clone() proto.Packet {
	return &SignedBeaconBlockBellatrix{}
}

type SignedBeaconBlockHeader struct {
	Header BeaconBlockHeader `json:"message" `

	Signature Signature `json:"signature" ssz-size:"96" `
}

func (typ *SignedBeaconBlockHeader) Clone() proto.Packet {
	return &SignedBeaconBlockHeader{}
}

type SignedVoluntaryExit struct {
	Exit VoluntaryExit `json:"message" `

	Signature Signature `json:"signature" ssz-size:"96" `
}

func (typ *SignedVoluntaryExit) Clone() proto.Packet {
	return &SignedVoluntaryExit{}
}

type SingleRoot struct {
	Root Root `json:"root" ssz-size:"32" `
}

func (typ *SingleRoot) Clone() proto.Packet {
	return &SingleRoot{}
}

type Status struct {
	ForkDigest Bytea `json:"fork_digest,omitempty" ssz-size:"4" `

	FinalizedRoot Bytea `json:"finalized_root,omitempty" ssz-size:"32" `

	FinalizedEpoch Epoch `json:"finalized_epoch,omitempty" `

	HeadRoot Bytea `json:"head_root,omitempty" ssz-size:"32" `

	HeadSlot Slot `json:"head_slot,omitempty" `
}

func (typ *Status) Clone() proto.Packet {
	return &Status{}
}

type SyncAggregate struct {
	SyncCommiteeBits [64]byte `json:"sync_committee_bits" ssz-size:"64" `

	SyncCommiteeSignature Signature `json:"sync_committee_signature" ssz-size:"96" `
}

func (typ *SyncAggregate) Clone() proto.Packet {
	return &SyncAggregate{}
}

type SyncCommittee struct {
	PubKeys [512][48]byte `json:"pubkeys" ssz-size:"512,48" `

	AggregatePubKey [48]byte `json:"aggregate_pubkey" ssz-size:"48" `
}

func (typ *SyncCommittee) Clone() proto.Packet {
	return &SyncCommittee{}
}

type VoluntaryExit struct {
	Epoch uint64 `json:"epoch" `

	ValidatorIndex uint64 `json:"validator_index" `
}

func (typ *VoluntaryExit) Clone() proto.Packet {
	return &VoluntaryExit{}
}
