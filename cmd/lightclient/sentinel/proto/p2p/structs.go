package p2p

//req resp

type Slot uint64
type Epoch uint64
type Bitvector64 uint64
type Bitvector4 uint8

type Root [32]byte
type Signature [96]byte

type Status struct {
	ForkDigest     []byte ` json:"fork_digest,omitempty" ssz-size:"4"`
	FinalizedRoot  []byte `json:"finalized_root,omitempty" ssz-size:"32"`
	FinalizedEpoch Epoch  `json:"finalized_epoch,omitempty"`
	HeadRoot       []byte `json:"head_root,omitempty" ssz-size:"32"`
	HeadSlot       Slot   `json:"head_slot,omitempty"`
}

type SingleRoot struct {
	Root [32]byte `json:"root" ssz-size:"32"`
}

type BeaconBlockHeader struct {
	Slot          uint64 `json:"slot"`
	ProposerIndex uint64 `json:"proposer_index"`
	ParentRoot    Root   `json:"parent_root" ssz-size:"32"`
	StateRoot     Root   `json:"state_root" ssz-size:"32"`
	BodyRoot      Root   `json:"body_root" ssz-size:"32"`
}

type LightClientBootstrap struct {
	Header                     BeaconBlockHeader `json:"header"`
	CurrentSyncCommittee       SyncCommittee     `json:"current_sync_committee"`
	CurrentSyncCommitteeBranch [][32]byte        `json:"current_sync_committee_branch" ssz-max:"5" ssz-size:",32"`
}

type LightClientUpdatesByRangeRequest struct {
	StartPeriod uint64 `json:"start_period"`
	Count       uint64 `json:"count"`
}
type LightClientUpdatesByRangeResponse struct {
	Updates []LightClientUpdate `json:"updates" ssz-max:"128"`
}

type LightClientUpdate struct {
	AttestedHeader          BeaconBlockHeader `json:"attested_header"`
	NextSyncCommittee       SyncCommittee     `json:"next_sync_committee"`
	NextSyncCommitteeBranch [][32]byte        `json:"next_sync_committee_branch" ssz-max:"5" ssz-size:",32"`

	FinalizedHeader BeaconBlockHeader `json:"finalized_header"`
	FinalityBranch  [][32]byte        `json:"finality_branch" ssz-max:"6" ssz-size:",32"`

	SyncAggregate SyncAggregate `json:"sync_aggregate"`

	SignatureSlot Slot `json:"signature_slot"`
}

type LightClientFinalityUpdate struct {
	AttestedHeader  BeaconBlockHeader `json:"attested_header"`
	FinalizedHeader BeaconBlockHeader `json:"finalized_header"`
	FinalityBranch  [][32]byte        `json:"finality_branch" ssz-max:"6" ssz-size:",32"`
	SyncAggregate   SyncAggregate     `json:"sync_aggregate"`
	SignatureSlot   Slot              `json:"signature_slot"`
}

type LightClientOptimisticUpdate struct {
	AttestedHeader BeaconBlockHeader `json:"attested_header"`
	SyncAggregate  SyncAggregate     `json:"sync_aggregate"`
	SignatureSlot  Slot              `json:"signature_slot"`
}

type SyncAggregate struct {
	SyncCommiteeBits      [64]byte  `json:"sync_committee_bits" ssz-size:"64"`
	SyncCommiteeSignature Signature `json:"sync_committee_signature" ssz-size:"96"`
}

type SyncCommittee struct {
	PubKeys         [512][48]byte `json:"pubkeys" ssz-size:"512,48"`
	AggregatePubKey [48]byte      `json:"aggregate_pubkey" ssz-size:"48"`
}

type Goodbye struct {
	Reason uint64 `json:"reason"`
}

type Ping struct {
	Id uint64 `json:"id"`
}

type MetadataV0 struct {
	SeqNumber uint64      `json:"seq_number,omitempty"`
	Attnets   Bitvector64 `json:"attnets,omitempty"  ssz-size:"8"`
}

type MetaDataV1 struct {
	SeqNumber uint64      `json:"seq_number,omitempty"`
	Attnets   Bitvector64 `json:"attnets,omitempty" ssz-size:"8"`
	Syncnets  Bitvector64 `json:"syncnets,omitempty" ssz-size:"1"`
}

type ENRForkID struct {
	CurrentForkDigest []byte `json:"current_fork_digest,omitempty" ssz-size:"4"`
	NextForkVersion   []byte `json:"next_fork_version,omitempty" ssz-size:"4"`
	NextForkEpoch     Epoch  `json:"next_fork_epoch,omitempty"`
}
