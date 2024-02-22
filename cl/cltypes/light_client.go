package cltypes

import (
	"github.com/ledgerwatch/erigon-lib/types/clonable"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	"github.com/ledgerwatch/erigon/cl/merkle_tree"
	ssz2 "github.com/ledgerwatch/erigon/cl/ssz"
)

const (
	ExecutionBranchSize            = 4
	SyncCommitteeBranchSize        = 5
	CurrentSyncCommitteeBranchSize = 5
	FinalizedBranchSize            = 6
)

type LightClientHeader struct {
	Beacon *BeaconBlockHeader `json:"beacon"`

	ExecutionPayloadHeader *Eth1Header         `json:"execution_payload_header,omitempty"`
	ExecutionBranch        solid.HashVectorSSZ `json:"execution_branch,omitempty"`

	version clparams.StateVersion
}

func NewLightClientHeader(version clparams.StateVersion) *LightClientHeader {
	if version < clparams.CapellaVersion {
		return &LightClientHeader{
			version: version,
			Beacon:  &BeaconBlockHeader{},
		}
	}
	return &LightClientHeader{
		version:                version,
		Beacon:                 &BeaconBlockHeader{},
		ExecutionBranch:        solid.NewHashVector(ExecutionBranchSize),
		ExecutionPayloadHeader: NewEth1Header(version),
	}
}

func (l *LightClientHeader) Version() clparams.StateVersion {
	return l.version
}

func (l *LightClientHeader) EncodeSSZ(buf []byte) ([]byte, error) {
	return ssz2.MarshalSSZ(buf, l.getSchema()...)
}

func (l *LightClientHeader) DecodeSSZ(buf []byte, version int) error {
	l.version = clparams.StateVersion(version)
	l.Beacon = &BeaconBlockHeader{}
	if version >= int(clparams.CapellaVersion) {
		l.ExecutionPayloadHeader = NewEth1Header(l.version)
		l.ExecutionBranch = solid.NewHashVector(ExecutionBranchSize)
	}
	return ssz2.UnmarshalSSZ(buf, version, l.getSchema()...)
}

func (l *LightClientHeader) EncodingSizeSSZ() int {
	size := l.Beacon.EncodingSizeSSZ()
	if l.version >= clparams.CapellaVersion {
		size += l.ExecutionPayloadHeader.EncodingSizeSSZ() + 4 // the extra 4 is for the offset
		size += l.ExecutionBranch.EncodingSizeSSZ()
	}
	return size
}

func (l *LightClientHeader) HashSSZ() ([32]byte, error) {
	return merkle_tree.HashTreeRoot(l.getSchema()...)
}

func (l *LightClientHeader) Static() bool {
	return l.version < clparams.CapellaVersion
}

func (l *LightClientHeader) Clone() clonable.Clonable {
	return NewLightClientHeader(l.version)
}

func (l *LightClientHeader) getSchema() []interface{} {
	schema := []interface{}{
		l.Beacon,
	}
	if l.version >= clparams.CapellaVersion {
		schema = append(schema, l.ExecutionPayloadHeader, l.ExecutionBranch)
	}
	return schema
}

type LightClientUpdate struct {
	AttestedHeader          *LightClientHeader   `json:"attested_header"`
	NextSyncCommittee       *solid.SyncCommittee `json:"next_sync_committee"`
	NextSyncCommitteeBranch solid.HashVectorSSZ  `json:"next_sync_committee_branch"`
	FinalizedHeader         *LightClientHeader   `json:"finalized_header"`
	FinalityBranch          solid.HashVectorSSZ  `json:"finality_branch"`
	SyncAggregate           *SyncAggregate       `json:"sync_aggregate"`
	SignatureSlot           uint64               `json:"signature_slot,string"`
}

func NewLightClientUpdate(version clparams.StateVersion) *LightClientUpdate {
	return &LightClientUpdate{
		AttestedHeader:          NewLightClientHeader(version),
		NextSyncCommittee:       &solid.SyncCommittee{},
		NextSyncCommitteeBranch: solid.NewHashVector(CurrentSyncCommitteeBranchSize),
		FinalizedHeader:         NewLightClientHeader(version),
		FinalityBranch:          solid.NewHashVector(FinalizedBranchSize),
		SyncAggregate:           &SyncAggregate{},
	}
}

func (l *LightClientUpdate) EncodeSSZ(buf []byte) ([]byte, error) {
	return ssz2.MarshalSSZ(buf, l.AttestedHeader, l.NextSyncCommittee, l.NextSyncCommitteeBranch, l.FinalizedHeader, l.FinalityBranch, l.SyncAggregate, &l.SignatureSlot)
}

func (l *LightClientUpdate) DecodeSSZ(buf []byte, version int) error {
	l.AttestedHeader = NewLightClientHeader(clparams.StateVersion(version))
	l.NextSyncCommittee = &solid.SyncCommittee{}
	l.NextSyncCommitteeBranch = solid.NewHashVector(CurrentSyncCommitteeBranchSize)
	l.FinalizedHeader = NewLightClientHeader(clparams.StateVersion(version))
	l.FinalityBranch = solid.NewHashVector(FinalizedBranchSize)
	l.SyncAggregate = &SyncAggregate{}
	return ssz2.UnmarshalSSZ(buf, version, l.AttestedHeader, l.NextSyncCommittee, l.NextSyncCommitteeBranch, l.FinalizedHeader, l.FinalityBranch, l.SyncAggregate, &l.SignatureSlot)
}

func (l *LightClientUpdate) EncodingSizeSSZ() int {
	size := l.AttestedHeader.EncodingSizeSSZ()
	if !l.AttestedHeader.Static() {
		size += 4 // the extra 4 is for the offset
	}
	size += l.NextSyncCommittee.EncodingSizeSSZ()
	size += l.NextSyncCommitteeBranch.EncodingSizeSSZ()
	size += l.FinalizedHeader.EncodingSizeSSZ()
	if !l.FinalizedHeader.Static() {
		size += 4 // the extra 4 is for the offset
	}
	size += l.FinalityBranch.EncodingSizeSSZ()
	size += l.SyncAggregate.EncodingSizeSSZ()
	size += 8 // for the slot
	return size
}

func (l *LightClientUpdate) HashSSZ() ([32]byte, error) {
	return merkle_tree.HashTreeRoot(l.AttestedHeader, l.NextSyncCommittee, l.NextSyncCommitteeBranch, l.FinalizedHeader, l.FinalityBranch, l.SyncAggregate, &l.SignatureSlot)
}

func (l *LightClientUpdate) Clone() clonable.Clonable {
	v := clparams.Phase0Version
	if l.AttestedHeader != nil {
		v = l.AttestedHeader.version
	}
	return NewLightClientUpdate(v)
}

type LightClientBootstrap struct {
	Header                     *LightClientHeader   `json:"header"`
	CurrentSyncCommittee       *solid.SyncCommittee `json:"current_sync_committee"`
	CurrentSyncCommitteeBranch solid.HashVectorSSZ  `json:"current_sync_committee_branch"`
}

func NewLightClientBootstrap(version clparams.StateVersion) *LightClientBootstrap {
	return &LightClientBootstrap{
		Header:                     NewLightClientHeader(version),
		CurrentSyncCommittee:       &solid.SyncCommittee{},
		CurrentSyncCommitteeBranch: solid.NewHashVector(CurrentSyncCommitteeBranchSize),
	}
}

func (l *LightClientBootstrap) EncodeSSZ(buf []byte) ([]byte, error) {
	return ssz2.MarshalSSZ(buf, l.Header, l.CurrentSyncCommittee, l.CurrentSyncCommitteeBranch)
}

func (l *LightClientBootstrap) DecodeSSZ(buf []byte, version int) error {
	l.Header = NewLightClientHeader(clparams.StateVersion(version))
	l.CurrentSyncCommittee = &solid.SyncCommittee{}
	l.CurrentSyncCommitteeBranch = solid.NewHashVector(CurrentSyncCommitteeBranchSize)
	return ssz2.UnmarshalSSZ(buf, version, l.Header, l.CurrentSyncCommittee, l.CurrentSyncCommitteeBranch)
}

func (l *LightClientBootstrap) EncodingSizeSSZ() int {
	size := l.Header.EncodingSizeSSZ()
	if !l.Header.Static() {
		size += 4 // the extra 4 is for the offset
	}
	size += l.CurrentSyncCommittee.EncodingSizeSSZ()
	size += l.CurrentSyncCommitteeBranch.EncodingSizeSSZ()
	return size
}

func (l *LightClientBootstrap) HashSSZ() ([32]byte, error) {
	return merkle_tree.HashTreeRoot(l.Header, l.CurrentSyncCommittee, l.CurrentSyncCommitteeBranch)
}

func (l *LightClientBootstrap) Clone() clonable.Clonable {
	v := clparams.Phase0Version
	if l.Header != nil {
		v = l.Header.version
	}
	return NewLightClientBootstrap(v)
}

type LightClientFinalityUpdate struct {
	AttestedHeader  *LightClientHeader  `json:"attested_header"`
	FinalizedHeader *LightClientHeader  `json:"finalized_header"`
	FinalityBranch  solid.HashVectorSSZ `json:"finality_branch"`
	SyncAggregate   *SyncAggregate      `json:"sync_aggregate"`
	SignatureSlot   uint64              `json:"signature_slot,string"`
}

func NewLightClientFinalityUpdate(version clparams.StateVersion) *LightClientFinalityUpdate {
	return &LightClientFinalityUpdate{
		AttestedHeader:  NewLightClientHeader(version),
		FinalizedHeader: NewLightClientHeader(version),
		FinalityBranch:  solid.NewHashVector(FinalizedBranchSize),
		SyncAggregate:   &SyncAggregate{},
	}
}

func (l *LightClientFinalityUpdate) EncodeSSZ(buf []byte) ([]byte, error) {
	return ssz2.MarshalSSZ(buf, l.AttestedHeader, l.FinalizedHeader, l.FinalityBranch, l.SyncAggregate, &l.SignatureSlot)
}

func (l *LightClientFinalityUpdate) DecodeSSZ(buf []byte, version int) error {
	l.AttestedHeader = NewLightClientHeader(clparams.StateVersion(version))
	l.FinalizedHeader = NewLightClientHeader(clparams.StateVersion(version))
	l.FinalityBranch = solid.NewHashVector(FinalizedBranchSize)
	l.SyncAggregate = &SyncAggregate{}
	return ssz2.UnmarshalSSZ(buf, version, l.AttestedHeader, l.FinalizedHeader, l.FinalityBranch, l.SyncAggregate, &l.SignatureSlot)
}

func (l *LightClientFinalityUpdate) EncodingSizeSSZ() int {
	size := l.AttestedHeader.EncodingSizeSSZ()
	if !l.AttestedHeader.Static() {
		size += 4 // the extra 4 is for the offset
	}
	size += l.FinalizedHeader.EncodingSizeSSZ()
	if !l.FinalizedHeader.Static() {
		size += 4 // the extra 4 is for the offset
	}
	size += l.FinalityBranch.EncodingSizeSSZ()
	size += l.SyncAggregate.EncodingSizeSSZ()
	size += 8 // for the slot
	return size
}

func (l *LightClientFinalityUpdate) HashSSZ() ([32]byte, error) {
	return merkle_tree.HashTreeRoot(l.AttestedHeader, l.FinalizedHeader, l.FinalityBranch, l.SyncAggregate, &l.SignatureSlot)
}

func (l *LightClientFinalityUpdate) Clone() clonable.Clonable {
	v := clparams.Phase0Version
	if l.AttestedHeader != nil {
		v = l.AttestedHeader.version
	}
	return NewLightClientFinalityUpdate(v)
}

type LightClientOptimisticUpdate struct {
	AttestedHeader *LightClientHeader `json:"attested_header"`
	SyncAggregate  *SyncAggregate     `json:"sync_aggregate"`
	SignatureSlot  uint64             `json:"signature_slot,string"`
}

func NewLightClientOptimisticUpdate(version clparams.StateVersion) *LightClientOptimisticUpdate {
	return &LightClientOptimisticUpdate{
		AttestedHeader: NewLightClientHeader(version),
		SyncAggregate:  &SyncAggregate{},
	}
}

func (l *LightClientOptimisticUpdate) EncodeSSZ(buf []byte) ([]byte, error) {
	return ssz2.MarshalSSZ(buf, l.AttestedHeader, l.SyncAggregate, &l.SignatureSlot)
}

func (l *LightClientOptimisticUpdate) DecodeSSZ(buf []byte, version int) error {
	l.AttestedHeader = NewLightClientHeader(clparams.StateVersion(version))
	l.SyncAggregate = &SyncAggregate{}
	return ssz2.UnmarshalSSZ(buf, version, l.AttestedHeader, l.SyncAggregate, &l.SignatureSlot)
}

func (l *LightClientOptimisticUpdate) EncodingSizeSSZ() int {
	size := l.AttestedHeader.EncodingSizeSSZ()
	if !l.AttestedHeader.Static() {
		size += 4 // the extra 4 is for the offset
	}
	size += l.SyncAggregate.EncodingSizeSSZ()
	size += 8 // for the slot
	return size
}

func (l *LightClientOptimisticUpdate) HashSSZ() ([32]byte, error) {
	return merkle_tree.HashTreeRoot(l.AttestedHeader, l.SyncAggregate, &l.SignatureSlot)
}

func (l *LightClientOptimisticUpdate) Clone() clonable.Clonable {
	v := clparams.Phase0Version
	if l.AttestedHeader != nil {
		v = l.AttestedHeader.version
	}
	return NewLightClientOptimisticUpdate(v)
}
