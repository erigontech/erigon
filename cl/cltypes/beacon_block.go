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

package cltypes

import (
	"encoding/json"
	"errors"
	"fmt"

	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/types/clonable"
	"github.com/erigontech/erigon-lib/types/ssz"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/merkle_tree"
	ssz2 "github.com/erigontech/erigon/cl/ssz"
)

var (
	_ ssz2.SizedObjectSSZ = (*BeaconBody)(nil)
	_ ssz2.SizedObjectSSZ = (*BeaconBlock)(nil)
	_ ssz2.SizedObjectSSZ = (*SignedBeaconBlock)(nil)
	_ ssz2.SizedObjectSSZ = (*DenebBeaconBlock)(nil)
	_ ssz2.SizedObjectSSZ = (*DenebSignedBeaconBlock)(nil)
)

const (
	MaxAttesterSlashings         = 2
	MaxProposerSlashings         = 16
	MaxAttestations              = 128
	MaxDeposits                  = 16
	MaxVoluntaryExits            = 16
	MaxExecutionChanges          = 16
	MaxBlobsCommittmentsPerBlock = 4096
)

var (
	_ GenericBeaconBlock = (*BeaconBlock)(nil)
	_ GenericBeaconBlock = (*DenebBeaconBlock)(nil)

	_ GenericBeaconBody = (*BeaconBody)(nil)
)

// Definition of SignedBeaconBlock
type SignedBeaconBlock struct {
	Signature libcommon.Bytes96 `json:"signature"`
	Block     *BeaconBlock      `json:"message"`
}

func NewSignedBeaconBlock(beaconCfg *clparams.BeaconChainConfig) *SignedBeaconBlock {
	return &SignedBeaconBlock{Block: NewBeaconBlock(beaconCfg)}
}

func (b *SignedBeaconBlock) Blinded() (*SignedBlindedBeaconBlock, error) {
	blindedBlock, err := b.Block.Blinded()
	if err != nil {
		return nil, err
	}
	return &SignedBlindedBeaconBlock{
		Signature: b.Signature,
		Block:     blindedBlock,
	}, nil
}

func (s *SignedBeaconBlock) SignedBeaconBlockHeader() *SignedBeaconBlockHeader {
	bodyRoot, err := s.Block.Body.HashSSZ()
	if err != nil {
		panic(err)
	}
	return &SignedBeaconBlockHeader{
		Signature: s.Signature,
		Header: &BeaconBlockHeader{
			Slot:          s.Block.Slot,
			ProposerIndex: s.Block.ProposerIndex,
			ParentRoot:    s.Block.ParentRoot,
			Root:          s.Block.StateRoot,
			BodyRoot:      bodyRoot,
		},
	}
}

// Version returns beacon block version.
func (b *SignedBeaconBlock) Version() clparams.StateVersion {
	return b.Block.Body.Version
}

func (b *SignedBeaconBlock) EncodeSSZ(buf []byte) ([]byte, error) {
	return ssz2.MarshalSSZ(buf, b.Block, b.Signature[:])
}

func (b *SignedBeaconBlock) EncodingSizeSSZ() int {
	if b.Block == nil {
		return 100
	}
	return 100 + b.Block.EncodingSizeSSZ()
}

func (b *SignedBeaconBlock) DecodeSSZ(buf []byte, s int) error {
	return ssz2.UnmarshalSSZ(buf, s, b.Block, b.Signature[:])
}

func (b *SignedBeaconBlock) HashSSZ() ([32]byte, error) {
	return merkle_tree.HashTreeRoot(b.Block, b.Signature[:])
}

func (b *SignedBeaconBlock) Static() bool {
	return false
}

// Definition of BeaconBlock
type BeaconBlock struct {
	Slot          uint64         `json:"slot,string"`
	ProposerIndex uint64         `json:"proposer_index,string"`
	ParentRoot    libcommon.Hash `json:"parent_root"`
	StateRoot     libcommon.Hash `json:"state_root"`
	Body          *BeaconBody    `json:"body"`
}

func NewBeaconBlock(beaconCfg *clparams.BeaconChainConfig) *BeaconBlock {
	return &BeaconBlock{Body: NewBeaconBody(beaconCfg)}
}

func (b *BeaconBlock) Blinded() (*BlindedBeaconBlock, error) {
	body, err := b.Body.Blinded()
	if err != nil {
		return nil, err
	}
	return &BlindedBeaconBlock{
		Slot:          b.Slot,
		ProposerIndex: b.ProposerIndex,
		ParentRoot:    b.ParentRoot,
		StateRoot:     b.StateRoot,
		Body:          body,
	}, nil
}

// Version returns beacon block version.
func (b *BeaconBlock) Version() clparams.StateVersion {
	return b.Body.Version
}

func (b *BeaconBlock) SetVersion(version clparams.StateVersion) {
	b.Body.SetVersion(version)
}

func (b *BeaconBlock) EncodeSSZ(buf []byte) (dst []byte, err error) {
	return ssz2.MarshalSSZ(buf, b.Slot, b.ProposerIndex, b.ParentRoot[:], b.StateRoot[:], b.Body)
}

func (b *BeaconBlock) EncodingSizeSSZ() int {
	if b.Body == nil {
		return 80
	}
	return 80 + b.Body.EncodingSizeSSZ()
}

func (b *BeaconBlock) DecodeSSZ(buf []byte, version int) error {
	return ssz2.UnmarshalSSZ(buf, version, &b.Slot, &b.ProposerIndex, b.ParentRoot[:], b.StateRoot[:], b.Body)
}

func (b *BeaconBlock) HashSSZ() ([32]byte, error) {
	return merkle_tree.HashTreeRoot(b.Slot, b.ProposerIndex, b.ParentRoot[:], b.StateRoot[:], b.Body)
}

func (*BeaconBlock) Static() bool {
	return false
}

func (b *BeaconBlock) GetSlot() uint64 {
	return b.Slot
}

func (b *BeaconBlock) GetProposerIndex() uint64 {
	return b.ProposerIndex
}

func (b *BeaconBlock) GetParentRoot() libcommon.Hash {
	return b.ParentRoot
}

func (b *BeaconBlock) GetBody() GenericBeaconBody {
	return b.Body
}

// Definition of BeaconBody
type BeaconBody struct {
	// A byte array used for randomness in the beacon chain
	RandaoReveal libcommon.Bytes96 `json:"randao_reveal"`
	// Data related to the Ethereum 1.0 chain
	Eth1Data *Eth1Data `json:"eth1_data"`
	// A byte array used to customize validators' behavior
	Graffiti libcommon.Hash `json:"graffiti"`
	// A list of slashing events for validators who included invalid blocks in the chain
	ProposerSlashings *solid.ListSSZ[*ProposerSlashing] `json:"proposer_slashings"`
	// A list of slashing events for validators who included invalid attestations in the chain
	AttesterSlashings *solid.ListSSZ[*AttesterSlashing] `json:"attester_slashings"`
	// A list of attestations included in the block
	Attestations *solid.ListSSZ[*solid.Attestation] `json:"attestations"`
	// A list of deposits made to the Ethereum 1.0 chain
	Deposits *solid.ListSSZ[*Deposit] `json:"deposits"`
	// A list of validators who have voluntarily exited the beacon chain
	VoluntaryExits *solid.ListSSZ[*SignedVoluntaryExit] `json:"voluntary_exits"`
	// A summary of the current state of the beacon chain
	SyncAggregate *SyncAggregate `json:"sync_aggregate,omitempty"`
	// Data related to crosslink records and executing operations on the Ethereum 2.0 chain
	ExecutionPayload *Eth1Block `json:"execution_payload,omitempty"`
	// Withdrawals Diffs for Execution Layer
	ExecutionChanges *solid.ListSSZ[*SignedBLSToExecutionChange] `json:"bls_to_execution_changes,omitempty"`
	// The commitments for beacon chain blobs
	// With a max of 4 per block
	BlobKzgCommitments *solid.ListSSZ[*KZGCommitment] `json:"blob_kzg_commitments,omitempty"`
	// The version of the beacon chain
	Version   clparams.StateVersion `json:"-"`
	beaconCfg *clparams.BeaconChainConfig
}

func NewBeaconBody(beaconCfg *clparams.BeaconChainConfig) *BeaconBody {
	return &BeaconBody{
		beaconCfg:          beaconCfg,
		Eth1Data:           &Eth1Data{},
		ProposerSlashings:  solid.NewStaticListSSZ[*ProposerSlashing](MaxProposerSlashings, 416),
		AttesterSlashings:  solid.NewDynamicListSSZ[*AttesterSlashing](MaxAttesterSlashings),
		Attestations:       solid.NewDynamicListSSZ[*solid.Attestation](MaxAttestations),
		Deposits:           solid.NewStaticListSSZ[*Deposit](MaxDeposits, 1240),
		VoluntaryExits:     solid.NewStaticListSSZ[*SignedVoluntaryExit](MaxVoluntaryExits, 112),
		ExecutionPayload:   NewEth1Block(clparams.Phase0Version, beaconCfg),
		ExecutionChanges:   solid.NewStaticListSSZ[*SignedBLSToExecutionChange](MaxExecutionChanges, 172),
		BlobKzgCommitments: solid.NewStaticListSSZ[*KZGCommitment](MaxBlobsCommittmentsPerBlock, 48),
	}
}
func (b *BeaconBody) SetVersion(version clparams.StateVersion) {
	b.Version = version
	b.ExecutionPayload.SetVersion(version)
}

func (b *BeaconBody) EncodeSSZ(dst []byte) ([]byte, error) {
	return ssz2.MarshalSSZ(dst, b.getSchema(false)...)
}

func (b *BeaconBody) EncodingSizeSSZ() (size int) {

	if b.Eth1Data == nil {
		b.Eth1Data = &Eth1Data{}
	}
	if b.SyncAggregate == nil {
		b.SyncAggregate = &SyncAggregate{}
	}

	if b.ProposerSlashings == nil {
		b.ProposerSlashings = solid.NewStaticListSSZ[*ProposerSlashing](MaxProposerSlashings, 416)
	}
	if b.AttesterSlashings == nil {
		b.AttesterSlashings = solid.NewDynamicListSSZ[*AttesterSlashing](MaxAttesterSlashings)
	}
	if b.Attestations == nil {
		b.Attestations = solid.NewDynamicListSSZ[*solid.Attestation](MaxAttestations)
	}
	if b.Deposits == nil {
		b.Deposits = solid.NewStaticListSSZ[*Deposit](MaxDeposits, 1240)
	}
	if b.VoluntaryExits == nil {
		b.VoluntaryExits = solid.NewStaticListSSZ[*SignedVoluntaryExit](MaxVoluntaryExits, 112)
	}
	if b.ExecutionPayload == nil {
		b.ExecutionPayload = NewEth1Block(b.Version, b.beaconCfg)
	}
	if b.ExecutionChanges == nil {
		b.ExecutionChanges = solid.NewStaticListSSZ[*SignedBLSToExecutionChange](MaxExecutionChanges, 172)
	}
	if b.BlobKzgCommitments == nil {
		b.BlobKzgCommitments = solid.NewStaticListSSZ[*KZGCommitment](MaxBlobsCommittmentsPerBlock, 48)
	}

	size += b.ProposerSlashings.EncodingSizeSSZ()
	size += b.AttesterSlashings.EncodingSizeSSZ()
	size += b.Attestations.EncodingSizeSSZ()
	size += b.Deposits.EncodingSizeSSZ()
	size += b.VoluntaryExits.EncodingSizeSSZ()
	if b.Version >= clparams.BellatrixVersion {
		size += b.ExecutionPayload.EncodingSizeSSZ()
	}
	if b.Version >= clparams.CapellaVersion {
		size += b.ExecutionChanges.EncodingSizeSSZ()
	}
	if b.Version >= clparams.DenebVersion {
		size += b.ExecutionChanges.EncodingSizeSSZ()
	}

	return
}

func (b *BeaconBody) DecodeSSZ(buf []byte, version int) error {
	b.Version = clparams.StateVersion(version)

	if len(buf) < b.EncodingSizeSSZ() {
		return fmt.Errorf("[BeaconBody] err: %s", ssz.ErrLowBufferSize)
	}

	b.ExecutionPayload = NewEth1Block(b.Version, b.beaconCfg)

	err := ssz2.UnmarshalSSZ(buf, version, b.getSchema(false)...)
	return err
}

func (b *BeaconBody) Blinded() (*BlindedBeaconBody, error) {
	header, err := b.ExecutionPayload.PayloadHeader()
	if err != nil {
		return nil, err
	}
	return &BlindedBeaconBody{
		RandaoReveal:       b.RandaoReveal,
		Eth1Data:           b.Eth1Data,
		Graffiti:           b.Graffiti,
		ProposerSlashings:  b.ProposerSlashings,
		AttesterSlashings:  b.AttesterSlashings,
		Attestations:       b.Attestations,
		Deposits:           b.Deposits,
		VoluntaryExits:     b.VoluntaryExits,
		SyncAggregate:      b.SyncAggregate,
		ExecutionPayload:   header,
		ExecutionChanges:   b.ExecutionChanges,
		BlobKzgCommitments: b.BlobKzgCommitments,
		Version:            b.Version,
		beaconCfg:          b.beaconCfg,
	}, nil
}

func (b *BeaconBody) HashSSZ() ([32]byte, error) {
	return merkle_tree.HashTreeRoot(b.getSchema(false)...)
}

func (b *BeaconBody) getSchema(storage bool) []interface{} {
	s := []interface{}{b.RandaoReveal[:], b.Eth1Data, b.Graffiti[:], b.ProposerSlashings, b.AttesterSlashings, b.Attestations, b.Deposits, b.VoluntaryExits}
	if b.Version >= clparams.AltairVersion {
		s = append(s, b.SyncAggregate)
	}
	if b.Version >= clparams.BellatrixVersion && !storage {
		s = append(s, b.ExecutionPayload)
	}
	if b.Version >= clparams.CapellaVersion {
		s = append(s, b.ExecutionChanges)
	}
	if b.Version >= clparams.DenebVersion {
		s = append(s, b.BlobKzgCommitments)
	}
	return s
}

func (*BeaconBody) Static() bool {
	return false
}
func (b *BeaconBody) ExecutionPayloadMerkleProof() ([][32]byte, error) {
	return merkle_tree.MerkleProof(4, 9, b.getSchema(false)...)
}

func (b *BeaconBody) KzgCommitmentMerkleProof(index int) ([][32]byte, error) {
	if index >= b.BlobKzgCommitments.Len() {
		return nil, errors.New("index out of range")
	}
	kzgCommitmentsProof, err := merkle_tree.MerkleProof(4, 11, b.getSchema(false)...)
	if err != nil {
		return nil, err
	}
	branch := b.BlobKzgCommitments.ElementProof(index)
	return append(branch, kzgCommitmentsProof...), nil
}

func (b *BeaconBody) UnmarshalJSON(buf []byte) error {
	var tmp struct {
		RandaoReveal       libcommon.Bytes96                           `json:"randao_reveal"`
		Eth1Data           *Eth1Data                                   `json:"eth1_data"`
		Graffiti           libcommon.Hash                              `json:"graffiti"`
		ProposerSlashings  *solid.ListSSZ[*ProposerSlashing]           `json:"proposer_slashings"`
		AttesterSlashings  *solid.ListSSZ[*AttesterSlashing]           `json:"attester_slashings"`
		Attestations       *solid.ListSSZ[*solid.Attestation]          `json:"attestations"`
		Deposits           *solid.ListSSZ[*Deposit]                    `json:"deposits"`
		VoluntaryExits     *solid.ListSSZ[*SignedVoluntaryExit]        `json:"voluntary_exits"`
		SyncAggregate      *SyncAggregate                              `json:"sync_aggregate,omitempty"`
		ExecutionPayload   *Eth1Block                                  `json:"execution_payload,omitempty"`
		ExecutionChanges   *solid.ListSSZ[*SignedBLSToExecutionChange] `json:"bls_to_execution_changes,omitempty"`
		BlobKzgCommitments *solid.ListSSZ[*KZGCommitment]              `json:"blob_kzg_commitments,omitempty"`
	}
	tmp.ProposerSlashings = solid.NewStaticListSSZ[*ProposerSlashing](MaxProposerSlashings, 416)
	tmp.AttesterSlashings = solid.NewDynamicListSSZ[*AttesterSlashing](MaxAttesterSlashings)
	tmp.Attestations = solid.NewDynamicListSSZ[*solid.Attestation](MaxAttestations)
	tmp.Deposits = solid.NewStaticListSSZ[*Deposit](MaxDeposits, 1240)
	tmp.VoluntaryExits = solid.NewStaticListSSZ[*SignedVoluntaryExit](MaxVoluntaryExits, 112)
	tmp.ExecutionChanges = solid.NewStaticListSSZ[*SignedBLSToExecutionChange](MaxExecutionChanges, 172)
	tmp.BlobKzgCommitments = solid.NewStaticListSSZ[*KZGCommitment](MaxBlobsCommittmentsPerBlock, 48)
	tmp.ExecutionPayload = NewEth1Block(b.Version, b.beaconCfg)

	if err := json.Unmarshal(buf, &tmp); err != nil {
		return err
	}
	b.RandaoReveal = tmp.RandaoReveal
	b.Eth1Data = tmp.Eth1Data
	b.Graffiti = tmp.Graffiti
	b.ProposerSlashings = tmp.ProposerSlashings
	b.AttesterSlashings = tmp.AttesterSlashings
	b.Attestations = tmp.Attestations
	b.Deposits = tmp.Deposits
	b.VoluntaryExits = tmp.VoluntaryExits
	b.SyncAggregate = tmp.SyncAggregate
	b.ExecutionPayload = tmp.ExecutionPayload
	b.ExecutionChanges = tmp.ExecutionChanges
	b.BlobKzgCommitments = tmp.BlobKzgCommitments
	return nil
}

func (b *BeaconBody) GetPayloadHeader() (*Eth1Header, error) {
	return b.ExecutionPayload.PayloadHeader()
}

func (b *BeaconBody) GetRandaoReveal() libcommon.Bytes96 {
	return b.RandaoReveal
}

func (b *BeaconBody) GetEth1Data() *Eth1Data {
	return b.Eth1Data
}

func (b *BeaconBody) GetSyncAggregate() *SyncAggregate {
	return b.SyncAggregate
}

func (b *BeaconBody) GetProposerSlashings() *solid.ListSSZ[*ProposerSlashing] {
	return b.ProposerSlashings
}

func (b *BeaconBody) GetAttesterSlashings() *solid.ListSSZ[*AttesterSlashing] {
	return b.AttesterSlashings
}

func (b *BeaconBody) GetAttestations() *solid.ListSSZ[*solid.Attestation] {
	return b.Attestations
}

func (b *BeaconBody) GetDeposits() *solid.ListSSZ[*Deposit] {
	return b.Deposits
}

func (b *BeaconBody) GetVoluntaryExits() *solid.ListSSZ[*SignedVoluntaryExit] {
	return b.VoluntaryExits
}

func (b *BeaconBody) GetBlobKzgCommitments() *solid.ListSSZ[*KZGCommitment] {
	return b.BlobKzgCommitments
}

func (b *BeaconBody) GetExecutionChanges() *solid.ListSSZ[*SignedBLSToExecutionChange] {
	return b.ExecutionChanges
}

type DenebBeaconBlock struct {
	Block     *BeaconBlock              `json:"block"`
	KZGProofs *solid.ListSSZ[*KZGProof] `json:"kzg_proofs"`
	Blobs     *solid.ListSSZ[*Blob]     `json:"blobs"`
}

func NewDenebBeaconBlock(beaconCfg *clparams.BeaconChainConfig) *DenebBeaconBlock {
	maxBlobsPerBlock := int(beaconCfg.MaxBlobsPerBlock)
	b := &DenebBeaconBlock{
		Block:     NewBeaconBlock(beaconCfg),
		KZGProofs: solid.NewStaticListSSZ[*KZGProof](maxBlobsPerBlock, BYTES_KZG_PROOF),
		Blobs:     solid.NewStaticListSSZ[*Blob](maxBlobsPerBlock, int(BYTES_PER_BLOB)),
	}
	return b
}

func (b *DenebBeaconBlock) EncodeSSZ(buf []byte) ([]byte, error) {
	return ssz2.MarshalSSZ(buf, b.Block, b.KZGProofs, b.Blobs)
}

func (b *DenebBeaconBlock) DecodeSSZ(buf []byte, version int) error {
	return ssz2.UnmarshalSSZ(buf, version, b.Block, b.KZGProofs, b.Blobs)
}

func (b *DenebBeaconBlock) EncodingSizeSSZ() int {
	return b.Block.EncodingSizeSSZ() + b.KZGProofs.EncodingSizeSSZ() + b.Blobs.EncodingSizeSSZ()
}

func (b *DenebBeaconBlock) Clone() clonable.Clonable {
	return &DenebBeaconBlock{}
}

func (b *DenebBeaconBlock) Static() bool {
	// it's variable size
	return false
}

func (b *DenebBeaconBlock) Version() clparams.StateVersion {
	return b.Block.Version()
}

func (b *DenebBeaconBlock) GetSlot() uint64 {
	return b.Block.GetSlot()
}

func (b *DenebBeaconBlock) GetProposerIndex() uint64 {
	return b.Block.GetProposerIndex()
}

func (b *DenebBeaconBlock) GetParentRoot() libcommon.Hash {
	return b.Block.GetParentRoot()
}

func (b *DenebBeaconBlock) GetBody() GenericBeaconBody {
	return b.Block.GetBody()
}

type DenebSignedBeaconBlock struct {
	SignedBlock *SignedBeaconBlock        `json:"signed_block"`
	KZGProofs   *solid.ListSSZ[*KZGProof] `json:"kzg_proofs"`
	Blobs       *solid.ListSSZ[*Blob]     `json:"blobs"`
}

func NewDenebSignedBeaconBlock(beaconCfg *clparams.BeaconChainConfig) *DenebSignedBeaconBlock {
	maxBlobsPerBlock := int(beaconCfg.MaxBlobsPerBlock)
	b := &DenebSignedBeaconBlock{
		SignedBlock: NewSignedBeaconBlock(beaconCfg),
		KZGProofs:   solid.NewStaticListSSZ[*KZGProof](maxBlobsPerBlock, BYTES_KZG_PROOF),
		Blobs:       solid.NewStaticListSSZ[*Blob](maxBlobsPerBlock, int(BYTES_PER_BLOB)),
	}
	return b
}

func (b *DenebSignedBeaconBlock) EncodeSSZ(buf []byte) ([]byte, error) {
	return ssz2.MarshalSSZ(buf, b.SignedBlock, b.KZGProofs, b.Blobs)
}

func (b *DenebSignedBeaconBlock) DecodeSSZ(buf []byte, version int) error {
	return ssz2.UnmarshalSSZ(buf, version, b.SignedBlock, b.KZGProofs, b.Blobs)
}

func (b *DenebSignedBeaconBlock) EncodingSizeSSZ() int {
	return b.SignedBlock.EncodingSizeSSZ() + b.KZGProofs.EncodingSizeSSZ() + b.Blobs.EncodingSizeSSZ()
}

func (b *DenebSignedBeaconBlock) Clone() clonable.Clonable {
	return &DenebSignedBeaconBlock{}
}

func (b *DenebSignedBeaconBlock) Static() bool {
	// it's variable size
	return false
}
