package cltypes

import (
	"bytes"

	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/utils"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/ethdb/cbor"
)

type SignedBeaconBlock struct {
	signature [96]byte
	block     *BeaconBlock
}

type BeaconBlock struct {
	slot          uint64
	proposerIndex uint64
	parentRoot    common.Hash
	stateRoot     common.Hash
	body          *BeaconBody
}

type BeaconBody struct {
	// A byte array used for randomness in the beacon chain
	randaoReveal [96]byte
	// Data related to the Ethereum 1.0 chain
	eth1Data *Eth1Data
	// A byte array used to customize validators' behavior
	graffiti []byte
	// A list of slashing events for validators who included invalid blocks in the chain
	proposerSlashings []*ProposerSlashing
	// A list of slashing events for validators who included invalid attestations in the chain
	attesterSlashings []*AttesterSlashing
	// A list of attestations included in the block
	attestations []*Attestation
	// A list of deposits made to the Ethereum 1.0 chain
	deposits []*Deposit
	// A list of validators who have voluntarily exited the beacon chain
	voluntaryExits []*SignedVoluntaryExit
	// A summary of the current state of the beacon chain
	syncAggregate *SyncAggregate
	// Data related to crosslink records and executing operations on the Ethereum 2.0 chain
	executionPayload *ExecutionPayload
	// The version of the beacon chain
	version clparams.StateVersion
}

// Getters

// Signature returns signature.
func (b *SignedBeaconBlock) Signature() [96]byte {
	return b.signature
}

// Signature returns signature.
func (b *SignedBeaconBlock) SetSignature(signature [96]byte) {
	b.signature = signature
}

// Block returns beacon block.
func (b *SignedBeaconBlock) Block() *BeaconBlock {
	return b.block
}

// Version returns beacon block version.
func (b *SignedBeaconBlock) Version() clparams.StateVersion {
	return b.block.body.version
}

// Slot returns the beacon block slot.
func (b *BeaconBlock) Slot() uint64 {
	return b.slot
}

// SetSlot set beacon block slot.
func (b *BeaconBlock) SetSlot(slot uint64) {
	b.slot = slot
}

// ProposerIndex returns the proposer index.
func (b *BeaconBlock) ProposerIndex() uint64 {
	return b.proposerIndex
}

// SetProposerIndex set beacon block proposer index.
func (b *BeaconBlock) SetProposerIndex(index uint64) {
	b.proposerIndex = index
}

// ParentRoot returns the parent root.
func (b *BeaconBlock) ParentRoot() common.Hash {
	return b.parentRoot
}

// SetParentRoot returns the parent root.
func (b *BeaconBlock) SetParentRoot(root common.Hash) {
	b.parentRoot = root
}

// StateRoot returns the state root.
func (b *BeaconBlock) StateRoot() common.Hash {
	return b.stateRoot
}

// SetStateRoot returns the parent root.
func (b *BeaconBlock) SetStateRoot(root common.Hash) {
	b.stateRoot = root
}

// Version returns beacon block version.
func (b *BeaconBlock) Version() clparams.StateVersion {
	return b.body.version
}

// Body returns beacon body.
func (b *BeaconBlock) Body() *BeaconBody {
	return b.body
}

// RandaoReveal returns the RandaoReveal field of the BeaconBody struct
func (b *BeaconBody) RandaoReveal() [96]byte {
	return b.randaoReveal
}

// Eth1Data returns the Eth1Data field of the BeaconBody struct
func (b *BeaconBody) Eth1Data() *Eth1Data {
	return b.eth1Data
}

// Graffiti returns the Graffiti field of the BeaconBody struct
func (b *BeaconBody) Graffiti() []byte {
	return b.graffiti
}

// ProposerSlashings returns the ProposerSlashings field of the BeaconBody struct
func (b *BeaconBody) ProposerSlashings() []*ProposerSlashing {
	return b.proposerSlashings
}

// AttesterSlashings returns the AttesterSlashings field of the BeaconBody struct
func (b *BeaconBody) AttesterSlashings() []*AttesterSlashing {
	return b.attesterSlashings
}

// Attestations returns the Attestations field of the BeaconBody struct
func (b *BeaconBody) Attestations() []*Attestation {
	return b.attestations
}

// SetAttestations sets the Attestations field of the BeaconBody struct
func (b *BeaconBody) SetAttestations(atts []*Attestation) {
	b.attestations = atts
}

// Deposits returns the Deposits field of the BeaconBody struct
func (b *BeaconBody) Deposits() []*Deposit {
	return b.deposits
}

// VoluntaryExits returns the VoluntaryExits field of the BeaconBody struct
func (b *BeaconBody) VoluntaryExits() []*SignedVoluntaryExit {
	return b.voluntaryExits
}

// SyncAggregate returns the SyncAggregate field of the BeaconBody struct
func (b *BeaconBody) SyncAggregate() *SyncAggregate {
	return b.syncAggregate
}

// SetExecutionPayload sets the ExecutionPayload field of the BeaconBody struct
func (b *BeaconBody) SetExecutionPayload(payload *ExecutionPayload) {
	b.executionPayload = payload
}

// ExecutionPayload returns the ExecutionPayload field of the BeaconBody struct
func (b *BeaconBody) ExecutionPayload() *ExecutionPayload {
	return b.executionPayload
}

// Version returns the Version field of the BeaconBody struct
func (b *BeaconBody) Version() clparams.StateVersion {
	return b.version
}

// SSZ methods

func (b *BeaconBody) GetUnderlyingSSZ() ObjectSSZ {
	switch b.version {
	case clparams.Phase0Version:
		return &BeaconBodyPhase0{
			RandaoReveal:      b.randaoReveal,
			Eth1Data:          b.eth1Data,
			Graffiti:          b.graffiti,
			ProposerSlashings: b.proposerSlashings,
			AttesterSlashings: b.attesterSlashings,
			Attestations:      b.attestations,
			Deposits:          b.deposits,
			VoluntaryExits:    b.voluntaryExits,
		}
	case clparams.AltairVersion:
		return &BeaconBodyAltair{
			RandaoReveal:      b.randaoReveal,
			Eth1Data:          b.eth1Data,
			Graffiti:          b.graffiti,
			ProposerSlashings: b.proposerSlashings,
			AttesterSlashings: b.attesterSlashings,
			Attestations:      b.attestations,
			Deposits:          b.deposits,
			VoluntaryExits:    b.voluntaryExits,
			SyncAggregate:     b.syncAggregate,
		}
	case clparams.BellatrixVersion:
		return &BeaconBodyBellatrix{
			RandaoReveal:      b.randaoReveal,
			Eth1Data:          b.eth1Data,
			Graffiti:          b.graffiti,
			ProposerSlashings: b.proposerSlashings,
			AttesterSlashings: b.attesterSlashings,
			Attestations:      b.attestations,
			Deposits:          b.deposits,
			VoluntaryExits:    b.voluntaryExits,
			SyncAggregate:     b.syncAggregate,
			ExecutionPayload:  b.executionPayload,
		}
	default:
		panic("unimplemented block")

	}
}

func (b *BeaconBody) MarshalSSZTo(dst []byte) ([]byte, error) {
	return b.GetUnderlyingSSZ().MarshalSSZTo(dst)
}

func (b *BeaconBody) MarshalSSZ() ([]byte, error) {
	return b.GetUnderlyingSSZ().MarshalSSZ()
}

func (b *BeaconBody) SizeSSZ() int {
	return b.GetUnderlyingSSZ().SizeSSZ()
}

func (b *BeaconBody) UnmarshalSSZ(buf []byte) error {
	return b.GetUnderlyingSSZ().UnmarshalSSZ(buf)
}

func (b *BeaconBody) HashTreeRoot() ([32]byte, error) {
	return b.GetUnderlyingSSZ().HashTreeRoot()
}

func (b *BeaconBlock) GetUnderlyingSSZ() ObjectSSZ {
	switch b.Version() {
	case clparams.Phase0Version:
		return &BeaconBlockPhase0{
			Slot:          b.slot,
			ProposerIndex: b.proposerIndex,
			ParentRoot:    b.parentRoot,
			StateRoot:     b.stateRoot,
			Body:          b.body.GetUnderlyingSSZ().(*BeaconBodyPhase0),
		}
	case clparams.AltairVersion:
		return &BeaconBlockAltair{
			Slot:          b.slot,
			ProposerIndex: b.proposerIndex,
			ParentRoot:    b.parentRoot,
			StateRoot:     b.stateRoot,
			Body:          b.body.GetUnderlyingSSZ().(*BeaconBodyAltair),
		}
	case clparams.BellatrixVersion:
		return &BeaconBlockBellatrix{
			Slot:          b.slot,
			ProposerIndex: b.proposerIndex,
			ParentRoot:    b.parentRoot,
			StateRoot:     b.stateRoot,
			Body:          b.body.GetUnderlyingSSZ().(*BeaconBodyBellatrix),
		}
	default:
		panic("unimplemented block")

	}
}

func (b *BeaconBlock) MarshalSSZTo(dst []byte) ([]byte, error) {
	return b.GetUnderlyingSSZ().MarshalSSZTo(dst)
}

func (b *BeaconBlock) MarshalSSZ() ([]byte, error) {
	return b.GetUnderlyingSSZ().MarshalSSZ()
}

func (b *BeaconBlock) SizeSSZ() int {
	return b.GetUnderlyingSSZ().SizeSSZ()
}

func (b *BeaconBlock) UnmarshalSSZ(buf []byte) error {
	return b.GetUnderlyingSSZ().UnmarshalSSZ(buf)
}

func (b *BeaconBlock) HashTreeRoot() ([32]byte, error) {
	return b.GetUnderlyingSSZ().HashTreeRoot()
}

func (b *SignedBeaconBlock) GetUnderlyingSSZ() ObjectSSZ {
	switch b.Version() {
	case clparams.Phase0Version:
		return &SignedBeaconBlockPhase0{
			Signature: b.signature,
			Block:     b.block.GetUnderlyingSSZ().(*BeaconBlockPhase0),
		}
	case clparams.AltairVersion:
		return &SignedBeaconBlockAltair{
			Signature: b.signature,
			Block:     b.block.GetUnderlyingSSZ().(*BeaconBlockAltair),
		}
	case clparams.BellatrixVersion:
		return &SignedBeaconBlockBellatrix{
			Signature: b.signature,
			Block:     b.block.GetUnderlyingSSZ().(*BeaconBlockBellatrix),
		}
	default:
		panic("unimplemented block")

	}
}

func (b *SignedBeaconBlock) MarshalSSZTo(dst []byte) ([]byte, error) {
	return b.GetUnderlyingSSZ().MarshalSSZTo(dst)
}

func (b *SignedBeaconBlock) MarshalSSZ() ([]byte, error) {
	return b.GetUnderlyingSSZ().MarshalSSZ()
}

func (b *SignedBeaconBlock) SizeSSZ() int {
	return b.GetUnderlyingSSZ().SizeSSZ()
}

func (b *SignedBeaconBlock) UnmarshalSSZ(buf []byte) error {
	return b.GetUnderlyingSSZ().UnmarshalSSZ(buf)
}

func (b *SignedBeaconBlock) HashTreeRoot() ([32]byte, error) {
	return b.GetUnderlyingSSZ().HashTreeRoot()
}

// EncodeForStorage encodes beacon block in snappy compressed CBOR format.
func (b *SignedBeaconBlock) EncodeForStorage() ([]byte, error) {
	storageObject := &BeaconBlockForStorage{
		Signature:         b.signature,
		Slot:              b.block.slot,
		ProposerIndex:     b.block.proposerIndex,
		ParentRoot:        b.block.parentRoot,
		StateRoot:         b.block.stateRoot,
		RandaoReveal:      b.block.body.randaoReveal,
		Eth1Data:          b.block.body.eth1Data,
		Graffiti:          b.block.body.graffiti,
		ProposerSlashings: b.block.body.proposerSlashings,
		AttesterSlashings: b.block.body.attesterSlashings,
		Deposits:          b.block.body.deposits,
		VoluntaryExits:    b.block.body.voluntaryExits,
		SyncAggregate:     b.block.body.syncAggregate,
		Version:           uint8(b.Version()),
	}
	if b.Version() >= clparams.BellatrixVersion {
		storageObject.Eth1Number = b.block.body.executionPayload.BlockNumber
		storageObject.Eth1BlockHash = b.block.body.executionPayload.BlockHash
	}
	var buffer bytes.Buffer
	if err := cbor.Marshal(&buffer, storageObject); err != nil {
		return nil, err
	}
	return utils.CompressSnappy(buffer.Bytes()), nil
}

// DecodeBeaconBlockForStorage decodes beacon block in snappy compressed CBOR format.
func DecodeBeaconBlockForStorage(buf []byte) (block *SignedBeaconBlock, eth1Number uint64, eth1Hash common.Hash, err error) {
	decompressedBuf, err := utils.DecompressSnappy(buf)
	if err != nil {
		return nil, 0, common.Hash{}, err
	}
	storageObject := &BeaconBlockForStorage{}
	var buffer bytes.Buffer
	if _, err := buffer.Write(decompressedBuf); err != nil {
		return nil, 0, common.Hash{}, err
	}
	if err := cbor.Unmarshal(storageObject, &buffer); err != nil {
		return nil, 0, common.Hash{}, err
	}

	return &SignedBeaconBlock{
		signature: storageObject.Signature,
		block: &BeaconBlock{
			slot:          storageObject.Slot,
			proposerIndex: storageObject.ProposerIndex,
			parentRoot:    storageObject.ParentRoot,
			stateRoot:     storageObject.StateRoot,
			body: &BeaconBody{
				randaoReveal:      storageObject.RandaoReveal,
				eth1Data:          storageObject.Eth1Data,
				graffiti:          storageObject.Graffiti,
				proposerSlashings: storageObject.ProposerSlashings,
				attesterSlashings: storageObject.AttesterSlashings,
				deposits:          storageObject.Deposits,
				voluntaryExits:    storageObject.VoluntaryExits,
				syncAggregate:     storageObject.SyncAggregate,
				version:           clparams.StateVersion(storageObject.Version),
			},
		},
	}, storageObject.Eth1Number, storageObject.Eth1BlockHash, nil
}

func NewSignedBeaconBlock(obj ObjectSSZ) *SignedBeaconBlock {
	switch block := obj.(type) {
	case *SignedBeaconBlockPhase0:
		return &SignedBeaconBlock{
			signature: block.Signature,
			block:     NewBeaconBlock(block.Block),
		}
	case *SignedBeaconBlockAltair:
		return &SignedBeaconBlock{
			signature: block.Signature,
			block:     NewBeaconBlock(block.Block),
		}
	case *SignedBeaconBlockBellatrix:
		return &SignedBeaconBlock{
			signature: block.Signature,
			block:     NewBeaconBlock(block.Block),
		}
	default:
		panic("unimplemented")
	}
}

func NewBeaconBlock(obj ObjectSSZ) *BeaconBlock {
	switch block := obj.(type) {
	case *BeaconBlockPhase0:
		return &BeaconBlock{
			slot:          block.Slot,
			proposerIndex: block.ProposerIndex,
			parentRoot:    block.ParentRoot,
			stateRoot:     block.StateRoot,
			body:          NewBeaconBody(block.Body),
		}

	case *BeaconBlockAltair:
		return &BeaconBlock{
			slot:          block.Slot,
			proposerIndex: block.ProposerIndex,
			parentRoot:    block.ParentRoot,
			stateRoot:     block.StateRoot,
			body:          NewBeaconBody(block.Body),
		}
	case *BeaconBlockBellatrix:
		return &BeaconBlock{
			slot:          block.Slot,
			proposerIndex: block.ProposerIndex,
			parentRoot:    block.ParentRoot,
			stateRoot:     block.StateRoot,
			body:          NewBeaconBody(block.Body),
		}
	default:
		panic("unimplemented!")
	}
}

func NewBeaconBody(obj ObjectSSZ) *BeaconBody {
	switch body := obj.(type) {
	case *BeaconBodyPhase0:
		return &BeaconBody{
			randaoReveal:      body.RandaoReveal,
			eth1Data:          body.Eth1Data,
			graffiti:          body.Graffiti,
			proposerSlashings: body.ProposerSlashings,
			attesterSlashings: body.AttesterSlashings,
			attestations:      body.Attestations,
			deposits:          body.Deposits,
			voluntaryExits:    body.VoluntaryExits,
			version:           clparams.Phase0Version,
		}
	case *BeaconBodyAltair:
		return &BeaconBody{
			randaoReveal:      body.RandaoReveal,
			eth1Data:          body.Eth1Data,
			graffiti:          body.Graffiti,
			proposerSlashings: body.ProposerSlashings,
			attesterSlashings: body.AttesterSlashings,
			attestations:      body.Attestations,
			deposits:          body.Deposits,
			voluntaryExits:    body.VoluntaryExits,
			syncAggregate:     body.SyncAggregate,
			version:           clparams.AltairVersion,
		}
	case *BeaconBodyBellatrix:
		return &BeaconBody{
			randaoReveal:      body.RandaoReveal,
			eth1Data:          body.Eth1Data,
			graffiti:          body.Graffiti,
			proposerSlashings: body.ProposerSlashings,
			attesterSlashings: body.AttesterSlashings,
			attestations:      body.Attestations,
			deposits:          body.Deposits,
			voluntaryExits:    body.VoluntaryExits,
			syncAggregate:     body.SyncAggregate,
			executionPayload:  body.ExecutionPayload,
			version:           clparams.BellatrixVersion,
		}

	default:
		panic("unimplemented")
	}
}
