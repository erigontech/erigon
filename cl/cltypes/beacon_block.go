package cltypes

import (
	"bytes"

	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes/ssz_utils"
	"github.com/ledgerwatch/erigon/cl/utils"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/ethdb/cbor"
)

type SignedBeaconBlock struct {
	Signature [96]byte
	Block     *BeaconBlock
}

type BeaconBlock struct {
	Slot          uint64
	ProposerIndex uint64
	ParentRoot    common.Hash
	StateRoot     common.Hash
	Body          *BeaconBody
}

type BeaconBody struct {
	// A byte array used for randomness in the beacon chain
	RandaoReveal [96]byte
	// Data related to the Ethereum 1.0 chain
	Eth1Data *Eth1Data
	// A byte array used to customize validators' behavior
	Graffiti []byte
	// A list of slashing events for validators who included invalid blocks in the chain
	ProposerSlashings []*ProposerSlashing
	// A list of slashing events for validators who included invalid attestations in the chain
	AttesterSlashings []*AttesterSlashing
	// A list of attestations included in the block
	Attestations []*Attestation
	// A list of deposits made to the Ethereum 1.0 chain
	Deposits []*Deposit
	// A list of validators who have voluntarily exited the beacon chain
	VoluntaryExits []*SignedVoluntaryExit
	// A summary of the current state of the beacon chain
	SyncAggregate *SyncAggregate
	// Data related to crosslink records and executing operations on the Ethereum 2.0 chain
	ExecutionPayload *Eth1Block
	// The version of the beacon chain
	version clparams.StateVersion
}

// Getters

// Version returns beacon block version.
func (b *SignedBeaconBlock) Version() clparams.StateVersion {
	return b.Block.Body.version
}

// Version returns beacon block version.
func (b *BeaconBlock) Version() clparams.StateVersion {
	return b.Body.version
}

// Version returns the Version field of the BeaconBody struct
func (b *BeaconBody) Version() clparams.StateVersion {
	return b.version
}

// SSZ methods

func (b *BeaconBody) GetUnderlyingSSZ() ssz_utils.ObjectSSZ {
	switch b.version {
	case clparams.Phase0Version:
		return &BeaconBodyPhase0{
			RandaoReveal:      b.RandaoReveal,
			Eth1Data:          b.Eth1Data,
			Graffiti:          b.Graffiti,
			ProposerSlashings: b.ProposerSlashings,
			AttesterSlashings: b.AttesterSlashings,
			Attestations:      b.Attestations,
			Deposits:          b.Deposits,
			VoluntaryExits:    b.VoluntaryExits,
		}
	case clparams.AltairVersion:
		return &BeaconBodyAltair{
			RandaoReveal:      b.RandaoReveal,
			Eth1Data:          b.Eth1Data,
			Graffiti:          b.Graffiti,
			ProposerSlashings: b.ProposerSlashings,
			AttesterSlashings: b.AttesterSlashings,
			Attestations:      b.Attestations,
			Deposits:          b.Deposits,
			VoluntaryExits:    b.VoluntaryExits,
			SyncAggregate:     b.SyncAggregate,
		}
	case clparams.BellatrixVersion:
		return &BeaconBodyBellatrix{
			RandaoReveal:      b.RandaoReveal,
			Eth1Data:          b.Eth1Data,
			Graffiti:          b.Graffiti,
			ProposerSlashings: b.ProposerSlashings,
			AttesterSlashings: b.AttesterSlashings,
			Attestations:      b.Attestations,
			Deposits:          b.Deposits,
			VoluntaryExits:    b.VoluntaryExits,
			SyncAggregate:     b.SyncAggregate,
			ExecutionPayload:  b.ExecutionPayload,
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

func (b *BeaconBlock) GetUnderlyingSSZ() ssz_utils.ObjectSSZ {
	switch b.Version() {
	case clparams.Phase0Version:
		return &BeaconBlockPhase0{
			Slot:          b.Slot,
			ProposerIndex: b.ProposerIndex,
			ParentRoot:    b.ParentRoot,
			StateRoot:     b.StateRoot,
			Body:          b.Body.GetUnderlyingSSZ().(*BeaconBodyPhase0),
		}
	case clparams.AltairVersion:
		return &BeaconBlockAltair{
			Slot:          b.Slot,
			ProposerIndex: b.ProposerIndex,
			ParentRoot:    b.ParentRoot,
			StateRoot:     b.StateRoot,
			Body:          b.Body.GetUnderlyingSSZ().(*BeaconBodyAltair),
		}
	case clparams.BellatrixVersion:
		return &BeaconBlockBellatrix{
			Slot:          b.Slot,
			ProposerIndex: b.ProposerIndex,
			ParentRoot:    b.ParentRoot,
			StateRoot:     b.StateRoot,
			Body:          b.Body.GetUnderlyingSSZ().(*BeaconBodyBellatrix),
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

func (b *SignedBeaconBlock) GetUnderlyingSSZ() ssz_utils.ObjectSSZ {
	switch b.Version() {
	case clparams.Phase0Version:
		return &SignedBeaconBlockPhase0{
			Signature: b.Signature,
			Block:     b.Block.GetUnderlyingSSZ().(*BeaconBlockPhase0),
		}
	case clparams.AltairVersion:
		return &SignedBeaconBlockAltair{
			Signature: b.Signature,
			Block:     b.Block.GetUnderlyingSSZ().(*BeaconBlockAltair),
		}
	case clparams.BellatrixVersion:
		return &SignedBeaconBlockBellatrix{
			Signature: b.Signature,
			Block:     b.Block.GetUnderlyingSSZ().(*BeaconBlockBellatrix),
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
	var (
		blockRoot common.Hash
		err       error
	)
	if blockRoot, err = b.Block.HashTreeRoot(); err != nil {
		return nil, err
	}
	storageObject := &BeaconBlockForStorage{
		Signature:         b.Signature,
		Slot:              b.Block.Slot,
		ProposerIndex:     b.Block.ProposerIndex,
		ParentRoot:        b.Block.ParentRoot,
		StateRoot:         b.Block.StateRoot,
		RandaoReveal:      b.Block.Body.RandaoReveal,
		Eth1Data:          b.Block.Body.Eth1Data,
		Graffiti:          b.Block.Body.Graffiti,
		ProposerSlashings: b.Block.Body.ProposerSlashings,
		AttesterSlashings: b.Block.Body.AttesterSlashings,
		Deposits:          b.Block.Body.Deposits,
		VoluntaryExits:    b.Block.Body.VoluntaryExits,
		SyncAggregate:     b.Block.Body.SyncAggregate,
		Version:           uint8(b.Version()),
		Eth2BlockRoot:     blockRoot,
	}

	if b.Version() >= clparams.BellatrixVersion {
		eth1Block := b.Block.Body.ExecutionPayload
		storageObject.Eth1Number = eth1Block.NumberU64()
		storageObject.Eth1BlockHash = eth1Block.Header.BlockHashCL
	}
	var buffer bytes.Buffer
	if err := cbor.Marshal(&buffer, storageObject); err != nil {
		return nil, err
	}
	return utils.CompressSnappy(buffer.Bytes()), nil
}

// DecodeBeaconBlockForStorage decodes beacon block in snappy compressed CBOR format.
func DecodeBeaconBlockForStorage(buf []byte) (block *SignedBeaconBlock, eth1Number uint64, eth1Hash common.Hash, eth2Hash common.Hash, err error) {
	decompressedBuf, err := utils.DecompressSnappy(buf)
	if err != nil {
		return nil, 0, common.Hash{}, common.Hash{}, err
	}
	storageObject := &BeaconBlockForStorage{}
	var buffer bytes.Buffer
	if _, err := buffer.Write(decompressedBuf); err != nil {
		return nil, 0, common.Hash{}, common.Hash{}, err
	}
	if err := cbor.Unmarshal(storageObject, &buffer); err != nil {
		return nil, 0, common.Hash{}, common.Hash{}, err
	}

	return &SignedBeaconBlock{
		Signature: storageObject.Signature,
		Block: &BeaconBlock{
			Slot:          storageObject.Slot,
			ProposerIndex: storageObject.ProposerIndex,
			ParentRoot:    storageObject.ParentRoot,
			StateRoot:     storageObject.StateRoot,
			Body: &BeaconBody{
				RandaoReveal:      storageObject.RandaoReveal,
				Eth1Data:          storageObject.Eth1Data,
				Graffiti:          storageObject.Graffiti,
				ProposerSlashings: storageObject.ProposerSlashings,
				AttesterSlashings: storageObject.AttesterSlashings,
				Deposits:          storageObject.Deposits,
				VoluntaryExits:    storageObject.VoluntaryExits,
				SyncAggregate:     storageObject.SyncAggregate,
				version:           clparams.StateVersion(storageObject.Version),
			},
		},
	}, storageObject.Eth1Number, storageObject.Eth1BlockHash, storageObject.Eth2BlockRoot, nil
}

func NewSignedBeaconBlock(obj interface{}) *SignedBeaconBlock {
	switch block := obj.(type) {
	case *SignedBeaconBlockPhase0:
		return &SignedBeaconBlock{
			Signature: block.Signature,
			Block:     NewBeaconBlock(block.Block),
		}
	case *SignedBeaconBlockAltair:
		return &SignedBeaconBlock{
			Signature: block.Signature,
			Block:     NewBeaconBlock(block.Block),
		}
	case *SignedBeaconBlockBellatrix:
		return &SignedBeaconBlock{
			Signature: block.Signature,
			Block:     NewBeaconBlock(block.Block),
		}
	default:
		panic("unimplemented")
	}
}

func NewBeaconBlock(obj ssz_utils.ObjectSSZ) *BeaconBlock {
	switch block := obj.(type) {
	case *BeaconBlockPhase0:
		return &BeaconBlock{
			Slot:          block.Slot,
			ProposerIndex: block.ProposerIndex,
			ParentRoot:    block.ParentRoot,
			StateRoot:     block.StateRoot,
			Body:          NewBeaconBody(block.Body),
		}

	case *BeaconBlockAltair:
		return &BeaconBlock{
			Slot:          block.Slot,
			ProposerIndex: block.ProposerIndex,
			ParentRoot:    block.ParentRoot,
			StateRoot:     block.StateRoot,
			Body:          NewBeaconBody(block.Body),
		}
	case *BeaconBlockBellatrix:
		return &BeaconBlock{
			Slot:          block.Slot,
			ProposerIndex: block.ProposerIndex,
			ParentRoot:    block.ParentRoot,
			StateRoot:     block.StateRoot,
			Body:          NewBeaconBody(block.Body),
		}
	default:
		panic("unimplemented!")
	}
}

func NewBeaconBody(obj ssz_utils.ObjectSSZ) *BeaconBody {
	switch body := obj.(type) {
	case *BeaconBodyPhase0:
		return &BeaconBody{
			RandaoReveal:      body.RandaoReveal,
			Eth1Data:          body.Eth1Data,
			Graffiti:          body.Graffiti,
			ProposerSlashings: body.ProposerSlashings,
			AttesterSlashings: body.AttesterSlashings,
			Attestations:      body.Attestations,
			Deposits:          body.Deposits,
			VoluntaryExits:    body.VoluntaryExits,
			version:           clparams.Phase0Version,
		}
	case *BeaconBodyAltair:
		return &BeaconBody{
			RandaoReveal:      body.RandaoReveal,
			Eth1Data:          body.Eth1Data,
			Graffiti:          body.Graffiti,
			ProposerSlashings: body.ProposerSlashings,
			AttesterSlashings: body.AttesterSlashings,
			Attestations:      body.Attestations,
			Deposits:          body.Deposits,
			VoluntaryExits:    body.VoluntaryExits,
			SyncAggregate:     body.SyncAggregate,
			version:           clparams.AltairVersion,
		}
	case *BeaconBodyBellatrix:
		return &BeaconBody{
			RandaoReveal:      body.RandaoReveal,
			Eth1Data:          body.Eth1Data,
			Graffiti:          body.Graffiti,
			ProposerSlashings: body.ProposerSlashings,
			AttesterSlashings: body.AttesterSlashings,
			Attestations:      body.Attestations,
			Deposits:          body.Deposits,
			VoluntaryExits:    body.VoluntaryExits,
			SyncAggregate:     body.SyncAggregate,
			ExecutionPayload:  body.ExecutionPayload,
			version:           clparams.BellatrixVersion,
		}

	default:
		panic("unimplemented")
	}
}
