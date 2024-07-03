package cltypes

import (
	"errors"
	"math/big"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/types/clonable"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	ssz2 "github.com/ledgerwatch/erigon/cl/ssz"
)

var (
	_ ssz2.SizedObjectSSZ = (*DenebBeaconBlock)(nil)
)

type DenebBeaconBlock struct {
	Block     *BeaconBlock              `json:"block"`
	KZGProofs *solid.ListSSZ[*KZGProof] `json:"kzg_proofs"`
	Blobs     *solid.ListSSZ[*Blob]     `json:"blobs"`
}

func NewDenebBeaconBlock(maxBlobsPerBlock int) *DenebBeaconBlock {
	return &DenebBeaconBlock{
		KZGProofs: solid.NewStaticListSSZ[*KZGProof](maxBlobsPerBlock, BYTES_KZG_PROOF),
		Blobs:     solid.NewStaticListSSZ[*Blob](maxBlobsPerBlock, int(BYTES_PER_BLOB)),
	}
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

// BlindOrExecutionBeaconBlock is a union type that can be either a BlindedBeaconBlock or a BeaconBlock, depending on the context.
// It's a intermediate type used in the block production process.
type BlindOrExecutionBeaconBlock struct {
	Slot          uint64         `json:"-"`
	ProposerIndex uint64         `json:"-"`
	ParentRoot    libcommon.Hash `json:"-"`
	StateRoot     libcommon.Hash `json:"-"`
	// BeaconBody or BlindedBeaconBody with json tag "body"
	BeaconBody        *BeaconBody        `json:"-"`
	BlindedBeaconBody *BlindedBeaconBody `json:"-"`
	ExecutionValue    *big.Int           `json:"-"`
	Cfg               *clparams.BeaconChainConfig
}

func (b *BlindOrExecutionBeaconBlock) ToBlinded() *BlindedBeaconBlock {
	return &BlindedBeaconBlock{
		Slot:          b.Slot,
		ProposerIndex: b.ProposerIndex,
		ParentRoot:    b.ParentRoot,
		StateRoot:     b.StateRoot,
		Body:          b.BlindedBeaconBody,
	}
}

func (b *BlindOrExecutionBeaconBlock) ToExecution() *DenebBeaconBlock {
	beaconBlock := &BeaconBlock{
		Slot:          b.Slot,
		ProposerIndex: b.ProposerIndex,
		ParentRoot:    b.ParentRoot,
		StateRoot:     b.StateRoot,
		Body:          b.BeaconBody,
	}
	DenebBeaconBlock := NewDenebBeaconBlock(int(b.Cfg.MaxBlobsPerBlock))
	DenebBeaconBlock.Block = beaconBlock
	return DenebBeaconBlock
}

func (b *BlindOrExecutionBeaconBlock) MarshalJSON() ([]byte, error) {
	return []byte{}, errors.New("json marshal unsupported for BlindOrExecutionBeaconBlock")
}

func (b *BlindOrExecutionBeaconBlock) UnmarshalJSON(data []byte) error {
	return errors.New("json unmarshal unsupported for BlindOrExecutionBeaconBlock")
}

func (b *BlindOrExecutionBeaconBlock) IsBlinded() bool {
	return b.BlindedBeaconBody != nil
}

func (b *BlindOrExecutionBeaconBlock) GetExecutionValue() *big.Int {
	if b.ExecutionValue == nil {
		return big.NewInt(0)
	}
	return b.ExecutionValue
}

func (b *BlindOrExecutionBeaconBlock) Version() clparams.StateVersion {
	if b.BeaconBody != nil {
		return b.BeaconBody.Version
	}
	if b.BlindedBeaconBody != nil {
		return b.BlindedBeaconBody.Version
	}
	return clparams.Phase0Version
}
