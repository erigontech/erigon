package cltypes

import (
	"fmt"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/types/ssz"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	"github.com/ledgerwatch/erigon/cl/merkle_tree"
	ssz2 "github.com/ledgerwatch/erigon/cl/ssz"
	"github.com/ledgerwatch/erigon/core/types"
)

// ETH1Header represents the ethereum 1 header structure CL-side.
type Eth1Header struct {
	ParentHash    libcommon.Hash
	FeeRecipient  libcommon.Address
	StateRoot     libcommon.Hash
	ReceiptsRoot  libcommon.Hash
	LogsBloom     types.Bloom
	PrevRandao    libcommon.Hash
	BlockNumber   uint64
	GasLimit      uint64
	GasUsed       uint64
	Time          uint64
	Extra         *solid.ExtraData
	BaseFeePerGas [32]byte
	// Extra fields
	BlockHash        libcommon.Hash
	TransactionsRoot libcommon.Hash
	WithdrawalsRoot  libcommon.Hash
	BlobGasUsed      uint64
	ExcessBlobGas    uint64
	// internals
	version clparams.StateVersion
}

// NewEth1Header creates new header with given version.
func NewEth1Header(version clparams.StateVersion) *Eth1Header {
	return &Eth1Header{
		version: version,
		Extra:   solid.NewExtraData(),
	}
}

func (e *Eth1Header) Copy() *Eth1Header {
	copied := *e
	copied.Extra = solid.NewExtraData()
	copied.Extra.SetBytes(e.Extra.Bytes())
	return &copied
}

// Capella converts the header to capella version.
func (e *Eth1Header) Capella() {
	e.version = clparams.CapellaVersion
	e.WithdrawalsRoot = libcommon.Hash{}
}

// Capella converts the header to capella version.
func (e *Eth1Header) Deneb() {
	e.version = clparams.DenebVersion
	e.BlobGasUsed = 0
	e.ExcessBlobGas = 0
}

func (e *Eth1Header) IsZero() bool {
	if e.Extra == nil {
		e.Extra = solid.NewExtraData()
	}
	return e.ParentHash == libcommon.Hash{} && e.FeeRecipient == libcommon.Address{} && e.StateRoot == libcommon.Hash{} &&
		e.ReceiptsRoot == libcommon.Hash{} && e.LogsBloom == types.Bloom{} && e.PrevRandao == libcommon.Hash{} && e.BlockNumber == 0 &&
		e.GasLimit == 0 && e.GasUsed == 0 && e.Time == 0 && e.Extra.EncodingSizeSSZ() == 0 && e.BaseFeePerGas == [32]byte{} &&
		e.BlockHash == libcommon.Hash{} && e.TransactionsRoot == libcommon.Hash{} && e.WithdrawalsRoot == libcommon.Hash{} &&
		e.BlobGasUsed == 0 && e.ExcessBlobGas == 0
}

// EncodeSSZ encodes the header in SSZ format.
func (h *Eth1Header) EncodeSSZ(dst []byte) ([]byte, error) {
	return ssz2.MarshalSSZ(dst, h.getSchema()...)
}

// DecodeSSZ decodes given SSZ slice.
func (h *Eth1Header) DecodeSSZ(buf []byte, version int) error {
	h.version = clparams.StateVersion(version)
	if len(buf) < h.EncodingSizeSSZ() {
		return fmt.Errorf("[Eth1Header] err: %s", ssz.ErrLowBufferSize)
	}
	return ssz2.UnmarshalSSZ(buf, version, h.getSchema()...)
}

// EncodingSizeSSZ returns the ssz encoded size in bytes for the Header object
func (h *Eth1Header) EncodingSizeSSZ() int {
	size := 536

	if h.version >= clparams.CapellaVersion {
		size += 32
	}

	if h.version >= clparams.DenebVersion {
		size += 8 * 2 // BlobGasUsed + ExcessBlobGas
	}
	if h.Extra == nil {
		h.Extra = solid.NewExtraData()
	}

	return size + h.Extra.EncodingSizeSSZ()
}

// HashSSZ encodes the header in SSZ tree format.
func (h *Eth1Header) HashSSZ() ([32]byte, error) {
	return merkle_tree.HashTreeRoot(h.getSchema()...)
}

func (h *Eth1Header) getSchema() []interface{} {
	s := []interface{}{
		h.ParentHash[:], h.FeeRecipient[:], h.StateRoot[:], h.ReceiptsRoot[:], h.LogsBloom[:],
		h.PrevRandao[:], &h.BlockNumber, &h.GasLimit, &h.GasUsed, &h.Time, h.Extra, h.BaseFeePerGas[:], h.BlockHash[:], h.TransactionsRoot[:],
	}
	if h.version >= clparams.CapellaVersion {
		s = append(s, h.WithdrawalsRoot[:])
	}
	if h.version >= clparams.DenebVersion {
		s = append(s, &h.BlobGasUsed, &h.ExcessBlobGas)
	}
	return s
}

func (h *Eth1Header) Static() bool {
	return false
}
