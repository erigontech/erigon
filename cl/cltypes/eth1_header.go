package cltypes

import (
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes/ssz"
	"github.com/ledgerwatch/erigon/cl/merkle_tree"
	"github.com/ledgerwatch/erigon/common"
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
	Extra         []byte
	BaseFeePerGas [32]byte
	// Extra fields
	BlockHash        libcommon.Hash
	TransactionsRoot libcommon.Hash
	WithdrawalsRoot  libcommon.Hash
	// internals
	version clparams.StateVersion
}

// NewEth1Header creates new header with given version.
func NewEth1Header(version clparams.StateVersion) *Eth1Header {
	return &Eth1Header{version: version}
}

func (e *Eth1Header) Copy() *Eth1Header {
	copied := *e
	copied.Extra = libcommon.Copy(e.Extra)
	return &copied
}

// Capella converts the header to capella version.
func (e *Eth1Header) Capella() {
	e.version = clparams.CapellaVersion
	e.WithdrawalsRoot = libcommon.Hash{}
}

func (e *Eth1Header) IsZero() bool {
	return e.ParentHash == libcommon.Hash{} && e.FeeRecipient == libcommon.Address{} && e.StateRoot == libcommon.Hash{} &&
		e.ReceiptsRoot == libcommon.Hash{} && e.LogsBloom == types.Bloom{} && e.PrevRandao == libcommon.Hash{} && e.BlockNumber == 0 &&
		e.GasLimit == 0 && e.GasUsed == 0 && e.Time == 0 && len(e.Extra) == 0 && e.BaseFeePerGas == [32]byte{} && e.BlockHash == libcommon.Hash{} && e.TransactionsRoot == libcommon.Hash{}
}

// Encodes header data partially. used to not dupicate code across Eth1Block and Eth1Header.
func (h *Eth1Header) encodeHeaderMetadataForSSZ(dst []byte, extraDataOffset int) ([]byte, error) {
	buf := dst
	buf = append(buf, h.ParentHash[:]...)
	buf = append(buf, h.FeeRecipient[:]...)
	buf = append(buf, h.StateRoot[:]...)
	buf = append(buf, h.ReceiptsRoot[:]...)
	buf = append(buf, h.LogsBloom[:]...)
	buf = append(buf, h.PrevRandao[:]...)
	buf = append(buf, ssz.Uint64SSZ(h.BlockNumber)...)
	buf = append(buf, ssz.Uint64SSZ(h.GasLimit)...)
	buf = append(buf, ssz.Uint64SSZ(h.GasUsed)...)
	buf = append(buf, ssz.Uint64SSZ(h.Time)...)
	buf = append(buf, ssz.OffsetSSZ(uint32(extraDataOffset))...)

	// Add Base Fee
	buf = append(buf, h.BaseFeePerGas[:]...)
	buf = append(buf, h.BlockHash[:]...)
	return buf, nil
}

// EncodeSSZ encodes the header in SSZ format.
func (h *Eth1Header) EncodeSSZ(dst []byte) (buf []byte, err error) {
	buf = dst
	offset := ssz.BaseExtraDataSSZOffsetHeader

	if h.version >= clparams.CapellaVersion {
		offset += 32
	}

	buf, err = h.encodeHeaderMetadataForSSZ(buf, offset)
	if err != nil {
		return nil, err
	}
	buf = append(buf, h.TransactionsRoot[:]...)

	if h.version >= clparams.CapellaVersion {
		buf = append(buf, h.WithdrawalsRoot[:]...)
	}

	buf = append(buf, h.Extra...)
	return
}

// Decodes header data partially. used to not dupicate code across Eth1Block and Eth1Header.
func (h *Eth1Header) decodeHeaderMetadataForSSZ(buf []byte) (pos int, extraDataOffset int) {
	copy(h.ParentHash[:], buf)
	pos = len(h.ParentHash)

	copy(h.FeeRecipient[:], buf[pos:])
	pos += len(h.FeeRecipient)

	copy(h.StateRoot[:], buf[pos:])
	pos += len(h.StateRoot)

	copy(h.ReceiptsRoot[:], buf[pos:])
	pos += len(h.ReceiptsRoot)

	h.LogsBloom.SetBytes(buf[pos : pos+types.BloomByteLength])
	pos += types.BloomByteLength

	copy(h.PrevRandao[:], buf[pos:])
	pos += len(h.PrevRandao)

	h.BlockNumber = ssz.UnmarshalUint64SSZ(buf[pos:])
	h.GasLimit = ssz.UnmarshalUint64SSZ(buf[pos+8:])
	h.GasUsed = ssz.UnmarshalUint64SSZ(buf[pos+16:])
	h.Time = ssz.UnmarshalUint64SSZ(buf[pos+24:])
	pos += 32
	extraDataOffset = int(ssz.DecodeOffset(buf[pos:]))
	pos += 4
	// Add Base Fee
	copy(h.BaseFeePerGas[:], buf[pos:])
	pos += 32
	copy(h.BlockHash[:], buf[pos:])
	pos += 32
	return
}

// DecodeSSZWithVersion decodes given SSZ slice.
func (h *Eth1Header) DecodeSSZWithVersion(buf []byte, version int) error {
	h.version = clparams.StateVersion(version)
	if len(buf) < h.EncodingSizeSSZ() {
		return ssz.ErrLowBufferSize
	}
	pos, _ := h.decodeHeaderMetadataForSSZ(buf)
	copy(h.TransactionsRoot[:], buf[pos:])
	pos += len(h.TransactionsRoot)

	if h.version >= clparams.CapellaVersion {
		copy(h.WithdrawalsRoot[:], buf[pos:])
		pos += len(h.WithdrawalsRoot)
	}
	h.Extra = common.CopyBytes(buf[pos:])
	return nil
}

// EncodingSizeSSZ returns the ssz encoded size in bytes for the Header object
func (h *Eth1Header) EncodingSizeSSZ() int {
	size := 536

	if h.version >= clparams.CapellaVersion {
		size += 32
	}

	return size + len(h.Extra)
}

// HashSSZ encodes the header in SSZ tree format.
func (h *Eth1Header) HashSSZ() ([32]byte, error) {
	// Compute coinbase leaf
	var coinbase32 [32]byte
	copy(coinbase32[:], h.FeeRecipient[:])
	// Compute Bloom leaf
	bloomLeaf, err := merkle_tree.ArraysRoot([][32]byte{
		libcommon.BytesToHash(h.LogsBloom[:32]),
		libcommon.BytesToHash(h.LogsBloom[32:64]),
		libcommon.BytesToHash(h.LogsBloom[64:96]),
		libcommon.BytesToHash(h.LogsBloom[96:128]),
		libcommon.BytesToHash(h.LogsBloom[128:160]),
		libcommon.BytesToHash(h.LogsBloom[160:192]),
		libcommon.BytesToHash(h.LogsBloom[192:224]),
		libcommon.BytesToHash(h.LogsBloom[224:]),
	}, 8)
	if err != nil {
		return [32]byte{}, err
	}
	// Compute extra data leaf
	var extraLeaf libcommon.Hash

	var baseExtraLeaf [32]byte
	copy(baseExtraLeaf[:], h.Extra)
	extraLeaf, err = merkle_tree.ArraysRoot([][32]byte{
		baseExtraLeaf,
		merkle_tree.Uint64Root(uint64(len(h.Extra)))}, 2)
	if err != nil {
		return [32]byte{}, err
	}

	leaves := [][32]byte{
		h.ParentHash,
		coinbase32,
		h.StateRoot,
		h.ReceiptsRoot,
		bloomLeaf,
		h.PrevRandao,
		merkle_tree.Uint64Root(h.BlockNumber),
		merkle_tree.Uint64Root(h.GasLimit),
		merkle_tree.Uint64Root(h.GasUsed),
		merkle_tree.Uint64Root(h.Time),
		extraLeaf,
		h.BaseFeePerGas,
		h.BlockHash,
		h.TransactionsRoot,
	}
	if h.version >= clparams.CapellaVersion {
		leaves = append(leaves, h.WithdrawalsRoot)
	}
	return merkle_tree.ArraysRoot(leaves, 16)
}
