package cltypes

import (
	"fmt"
	"math/big"

	libcommon "github.com/ledgerwatch/erigon-lib/common"

	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes/ssz_utils"
	"github.com/ledgerwatch/erigon/cl/merkle_tree"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/consensus/serenity"
	"github.com/ledgerwatch/erigon/core/types"
)

// ETH1Block represents a block structure CL-side.
type Eth1Block struct {
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
	BlockHash    libcommon.Hash
	Transactions [][]byte
	Withdrawals  types.Withdrawals
	// internals
	version clparams.StateVersion
}

// NewEth1Block creates a new Eth1Block.
func NewEth1Block(version clparams.StateVersion) *Eth1Block {
	return &Eth1Block{version: version}
}

// NewEth1BlockFromHeaderAndBody with given header/body.
func NewEth1BlockFromHeaderAndBody(header *types.Header, body *types.RawBody) *Eth1Block {
	baseFeeBytes := header.BaseFee.Bytes()
	for i, j := 0, len(baseFeeBytes)-1; i < j; i, j = i+1, j-1 {
		baseFeeBytes[i], baseFeeBytes[j] = baseFeeBytes[j], baseFeeBytes[i]
	}
	var baseFee32 [32]byte
	copy(baseFee32[:], baseFeeBytes)

	block := &Eth1Block{
		ParentHash:    header.ParentHash,
		FeeRecipient:  header.Coinbase,
		StateRoot:     header.Root,
		ReceiptsRoot:  header.ReceiptHash,
		LogsBloom:     header.Bloom,
		PrevRandao:    header.MixDigest,
		BlockNumber:   header.Number.Uint64(),
		GasLimit:      header.GasLimit,
		GasUsed:       header.GasUsed,
		Time:          header.Time,
		Extra:         header.Extra,
		BaseFeePerGas: baseFee32,
		BlockHash:     header.Hash(),
		Transactions:  body.Transactions,
		Withdrawals:   body.Withdrawals,
	}
	if header.WithdrawalsHash == nil {
		block.version = clparams.BellatrixVersion
	} else {
		block.version = clparams.CapellaVersion
	}
	return block
}

// PayloadHeader returns the equivalent ExecutionPayloadHeader object.
func (b *Eth1Block) PayloadHeader() (*Eth1Header, error) {
	var err error
	var transactionsRoot, withdrawalsRoot libcommon.Hash
	if transactionsRoot, err = merkle_tree.TransactionsListRoot(b.Transactions); err != nil {
		return nil, err
	}
	if b.version >= clparams.CapellaVersion {
		withdrawalsRoot, err = b.Withdrawals.HashSSZ(16)
		if err != nil {
			return nil, err
		}
	}

	return &Eth1Header{
		ParentHash:       b.ParentHash,
		FeeRecipient:     b.FeeRecipient,
		StateRoot:        b.StateRoot,
		ReceiptsRoot:     b.ReceiptsRoot,
		LogsBloom:        b.LogsBloom,
		PrevRandao:       b.PrevRandao,
		BlockNumber:      b.BlockNumber,
		GasLimit:         b.GasLimit,
		GasUsed:          b.GasUsed,
		Time:             b.Time,
		Extra:            b.Extra,
		BaseFeePerGas:    b.BaseFeePerGas,
		BlockHash:        b.BlockHash,
		TransactionsRoot: transactionsRoot,
		WithdrawalsRoot:  withdrawalsRoot,
		version:          b.version,
	}, nil
}

// Return minimum required buffer length to be an acceptable SSZ encoding.
func (b *Eth1Block) EncodingSizeSSZ() (size int) {
	size = 508
	// Field (10) 'ExtraData'
	size += len(b.Extra)
	// Field (13) 'Transactions'
	for _, tx := range b.Transactions {
		size += 4
		size += len(tx)
	}

	if b.version >= clparams.CapellaVersion {
		size += len(b.Withdrawals)*44 + 4
	}

	return
}

// Need a version
func (b *Eth1Block) DecodeSSZ(buf []byte) error {
	panic("stop")
}

// DecodeSSZWithVersion decodes the block in SSZ format.
func (b *Eth1Block) DecodeSSZWithVersion(buf []byte, version int) error {
	b.version = clparams.StateVersion(version)
	if len(buf) < b.EncodingSizeSSZ() {
		return ssz_utils.ErrLowBufferSize
	}
	// We can reuse code from eth1-header for partial decoding
	payloadHeader := Eth1Header{}
	pos, extraDataOffset := payloadHeader.decodeHeaderMetadataForSSZ(buf)
	// Set all header shared fields accordingly
	b.ParentHash = payloadHeader.ParentHash
	b.FeeRecipient = payloadHeader.FeeRecipient
	b.StateRoot = payloadHeader.StateRoot
	b.ReceiptsRoot = payloadHeader.ReceiptsRoot
	b.BlockHash = payloadHeader.BlockHash
	b.LogsBloom = payloadHeader.LogsBloom
	b.PrevRandao = payloadHeader.PrevRandao
	b.BlockNumber = payloadHeader.BlockNumber
	b.GasLimit = payloadHeader.GasLimit
	b.GasUsed = payloadHeader.GasUsed
	b.Time = payloadHeader.Time
	b.BaseFeePerGas = payloadHeader.BaseFeePerGas
	// Decode the rest
	transactionsOffset := ssz_utils.DecodeOffset(buf[pos:])
	pos += 4
	var withdrawalOffset *uint32
	if version >= int(clparams.CapellaVersion) {
		withdrawalOffset = new(uint32)
		*withdrawalOffset = ssz_utils.DecodeOffset(buf[pos:])
	}
	// Compute extra data.
	b.Extra = common.CopyBytes(buf[extraDataOffset:transactionsOffset])
	if len(b.Extra) > 32 {
		return fmt.Errorf("Decode(SSZ): Extra data field length should be less or equal to 32, got %d", len(b.Extra))
	}
	// Compute transactions
	var transactionsBuffer []byte
	if withdrawalOffset == nil {
		if len(transactionsBuffer) > len(buf) {
			return ssz_utils.ErrLowBufferSize
		}
		transactionsBuffer = buf[transactionsOffset:]
	} else {
		if len(transactionsBuffer) > int(*withdrawalOffset) || int(*withdrawalOffset) > len(buf) {
			return ssz_utils.ErrBadOffset
		}
		transactionsBuffer = buf[transactionsOffset:*withdrawalOffset]
	}

	length := uint32(0)
	transactionsPosition := 4
	var txOffset uint32
	if len(transactionsBuffer) == 0 {
		length = 0
	} else {
		if len(transactionsBuffer) < 4 {
			return ssz_utils.ErrLowBufferSize
		}
		txOffset = ssz_utils.DecodeOffset(transactionsBuffer)
		length = txOffset / 4
		// Retrieve tx length
		if txOffset%4 != 0 {
			return ssz_utils.ErrBadDynamicLength
		}
	}

	b.Transactions = make([][]byte, length)
	txIdx := 0
	// Loop through each transaction
	for length > 0 {
		var txEndOffset uint32
		if length == 1 {
			txEndOffset = uint32(len(transactionsBuffer))
		} else {
			txEndOffset = ssz_utils.DecodeOffset(transactionsBuffer[transactionsPosition:])
		}
		transactionsPosition += 4
		if txOffset > txEndOffset {
			return ssz_utils.ErrBadOffset
		}
		b.Transactions[txIdx] = transactionsBuffer[txOffset:txEndOffset]
		// Decode RLP and put it in the tx list.
		// Update parameters for next iteration
		txOffset = txEndOffset
		txIdx++
		length--
	}
	// If withdrawals are enabled, process them.
	if withdrawalOffset != nil {
		var err error
		b.Withdrawals, err = ssz_utils.DecodeStaticList[*types.Withdrawal](buf, *withdrawalOffset, uint32(len(buf)), 44, 16)
		if err != nil {
			return err
		}
	}
	return nil
}

// EncodeSSZ encodes the block in SSZ format.
func (b *Eth1Block) EncodeSSZ(dst []byte) ([]byte, error) {
	buf := dst
	var err error
	currentOffset := ssz_utils.BaseExtraDataSSZOffsetBlock

	if b.version >= clparams.CapellaVersion {
		currentOffset += 4
	}
	payloadHeader, err := b.PayloadHeader()
	if err != nil {
		return nil, err
	}
	buf, err = payloadHeader.encodeHeaderMetadataForSSZ(buf, currentOffset)
	if err != nil {
		return nil, err
	}
	currentOffset += len(b.Extra)
	// Write transaction offset
	buf = append(buf, ssz_utils.OffsetSSZ(uint32(currentOffset))...)

	for _, tx := range b.Transactions {
		currentOffset += len(tx) + 4
	}
	// Write withdrawals offset if exist
	if b.version >= clparams.CapellaVersion {
		buf = append(buf, ssz_utils.OffsetSSZ(uint32(currentOffset))...)
	}
	// Sanity check for extra data then write it.
	if len(b.Extra) > 32 {
		return nil, fmt.Errorf("Encode(SSZ): Extra data field length should be less or equal to 32, got %d", len(b.Extra))
	}
	buf = append(buf, b.Extra...)
	// Write all tx offsets
	txOffset := len(b.Transactions) * 4
	for _, tx := range b.Transactions {
		buf = append(buf, ssz_utils.OffsetSSZ(uint32(txOffset))...)
		txOffset += len(tx)
	}
	// Write all transactions
	for _, tx := range b.Transactions {
		buf = append(buf, tx...)
	}

	// Append all withdrawals SSZ
	for _, withdrawal := range b.Withdrawals {
		buf = append(buf, withdrawal.EncodeSSZ()...)
	}

	return buf, nil
}

// HashSSZ calculates the SSZ hash of the Eth1Block's payload header.
func (b *Eth1Block) HashSSZ(version clparams.StateVersion) ([32]byte, error) {
	// Get the payload header.
	header, err := b.PayloadHeader()
	if err != nil {
		return [32]byte{}, err
	}

	// Calculate the SSZ hash of the header and return it.
	return header.HashSSZ()
}

// RlpHeader returns the equivalent types.Header struct with RLP-based fields.
func (b *Eth1Block) RlpHeader() (*types.Header, error) {
	// Reverse the order of the bytes in the BaseFeePerGas array and convert it to a big integer.
	reversedBaseFeePerGas := b.BaseFeePerGas[:]
	for i, j := 0, len(reversedBaseFeePerGas)-1; i < j; i, j = i+1, j-1 {
		reversedBaseFeePerGas[i], reversedBaseFeePerGas[j] = reversedBaseFeePerGas[j], reversedBaseFeePerGas[i]
	}
	baseFee := new(big.Int).SetBytes(reversedBaseFeePerGas)

	// If the block version is Capella or later, calculate the withdrawals hash.
	var withdrawalsHash *libcommon.Hash
	if b.version >= clparams.CapellaVersion {
		withdrawalsHash = new(libcommon.Hash)
		*withdrawalsHash = types.DeriveSha(b.Withdrawals)
	}

	header := &types.Header{
		ParentHash:      b.ParentHash,
		UncleHash:       types.EmptyUncleHash,
		Coinbase:        b.FeeRecipient,
		Root:            b.StateRoot,
		TxHash:          types.DeriveSha(types.BinaryTransactions(b.Transactions)),
		ReceiptHash:     b.ReceiptsRoot,
		Bloom:           b.LogsBloom,
		Difficulty:      serenity.SerenityDifficulty,
		Number:          big.NewInt(int64(b.BlockNumber)),
		GasLimit:        b.GasLimit,
		GasUsed:         b.GasUsed,
		Time:            b.Time,
		Extra:           b.Extra,
		MixDigest:       b.PrevRandao,
		Nonce:           serenity.SerenityNonce,
		BaseFee:         baseFee,
		WithdrawalsHash: withdrawalsHash,
	}

	// If the header hash does not match the block hash, return an error.
	if header.Hash() != b.BlockHash {
		return nil, fmt.Errorf("cannot derive rlp header: mismatching hash")
	}

	return header, nil
}

// Body returns the equivalent raw body (only eth1 body section).
func (b *Eth1Block) Body() *types.RawBody {
	return &types.RawBody{
		Transactions: b.Transactions,
		Withdrawals:  b.Withdrawals,
	}
}
