package cltypes

import (
	"fmt"

	libcommon "github.com/ledgerwatch/erigon-lib/common"

	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes/ssz_utils"
	"github.com/ledgerwatch/erigon/cl/merkle_tree"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/core/types"
)

// ETH1Block represents a block structure CL-side.
type Eth1Block struct {
	Header *types.Header
	// Transactions can be kept in bytes.
	Body *types.RawBody
}

func (b *Eth1Block) NumberU64() uint64 {
	return b.Header.Number.Uint64()
}

func (b *Eth1Block) Withdrawals() types.Withdrawals {
	return types.Withdrawals(b.Body.Withdrawals)
}

func (b *Eth1Block) EncodingSizeSSZ(version clparams.StateVersion) (size int) {
	size = 508

	if b.Header == nil {
		return
	}
	// Field (10) 'ExtraData'
	size += len(b.Header.Extra)
	// Field (13) 'Transactions'
	for _, tx := range b.Body.Transactions {
		size += 4
		size += len(tx)
	}

	if version >= clparams.CapellaVersion {
		size += len(b.Body.Withdrawals)*44 + 4
	}

	return
}

func (b *Eth1Block) DecodeSSZ(buf []byte, version clparams.StateVersion) error {
	if len(buf) < b.EncodingSizeSSZ(clparams.BellatrixVersion) {
		return ssz_utils.ErrLowBufferSize
	}
	b.Header = new(types.Header)

	pos := b.Header.DecodeHeaderMetadataForSSZ(buf)
	// Compute block SSZ offsets.
	extraDataOffset := ssz_utils.BaseExtraDataSSZOffsetBlock
	transactionsOffset := ssz_utils.DecodeOffset(buf[pos:])
	pos += 4
	var withdrawalOffset *uint32
	if version >= clparams.CapellaVersion {
		withdrawalOffset = new(uint32)
		*withdrawalOffset = ssz_utils.DecodeOffset(buf[pos:])
	}
	// Compute extra data.
	b.Header.Extra = common.CopyBytes(buf[extraDataOffset:transactionsOffset])
	if len(b.Header.Extra) > 32 {
		return fmt.Errorf("Decode(SSZ): Extra data field length should be less or equal to 32, got %d", len(b.Header.Extra))
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

	b.Body = new(types.RawBody)
	b.Body.Transactions = make([][]byte, length)
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
		b.Body.Transactions[txIdx] = transactionsBuffer[txOffset:txEndOffset]
		// Decode RLP and put it in the tx list.
		// Update parameters for next iteration
		txOffset = txEndOffset
		txIdx++
		length--
	}
	// Cache transaction ssz root.
	var err error
	b.Header.TxHashSSZ, err = merkle_tree.TransactionsListRoot(b.Body.Transactions)
	if err != nil {
		return err
	}
	// Cache transaction rlp hash.
	b.Header.TxHash = types.DeriveSha(types.BinaryTransactions(b.Body.Transactions))
	// If withdrawals are enabled, process them.
	if withdrawalOffset != nil {
		withdrawalsCount := (uint32(len(buf)) - *withdrawalOffset) / 44
		if withdrawalsCount > 16 {
			return fmt.Errorf("Decode(SSZ): Withdrawals field length should be less or equal to 16, got %d", withdrawalsCount)
		}
		b.Body.Withdrawals = make([]*types.Withdrawal, withdrawalsCount)
		for i := range b.Body.Withdrawals {
			b.Body.Withdrawals[i] = new(types.Withdrawal)
			b.Body.Withdrawals[i].DecodeSSZ(buf[(*withdrawalOffset)+uint32(i)*44:])
		}
		// Cache withdrawal root.
		b.Header.WithdrawalsHash = new(libcommon.Hash)
		withdrawalRoot, err := b.Withdrawals().HashSSZ(16)
		if err != nil {
			return err
		}
		*b.Header.WithdrawalsHash = withdrawalRoot
	}
	return nil
}

func (b *Eth1Block) EncodeSSZ(dst []byte, version clparams.StateVersion) ([]byte, error) {
	buf := dst
	var err error
	currentOffset := ssz_utils.BaseExtraDataSSZOffsetBlock

	if version >= clparams.CapellaVersion {
		currentOffset += 4
	}
	buf, err = b.Header.EncodeHeaderMetadataForSSZ(buf, currentOffset)
	if err != nil {
		return nil, err
	}
	currentOffset += len(b.Header.Extra)
	// use raw body for encoded txs and offsets.
	body := b.Body
	// Write transaction offset
	buf = append(buf, ssz_utils.OffsetSSZ(uint32(currentOffset))...)

	for _, tx := range body.Transactions {
		currentOffset += len(tx) + 4
	}
	// Write withdrawals offset if exist
	if version >= clparams.CapellaVersion {
		buf = append(buf, ssz_utils.OffsetSSZ(uint32(currentOffset))...)
	}
	// Sanity check for extra data then write it.
	if len(b.Header.Extra) > 32 {
		return nil, fmt.Errorf("Encode(SSZ): Extra data field length should be less or equal to 32, got %d", len(b.Header.Extra))
	}
	buf = append(buf, b.Header.Extra...)
	// Write all tx offsets
	txOffset := len(body.Transactions) * 4
	for _, tx := range body.Transactions {
		buf = append(buf, ssz_utils.OffsetSSZ(uint32(txOffset))...)
		txOffset += len(tx)
	}
	// Write all transactions
	for _, tx := range body.Transactions {
		buf = append(buf, tx...)
	}

	if version >= clparams.CapellaVersion {
		// Append all withdrawals SSZ
		for _, withdrawal := range body.Withdrawals {
			buf = append(buf, withdrawal.EncodeSSZ()...)
		}
	}
	return buf, nil
}

func (b *Eth1Block) HashSSZ(version clparams.StateVersion) ([32]byte, error) {
	var err error
	if b.Header.TxHashSSZ, err = merkle_tree.TransactionsListRoot(b.Body.Transactions); err != nil {
		return [32]byte{}, err
	}
	if version >= clparams.CapellaVersion {
		b.Header.WithdrawalsHash = new(libcommon.Hash)
		if *b.Header.WithdrawalsHash, err = types.Withdrawals(b.Body.Withdrawals).HashSSZ(16); err != nil {
			return [32]byte{}, err
		}
	} else {
		b.Header.WithdrawalsHash = nil
	}
	return b.Header.HashSSZ()
}

func (b *Eth1Block) Hash() libcommon.Hash {
	return b.Header.Hash()
}
