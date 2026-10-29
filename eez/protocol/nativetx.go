// Copyright 2026 The Erigon Authors
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

package protocol

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/rlp"
)

// EIP-2718 types of the unsigned transactions of eez-rollupX v0.1: a native
// cross-chain call to EEZL2, and an L1 history update.
const (
	NativeTxType  byte = 0x45
	HistoryTxType byte = 0x46
)

const (
	// NativeTxGasCap bounds the gas of a native transaction: the EIP-7825 cap.
	NativeTxGasCap = params.MaxTxnGasLimit
	// HistoryTxGasCap bounds the gas of a history update.
	HistoryTxGasCap uint64 = 2_000_000
	// MaxHistoryBlocks bounds the L1 blocks one history update imports.
	MaxHistoryBlocks = 64

	historyTxVersion  uint64 = 1
	sourceEntryDomain        = "SOURCE_ENTRY_V1"
)

var (
	// NativeSystemAddress is the implicit sender of native transactions and
	// history updates.
	NativeSystemAddress = common.HexToAddress("0xfffffffffffffffffffffffffffffffffffffffe")
	// HistoryAddress is the L1 history predeploy, the target of history
	// updates.
	HistoryAddress = common.HexToAddress("0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee0077")

	importL1BlocksSelector = crypto.Keccak256([]byte("importL1Blocks((uint64,bytes32)[])"))[:4]
)

var (
	// ErrNativeTx reports bytes that are not a valid type-0x45 transaction.
	ErrNativeTx = errors.New("invalid native transaction")
	// ErrHistoryTx reports bytes that are not a valid type-0x46 transaction.
	ErrHistoryTx = errors.New("invalid history transaction")
)

// NativeTx is the body of a type-0x45 transaction,
// rlp([chainId, sourceHash, gasLimit, to, value, input]). Derivation fixes
// every field: the source hash identifies its cross-chain group, the gas
// limit is the gas its block has left at its position.
type NativeTx struct {
	ChainID    uint64
	SourceHash common.Hash
	GasLimit   uint64
	To         common.Address
	Value      uint256.Int
	Input      []byte
}

// Validate rejects a target other than EEZL2 and a gas limit above the cap.
func (tx *NativeTx) Validate() error {
	switch {
	case tx.To != RollupXEEZL2Address:
		return fmt.Errorf("%w: target %v is not EEZL2", ErrNativeTx, tx.To)
	case tx.GasLimit > NativeTxGasCap:
		return fmt.Errorf("%w: gas limit %d above %d", ErrNativeTx, tx.GasLimit, NativeTxGasCap)
	}
	return nil
}

// MarshalBinary returns the EIP-2718 encoding 0x45 ‖ rlp(body).
func (tx *NativeTx) MarshalBinary() ([]byte, error) {
	if err := tx.Validate(); err != nil {
		return nil, err
	}
	body, err := rlp.EncodeToBytes(tx)
	if err != nil {
		return nil, err
	}
	return append([]byte{NativeTxType}, body...), nil
}

// Hash returns the transaction hash, keccak256 of the EIP-2718 encoding.
func (tx *NativeTx) Hash() (common.Hash, error) {
	enc, err := tx.MarshalBinary()
	if err != nil {
		return common.Hash{}, err
	}
	return crypto.Keccak256Hash(enc), nil
}

// DecodeNativeTx decodes a type-0x45 transaction: canonical RLP of exactly
// the six body fields, nothing after it.
func DecodeNativeTx(enc []byte) (*NativeTx, error) {
	if len(enc) == 0 || enc[0] != NativeTxType {
		return nil, fmt.Errorf("%w: not type 0x45", ErrNativeTx)
	}
	return DecodeNativeTxBody(enc[1:])
}

// DecodeNativeTxBody decodes the RLP list that follows the type byte.
func DecodeNativeTxBody(body []byte) (*NativeTx, error) {
	var tx NativeTx
	if err := rlp.DecodeBytes(body, &tx); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrNativeTx, err)
	}
	if err := tx.Validate(); err != nil {
		return nil, err
	}
	return &tx, nil
}

// NativeTxGasLimit is the gas limit of a native transaction after
// transactions that used cumulativeGasUsed of a block's gasLimit: what the
// block has left, at most NativeTxGasCap.
func NativeTxGasLimit(gasLimit, cumulativeGasUsed uint64) uint64 {
	if cumulativeGasUsed >= gasLimit {
		return 0
	}
	return min(gasLimit-cumulativeGasUsed, NativeTxGasCap)
}

// SourceHash identifies the complete cross-chain group at index of a Sync
// block whose L1 parent is parentHash:
// keccak256(abi.encode("SOURCE_ENTRY_V1", uint256 chainId, bytes32 parentHash, uint64 index)).
func SourceHash(chainID uint64, parentHash common.Hash, index uint64) common.Hash {
	var enc [192]byte
	enc[31] = 0x80 // offset of the string
	binary.BigEndian.PutUint64(enc[56:64], chainID)
	copy(enc[64:96], parentHash[:])
	binary.BigEndian.PutUint64(enc[120:128], index)
	enc[159] = byte(len(sourceEntryDomain))
	copy(enc[160:], sourceEntryDomain)
	return crypto.Keccak256Hash(enc[:])
}

// HistoryBlock is an L1 block a history update imports.
type HistoryBlock struct {
	Number uint64
	Hash   common.Hash
}

// HistoryTx is the body of a type-0x46 transaction,
// rlp([1, chainId, [[number, hash], …]]): 1 to MaxHistoryBlocks L1 blocks in
// increasing order. Execution derives its call from them.
type HistoryTx struct {
	ChainID uint64
	Blocks  []HistoryBlock
}

type historyTxRLP struct {
	Version uint64
	ChainID uint64
	Blocks  []HistoryBlock
}

// Validate rejects an empty or oversized block list, block zero, a zero hash
// and numbers that do not increase.
func (tx *HistoryTx) Validate() error {
	if len(tx.Blocks) == 0 || len(tx.Blocks) > MaxHistoryBlocks {
		return fmt.Errorf("%w: %d blocks", ErrHistoryTx, len(tx.Blocks))
	}
	for i, b := range tx.Blocks {
		switch {
		case b.Number == 0 || b.Hash == (common.Hash{}):
			return fmt.Errorf("%w: block %d: number %d, hash %x", ErrHistoryTx, i, b.Number, b.Hash)
		case i > 0 && b.Number <= tx.Blocks[i-1].Number:
			return fmt.Errorf("%w: block %d: number %d after %d", ErrHistoryTx, i, b.Number, tx.Blocks[i-1].Number)
		}
	}
	return nil
}

// MarshalBinary returns the EIP-2718 encoding 0x46 ‖ rlp(body).
func (tx *HistoryTx) MarshalBinary() ([]byte, error) {
	if err := tx.Validate(); err != nil {
		return nil, err
	}
	body, err := rlp.EncodeToBytes(&historyTxRLP{Version: historyTxVersion, ChainID: tx.ChainID, Blocks: tx.Blocks})
	if err != nil {
		return nil, err
	}
	return append([]byte{HistoryTxType}, body...), nil
}

// Hash returns the transaction hash, keccak256 of the EIP-2718 encoding.
func (tx *HistoryTx) Hash() (common.Hash, error) {
	enc, err := tx.MarshalBinary()
	if err != nil {
		return common.Hash{}, err
	}
	return crypto.Keccak256Hash(enc), nil
}

// DecodeHistoryTx decodes a type-0x46 transaction: canonical RLP of version
// 1, the chain id and the block list, nothing after it.
func DecodeHistoryTx(enc []byte) (*HistoryTx, error) {
	if len(enc) == 0 || enc[0] != HistoryTxType {
		return nil, fmt.Errorf("%w: not type 0x46", ErrHistoryTx)
	}
	return DecodeHistoryTxBody(enc[1:])
}

// DecodeHistoryTxBody decodes the RLP list that follows the type byte.
func DecodeHistoryTxBody(body []byte) (*HistoryTx, error) {
	var dec historyTxRLP
	if err := rlp.DecodeBytes(body, &dec); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrHistoryTx, err)
	}
	if dec.Version != historyTxVersion {
		return nil, fmt.Errorf("%w: version %d", ErrHistoryTx, dec.Version)
	}
	tx := &HistoryTx{ChainID: dec.ChainID, Blocks: dec.Blocks}
	if err := tx.Validate(); err != nil {
		return nil, err
	}
	return tx, nil
}

// Calldata returns the call a history update makes to HistoryAddress:
// importL1Blocks((uint64,bytes32)[]) with its blocks.
func (tx *HistoryTx) Calldata() []byte {
	out := make([]byte, 4+64+64*len(tx.Blocks))
	copy(out, importL1BlocksSelector)
	out[4+31] = 0x20 // offset of the array
	binary.BigEndian.PutUint64(out[4+56:4+64], uint64(len(tx.Blocks)))
	for i, b := range tx.Blocks {
		o := 4 + 64 + 64*i
		binary.BigEndian.PutUint64(out[o+24:o+32], b.Number)
		copy(out[o+32:o+64], b.Hash[:])
	}
	return out
}
