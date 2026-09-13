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

package sszblocks

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/merkle_tree"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol/misc"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/types"
)

const executionPayloadFieldCount = 18

type GasAmounts struct {
	Regular uint64
	Blob    uint64
}

func (g GasAmounts) HashSSZ() ([32]byte, error) {
	return merkle_tree.ProgressiveContainerRootAll(g.Regular, g.Blob)
}

type BlobFeesPerGas struct {
	Regular uint256.Int
	Blob    uint256.Int
}

func (f BlobFeesPerGas) HashSSZ() ([32]byte, error) {
	regular, err := f.Regular.MarshalSSZ()
	if err != nil {
		return [32]byte{}, err
	}
	blob, err := f.Blob.MarshalSSZ()
	if err != nil {
		return [32]byte{}, err
	}
	return merkle_tree.ProgressiveContainerRootAll(regular, blob)
}

type ExecutionPayload struct {
	ParentHash            common.Hash
	Miner                 common.Address
	StateRoot             common.Hash
	Transactions          [][]byte
	Receipts              [][]byte
	Number                uint64
	GasLimits             GasAmounts
	GasUsed               GasAmounts
	Timestamp             uint64
	ExtraData             []byte
	MixHash               common.Hash
	BaseFeesPerGas        BlobFeesPerGas
	Withdrawals           [][]byte
	ExcessGas             GasAmounts
	ParentBeaconBlockRoot common.Hash
	RequestsHash          common.Hash
	BlockAccessList       []byte
	SlotNumber            uint64
}

type BlockAdapterConfig struct {
	ChainConfig  *chain.Config
	BeaconConfig *clparams.BeaconChainConfig

	// RegularExcessGas is explicit because the legacy Erigon header has no equivalent field.
	RegularExcessGas *uint64
}

func ExecutionPayloadFromBlock(input *types.BlockWithReceipts, cfg BlockAdapterConfig) (*ExecutionPayload, error) {
	if input == nil {
		return nil, errors.New("nil block with receipts")
	}
	if input.Block == nil {
		return nil, errors.New("nil block")
	}
	if cfg.ChainConfig == nil {
		return nil, errors.New("nil chain config")
	}
	if cfg.BeaconConfig == nil {
		return nil, errors.New("nil beacon config")
	}
	if cfg.RegularExcessGas == nil {
		return nil, errors.New("regular excess gas is required")
	}

	block := input.Block
	header := block.Header()
	if !header.Number.IsUint64() {
		return nil, errors.New("block number does not fit uint64")
	}
	if header.BaseFee == nil {
		return nil, errors.New("header is missing base fee")
	}
	if header.BlobGasUsed == nil {
		return nil, errors.New("header is missing blob gas used")
	}
	if header.ExcessBlobGas == nil {
		return nil, errors.New("header is missing excess blob gas")
	}
	if header.ParentBeaconBlockRoot == nil {
		return nil, errors.New("header is missing parent beacon block root")
	}
	if header.SlotNumber == nil {
		return nil, errors.New("header is missing slot number")
	}
	if header.BlockAccessListHash == nil {
		return nil, errors.New("header is missing block access list hash")
	}
	if header.RequestsHash == nil {
		return nil, errors.New("header is missing execution requests hash")
	}
	if header.WithdrawalsHash == nil {
		return nil, errors.New("header is missing withdrawals hash")
	}
	if len(header.Extra) > 32 {
		return nil, fmt.Errorf("extra data length %d exceeds limit 32", len(header.Extra))
	}
	if block.Withdrawals() == nil {
		return nil, errors.New("block is missing withdrawals")
	}
	if input.Requests == nil {
		return nil, errors.New("block is missing execution requests")
	}
	if len(block.Transactions()) != len(input.Receipts) {
		return nil, fmt.Errorf("transaction and receipt counts differ: %d != %d", len(block.Transactions()), len(input.Receipts))
	}
	requestsCommitment := input.Requests.Hash()
	if *requestsCommitment != *header.RequestsHash {
		return nil, fmt.Errorf("execution requests hash mismatch: header %x, computed %x", *header.RequestsHash, *requestsCommitment)
	}

	transactions, err := encodeTransactions(block.Transactions())
	if err != nil {
		return nil, err
	}
	transactionCommitment := types.DeriveSha(block.Transactions())
	if transactionCommitment != header.TxHash {
		return nil, fmt.Errorf("transaction hash mismatch: header %x, computed %x", header.TxHash, transactionCommitment)
	}
	receipts, err := encodeReceipts(input.Receipts)
	if err != nil {
		return nil, err
	}
	receiptCommitment := types.DeriveSha(input.Receipts)
	if receiptCommitment != header.ReceiptHash {
		return nil, fmt.Errorf("receipt hash mismatch: header %x, computed %x", header.ReceiptHash, receiptCommitment)
	}
	withdrawals, err := encodeWithdrawals(block.Withdrawals())
	if err != nil {
		return nil, err
	}
	withdrawalsCommitment := types.DeriveSha(block.Withdrawals())
	if withdrawalsCommitment != *header.WithdrawalsHash {
		return nil, fmt.Errorf("withdrawals hash mismatch: header %x, computed %x", *header.WithdrawalsHash, withdrawalsCommitment)
	}
	requestsHash, err := executionRequestsRoot(input.Requests, cfg.BeaconConfig)
	if err != nil {
		return nil, err
	}
	blockAccessList, err := blockAccessListBytes(input)
	if err != nil {
		return nil, err
	}
	if got := crypto.Keccak256Hash(blockAccessList); got != *header.BlockAccessListHash {
		return nil, fmt.Errorf("block access list hash mismatch: header %x, computed %x", *header.BlockAccessListHash, got)
	}

	blobConfig := cfg.ChainConfig.GetBlobConfig(header.Time)
	if blobConfig == nil {
		return nil, errors.New("chain config has no blob schedule for block timestamp")
	}
	blobBaseFee, err := misc.GetBlobGasPrice(cfg.ChainConfig, *header.ExcessBlobGas, header.Time)
	if err != nil {
		return nil, fmt.Errorf("compute blob base fee: %w", err)
	}

	return &ExecutionPayload{
		ParentHash:   header.ParentHash,
		Miner:        header.Coinbase,
		StateRoot:    header.Root,
		Transactions: transactions,
		Receipts:     receipts,
		Number:       header.Number.Uint64(),
		GasLimits: GasAmounts{
			Regular: header.GasLimit,
			Blob:    cfg.ChainConfig.GetMaxBlobGasPerBlock(header.Time),
		},
		GasUsed: GasAmounts{
			Regular: header.GasUsed,
			Blob:    *header.BlobGasUsed,
		},
		Timestamp:      header.Time,
		ExtraData:      bytes.Clone(header.Extra),
		MixHash:        header.MixDigest,
		BaseFeesPerGas: BlobFeesPerGas{Regular: *header.BaseFee, Blob: blobBaseFee},
		Withdrawals:    withdrawals,
		ExcessGas: GasAmounts{
			Regular: *cfg.RegularExcessGas,
			Blob:    *header.ExcessBlobGas,
		},
		ParentBeaconBlockRoot: *header.ParentBeaconBlockRoot,
		RequestsHash:          common.Hash(requestsHash),
		BlockAccessList:       blockAccessList,
		SlotNumber:            *header.SlotNumber,
	}, nil
}

func (p *ExecutionPayload) HashSSZ() ([32]byte, error) {
	roots, err := p.FieldRoots()
	if err != nil {
		return [32]byte{}, err
	}
	schema := make([]any, len(roots))
	for i := range roots {
		schema[i] = roots[i][:]
	}
	return merkle_tree.ProgressiveContainerRootAll(schema...)
}

func (p *ExecutionPayload) FieldRoots() ([executionPayloadFieldCount][32]byte, error) {
	var roots [executionPayloadFieldCount][32]byte
	if p == nil {
		return roots, errors.New("nil execution payload")
	}
	if len(p.ExtraData) > 32 {
		return roots, fmt.Errorf("extra data length %d exceeds limit 32", len(p.ExtraData))
	}

	transactionsRoot, err := progressiveByteListListRoot(p.Transactions)
	if err != nil {
		return roots, fmt.Errorf("hash transactions: %w", err)
	}
	receiptsRoot, err := progressiveByteListListRoot(p.Receipts)
	if err != nil {
		return roots, fmt.Errorf("hash receipts: %w", err)
	}
	gasLimitsRoot, err := p.GasLimits.HashSSZ()
	if err != nil {
		return roots, fmt.Errorf("hash gas limits: %w", err)
	}
	gasUsedRoot, err := p.GasUsed.HashSSZ()
	if err != nil {
		return roots, fmt.Errorf("hash gas used: %w", err)
	}
	extraData := solid.NewExtraData()
	extraData.SetBytes(p.ExtraData)
	extraDataRoot, err := extraData.HashSSZ()
	if err != nil {
		return roots, fmt.Errorf("hash extra data: %w", err)
	}
	baseFeesRoot, err := p.BaseFeesPerGas.HashSSZ()
	if err != nil {
		return roots, fmt.Errorf("hash base fees per gas: %w", err)
	}
	withdrawalsRoot, err := progressiveByteListListRoot(p.Withdrawals)
	if err != nil {
		return roots, fmt.Errorf("hash withdrawals: %w", err)
	}
	excessGasRoot, err := p.ExcessGas.HashSSZ()
	if err != nil {
		return roots, fmt.Errorf("hash excess gas: %w", err)
	}
	blockAccessListRoot, err := merkle_tree.ProgressiveByteListRoot(p.BlockAccessList)
	if err != nil {
		return roots, fmt.Errorf("hash block access list: %w", err)
	}

	roots[0] = bytesRoot(p.ParentHash[:])
	roots[1] = bytesRoot(p.Miner[:])
	roots[2] = bytesRoot(p.StateRoot[:])
	roots[3] = transactionsRoot
	roots[4] = receiptsRoot
	roots[5] = uint64Root(p.Number)
	roots[6] = gasLimitsRoot
	roots[7] = gasUsedRoot
	roots[8] = uint64Root(p.Timestamp)
	roots[9] = extraDataRoot
	roots[10] = bytesRoot(p.MixHash[:])
	roots[11] = baseFeesRoot
	roots[12] = withdrawalsRoot
	roots[13] = excessGasRoot
	roots[14] = bytesRoot(p.ParentBeaconBlockRoot[:])
	roots[15] = bytesRoot(p.RequestsHash[:])
	roots[16] = blockAccessListRoot
	roots[17] = uint64Root(p.SlotNumber)
	return roots, nil
}

func encodeTransactions(transactions types.Transactions) ([][]byte, error) {
	for i, transaction := range transactions {
		if transaction == nil {
			return nil, fmt.Errorf("transaction %d is nil", i)
		}
	}
	encoded, err := types.MarshalTransactionsBinary(transactions)
	if err != nil {
		return nil, fmt.Errorf("encode transactions: %w", err)
	}
	return encoded, nil
}

func encodeReceipts(receipts types.Receipts) ([][]byte, error) {
	encoded := make([][]byte, len(receipts))
	for i, receipt := range receipts {
		if receipt == nil {
			return nil, fmt.Errorf("receipt %d is nil", i)
		}
		var err error
		encoded[i], err = receipt.MarshalBinary()
		if err != nil {
			return nil, fmt.Errorf("encode receipt %d: %w", i, err)
		}
	}
	return encoded, nil
}

func encodeWithdrawals(withdrawals types.Withdrawals) ([][]byte, error) {
	encoded := make([][]byte, len(withdrawals))
	for i, withdrawal := range withdrawals {
		if withdrawal == nil {
			return nil, fmt.Errorf("withdrawal %d is nil", i)
		}
		var err error
		encoded[i], err = rlp.EncodeToBytes(withdrawal)
		if err != nil {
			return nil, fmt.Errorf("encode withdrawal %d: %w", i, err)
		}
	}
	return encoded, nil
}

func executionRequestsRoot(requests types.FlatRequests, cfg *clparams.BeaconChainConfig) ([32]byte, error) {
	encoded := make([]hexutil.Bytes, len(requests))
	for i := range requests {
		encoded[i] = requests[i].Encode()
	}
	decoded, err := cltypes.DecodeExecutionRequestsList(cfg, encoded, clparams.GloasVersion)
	if err != nil {
		return [32]byte{}, fmt.Errorf("decode execution requests: %w", err)
	}
	root, err := decoded.HashSSZ()
	if err != nil {
		return [32]byte{}, fmt.Errorf("hash execution requests: %w", err)
	}
	return root, nil
}

func blockAccessListBytes(input *types.BlockWithReceipts) ([]byte, error) {
	var fromSidecar []byte
	if sidecar := input.Block.BlockAccessListSidecar(); sidecar != nil {
		var err error
		fromSidecar, err = sidecar.Bytes()
		if err != nil {
			return nil, fmt.Errorf("encode block access list sidecar: %w", err)
		}
	}
	if input.BlockAccessList == nil {
		if fromSidecar == nil {
			return nil, errors.New("block is missing block access list")
		}
		return bytes.Clone(fromSidecar), nil
	}
	fromResult, err := types.EncodeBlockAccessListBytes(input.BlockAccessList)
	if err != nil {
		return nil, fmt.Errorf("encode block access list: %w", err)
	}
	if fromSidecar != nil && !bytes.Equal(fromSidecar, fromResult) {
		return nil, errors.New("block access list result differs from block sidecar")
	}
	return fromResult, nil
}

func progressiveByteListListRoot(items [][]byte) ([32]byte, error) {
	roots := make([][32]byte, len(items))
	for i := range items {
		root, err := merkle_tree.ProgressiveByteListRoot(items[i])
		if err != nil {
			return [32]byte{}, err
		}
		roots[i] = root
	}
	return merkle_tree.ProgressiveListRoot(roots, uint64(len(roots)))
}

func bytesRoot(data []byte) [32]byte {
	var root [32]byte
	copy(root[:], data)
	return root
}

func uint64Root(value uint64) [32]byte {
	var root [32]byte
	binary.LittleEndian.PutUint64(root[:], value)
	return root
}
