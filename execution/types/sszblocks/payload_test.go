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
	"crypto/sha256"
	"encoding/binary"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/types"
)

const eip7807FixtureRevision = "ethereum/EIPs EIPS/eip-7807.md blob 3f2dc991ac65153842fd9b894c04ba590d58d3e6, retrieved 2026-09-07"

func TestExecutionPayloadFromBlockAndHashSSZ(t *testing.T) {
	t.Parallel()

	block, cfg := payloadFixture(t)
	payload, err := ExecutionPayloadFromBlock(block, cfg)
	require.NoError(t, err)

	require.Equal(t, block.Block.ParentHash(), payload.ParentHash)
	require.Equal(t, block.Block.Coinbase(), payload.Miner)
	require.Equal(t, block.Block.Root(), payload.StateRoot)
	require.Len(t, payload.Transactions, 2)
	require.Equal(t, byte(types.DynamicFeeTxType), payload.Transactions[1][0])
	require.Len(t, payload.Receipts, 2)
	require.Len(t, payload.Withdrawals, 1)
	require.Equal(t, []byte{0xc0}, payload.BlockAccessList)
	require.Equal(t, uint64(2*params.GasPerBlob), payload.GasLimits.Blob)
	require.Equal(t, uint64(7), payload.ExcessGas.Regular)
	require.Equal(t, uint64(4096), payload.ExcessGas.Blob)
	require.Equal(t, uint64(12345), payload.SlotNumber)

	root, err := payload.HashSSZ()
	require.NoError(t, err)
	require.Equal(t, referencePayloadRoot(t, payload), root)
	require.Equal(t,
		common.HexToHash("0x012a1eff0c6a7e4a22fb624721866d10ddb94283c678e9e0117d9fddffc43a73"),
		common.Hash(root),
		eip7807FixtureRevision,
	)

	fieldRoots, err := payload.FieldRoots()
	require.NoError(t, err)
	require.NotEqual(t, block.Block.ReceiptHash(), common.Hash(fieldRoots[4]))
	require.NotEqual(t, *block.Block.RequestsHash(), payload.RequestsHash)
}

func TestExecutionPayloadFromBlockRejectsMissingRequests(t *testing.T) {
	t.Parallel()

	block, cfg := payloadFixture(t)
	block.Requests = nil
	_, err := ExecutionPayloadFromBlock(block, cfg)
	require.EqualError(t, err, "block is missing execution requests")
}

func TestExecutionPayloadFromBlockRejectsMissingSlotNumber(t *testing.T) {
	t.Parallel()

	block, cfg := payloadFixture(t)
	header := block.Block.Header()
	header.SlotNumber = nil
	block.Block = types.NewBlock(
		header,
		block.Block.Transactions(),
		nil,
		block.Receipts,
		block.Block.Withdrawals(),
		block.Block.BlockAccessListSidecar(),
	)
	_, err := ExecutionPayloadFromBlock(block, cfg)
	require.EqualError(t, err, "header is missing slot number")
}

func TestExecutionPayloadFromBlockRejectsReceiptCountMismatch(t *testing.T) {
	t.Parallel()

	block, cfg := payloadFixture(t)
	block.Receipts = block.Receipts[:1]
	_, err := ExecutionPayloadFromBlock(block, cfg)
	require.EqualError(t, err, "transaction and receipt counts differ: 2 != 1")
}

func TestExecutionPayloadFromBlockRequiresRegularExcessGas(t *testing.T) {
	t.Parallel()

	block, cfg := payloadFixture(t)
	cfg.RegularExcessGas = nil
	_, err := ExecutionPayloadFromBlock(block, cfg)
	require.EqualError(t, err, "regular excess gas is required")
}

func TestExecutionPayloadFromBlockUsesBlockAccessListSidecar(t *testing.T) {
	t.Parallel()

	block, cfg := payloadFixture(t)
	block.BlockAccessList = nil
	payload, err := ExecutionPayloadFromBlock(block, cfg)
	require.NoError(t, err)
	require.Equal(t, []byte{0xc0}, payload.BlockAccessList)
}

func TestExecutionPayloadFromBlockRejectsMismatchedBlockAccessLists(t *testing.T) {
	t.Parallel()

	block, cfg := payloadFixture(t)
	block.BlockAccessList = types.BlockAccessList{{}}
	_, err := ExecutionPayloadFromBlock(block, cfg)
	require.EqualError(t, err, "block access list result differs from block sidecar")
}

func TestExecutionPayloadFromBlockRejectsDuplicateRequestType(t *testing.T) {
	t.Parallel()

	block, cfg := payloadFixture(t)
	block.Requests = append(block.Requests, block.Requests[0])
	_, err := ExecutionPayloadFromBlock(block, cfg)
	require.EqualError(t, err, "decode execution requests: execution request type 1 is not strictly ascending")
}

func TestExecutionPayloadHashSSZRejectsOversizedExtraData(t *testing.T) {
	t.Parallel()

	payload := &ExecutionPayload{ExtraData: make([]byte, 33)}
	_, err := payload.HashSSZ()
	require.EqualError(t, err, "extra data length 33 exceeds limit 32")
}

func payloadFixture(t *testing.T) (*types.BlockWithReceipts, BlockAdapterConfig) {
	t.Helper()

	to := common.HexToAddress("0x1111111111111111111111111111111111111111")
	legacyTx := types.NewTransaction(1, to, uint256.NewInt(2), 21_000, uint256.NewInt(3), []byte{0xaa})
	dynamicTx := &types.DynamicFeeTransaction{
		CommonTx: types.CommonTx{
			Nonce:    2,
			GasLimit: 50_000,
			To:       &to,
			Value:    *uint256.NewInt(4),
			Data:     []byte{0xbb, 0xcc},
		},
		ChainID: *uint256.NewInt(1),
		TipCap:  *uint256.NewInt(5),
		FeeCap:  *uint256.NewInt(6),
	}
	receipts := types.Receipts{
		&types.Receipt{Type: types.LegacyTxType, Status: types.ReceiptStatusSuccessful, CumulativeGasUsed: 21_000},
		&types.Receipt{Type: types.DynamicFeeTxType, Status: types.ReceiptStatusFailed, CumulativeGasUsed: 42_000},
	}
	withdrawals := types.Withdrawals{
		&types.Withdrawal{Index: 1, Validator: 2, Address: to, Amount: 3},
	}

	beaconCfg := clparams.MainnetBeaconConfig
	request := &solid.WithdrawalRequest{SourceAddress: to, Amount: 8}
	requestData, err := request.EncodeSSZ(nil)
	require.NoError(t, err)
	requests := types.FlatRequests{{Type: byte(beaconCfg.WithdrawalRequestType), RequestData: requestData}}

	bal := make(types.BlockAccessList, 0)
	sidecar := types.NewBlockAccessListSidecar(bal)
	balHash, err := sidecar.Hash()
	require.NoError(t, err)

	blobGasUsed := uint64(params.GasPerBlob)
	excessBlobGas := uint64(4096)
	parentBeaconBlockRoot := common.HexToHash("0x4444444444444444444444444444444444444444444444444444444444444444")
	slotNumber := uint64(12345)
	header := &types.Header{
		ParentHash:            common.HexToHash("0x1111111111111111111111111111111111111111111111111111111111111111"),
		Coinbase:              common.HexToAddress("0x2222222222222222222222222222222222222222"),
		Root:                  common.HexToHash("0x3333333333333333333333333333333333333333333333333333333333333333"),
		Number:                *uint256.NewInt(99),
		GasLimit:              30_000_000,
		GasUsed:               42_000,
		Time:                  1,
		Extra:                 []byte{0xde, 0xad, 0xbe, 0xef},
		MixDigest:             common.HexToHash("0x5555555555555555555555555555555555555555555555555555555555555555"),
		BaseFee:               uint256.NewInt(1_000_000_000),
		BlobGasUsed:           &blobGasUsed,
		ExcessBlobGas:         &excessBlobGas,
		ParentBeaconBlockRoot: &parentBeaconBlockRoot,
		RequestsHash:          requests.Hash(),
		BlockAccessListHash:   &balHash,
		SlotNumber:            &slotNumber,
	}
	block := types.NewBlock(header, []types.Transaction{legacyTx, dynamicTx}, nil, receipts, withdrawals, sidecar)

	zero := uint64(0)
	chainCfg := &chain.Config{
		CancunTime: &zero,
		BlobSchedule: map[string]*params.BlobConfig{
			"cancun": {Target: 1, Max: 2, BaseFeeUpdateFraction: 10_000},
		},
	}
	regularExcessGas := uint64(7)
	return &types.BlockWithReceipts{
			Block:           block,
			Receipts:        receipts,
			Requests:        requests,
			BlockAccessList: bal,
		}, BlockAdapterConfig{
			ChainConfig:      chainCfg,
			BeaconConfig:     &beaconCfg,
			RegularExcessGas: &regularExcessGas,
		}
}

func referencePayloadRoot(t *testing.T, payload *ExecutionPayload) [32]byte {
	t.Helper()

	transactionRoots := make([][32]byte, len(payload.Transactions))
	for i := range payload.Transactions {
		transactionRoots[i] = referenceProgressiveByteListRoot(payload.Transactions[i])
	}
	receiptRoots := make([][32]byte, len(payload.Receipts))
	for i := range payload.Receipts {
		receiptRoots[i] = referenceProgressiveByteListRoot(payload.Receipts[i])
	}
	withdrawalRoots := make([][32]byte, len(payload.Withdrawals))
	for i := range payload.Withdrawals {
		withdrawalRoots[i] = referenceProgressiveByteListRoot(payload.Withdrawals[i])
	}
	regularFee, err := payload.BaseFeesPerGas.Regular.MarshalSSZ()
	require.NoError(t, err)
	blobFee, err := payload.BaseFeesPerGas.Blob.MarshalSSZ()
	require.NoError(t, err)

	fields := [][32]byte{
		referenceBytesRoot(payload.ParentHash[:]),
		referenceBytesRoot(payload.Miner[:]),
		referenceBytesRoot(payload.StateRoot[:]),
		referenceProgressiveListRoot(transactionRoots, uint64(len(transactionRoots))),
		referenceProgressiveListRoot(receiptRoots, uint64(len(receiptRoots))),
		referenceUint64Root(payload.Number),
		referenceProgressiveContainerRoot([][32]byte{
			referenceUint64Root(payload.GasLimits.Regular),
			referenceUint64Root(payload.GasLimits.Blob),
		}),
		referenceProgressiveContainerRoot([][32]byte{
			referenceUint64Root(payload.GasUsed.Regular),
			referenceUint64Root(payload.GasUsed.Blob),
		}),
		referenceUint64Root(payload.Timestamp),
		referenceByteListRoot(payload.ExtraData),
		referenceBytesRoot(payload.MixHash[:]),
		referenceProgressiveContainerRoot([][32]byte{
			referenceBytesRoot(regularFee),
			referenceBytesRoot(blobFee),
		}),
		referenceProgressiveListRoot(withdrawalRoots, uint64(len(withdrawalRoots))),
		referenceProgressiveContainerRoot([][32]byte{
			referenceUint64Root(payload.ExcessGas.Regular),
			referenceUint64Root(payload.ExcessGas.Blob),
		}),
		referenceBytesRoot(payload.ParentBeaconBlockRoot[:]),
		referenceBytesRoot(payload.RequestsHash[:]),
		referenceProgressiveByteListRoot(payload.BlockAccessList),
		referenceUint64Root(payload.SlotNumber),
	}
	return referenceProgressiveContainerRoot(fields)
}

func referenceProgressiveContainerRoot(fields [][32]byte) [32]byte {
	activeFields := [32]byte{}
	for i := range fields {
		activeFields[i/8] |= 1 << uint(i%8)
	}
	return referenceHashPair(referenceMerkleizeProgressive(fields, 1), activeFields)
}

func referenceProgressiveByteListRoot(data []byte) [32]byte {
	chunks := make([][32]byte, (len(data)+31)/32)
	for i := range data {
		chunks[i/32][i%32] = data[i]
	}
	return referenceProgressiveListRoot(chunks, uint64(len(data)))
}

func referenceByteListRoot(data []byte) [32]byte {
	return referenceHashPair(referenceBytesRoot(data), referenceUint64Root(uint64(len(data))))
}

func referenceProgressiveListRoot(chunks [][32]byte, length uint64) [32]byte {
	return referenceHashPair(referenceMerkleizeProgressive(chunks, 1), referenceUint64Root(length))
}

func referenceMerkleizeProgressive(chunks [][32]byte, capacity int) [32]byte {
	if len(chunks) == 0 {
		return [32]byte{}
	}
	count := min(len(chunks), capacity)
	left := referenceMerkleizeVector(chunks[:count], capacity)
	right := referenceMerkleizeProgressive(chunks[count:], capacity*4)
	return referenceHashPair(left, right)
}

func referenceMerkleizeVector(chunks [][32]byte, capacity int) [32]byte {
	nodes := make([][32]byte, capacity)
	copy(nodes, chunks)
	for len(nodes) > 1 {
		next := make([][32]byte, len(nodes)/2)
		for i := range next {
			next[i] = referenceHashPair(nodes[i*2], nodes[i*2+1])
		}
		nodes = next
	}
	return nodes[0]
}

func referenceBytesRoot(data []byte) [32]byte {
	chunks := make([][32]byte, max(1, (len(data)+31)/32))
	for i := range data {
		chunks[i/32][i%32] = data[i]
	}
	return referenceMerkleizeVector(chunks, len(chunks))
}

func referenceUint64Root(value uint64) [32]byte {
	var root [32]byte
	binary.LittleEndian.PutUint64(root[:], value)
	return root
}

func referenceHashPair(left, right [32]byte) [32]byte {
	var pair [64]byte
	copy(pair[:32], left[:])
	copy(pair[32:], right[:])
	return sha256.Sum256(pair[:])
}
