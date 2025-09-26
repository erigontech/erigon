// Copyright 2024 The Erigon Authors
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

package cltypes

import (
	"encoding/json"
	"fmt"
	"math/big"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/empty"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/merkle_tree"
	ssz2 "github.com/erigontech/erigon/cl/ssz"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/execution/consensus/merge"
	"github.com/erigontech/erigon/execution/types"
)

// ETH1Block represents a block structure CL-side.
type Eth1Block struct {
	ParentHash    common.Hash      `json:"parent_hash"`
	FeeRecipient  common.Address   `json:"fee_recipient"`
	StateRoot     common.Hash      `json:"state_root"`
	ReceiptsRoot  common.Hash      `json:"receipts_root"`
	LogsBloom     types.Bloom      `json:"logs_bloom"`
	PrevRandao    common.Hash      `json:"prev_randao"`
	BlockNumber   uint64           `json:"block_number,string"`
	GasLimit      uint64           `json:"gas_limit,string"`
	GasUsed       uint64           `json:"gas_used,string"`
	Time          uint64           `json:"timestamp,string"`
	Extra         *solid.ExtraData `json:"extra_data"`
	BaseFeePerGas common.Hash      `json:"base_fee_per_gas"`
	// Extra fields
	BlockHash     common.Hash                 `json:"block_hash"`
	Transactions  *solid.TransactionsSSZ      `json:"transactions"`
	Withdrawals   *solid.ListSSZ[*Withdrawal] `json:"withdrawals,omitempty"`
	BlobGasUsed   uint64                      `json:"blob_gas_used,string"`
	ExcessBlobGas uint64                      `json:"excess_blob_gas,string"`
	// internals
	version   clparams.StateVersion
	beaconCfg *clparams.BeaconChainConfig
}

// NewEth1Block creates a new Eth1Block.
func NewEth1Block(version clparams.StateVersion, beaconCfg *clparams.BeaconChainConfig) *Eth1Block {
	return &Eth1Block{
		version:   version,
		beaconCfg: beaconCfg,
	}
}

// NewEth1BlockFromHeaderAndBody with given header/body.
func NewEth1BlockFromHeaderAndBody(header *types.Header, body *types.RawBody, beaconCfg *clparams.BeaconChainConfig) *Eth1Block {
	baseFeeBytes := header.BaseFee.Bytes()
	for i, j := 0, len(baseFeeBytes)-1; i < j; i, j = i+1, j-1 {
		baseFeeBytes[i], baseFeeBytes[j] = baseFeeBytes[j], baseFeeBytes[i]
	}
	var baseFee32 [32]byte
	copy(baseFee32[:], baseFeeBytes)

	extra := solid.NewExtraData()
	extra.SetBytes(header.Extra)
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
		Extra:         extra,
		BaseFeePerGas: baseFee32,
		BlockHash:     header.Hash(),
		Transactions:  solid.NewTransactionsSSZFromTransactions(body.Transactions),
		Withdrawals:   solid.NewStaticListSSZFromList(convertExecutionWithdrawalsToConsensusWithdrawals(body.Withdrawals), int(beaconCfg.MaxWithdrawalsPerPayload), 44),
		beaconCfg:     beaconCfg,
	}

	if header.BlobGasUsed != nil && header.ExcessBlobGas != nil {
		block.BlobGasUsed = *header.BlobGasUsed
		block.ExcessBlobGas = *header.ExcessBlobGas
		block.version = clparams.DenebVersion
	} else if header.WithdrawalsHash != nil {
		block.version = clparams.CapellaVersion
	} else {
		block.version = clparams.BellatrixVersion
	}
	return block
}

func (*Eth1Block) Static() bool {
	return false
}

func (b *Eth1Block) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		ParentHash    common.Hash                 `json:"parent_hash"`
		FeeRecipient  common.Address              `json:"fee_recipient"`
		StateRoot     common.Hash                 `json:"state_root"`
		ReceiptsRoot  common.Hash                 `json:"receipts_root"`
		LogsBloom     types.Bloom                 `json:"logs_bloom"`
		PrevRandao    common.Hash                 `json:"prev_randao"`
		BlockNumber   uint64                      `json:"block_number,string"`
		GasLimit      uint64                      `json:"gas_limit,string"`
		GasUsed       uint64                      `json:"gas_used,string"`
		Time          uint64                      `json:"timestamp,string"`
		Extra         *solid.ExtraData            `json:"extra_data"`
		BaseFeePerGas string                      `json:"base_fee_per_gas"`
		BlockHash     common.Hash                 `json:"block_hash"`
		Transactions  *solid.TransactionsSSZ      `json:"transactions"`
		Withdrawals   *solid.ListSSZ[*Withdrawal] `json:"withdrawals,omitempty"`
		BlobGasUsed   uint64                      `json:"blob_gas_used,string"`
		ExcessBlobGas uint64                      `json:"excess_blob_gas,string"`
	}{
		ParentHash:    b.ParentHash,
		FeeRecipient:  b.FeeRecipient,
		StateRoot:     b.StateRoot,
		ReceiptsRoot:  b.ReceiptsRoot,
		LogsBloom:     b.LogsBloom,
		PrevRandao:    b.PrevRandao,
		BlockNumber:   b.BlockNumber,
		GasLimit:      b.GasLimit,
		GasUsed:       b.GasUsed,
		Time:          b.Time,
		Extra:         b.Extra,
		BaseFeePerGas: uint256.NewInt(0).SetBytes32(utils.ReverseOfByteSlice(b.BaseFeePerGas[:])).Dec(),
		BlockHash:     b.BlockHash,
		Transactions:  b.Transactions,
		Withdrawals:   b.Withdrawals,
		BlobGasUsed:   b.BlobGasUsed,
		ExcessBlobGas: b.ExcessBlobGas,
	})
}

func (b *Eth1Block) UnmarshalJSON(data []byte) error {
	var aux struct {
		ParentHash    common.Hash                 `json:"parent_hash"`
		FeeRecipient  common.Address              `json:"fee_recipient"`
		StateRoot     common.Hash                 `json:"state_root"`
		ReceiptsRoot  common.Hash                 `json:"receipts_root"`
		LogsBloom     types.Bloom                 `json:"logs_bloom"`
		PrevRandao    common.Hash                 `json:"prev_randao"`
		BlockNumber   uint64                      `json:"block_number,string"`
		GasLimit      uint64                      `json:"gas_limit,string"`
		GasUsed       uint64                      `json:"gas_used,string"`
		Time          uint64                      `json:"timestamp,string"`
		Extra         *solid.ExtraData            `json:"extra_data"`
		BaseFeePerGas string                      `json:"base_fee_per_gas"`
		BlockHash     common.Hash                 `json:"block_hash"`
		Transactions  *solid.TransactionsSSZ      `json:"transactions"`
		Withdrawals   *solid.ListSSZ[*Withdrawal] `json:"withdrawals"`
		BlobGasUsed   uint64                      `json:"blob_gas_used,string"`
		ExcessBlobGas uint64                      `json:"excess_blob_gas,string"`
	}
	aux.Withdrawals = solid.NewStaticListSSZ[*Withdrawal](int(b.beaconCfg.MaxWithdrawalsPerPayload), 44)
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}
	b.ParentHash = aux.ParentHash
	b.FeeRecipient = aux.FeeRecipient
	b.StateRoot = aux.StateRoot
	b.ReceiptsRoot = aux.ReceiptsRoot
	b.LogsBloom = aux.LogsBloom
	b.PrevRandao = aux.PrevRandao
	b.BlockNumber = aux.BlockNumber
	b.GasLimit = aux.GasLimit
	b.GasUsed = aux.GasUsed
	b.Time = aux.Time
	b.Extra = aux.Extra
	tmp := uint256.NewInt(0)
	if err := tmp.SetFromDecimal(aux.BaseFeePerGas); err != nil {
		return err
	}
	tmpBaseFee := tmp.Bytes32()
	b.BaseFeePerGas = common.Hash{}
	copy(b.BaseFeePerGas[:], utils.ReverseOfByteSlice(tmpBaseFee[:]))
	b.BlockHash = aux.BlockHash
	b.Transactions = aux.Transactions
	b.Withdrawals = aux.Withdrawals
	b.BlobGasUsed = aux.BlobGasUsed
	b.ExcessBlobGas = aux.ExcessBlobGas
	return nil
}

// PayloadHeader returns the equivalent ExecutionPayloadHeader object.
func (b *Eth1Block) PayloadHeader() (*Eth1Header, error) {
	var err error
	var transactionsRoot, withdrawalsRoot common.Hash
	// Corner case: before TTD this is 0, since all fields are 0, a 0 hash check will suffice.
	if b.BlockHash != (common.Hash{}) {
		if transactionsRoot, err = b.Transactions.HashSSZ(); err != nil {
			return nil, err
		}
	}
	if b.version >= clparams.CapellaVersion {
		withdrawalsRoot, err = b.Withdrawals.HashSSZ()
		if err != nil {
			return nil, err
		}
	}

	var blobGasUsed, excessBlobGas uint64
	if b.version >= clparams.DenebVersion {
		blobGasUsed = b.BlobGasUsed
		excessBlobGas = b.ExcessBlobGas
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
		BlobGasUsed:      blobGasUsed,
		ExcessBlobGas:    excessBlobGas,
		version:          b.version,
	}, nil
}

// Return minimum required buffer length to be an acceptable SSZ encoding.
func (b *Eth1Block) EncodingSizeSSZ() (size int) {
	size = 508
	if b.Extra == nil {
		b.Extra = solid.NewExtraData()
	}
	// Field (10) 'ExtraData'
	size += b.Extra.EncodingSizeSSZ()
	// Field (13) 'Transactions'
	size += b.Transactions.EncodingSizeSSZ()

	if b.version >= clparams.CapellaVersion {
		if b.Withdrawals == nil {
			b.Withdrawals = solid.NewStaticListSSZ[*Withdrawal](int(b.beaconCfg.MaxWithdrawalsPerPayload), 44)
		}
		size += b.Withdrawals.EncodingSizeSSZ() + 4
	}

	if b.version >= clparams.DenebVersion {
		size += 8 * 2 // BlobGasUsed + ExcessBlobGas
	}

	return
}

// DecodeSSZ decodes the block in SSZ format.
func (b *Eth1Block) DecodeSSZ(buf []byte, version int) error {
	b.Extra = solid.NewExtraData()
	b.Transactions = &solid.TransactionsSSZ{}
	b.Withdrawals = solid.NewStaticListSSZ[*Withdrawal](int(b.beaconCfg.MaxWithdrawalsPerPayload), 44)
	b.version = clparams.StateVersion(version)
	return ssz2.UnmarshalSSZ(buf, version, b.getSchema()...)
}

// EncodeSSZ encodes the block in SSZ format.
func (b *Eth1Block) EncodeSSZ(dst []byte) ([]byte, error) {
	return ssz2.MarshalSSZ(dst, b.getSchema()...)
}

// HashSSZ calculates the SSZ hash of the Eth1Block's payload header.
func (b *Eth1Block) HashSSZ() ([32]byte, error) {
	return merkle_tree.HashTreeRoot(b.getSchema()...)
}

func (b *Eth1Block) getSchema() []interface{} {
	s := []interface{}{b.ParentHash[:], b.FeeRecipient[:], b.StateRoot[:], b.ReceiptsRoot[:], b.LogsBloom[:],
		b.PrevRandao[:], &b.BlockNumber, &b.GasLimit, &b.GasUsed, &b.Time, b.Extra, b.BaseFeePerGas[:], b.BlockHash[:], b.Transactions}
	if b.version >= clparams.CapellaVersion {
		s = append(s, b.Withdrawals)
	}
	if b.version >= clparams.DenebVersion {
		s = append(s, &b.BlobGasUsed, &b.ExcessBlobGas)
	}
	return s
}

// RlpHeader returns the equivalent types.Header struct with RLP-based fields.
func (b *Eth1Block) RlpHeader(parentRoot *common.Hash, executionReqHash common.Hash) (*types.Header, error) {
	// Reverse the order of the bytes in the BaseFeePerGas array and convert it to a big integer.
	reversedBaseFeePerGas := common.Copy(b.BaseFeePerGas[:])
	for i, j := 0, len(reversedBaseFeePerGas)-1; i < j; i, j = i+1, j-1 {
		reversedBaseFeePerGas[i], reversedBaseFeePerGas[j] = reversedBaseFeePerGas[j], reversedBaseFeePerGas[i]
	}
	baseFee := new(big.Int).SetBytes(reversedBaseFeePerGas)
	// If the block version is Capella or later, calculate the withdrawals hash.
	var withdrawalsHash *common.Hash
	if b.version >= clparams.CapellaVersion {
		withdrawalsHash = new(common.Hash)
		// extract all withdrawals from itearable list
		withdrawals := make([]*types.Withdrawal, b.Withdrawals.Len())
		b.Withdrawals.Range(func(idx int, w *Withdrawal, _ int) bool {
			withdrawals[idx] = convertConsensusWithdrawalToExecutionWithdrawal(w)
			return true
		})
		*withdrawalsHash = types.DeriveSha(types.Withdrawals(withdrawals))
	}
	if b.version < clparams.DenebVersion {
		log.Debug("ParentRoot is nil", "parentRoot", parentRoot, "version", b.version)
		parentRoot = nil
	}

	header := &types.Header{
		ParentHash:            b.ParentHash,
		UncleHash:             empty.UncleHash,
		Coinbase:              b.FeeRecipient,
		Root:                  b.StateRoot,
		TxHash:                types.DeriveSha(types.BinaryTransactions(b.Transactions.UnderlyngReference())),
		ReceiptHash:           b.ReceiptsRoot,
		Bloom:                 b.LogsBloom,
		Difficulty:            merge.ProofOfStakeDifficulty,
		Number:                new(big.Int).SetUint64(b.BlockNumber),
		GasLimit:              b.GasLimit,
		GasUsed:               b.GasUsed,
		Time:                  b.Time,
		Extra:                 b.Extra.Bytes(),
		MixDigest:             b.PrevRandao,
		Nonce:                 merge.ProofOfStakeNonce,
		BaseFee:               baseFee,
		WithdrawalsHash:       withdrawalsHash,
		ParentBeaconBlockRoot: parentRoot,
	}

	if b.version >= clparams.DenebVersion {
		blobGasUsed := b.BlobGasUsed
		header.BlobGasUsed = &blobGasUsed
		excessBlobGas := b.ExcessBlobGas
		header.ExcessBlobGas = &excessBlobGas
	}

	if b.version >= clparams.ElectraVersion {
		header.RequestsHash = &executionReqHash
	}

	// If the header hash does not match the block hash, return an error.
	if header.Hash() != b.BlockHash {
		return nil, fmt.Errorf("cannot derive rlp header: mismatching hash: %s != %s, %d", header.Hash(), b.BlockHash, header.Number)
	}

	return header, nil
}

func (b *Eth1Block) Version() clparams.StateVersion {
	return b.version
}

// Body returns the equivalent raw body (only eth1 body section).
func (b *Eth1Block) Body() *types.RawBody {
	withdrawals := make([]*types.Withdrawal, b.Withdrawals.Len())
	b.Withdrawals.Range(func(idx int, w *Withdrawal, _ int) bool {
		withdrawals[idx] = convertConsensusWithdrawalToExecutionWithdrawal(w)
		return true
	})
	return &types.RawBody{
		Transactions: b.Transactions.UnderlyngReference(),
		Withdrawals:  types.Withdrawals(withdrawals),
	}
}
