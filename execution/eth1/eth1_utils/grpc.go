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

package eth1_utils

import (
	"encoding/binary"
	"fmt"
	"math/big"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/gointerfaces"
	"github.com/erigontech/erigon/node/gointerfaces/executionproto"
	"github.com/erigontech/erigon/node/gointerfaces/typesproto"
)

func HeaderToHeaderRPC(header *types.Header) *executionproto.Header {
	difficulty := new(uint256.Int)
	difficulty.SetFromBig(header.Difficulty)

	var baseFeeReply *typesproto.H256
	if header.BaseFee != nil {
		var baseFee uint256.Int
		baseFee.SetFromBig(header.BaseFee)
		baseFeeReply = gointerfaces.ConvertUint256IntToH256(&baseFee)
	}

	h := &executionproto.Header{
		ParentHash:      gointerfaces.ConvertHashToH256(header.ParentHash),
		Coinbase:        gointerfaces.ConvertAddressToH160(header.Coinbase),
		StateRoot:       gointerfaces.ConvertHashToH256(header.Root),
		TransactionHash: gointerfaces.ConvertHashToH256(header.TxHash),
		LogsBloom:       gointerfaces.ConvertBytesToH2048(header.Bloom[:]),
		ReceiptRoot:     gointerfaces.ConvertHashToH256(header.ReceiptHash),
		PrevRandao:      gointerfaces.ConvertHashToH256(header.MixDigest),
		BlockNumber:     header.Number.Uint64(),
		Nonce:           header.Nonce.Uint64(),
		GasLimit:        header.GasLimit,
		GasUsed:         header.GasUsed,
		Timestamp:       header.Time,
		ExtraData:       header.Extra,
		Difficulty:      gointerfaces.ConvertUint256IntToH256(difficulty),
		BlockHash:       gointerfaces.ConvertHashToH256(header.Hash()),
		OmmerHash:       gointerfaces.ConvertHashToH256(header.UncleHash),
		BaseFeePerGas:   baseFeeReply,
	}

	if header.ExcessBlobGas != nil {
		h.ExcessBlobGas = header.ExcessBlobGas
		h.BlobGasUsed = header.BlobGasUsed
	}

	if header.WithdrawalsHash != nil {
		h.WithdrawalHash = gointerfaces.ConvertHashToH256(*header.WithdrawalsHash)
	}

	if header.ParentBeaconBlockRoot != nil {
		h.ParentBeaconBlockRoot = gointerfaces.ConvertHashToH256(*header.ParentBeaconBlockRoot)
	}

	if header.RequestsHash != nil {
		h.RequestsHash = gointerfaces.ConvertHashToH256(*header.RequestsHash)
	}

	if header.BlockAccessListHash != nil {
		h.BlockAccessListHash = gointerfaces.ConvertHashToH256(*header.BlockAccessListHash)
	}

	if len(header.AuRaSeal) > 0 {
		h.AuraSeal = header.AuRaSeal
		h.AuraStep = &header.AuRaStep
	}
	return h
}

func HeadersToHeadersRPC(headers []*types.Header) []*executionproto.Header {
	if headers == nil {
		return nil
	}
	ret := make([]*executionproto.Header, 0, len(headers))
	for _, header := range headers {
		ret = append(ret, HeaderToHeaderRPC(header))
	}
	return ret
}

func ConvertBlocksToRPC(blocks []*types.Block) []*executionproto.Block {
	ret := []*executionproto.Block{}
	for _, block := range blocks {
		ret = append(ret, ConvertBlockToRPC(block))
	}
	return ret
}

func ConvertBlockToRPC(block *types.Block) *executionproto.Block {
	h := HeaderToHeaderRPC(block.Header())
	blockHash := block.Hash()
	h.BlockHash = gointerfaces.ConvertHashToH256(blockHash)

	return &executionproto.Block{
		Header: h,
		Body:   ConvertRawBlockBodyToRpc(block.RawBody(), h.BlockNumber, blockHash),
	}
}

func HeaderRpcToHeader(header *executionproto.Header) (*types.Header, error) {
	var blockNonce types.BlockNonce
	binary.BigEndian.PutUint64(blockNonce[:], header.Nonce)
	h := &types.Header{
		ParentHash:    gointerfaces.ConvertH256ToHash(header.ParentHash),
		UncleHash:     gointerfaces.ConvertH256ToHash(header.OmmerHash),
		Coinbase:      gointerfaces.ConvertH160toAddress(header.Coinbase),
		Root:          gointerfaces.ConvertH256ToHash(header.StateRoot),
		TxHash:        gointerfaces.ConvertH256ToHash(header.TransactionHash),
		ReceiptHash:   gointerfaces.ConvertH256ToHash(header.ReceiptRoot),
		Bloom:         gointerfaces.ConvertH2048ToBloom(header.LogsBloom),
		Difficulty:    gointerfaces.ConvertH256ToUint256Int(header.Difficulty).ToBig(),
		Number:        new(big.Int).SetUint64(header.BlockNumber),
		GasLimit:      header.GasLimit,
		GasUsed:       header.GasUsed,
		Time:          header.Timestamp,
		Extra:         header.ExtraData,
		MixDigest:     gointerfaces.ConvertH256ToHash(header.PrevRandao),
		Nonce:         blockNonce,
		BlobGasUsed:   header.BlobGasUsed,
		ExcessBlobGas: header.ExcessBlobGas,
	}
	if header.AuraStep != nil {
		h.AuRaSeal = header.AuraSeal
		h.AuRaStep = *header.AuraStep
	}
	if header.BaseFeePerGas != nil {
		h.BaseFee = gointerfaces.ConvertH256ToUint256Int(header.BaseFeePerGas).ToBig()
	}
	if header.WithdrawalHash != nil {
		h.WithdrawalsHash = new(common.Hash)
		*h.WithdrawalsHash = gointerfaces.ConvertH256ToHash(header.WithdrawalHash)
	}
	if header.ParentBeaconBlockRoot != nil {
		h.ParentBeaconBlockRoot = new(common.Hash)
		*h.ParentBeaconBlockRoot = gointerfaces.ConvertH256ToHash(header.ParentBeaconBlockRoot)
	}
	if header.RequestsHash != nil {
		h.RequestsHash = new(common.Hash)
		*h.RequestsHash = gointerfaces.ConvertH256ToHash(header.RequestsHash)
	}
	if header.BlockAccessListHash != nil {
		h.BlockAccessListHash = new(common.Hash)
		*h.BlockAccessListHash = gointerfaces.ConvertH256ToHash(header.BlockAccessListHash)
	}
	blockHash := gointerfaces.ConvertH256ToHash(header.BlockHash)
	if blockHash != h.Hash() {
		return nil, fmt.Errorf("block %d, %x has invalid hash. expected: %x", header.BlockNumber, h.Hash(), blockHash)
	}

	return h, nil
}

func HeadersRpcToHeaders(headers []*executionproto.Header) ([]*types.Header, error) {
	if headers == nil {
		return nil, nil
	}
	out := make([]*types.Header, 0, len(headers))
	for _, h := range headers {
		header, err := HeaderRpcToHeader(h)
		if err != nil {
			return nil, err
		}
		out = append(out, header)
	}
	return out, nil
}

func ConvertWithdrawalsFromRpc(in []*typesproto.Withdrawal) []*types.Withdrawal {
	if in == nil {
		return nil
	}
	out := make([]*types.Withdrawal, 0, len(in))
	for _, w := range in {
		out = append(out, &types.Withdrawal{
			Index:     w.Index,
			Validator: w.ValidatorIndex,
			Address:   gointerfaces.ConvertH160toAddress(w.Address),
			Amount:    w.Amount,
		})
	}
	return out
}

func ConvertWithdrawalsToRpc(in []*types.Withdrawal) []*typesproto.Withdrawal {
	if in == nil {
		return nil
	}
	out := make([]*typesproto.Withdrawal, 0, len(in))
	for _, w := range in {
		out = append(out, &typesproto.Withdrawal{
			Index:          w.Index,
			ValidatorIndex: w.Validator,
			Address:        gointerfaces.ConvertAddressToH160(w.Address),
			Amount:         w.Amount,
		})
	}
	return out
}

func ConvertRawBlockBodyToRpc(in *types.RawBody, blockNumber uint64, blockHash common.Hash) *executionproto.BlockBody {
	if in == nil {
		return nil
	}

	return &executionproto.BlockBody{
		BlockNumber:  blockNumber,
		BlockHash:    gointerfaces.ConvertHashToH256(blockHash),
		Transactions: in.Transactions,
		Uncles:       HeadersToHeadersRPC(in.Uncles),
		Withdrawals:  ConvertWithdrawalsToRpc(in.Withdrawals),
	}
}

func ConvertRawBlockBodiesToRpc(in []*types.RawBody, blockNumbers []uint64, blockHashes []common.Hash) []*executionproto.BlockBody {
	ret := []*executionproto.BlockBody{}

	for i, body := range in {
		ret = append(ret, ConvertRawBlockBodyToRpc(body, blockNumbers[i], blockHashes[i]))
	}
	return ret
}

func ConvertRawBlockBodyFromRpc(in *executionproto.BlockBody) (*types.RawBody, error) {
	if in == nil {
		return nil, nil
	}
	uncles, err := HeadersRpcToHeaders(in.Uncles)
	if err != nil {
		return nil, err
	}
	return &types.RawBody{
		Transactions: in.Transactions,
		Uncles:       uncles,
		Withdrawals:  ConvertWithdrawalsFromRpc(in.Withdrawals),
	}, nil
}

func ConvertBigIntFromRpc(in *typesproto.H256) *big.Int {
	if in == nil {
		return nil
	}
	base := gointerfaces.ConvertH256ToUint256Int(in)
	return base.ToBig()
}

func ConvertBigIntToRpc(in *big.Int) *typesproto.H256 {
	if in == nil {
		return nil
	}
	base := new(uint256.Int)
	base.SetFromBig(in)
	return gointerfaces.ConvertUint256IntToH256(base)
}
