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

// Package seal computes the hash a Parlia validator signs.
package seal

import (
	"errors"
	"math/big"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/types"
)

var (
	ErrMissingSignature = errors.New("parlia: extra-data too short to hold a seal")
	errNegativeChainID  = errors.New("parlia: negative chain id")
)

// Hash returns the Keccak256 of the header as signed by its validator: chainID first,
// the seal stripped from Extra, and the post-London fields sealed only from Bohr on,
// which is when ParentBeaconBlockRoot appears. chainID is unbounded because the
// double-sign precompile takes it from calldata.
func Hash(header *types.Header, chainID *big.Int) (common.Hash, error) {
	if len(header.Extra) < crypto.SignatureLength {
		return common.Hash{}, ErrMissingSignature
	}
	if chainID.Sign() < 0 {
		return common.Hash{}, errNegativeChainID
	}
	fields := []any{
		chainID.Bytes(),
		header.ParentHash,
		header.UncleHash,
		header.Coinbase,
		header.Root,
		header.TxHash,
		header.ReceiptHash,
		header.Bloom,
		&header.Difficulty,
		&header.Number,
		header.GasLimit,
		header.GasUsed,
		header.Time,
		header.Extra[:len(header.Extra)-crypto.SignatureLength],
		header.MixDigest,
		header.Nonce,
	}
	if header.ParentBeaconBlockRoot != nil {
		fields = append(fields, header.BaseFee, header.WithdrawalsHash, header.BlobGasUsed, header.ExcessBlobGas, header.ParentBeaconBlockRoot)
		if header.RequestsHash != nil {
			fields = append(fields, header.RequestsHash)
		}
	}
	enc, err := rlp.EncodeToBytes(fields)
	if err != nil {
		return common.Hash{}, err
	}
	return crypto.Keccak256Hash(enc), nil
}
