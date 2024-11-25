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

package ethutils

import (
	"errors"
	"reflect"

	libcommon "github.com/erigontech/erigon/erigon-lib/common"
	"github.com/erigontech/erigon/erigon-lib/crypto/kzg"
	"github.com/erigontech/erigon/erigon-lib/log/v3"

	"github.com/erigontech/erigon/consensus"
	"github.com/erigontech/erigon/core/types"
)

var (
	ErrNilBlobHashes       = errors.New("nil blob hashes array")
	ErrMaxBlobGasUsed      = errors.New("max blob gas used")
	ErrMismatchBlobHashes  = errors.New("mismatch blob hashes")
	ErrInvalidVersiondHash = errors.New("invalid blob versioned hash, must start with VERSIONED_HASH_VERSION_KZG")
)

// IsLocalBlock checks whether the specified block is mined
// by local miner accounts.
//
// We regard two types of accounts as local miner account: etherbase
// and accounts specified via `txpool.locals` flag.
func IsLocalBlock(engine consensus.Engine, etherbase libcommon.Address, txPoolLocals []libcommon.Address, header *types.Header) bool {
	author, err := engine.Author(header)
	if err != nil {
		log.Warn("Failed to retrieve block author", "number", header.Number, "header_hash", header.Hash(), "err", err)
		return false
	}
	// Check whether the given address is etherbase.
	if author == etherbase {
		return true
	}
	// Check whether the given address is specified by `txpool.local`
	// CLI flag.
	for _, account := range txPoolLocals {
		if account == author {
			return true
		}
	}
	return false
}

func ValidateBlobs(blobGasUsed, maxBlobsGas, maxBlobsPerBlock uint64, expectedBlobHashes []libcommon.Hash, transactions *[]types.Transaction) error {
	if expectedBlobHashes == nil {
		return ErrNilBlobHashes
	}
	actualBlobHashes := []libcommon.Hash{}
	for _, txn := range *transactions {
		if txn.Type() == types.BlobTxType {
			for _, h := range txn.GetBlobHashes() {
				if h[0] != kzg.BlobCommitmentVersionKZG {
					return ErrInvalidVersiondHash
				}
				actualBlobHashes = append(actualBlobHashes, h)
			}
		}
	}
	if len(actualBlobHashes) > int(maxBlobsPerBlock) || blobGasUsed > maxBlobsGas {
		return ErrMaxBlobGasUsed
	}
	if !reflect.DeepEqual(actualBlobHashes, expectedBlobHashes) {
		return ErrMismatchBlobHashes
	}
	return nil
}
