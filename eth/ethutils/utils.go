package ethutils

import (
	"errors"
	"reflect"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/core/types"
)

var (
	ErrNilBlobHashes      = errors.New("nil blob hashes array")
	ErrMaxBlobGasUsed     = errors.New("max blob gas used")
	ErrMismatchBlobHashes = errors.New("mismatch blob hashes")
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
		actualBlobHashes = append(actualBlobHashes, txn.GetBlobHashes()...)
	}
	if len(actualBlobHashes) > int(maxBlobsPerBlock) || blobGasUsed > maxBlobsGas {
		return ErrMaxBlobGasUsed
	}
	if !reflect.DeepEqual(actualBlobHashes, expectedBlobHashes) {
		return ErrMismatchBlobHashes
	}
	return nil
}
