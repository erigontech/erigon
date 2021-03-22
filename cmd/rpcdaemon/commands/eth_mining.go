package commands

import (
	"context"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/hexutil"
	"github.com/ledgerwatch/turbo-geth/core/types"
)

// Coinbase implements eth_coinbase. Returns the current client coinbase address.
func (api *APIImpl) Coinbase(ctx context.Context) (common.Address, error) {
	return api.ethBackend.Etherbase(ctx)
}

// Hashrate implements eth_hashrate. Returns the number of hashes per second that the node is mining with.
func (api *APIImpl) Hashrate(ctx context.Context) (uint64, error) {
	return api.ethBackend.GetHashRate(ctx)
}

// Mining returns an indication if this node is currently mining.
func (api *APIImpl) Mining(ctx context.Context) (bool, error) {
	return api.ethBackend.Mining(ctx)
}

// GetWork returns a work package for external miner.
//
// The work package consists of 3 strings:
//   result[0] - 32 bytes hex encoded current block header pow-hash
//   result[1] - 32 bytes hex encoded seed hash used for DAG
//   result[2] - 32 bytes hex encoded boundary condition ("target"), 2^256/difficulty
//   result[3] - hex encoded block number
func (api *APIImpl) GetWork(ctx context.Context) ([4]string, error) {
	return api.ethBackend.GetWork(ctx)
}

// SubmitWork can be used by external miner to submit their POW solution.
// It returns an indication if the work was accepted.
// Note either an invalid solution, a stale work a non-existent work will return false.
func (api *APIImpl) SubmitWork(ctx context.Context, nonce types.BlockNonce, powHash, digest common.Hash) (bool, error) {
	return api.ethBackend.SubmitWork(ctx, nonce, powHash, digest)
}

// SubmitHashrate can be used for remote miners to submit their hash rate.
// This enables the node to report the combined hash rate of all miners
// which submit work through this node.
//
// It accepts the miner hash rate and an identifier which must be unique
func (api *APIImpl) SubmitHashrate(ctx context.Context, hashRate hexutil.Uint64, id common.Hash) (bool, error) {
	return api.ethBackend.SubmitHashRate(ctx, hashRate, id)
}

// GetHashrate returns the current hashrate for local CPU miner and remote miner.
func (api *APIImpl) GetHashrate(ctx context.Context) (uint64, error) {
	return api.ethBackend.GetHashRate(ctx)
}
