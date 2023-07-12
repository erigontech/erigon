package jsonrpc

import (
	"context"
	"errors"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/txpool"
	"google.golang.org/grpc/status"

	"github.com/ledgerwatch/erigon/common/hexutil"
	"github.com/ledgerwatch/erigon/core/types"
)

// Coinbase implements eth_coinbase. Returns the current client coinbase address.
func (api *APIImpl) Coinbase(ctx context.Context) (libcommon.Address, error) {
	return api.ethBackend.Etherbase(ctx)
}

// Hashrate implements eth_hashrate. Returns the number of hashes per second that the node is mining with.
func (api *APIImpl) Hashrate(ctx context.Context) (uint64, error) {
	repl, err := api.mining.HashRate(ctx, &txpool.HashRateRequest{})
	if err != nil {
		if s, ok := status.FromError(err); ok {
			return 0, errors.New(s.Message())
		}
		return 0, err
	}
	return repl.HashRate, err
}

// Mining returns an indication if this node is currently mining.
func (api *APIImpl) Mining(ctx context.Context) (bool, error) {
	repl, err := api.mining.Mining(ctx, &txpool.MiningRequest{})
	if err != nil {
		if s, ok := status.FromError(err); ok {
			return false, errors.New(s.Message())
		}
		return false, err
	}
	return repl.Enabled && repl.Running, err
}

// GetWork returns a work package for external miner.
//
// The work package consists of 3 strings:
//
//	result[0] - 32 bytes hex encoded current block header pow-hash
//	result[1] - 32 bytes hex encoded seed hash used for DAG
//	result[2] - 32 bytes hex encoded boundary condition ("target"), 2^256/difficulty
//	result[3] - hex encoded block number
func (api *APIImpl) GetWork(ctx context.Context) ([4]string, error) {
	var res [4]string
	repl, err := api.mining.GetWork(ctx, &txpool.GetWorkRequest{})
	if err != nil {
		if s, ok := status.FromError(err); ok {
			return res, errors.New(s.Message())
		}
		return res, err
	}
	res[0] = repl.HeaderHash
	res[1] = repl.SeedHash
	res[2] = repl.Target
	res[3] = repl.BlockNumber
	return res, nil
}

// SubmitWork can be used by external miner to submit their POW solution.
// It returns an indication if the work was accepted.
// Note either an invalid solution, a stale work a non-existent work will return false.
func (api *APIImpl) SubmitWork(ctx context.Context, nonce types.BlockNonce, powHash, digest libcommon.Hash) (bool, error) {
	repl, err := api.mining.SubmitWork(ctx, &txpool.SubmitWorkRequest{BlockNonce: nonce[:], PowHash: powHash.Bytes(), Digest: digest.Bytes()})
	if err != nil {
		if s, ok := status.FromError(err); ok {
			return false, errors.New(s.Message())
		}
		return false, err
	}
	return repl.Ok, nil
}

// SubmitHashrate can be used for remote miners to submit their hash rate.
// This enables the node to report the combined hash rate of all miners
// which submit work through this node.
//
// It accepts the miner hash rate and an identifier which must be unique
func (api *APIImpl) SubmitHashrate(ctx context.Context, hashRate hexutil.Uint64, id libcommon.Hash) (bool, error) {
	repl, err := api.mining.SubmitHashRate(ctx, &txpool.SubmitHashRateRequest{Rate: uint64(hashRate), Id: id.Bytes()})
	if err != nil {
		if s, ok := status.FromError(err); ok {
			return false, errors.New(s.Message())
		}
		return false, err
	}
	return repl.Ok, nil
}
