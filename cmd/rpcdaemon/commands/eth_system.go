package commands

import (
	"context"
	"fmt"

	"github.com/ledgerwatch/turbo-geth/common/hexutil"
	"github.com/ledgerwatch/turbo-geth/eth"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync/stages"
)

// BlockNumber returns the latest block number of the chain
func (api *APIImpl) BlockNumber(ctx context.Context) (hexutil.Uint64, error) {
	execution, _, err := stages.GetStageProgress(api.dbReader, stages.Finish)
	if err != nil {
		return 0, err
	}
	return hexutil.Uint64(execution), nil
}

// Syncing - we can return the progress of the very first stage as the highest block, and then the progress of the very last stage as the current block
func (api *APIImpl) Syncing(ctx context.Context) (interface{}, error) {
	highestBlock, _, err := stages.GetStageProgress(api.dbReader, stages.Headers)
	if err != nil {
		return false, err
	}

	currentBlock, _, err := stages.GetStageProgress(api.dbReader, stages.Finish)
	if err != nil {
		return false, err
	}

	// Return not syncing if the synchronisation already completed
	if currentBlock >= highestBlock {
		return false, nil
	}
	// Otherwise gather the block sync stats
	return map[string]hexutil.Uint64{
		"currentBlock": hexutil.Uint64(currentBlock),
		"highestBlock": hexutil.Uint64(highestBlock),
	}, nil
}

// ChainId returns the chain id from the config
func (api *APIImpl) ChainId(ctx context.Context) (hexutil.Uint64, error) {
	tx, err := api.dbReader.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	chainConfig := getChainConfig(tx)
	return hexutil.Uint64(chainConfig.ChainID.Uint64()), nil
}

// ProtocolVersion returns the chain id from the config
func (api *APIImpl) ProtocolVersion(_ context.Context) (hexutil.Uint, error) {
	return hexutil.Uint(eth.ProtocolVersions[0]), nil
}

// GasPrice returns a suggestion for a gas price.
func (api *APIImpl) GasPrice(ctx context.Context) (*hexutil.Big, error) {
	return nil, fmt.Errorf(NotImplemented, "eth_getPrice")
	// price, err := eth.SuggestPrice(ctx)
	// return (*hexutil.Big)(price), err
}
