package commands

import (
	"context"

	"github.com/ledgerwatch/turbo-geth/common/hexutil"
	"github.com/ledgerwatch/turbo-geth/eth"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync/stages"
	"github.com/ledgerwatch/turbo-geth/ethdb"
)

// BlockNumber implements eth_blockNumber. Returns the block number of most recent block.
func (api *APIImpl) BlockNumber(_ context.Context) (hexutil.Uint64, error) {
	execution, _, err := stages.GetStageProgress(api.dbReader, stages.Finish)
	if err != nil {
		return 0, err
	}
	return hexutil.Uint64(execution), nil
}

// Syncing implements eth_syncing. Returns a data object detaling the status of the sync process or false if not syncing.
func (api *APIImpl) Syncing(_ context.Context) (interface{}, error) {
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

// ChainId implements eth_chainId. Returns the current ethereum chainId.
func (api *APIImpl) ChainId(ctx context.Context) (hexutil.Uint64, error) {
	tx, err := api.dbReader.Begin(ctx, ethdb.RO)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	chainConfig, err := getChainConfig(tx)
	if err != nil {
		return 0, err
	}
	return hexutil.Uint64(chainConfig.ChainID.Uint64()), nil
}

// ProtocolVersion implements eth_protocolVersion. Returns the current ethereum protocol version.
func (api *APIImpl) ProtocolVersion(_ context.Context) (hexutil.Uint, error) {
	return hexutil.Uint(eth.ProtocolVersions[0]), nil
}

// GasPrice implements eth_gasPrice. Returns the current price per gas in wei.
func (api *APIImpl) GasPrice(_ context.Context) (*hexutil.Big, error) {
	return &hexutil.Big{}, nil

	/*
		return nil, fmt.Errorf(NotImplemented, "eth_getPrice")
		// price, err := eth.SuggestPrice(ctx)
		// return (*hexutil.Big)(price), err
	*/
}
