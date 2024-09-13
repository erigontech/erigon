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

package jsonrpc

import (
	"context"
	"fmt"
	"math"
	"math/big"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/hexutil"

	"github.com/erigontech/erigon-lib/chain"
	"github.com/erigontech/erigon-lib/kv"

	"github.com/erigontech/erigon/consensus/misc"
	"github.com/erigontech/erigon/core/rawdb"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/eth/ethconfig"
	"github.com/erigontech/erigon/eth/gasprice"
	"github.com/erigontech/erigon/eth/stagedsync/stages"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/turbo/rpchelper"
)

// BlockNumber implements eth_blockNumber. Returns the block number of most recent block.
func (api *APIImpl) BlockNumber(ctx context.Context) (hexutil.Uint64, error) {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	blockNum, err := rpchelper.GetLatestBlockNumber(tx)
	if err != nil {
		return 0, err
	}
	return hexutil.Uint64(blockNum), nil
}

// Syncing implements eth_syncing. Returns a data object detailing the status of the sync process or false if not syncing.
func (api *APIImpl) Syncing(ctx context.Context) (interface{}, error) {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	highestBlock, err := rawdb.ReadLastNewBlockSeen(tx)
	if err != nil {
		return false, err
	}

	currentBlock, err := stages.GetStageProgress(tx, stages.Execution)
	if err != nil {
		return false, err
	}

	frozenBlocks := api._blockReader.FrozenBlocks()
	if highestBlock < frozenBlocks {
		highestBlock = frozenBlocks
	}

	// Maybe it is still downloading snapshots. Impossible to determine the highest block.
	if highestBlock == 0 {
		return map[string]interface{}{
			"startingBlock": "0x0", // TODO: this is a placeholder, I do not think it matters what we return here, but 0x0 is probably a good placeholder.
			"currentBlock":  hexutil.Uint64(currentBlock),
			"highestBlock":  hexutil.Uint64(math.MaxUint64),
		}, nil
	}
	reorgRange := 8

	// If the distance between the current block and the highest block is less than the reorg range, we are not syncing. abs(highestBlock - currentBlock) < reorgRange
	if math.Abs(float64(highestBlock)-float64(currentBlock)) < float64(reorgRange) {
		return false, nil
	}

	// Otherwise gather the block sync stats
	type S struct {
		StageName   string         `json:"stage_name"`
		BlockNumber hexutil.Uint64 `json:"block_number"`
	}
	stagesMap := make([]S, len(stages.AllStages))
	for i, stage := range stages.AllStages {
		progress, err := stages.GetStageProgress(tx, stage)
		if err != nil {
			return nil, err
		}
		stagesMap[i].StageName = string(stage)
		stagesMap[i].BlockNumber = hexutil.Uint64(progress)
	}

	return map[string]interface{}{
		"startingBlock": "0x0", // TODO: this is a placeholder, I do not think it matters what we return here, but 0x0 is probably a good placeholder.
		"currentBlock":  hexutil.Uint64(currentBlock),
		"highestBlock":  hexutil.Uint64(highestBlock),
		"stages":        stagesMap,
	}, nil
}

// ChainId implements eth_chainId. Returns the current ethereum chainId.
func (api *APIImpl) ChainId(ctx context.Context) (hexutil.Uint64, error) {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	chainConfig, err := api.chainConfig(ctx, tx)
	if err != nil {
		return 0, err
	}
	return hexutil.Uint64(chainConfig.ChainID.Uint64()), nil
}

// ChainID alias of ChainId - just for convenience
func (api *APIImpl) ChainID(ctx context.Context) (hexutil.Uint64, error) {
	return api.ChainId(ctx)
}

// ProtocolVersion implements eth_protocolVersion. Returns the current ethereum protocol version.
func (api *APIImpl) ProtocolVersion(ctx context.Context) (hexutil.Uint, error) {
	ver, err := api.ethBackend.ProtocolVersion(ctx)
	if err != nil {
		return 0, err
	}
	return hexutil.Uint(ver), nil
}

// GasPrice implements eth_gasPrice. Returns the current price per gas in wei.
func (api *APIImpl) GasPrice(ctx context.Context) (*hexutil.Big, error) {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	oracle := gasprice.NewOracle(NewGasPriceOracleBackend(tx, api.BaseAPI), ethconfig.Defaults.GPO, api.gasCache, api.logger.New("app", "gasPriceOracle"))
	tipcap, err := oracle.SuggestTipCap(ctx)
	gasResult := big.NewInt(0)

	gasResult.Set(tipcap)
	if err != nil {
		return nil, err
	}
	if head := rawdb.ReadCurrentHeader(tx); head != nil && head.BaseFee != nil {
		gasResult.Add(tipcap, head.BaseFee)
	}

	return (*hexutil.Big)(gasResult), err
}

// MaxPriorityFeePerGas returns a suggestion for a gas tip cap for dynamic fee transactions.
func (api *APIImpl) MaxPriorityFeePerGas(ctx context.Context) (*hexutil.Big, error) {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	oracle := gasprice.NewOracle(NewGasPriceOracleBackend(tx, api.BaseAPI), ethconfig.Defaults.GPO, api.gasCache, api.logger.New("app", "gasPriceOracle"))
	tipcap, err := oracle.SuggestTipCap(ctx)
	if err != nil {
		return nil, err
	}
	return (*hexutil.Big)(tipcap), err
}

type feeHistoryResult struct {
	OldestBlock      *hexutil.Big     `json:"oldestBlock"`
	Reward           [][]*hexutil.Big `json:"reward,omitempty"`
	BaseFee          []*hexutil.Big   `json:"baseFeePerGas,omitempty"`
	GasUsedRatio     []float64        `json:"gasUsedRatio"`
	BlobBaseFee      []*hexutil.Big   `json:"baseFeePerBlobGas,omitempty"`
	BlobGasUsedRatio []float64        `json:"blobGasUsedRatio,omitempty"`
}

func (api *APIImpl) FeeHistory(ctx context.Context, blockCount rpc.DecimalOrHex, lastBlock rpc.BlockNumber, rewardPercentiles []float64) (*feeHistoryResult, error) {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	oracle := gasprice.NewOracle(NewGasPriceOracleBackend(tx, api.BaseAPI), ethconfig.Defaults.GPO, api.gasCache, api.logger.New("app", "gasPriceOracle"))

	oldest, reward, baseFee, gasUsed, blobBaseFee, blobGasUsedRatio, err := oracle.FeeHistory(ctx, int(blockCount), lastBlock, rewardPercentiles)
	if err != nil {
		return nil, err
	}
	results := &feeHistoryResult{
		OldestBlock:  (*hexutil.Big)(oldest),
		GasUsedRatio: gasUsed,
	}
	if reward != nil {
		results.Reward = make([][]*hexutil.Big, len(reward))
		for i, w := range reward {
			results.Reward[i] = make([]*hexutil.Big, len(w))
			for j, v := range w {
				results.Reward[i][j] = (*hexutil.Big)(v)
			}
		}
	}
	if baseFee != nil {
		results.BaseFee = make([]*hexutil.Big, len(baseFee))
		for i, v := range baseFee {
			results.BaseFee[i] = (*hexutil.Big)(v)
		}
	}
	if blobBaseFee != nil {
		results.BlobBaseFee = make([]*hexutil.Big, len(blobBaseFee))
		for i, v := range blobBaseFee {
			results.BlobBaseFee[i] = (*hexutil.Big)(v)
		}
	}
	if blobGasUsedRatio != nil {
		results.BlobGasUsedRatio = blobGasUsedRatio
	}
	return results, nil
}

// BlobBaseFee returns the base fee for blob gas at the current head.
func (api *APIImpl) BlobBaseFee(ctx context.Context) (*hexutil.Big, error) {
	// read current header
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return nil, nil
	}
	defer tx.Rollback()
	header := rawdb.ReadCurrentHeader(tx)
	if header == nil || header.BlobGasUsed == nil {
		return (*hexutil.Big)(common.Big0), nil
	}
	config, err := api.BaseAPI.chainConfig(ctx, tx)
	if err != nil {
		return nil, err
	}
	if config == nil {
		return (*hexutil.Big)(common.Big0), nil
	}
	return (*hexutil.Big)(misc.CalcBlobFee(config, misc.CalcExcessBlobGas(config, header))), nil
}

// BaseFee returns the base fee at the current head.
func (api *APIImpl) BaseFee(ctx context.Context) *hexutil.Big {
	// read current header
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return nil
	}
	defer tx.Rollback()
	header := rawdb.ReadCurrentHeader(tx)
	if header == nil {
		fmt.Println("a")
		return (*hexutil.Big)(common.Big0)
	}
	config := api._chainConfig.Load()
	fmt.Println(config)
	if config == nil {
		fmt.Println("b")
		return (*hexutil.Big)(common.Big0)
	}
	if !config.IsLondon(header.Number.Uint64() + 1) {
		return (*hexutil.Big)(common.Big0)
	}
	return (*hexutil.Big)(misc.CalcBaseFee(config, header))
}

type GasPriceOracleBackend struct {
	tx      kv.Tx
	baseApi *BaseAPI
}

func NewGasPriceOracleBackend(tx kv.Tx, baseApi *BaseAPI) *GasPriceOracleBackend {
	return &GasPriceOracleBackend{tx: tx, baseApi: baseApi}
}

func (b *GasPriceOracleBackend) HeaderByNumber(ctx context.Context, number rpc.BlockNumber) (*types.Header, error) {
	header, err := b.baseApi.headerByRPCNumber(ctx, number, b.tx)
	if err != nil {
		return nil, err
	}
	if header == nil {
		return nil, nil
	}
	return header, nil
}
func (b *GasPriceOracleBackend) BlockByNumber(ctx context.Context, number rpc.BlockNumber) (*types.Block, error) {
	return b.baseApi.blockByRPCNumber(ctx, number, b.tx)
}
func (b *GasPriceOracleBackend) ChainConfig() *chain.Config {
	cc, _ := b.baseApi.chainConfig(context.Background(), b.tx)
	return cc
}
func (b *GasPriceOracleBackend) GetReceipts(ctx context.Context, block *types.Block) (types.Receipts, error) {
	return b.baseApi.getReceipts(ctx, b.tx, block)
}
func (b *GasPriceOracleBackend) PendingBlockAndReceipts() (*types.Block, types.Receipts) {
	return nil, nil
}
