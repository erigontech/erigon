package jsonrpc

import (
	"context"
	"math/big"

	"github.com/erigontech/erigon-lib/chain"
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/hexutil"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon/consensus/misc"
	"github.com/erigontech/erigon/core/rawdb"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/eth/ethconfig"
	"github.com/erigontech/erigon/eth/gasprice"
	"github.com/erigontech/erigon/ethclient"

	"github.com/erigontech/erigon/eth/stagedsync/stages"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/turbo/rpchelper"
	stageszk "github.com/erigontech/erigon/zk/stages"
)

// BlockNumber implements eth_blockNumber. Returns the block number of most recent block.
func (api *APIImpl) BlockNumber(ctx context.Context) (hexutil.Uint64, error) {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	blockNum, err := rpchelper.GetLatestFinishedBlockNumber(tx)
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
	highestBlock, err := stages.GetStageProgress(tx, stages.Batches)
	if err != nil {
		return false, err
	}

	currentBlock, err := stages.GetStageProgress(tx, stages.Finish)
	if err != nil {
		return false, err
	}

	if currentBlock > 0 && currentBlock >= highestBlock { // Return not syncing if the synchronisation already completed
		return false, nil
	}

	// Otherwise gather the block sync stats
	type S struct {
		StageName   string         `json:"stage_name"`
		BlockNumber hexutil.Uint64 `json:"block_number"`
	}
	stagesMap := make([]S, len(stageszk.AllStagesZk))
	for i, stage := range stageszk.AllStagesZk {
		progress, err := stages.GetStageProgress(tx, stage)
		if err != nil {
			return nil, err
		}
		stagesMap[i].StageName = string(stage)
		stagesMap[i].BlockNumber = hexutil.Uint64(progress)
	}

	return map[string]interface{}{
		"currentBlock": hexutil.Uint64(currentBlock),
		"highestBlock": hexutil.Uint64(highestBlock),
		"stages":       stagesMap,
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
func (api *APIImpl) GasPrice_deprecated(ctx context.Context) (*hexutil.Big, error) {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	oracle := gasprice.NewOracle(NewGasPriceOracleBackend(tx, api.BaseAPI), ethconfig.Defaults.GPO, api.gasCache)
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

	// ZK-specific behavior:
	// - Sequencer: return a non-zero floor tip based on gas tracker lowest price
	//   (aligns wallet hints with sequencer admission floor).
	// - Non-sequencer: proxy to upstream L2 for SuggestGasTipCap
	// Non-ZK: fall back to local oracle
	if cc, err := api.chainConfig(ctx, tx); err == nil && cc != nil {
		chainId := cc.ChainID
		zk := chain.IsZk(chainId.Uint64())
		if zk {
			if !api.isZkNonSequencer(chainId) {
				// Sequencer: if gasless, return zero tip
				if api.BaseAPI.gasless {
					var zero hexutil.Big
					return &zero, nil
				}
				// Prefer tracker lowest price; fallback to default if tracker unavailable
				var tip *big.Int
				if api.gasTracker != nil {
					tip = api.gasTracker.GetLowestPrice()
				}
				if tip == nil {
					tip = new(big.Int).SetUint64(api.DefaultGasPrice)
				}
				return (*hexutil.Big)(tip), nil
			} else { // non-sequencer
				if api.BaseAPI.gasless {
					var price hexutil.Big
					return &price, nil
				}

				client, err := ethclient.DialContext(ctx, api.l2RpcUrl)
				if err != nil {
					return nil, err
				}
				defer client.Close()

				price, err := client.SuggestGasTipCap(ctx)
				if err != nil {
					return nil, err
				}

				return (*hexutil.Big)(price), nil
			}
		}
	}

	oracle := gasprice.NewOracle(NewGasPriceOracleBackend(tx, api.BaseAPI), ethconfig.Defaults.GPO, api.gasCache)
	tipcap, err := oracle.SuggestTipCap(ctx)
	if err != nil {
		return nil, err
	}
	return (*hexutil.Big)(tipcap), err
}

func (api *APIImpl) FeeHistory(ctx context.Context, blockCount rpc.DecimalOrHex, lastBlock rpc.BlockNumber, rewardPercentiles []float64) (*ethclient.FeeHistory, error) {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	oracle := gasprice.NewOracle(NewGasPriceOracleBackend(tx, api.BaseAPI), ethconfig.Defaults.GPO, api.gasCache)

	oldest, reward, baseFee, gasUsed, blobBaseFee, blobGasUsedRatio, err := oracle.FeeHistory(ctx, int(blockCount), lastBlock, rewardPercentiles)
	if err != nil {
		return nil, err
	}

	// By default, use oracle-provided values
	results := &ethclient.FeeHistory{
		OldestBlock:  (*hexutil.Big)(oldest),
		GasUsedRatio: gasUsed,
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

	if cc, err2 := api.chainConfig(ctx, tx); err2 == nil && cc != nil {
		zk := chain.IsZk(cc.ChainID.Uint64())
		if zk {
			if !api.isZkNonSequencer(cc.ChainID) {
				// Sequencer: start from oracle rewards; if a row is all zeros or the block is empty,
				// fill higher percentiles with a non-zero floor, leaving the smallest percentile 0
				// when multiple percentiles are requested. This mirrors OP-like behavior.
				if api.BaseAPI.gasless {
					// Return a zeroed rewards matrix
					results.Reward = zeroRewardsMatrix(len(reward), len(rewardPercentiles))
					return results, nil
				}
				// Use gas tracker lowest price to align with admission floor. Fallback to default.
				floor := new(big.Int).SetUint64(api.DefaultGasPrice)
				if api.gasTracker != nil {
					if minTip := api.gasTracker.GetLowestPrice(); minTip != nil {
						floor = new(big.Int).Set(minTip)
					}
				}
				blocks := len(reward)
				results.Reward = make([][]*hexutil.Big, blocks)
				for i := 0; i < blocks; i++ {
					row := make([]*hexutil.Big, len(rewardPercentiles))
					allZero := true
					for j := range row {
						if j < len(reward[i]) && reward[i][j] != nil {
							if reward[i][j].Sign() > 0 {
								allZero = false
							}
							row[j] = (*hexutil.Big)(reward[i][j])
						}
					}
					// Determine emptiness by gasUsed ratio when available
					emptyBlock := false
					if i < len(gasUsed) {
						emptyBlock = gasUsed[i] == 0
					}
					if allZero || emptyBlock {
						for j := range row {
							if j == 0 && len(rewardPercentiles) > 1 {
								var zero hexutil.Big
								row[j] = &zero
							} else {
								row[j] = (*hexutil.Big)(new(big.Int).Set(floor))
							}
						}
					}
					results.Reward[i] = row
				}
				return results, nil
			} else {
				if api.BaseAPI.gasless {
					// return zeroed reward matrix
					results.Reward = zeroRewardsMatrix(len(reward), len(rewardPercentiles))
					return results, nil
				}

				client, err := ethclient.DialContext(ctx, api.l2RpcUrl)
				if err != nil {
					return nil, err
				}
				defer client.Close()

				feeHistory, err := client.FeeHistory(ctx, uint64(blockCount), lastBlock, rewardPercentiles)
				if err != nil {
					return nil, err
				}

				return feeHistory, nil
			}
		}
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
	return results, nil
}

// BlobBaseFee returns the base fee for blob gas at the current head.
func (api *APIImpl) BlobBaseFee(ctx context.Context) (*hexutil.Big, error) {
	// read current header
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return nil, err
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
	nextBlockTime := header.Time + config.SecondsPerSlot()
	ret256, err := misc.GetBlobGasPrice(config, misc.CalcExcessBlobGas(config, header, nextBlockTime), nextBlockTime)
	if err != nil {
		return nil, err
	}
	return (*hexutil.Big)(ret256.ToBig()), nil
}

// BaseFee returns the base fee at the current head.
func (api *APIImpl) BaseFee(ctx context.Context) (*hexutil.Big, error) {
	// read current header
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	header := rawdb.ReadCurrentHeader(tx)
	if header == nil {
		return (*hexutil.Big)(common.Big0), nil
	}
	config, err := api.BaseAPI.chainConfig(ctx, tx)
	if err != nil {
		return nil, err
	}
	if config == nil {
		return (*hexutil.Big)(common.Big0), nil
	}
	if !config.IsLondon(header.Number.Uint64() + 1) {
		return (*hexutil.Big)(common.Big0), nil
	}
	return (*hexutil.Big)(misc.CalcBaseFee(config, header)), nil
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
	return b.baseApi.getReceipts(ctx, b.tx, block, nil)
}
func (b *GasPriceOracleBackend) PendingBlockAndReceipts() (*types.Block, types.Receipts) {
	return nil, nil
}

// zeroRewardsMatrix builds a blocks x percentiles matrix filled with zero values.
func zeroRewardsMatrix(blocks, percLen int) [][]*hexutil.Big {
	out := make([][]*hexutil.Big, blocks)
	for i := 0; i < blocks; i++ {
		row := make([]*hexutil.Big, percLen)
		for j := 0; j < percLen; j++ {
			var z hexutil.Big
			row[j] = &z
		}
		out[i] = row
	}
	return out
}
