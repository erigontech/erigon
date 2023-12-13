package jsonrpc

import (
	"context"
	"github.com/ledgerwatch/erigon-lib/common/hexutil"
	"math/big"

	"github.com/ledgerwatch/erigon-lib/chain"
	"github.com/ledgerwatch/erigon-lib/kv"

	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/eth/gasprice"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/rpc"
	"github.com/ledgerwatch/erigon/turbo/rpchelper"
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
	highestBlock, err := stages.GetStageProgress(tx, stages.Headers)
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

	chainConfig, err := api.chainConfig(tx)
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
	cc, err := api.chainConfig(tx)
	if err != nil {
		return nil, err
	}

	oracle := gasprice.NewOracle(NewGasPriceOracleBackend(tx, cc, api.BaseAPI), ethconfig.Defaults.GPO, api.gasCache)
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
	cc, err := api.chainConfig(tx)
	if err != nil {
		return nil, err
	}
	oracle := gasprice.NewOracle(NewGasPriceOracleBackend(tx, cc, api.BaseAPI), ethconfig.Defaults.GPO, api.gasCache)
	tipcap, err := oracle.SuggestTipCap(ctx)
	if err != nil {
		return nil, err
	}
	return (*hexutil.Big)(tipcap), err
}

type feeHistoryResult struct {
	OldestBlock  *hexutil.Big     `json:"oldestBlock"`
	Reward       [][]*hexutil.Big `json:"reward,omitempty"`
	BaseFee      []*hexutil.Big   `json:"baseFeePerGas,omitempty"`
	GasUsedRatio []float64        `json:"gasUsedRatio"`
}

func (api *APIImpl) FeeHistory(ctx context.Context, blockCount rpc.DecimalOrHex, lastBlock rpc.BlockNumber, rewardPercentiles []float64) (*feeHistoryResult, error) {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	cc, err := api.chainConfig(tx)
	if err != nil {
		return nil, err
	}
	oracle := gasprice.NewOracle(NewGasPriceOracleBackend(tx, cc, api.BaseAPI), ethconfig.Defaults.GPO, api.gasCache)

	oldest, reward, baseFee, gasUsed, err := oracle.FeeHistory(ctx, int(blockCount), lastBlock, rewardPercentiles)
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
	return results, nil
}

type GasPriceOracleBackend struct {
	tx      kv.Tx
	cc      *chain.Config
	baseApi *BaseAPI
}

func NewGasPriceOracleBackend(tx kv.Tx, cc *chain.Config, baseApi *BaseAPI) *GasPriceOracleBackend {
	return &GasPriceOracleBackend{tx: tx, cc: cc, baseApi: baseApi}
}

func (b *GasPriceOracleBackend) HeaderByNumber(ctx context.Context, number rpc.BlockNumber) (*types.Header, error) {
	header, err := b.baseApi.headerByRPCNumber(number, b.tx)
	if err != nil {
		return nil, err
	}
	if header == nil {
		return nil, nil
	}
	return header, nil
}
func (b *GasPriceOracleBackend) BlockByNumber(ctx context.Context, number rpc.BlockNumber) (*types.Block, error) {
	return b.baseApi.blockByRPCNumber(number, b.tx)
}
func (b *GasPriceOracleBackend) ChainConfig() *chain.Config {
	return b.cc
}
func (b *GasPriceOracleBackend) GetReceipts(ctx context.Context, block *types.Block) (types.Receipts, error) {
	return rawdb.ReadReceipts(b.tx, block, nil), nil
}
func (b *GasPriceOracleBackend) PendingBlockAndReceipts() (*types.Block, types.Receipts) {
	return nil, nil
}
