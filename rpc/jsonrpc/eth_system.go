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
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"math"
	"math/big"
	"time"

	"github.com/erigontech/erigon-db/rawdb"
	"github.com/erigontech/erigon-lib/chain"
	"github.com/erigontech/erigon-lib/chain/params"
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/hexutil"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/types"
	"github.com/erigontech/erigon/core/vm"
	"github.com/erigontech/erigon/eth/ethconfig"
	"github.com/erigontech/erigon/eth/gasprice"
	"github.com/erigontech/erigon/execution/consensus/misc"
	"github.com/erigontech/erigon/p2p/forkid"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/rpchelper"
)

// BlockNumber implements eth_blockNumber. Returns the block number of most recent block.
func (api *APIImpl) BlockNumber(ctx context.Context) (hexutil.Uint64, error) {
	tx, err := api.db.BeginTemporalRo(ctx)
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
	reply, err := api.ethBackend.Syncing(ctx)
	if err != nil {
		return false, err
	}
	if !reply.Syncing {
		return false, nil
	}

	// Still sync-ing, gather the block sync stats
	highestBlock := reply.LastNewBlockSeen
	currentBlock := reply.CurrentBlock
	type S struct {
		StageName   string         `json:"stage_name"`
		BlockNumber hexutil.Uint64 `json:"block_number"`
	}
	stagesMap := make([]S, len(reply.Stages))
	for i, stage := range reply.Stages {
		stagesMap[i].StageName = stage.StageName
		stagesMap[i].BlockNumber = hexutil.Uint64(stage.BlockNumber)
	}

	return map[string]interface{}{
		"startingBlock": "0x0", // 0x0 is a placeholder, I do not think it matters what we return here
		"currentBlock":  hexutil.Uint64(currentBlock),
		"highestBlock":  hexutil.Uint64(highestBlock),
		"stages":        stagesMap,
	}, nil
}

// ChainId implements eth_chainId. Returns the current ethereum chainId.
func (api *APIImpl) ChainId(ctx context.Context) (hexutil.Uint64, error) {
	tx, err := api.db.BeginTemporalRo(ctx)
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
	tx, err := api.db.BeginTemporalRo(ctx)
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
	tx, err := api.db.BeginTemporalRo(ctx)
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
	tx, err := api.db.BeginTemporalRo(ctx)
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
	tx, err := api.db.BeginTemporalRo(ctx)
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
	tx, err := api.db.BeginTemporalRo(ctx)
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

// EthHardForkConfig represents config of a hard-fork
type EthHardForkConfig struct {
	ActivationTime  uint64                    `json:"activationTime"`
	BlobSchedule    params.BlobConfig         `json:"blobSchedule"`
	ChainId         hexutil.Uint              `json:"chainId"`
	Precompiles     map[common.Address]string `json:"precompiles"`
	SystemContracts map[string]common.Address `json:"systemContracts"`
}

// EthConfigResp is the response type of eth_config
type EthConfigResp struct {
	Current       *EthHardForkConfig `json:"current"`
	CurrentHash   hexutil.Bytes      `json:"currentHash"`
	CurrentForkId hexutil.Bytes      `json:"currentForkId"`
	Next          *EthHardForkConfig `json:"next"`
	NextHash      *hexutil.Bytes     `json:"nextHash"`
	NextForkId    *hexutil.Bytes     `json:"nextForkId"`
	Last          *EthHardForkConfig `json:"last"`
	LastHash      *hexutil.Bytes     `json:"lastHash"`
	LastForkId    *hexutil.Bytes     `json:"lastForkId"`
}

// Config returns the HardFork config for current and upcoming forks:
// assuming linear fork progression and ethereum-like schedule
func (api *APIImpl) Config(ctx context.Context, timeArg *hexutil.Uint64) (*EthConfigResp, error) {
	var timeUnix uint64
	if timeArg != nil {
		timeUnix = timeArg.Uint64()
	} else {
		timeUnix = uint64(time.Now().Unix())
	}
	tx, err := api.db.BeginTemporalRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	chainConfig, genesis, err := api.chainConfigWithGenesis(ctx, tx)
	if err != nil {
		return nil, err
	}
	if !chainConfig.IsCancun(timeUnix) {
		return &EthConfigResp{}, fmt.Errorf("not supported: %w: time=%v", ErrForkTimeBeforeCancun, timeUnix)
	}

	response := EthConfigResp{}
	forkBlockNums, forkTimes := forkid.GatherForks(chainConfig, genesis.Time())
	// current fork config
	currentForkId := forkid.NewIDFromForks(forkBlockNums, forkTimes, genesis.Hash(), math.MaxUint64, timeUnix)
	response.CurrentForkId = currentForkId.Hash[:]
	response.Current = fillForkConfig(chainConfig, currentForkId.Activation)
	response.CurrentHash, err = checkSumConfig(response.Current)
	if err != nil {
		return nil, err
	}

	// next fork config
	if currentForkId.Next == 0 {
		// means there are no later forks setup to be activated after the current one
		return &response, nil
	}

	nextForkId := forkid.NewIDFromForks(forkBlockNums, forkTimes, genesis.Hash(), math.MaxUint64, currentForkId.Next)
	response.Next = fillForkConfig(chainConfig, nextForkId.Activation)
	nextForkHash, err := checkSumConfig(response.Next)
	if err != nil {
		return nil, err
	}

	nextForkIdBytes := hexutil.Bytes(nextForkId.Hash[:])
	response.NextForkId = &nextForkIdBytes
	response.NextHash = &nextForkHash

	// last fork config
	lastForkId := forkid.NewIDFromForks(forkBlockNums, forkTimes, genesis.Hash(), math.MaxUint64, math.MaxUint64)
	response.Last = fillForkConfig(chainConfig, lastForkId.Activation)
	lastForkHash, err := checkSumConfig(response.Last)
	if err != nil {
		return nil, err
	}

	lastForkIdBytes := hexutil.Bytes(lastForkId.Hash[:])
	response.LastForkId = &lastForkIdBytes
	response.LastHash = &lastForkHash

	return &response, nil
}

var ErrForkTimeBeforeCancun = errors.New("fork time before cancun")

func fillForkConfig(chainConfig *chain.Config, activationTime uint64) *EthHardForkConfig {
	forkConfig := EthHardForkConfig{}
	forkConfig.ActivationTime = activationTime
	forkConfig.BlobSchedule = *chainConfig.GetBlobConfig(activationTime)
	forkConfig.ChainId = hexutil.Uint(chainConfig.ChainID.Uint64())
	precompiles := vm.Precompiles(chainConfig.Rules(math.MaxUint64, activationTime))
	forkConfig.Precompiles = make(map[common.Address]string, len(precompiles))
	for addr, precompile := range precompiles {
		forkConfig.Precompiles[addr] = precompile.Name()
	}
	forkConfig.SystemContracts = chainConfig.SystemContracts(activationTime)
	return &forkConfig
}

func checkSumConfig(ehfc *EthHardForkConfig) (hexutil.Bytes, error) {
	ms, err := json.Marshal(ehfc)
	if err != nil {
		return nil, fmt.Errorf("checkSumConfig: error occurred while json marshalling config: %w", err)
	}
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, crc32.ChecksumIEEE(ms))
	return b, nil
}

type GasPriceOracleBackend struct {
	tx      kv.TemporalTx
	baseApi *BaseAPI
}

func NewGasPriceOracleBackend(tx kv.TemporalTx, baseApi *BaseAPI) *GasPriceOracleBackend {
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

func (b *GasPriceOracleBackend) GetReceiptsGasUsed(ctx context.Context, block *types.Block) (types.Receipts, error) {
	return b.baseApi.getReceiptsGasUsed(ctx, b.tx, block)
}
